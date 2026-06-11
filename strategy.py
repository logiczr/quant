"""
strategy.py — 策略引擎

职责：
  - 自动发现 strategies/ 目录下的策略模块（*.py）
  - 每个模块导出 STRATEGY 字典 + compute 函数
  - 查询时：检查缓存 → 无则计算写表 → 查表返回
  - Screener：动态条件选股（无持久化）

策略模块约定：
  每个策略文件（如 strategies/ff3_factor.py）导出：
    STRATEGY = {
        "description": "...",
        "columns": [...],
        "primary_key": [...],
        "params": {...},
        "param_ui": {...},
    }
    def compute(date, **kwargs) -> pd.DataFrame

  框架自动注入：
    name = 文件名（去掉 .py）
    table = "strategy_" + name

依赖：
  pip install duckdb pandas
"""

from __future__ import annotations

import importlib
import logging
from pathlib import Path
from typing import Any, Callable

import duckdb
import pandas as pd

import duckdb_tools as dt

# ─────────────────────────────────────────────────────────────────────────────
# 日志
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("strategy")

# ─────────────────────────────────────────────────────────────────────────────
# 策略目录 + 缓存
# ─────────────────────────────────────────────────────────────────────────────

_STRATEGIES_DIR = Path(__file__).parent / "strategies"

# 策略名 → {strategy dict, compute fn}
_STRATEGY_CACHE: dict[str, dict] = {}


def _load_strategies() -> dict[str, dict]:
    """扫描 strategies/ 目录下的 *.py，导入并缓存。"""
    if _STRATEGY_CACHE:
        return _STRATEGY_CACHE

    if not _STRATEGIES_DIR.exists():
        logger.warning(f"策略目录不存在: {_STRATEGIES_DIR}")
        return _STRATEGY_CACHE

    for f in sorted(_STRATEGIES_DIR.glob("*.py")):
        if f.name.startswith("_"):
            continue

        name = f.stem
        try:
            mod = importlib.import_module(f"strategies.{name}")
        except Exception as e:
            logger.error(f"导入策略模块失败 {name}: {e}")
            continue

        strategy_dict = getattr(mod, "STRATEGY", None)
        compute_fn = getattr(mod, "compute", None)

        if strategy_dict is None or compute_fn is None:
            logger.warning(f"策略模块 {name} 缺少 STRATEGY 或 compute，跳过")
            continue

        # 框架注入 name 和 table
        strategy_dict["name"] = name
        strategy_dict["table"] = f"strategy_{name}"

        _STRATEGY_CACHE[name] = {
            "strategy": strategy_dict,
            "compute_fn": compute_fn,
        }
        logger.debug(f"加载策略: {name}")

    logger.info(f"已加载 {len(_STRATEGY_CACHE)} 个策略")
    return _STRATEGY_CACHE


def list_strategies() -> list[dict]:
    """返回所有策略定义列表。"""
    cache = _load_strategies()
    return [v["strategy"] for v in cache.values()]


def get_strategy(name: str) -> dict | None:
    """按策略名获取定义。"""
    cache = _load_strategies()
    entry = cache.get(name)
    return entry["strategy"] if entry else None


def get_compute_fn(name: str) -> Callable | None:
    """按策略名获取计算函数。"""
    cache = _load_strategies()
    entry = cache.get(name)
    return entry["compute_fn"] if entry else None


# ═════════════════════════════════════════════════════════════════════════════
# 策略表初始化（从 columns + primary_key 拼 DDL）
# ═════════════════════════════════════════════════════════════════════════════

def _build_create_sql(strategy: dict) -> str:
    """从 columns + primary_key 拼 CREATE TABLE 语句。"""
    table = strategy["table"]
    cols = strategy["columns"]
    pk = strategy.get("primary_key", ["code", "date"])

    col_defs = []
    for c in cols:
        s = f"{c['name']} {c['type']}"
        if c.get("not_null"):
            s += " NOT NULL"
        col_defs.append(s)

    col_str = ", ".join(col_defs)
    pk_str = ", ".join(pk)
    return f"CREATE TABLE IF NOT EXISTS {table} ({col_str}, PRIMARY KEY({pk_str}))"


def ensure_strategy_table(strategy: dict, conn=None) -> None:
    """根据 columns + primary_key 建表（IF NOT EXISTS），不删旧表。"""
    table = strategy.get("table")
    if not table:
        return

    own_conn = conn is None
    if own_conn:
        conn = dt.get_connection()
    try:
        # 表不存在才建
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
            [table],
        ).fetchall()

        if not tables:
            ddl = _build_create_sql(strategy)
            conn.execute(ddl)
            logger.info(f"策略表 {table} 已创建")
    finally:
        if own_conn:
            conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# last_date 管理（存到 DuckDB）
# ═════════════════════════════════════════════════════════════════════════════

_META_TABLE = "_strategy_meta"


def _ensure_meta_table() -> None:
    """确保元数据表存在。"""
    conn = dt.get_connection()
    try:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_META_TABLE} (
                name VARCHAR NOT NULL,
                last_date DATE,
                PRIMARY KEY(name)
            )
        """)
    finally:
        conn.close()


def _update_last_date(name: str, date: str) -> None:
    """更新策略的 last_date。"""
    _ensure_meta_table()
    conn = dt.get_connection()
    try:
        conn.execute(
            f"DELETE FROM {_META_TABLE} WHERE name = ?",
            [name],
        )
        conn.execute(
            f"INSERT INTO {_META_TABLE} VALUES (?, ?)",
            [name, date],
        )
    finally:
        conn.close()


def _get_last_date(name: str) -> str | None:
    """获取策略的 last_date。"""
    _ensure_meta_table()
    conn = dt.get_read_connection()
    try:
        row = conn.execute(
            f"SELECT last_date FROM {_META_TABLE} WHERE name = ?",
            [name],
        ).fetchone()
        return str(row[0]) if row and row[0] else None
    finally:
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# 执行计算
# ═════════════════════════════════════════════════════════════════════════════

def _compute_and_write(strategy: dict, compute_fn: Callable, date: str) -> int:
    """调用计算函数，写入策略表。"""
    name = strategy["name"]
    params = strategy.get("params", {})

    # 函数签名: compute(date, **kwargs) -> pd.DataFrame
    result_df = compute_fn(date=date, **params)

    if result_df is None or result_df.empty:
        logger.warning(f"策略 {name} 计算返回空数据 (date={date})")
        return 0

    # 写入策略表
    count = write_strategy_result(name, result_df)
    return count


def write_strategy_result(name: str, result_df: pd.DataFrame) -> int:
    """将计算结果写入策略表（upsert by primary_key）。供外部直接调用。"""
    strategy = get_strategy(name)
    if strategy is None:
        raise ValueError(f"策略不存在: {name}")

    table = strategy["table"]
    conn = dt.get_connection()
    try:
        ensure_strategy_table(strategy, conn=conn)

        # DELETE：根据 result_df 的 primary_key 值精确删除旧记录
        pk = strategy.get("primary_key", [])
        if pk and all(c in result_df.columns for c in pk):
            pk_values = result_df[pk].drop_duplicates()
            for _, row in pk_values.iterrows():
                where_clause = " AND ".join(f"{c} = ?" for c in pk)
                vals = [row[c] for c in pk]
                conn.execute(f"DELETE FROM {table} WHERE {where_clause}", vals)
        else:
            conn.execute(f"DELETE FROM {table} WHERE date = ?", [result_df["date"].iloc[0]])

        # INSERT：统一用 Python DataFrame
        conn.execute(f"INSERT INTO {table} SELECT * FROM result_df")

        count = len(result_df)
        logger.info(f"策略 {name} 写入 {count} 条数据")
        return count
    finally:
        conn.close()


def compute_strategy(strategy: dict, date: str) -> int:
    """执行策略计算，返回写入条数。"""
    name = strategy["name"]
    compute_fn = get_compute_fn(name)
    if compute_fn is None:
        logger.error(f"策略 {name} 没有计算函数")
        return 0
    return _compute_and_write(strategy, compute_fn, date)


# ═════════════════════════════════════════════════════════════════════════════
# 核心入口：查询策略数据
# ═════════════════════════════════════════════════════════════════════════════

def query_strategy(
    name: str,
    date: str,
    force_compute: bool = False,
    **params_override,
) -> pd.DataFrame:
    """
    查询策略结果。

    流程：
      1. 合并 params（默认 + 调用方覆盖）
      2. force_compute=False → 尝试从表中读取已有数据
      3. 无缓存或 force_compute=True → 计算 → 写表 → 查表返回

    params_override:
      策略自定义参数，如 period_days=30。
      会与默认 params 合并（调用方优先），
      并参与缓存判断（不同参数组合独立缓存）。
    """
    strategy = get_strategy(name)
    if strategy is None:
        raise ValueError(f"策略不存在: {name}")

    table = strategy.get("table")
    if not table:
        raise ValueError(f"策略 {name} 缺少 table 定义")

    # 合并参数：默认值 + 调用方覆盖
    params = {**strategy.get("params", {}), **params_override}

    # 尝试读缓存
    if not force_compute:
        cached = _try_read_cache(strategy, date, params)
        if cached is not None and not cached.empty:
            logger.debug(f"策略 {name} 命中缓存 (date={date}, params={params})")
            return cached

    # 只读模式下不触发计算（由 daemon 负责），返回空数据
    if dt.is_readonly_mode():
        logger.warning(f"策略 {name} 无缓存，且当前为只读模式，跳过计算")
        return pd.DataFrame()

    # 计算：临时替换 params
    logger.info(f"策略 {name} 开始计算 (date={date}, params={params})...")
    import copy
    strategy_copy = copy.deepcopy(strategy)
    strategy_copy["params"] = params
    count = compute_strategy(strategy_copy, date)

    if count == 0:
        logger.warning(f"策略 {name} 计算结果为空")
        return pd.DataFrame()

    # 更新 last_date
    _update_last_date(name, date)

    # 查表返回
    return _try_read_cache(strategy, date, params)


def _try_read_cache(
    strategy: dict, date: str, params: dict
) -> pd.DataFrame | None:
    """
    按 primary_key + params 构建查询，尝试从策略表读取已有数据。

    primary_key 中的列如果出现在 params 中，会自动加入 WHERE 条件。
    例如 primary_key = ["code", "date", "period_days"]，params = {"period_days": 20}
    → WHERE date = ? AND period_days = ?
    """
    table = strategy.get("table")
    if not table:
        return None

    pk = strategy.get("primary_key", ["code", "date"])

    # 构建 WHERE：date 必有，params 中与 pk 交集的列也加入
    where_parts = ["date = ?"]
    where_values: list = [date]

    for k, v in params.items():
        if k in pk:
            where_parts.append(f"{k} = ?")
            where_values.append(v)

    where_clause = " AND ".join(where_parts)

    # ORDER BY: 有 rank 列按 rank 排，否则按第一个主键列排
    columns_info = strategy.get("columns", [])
    has_rank = any(c["name"] == "rank" for c in columns_info)
    order_by = "rank" if has_rank else pk[0]

    sql = f"SELECT * FROM {table} WHERE {where_clause} ORDER BY {order_by}"

    try:
        conn = dt.get_read_connection()
        try:
            result = conn.execute(sql, where_values).df()
        finally:
            conn.close()
        return result
    except Exception:
        # 表可能不存在
        return None


def query_strategy_range(
    name: str,
    start_date: str,
    end_date: str,
    force_compute: bool = False,
) -> pd.DataFrame:
    """查询策略在日期范围内的结果。"""
    strategy = get_strategy(name)
    if strategy is None:
        raise ValueError(f"策略不存在: {name}")

    if force_compute:
        compute_strategy(strategy, end_date)
        _update_last_date(name, end_date)

    table = strategy["table"]
    conn = dt.get_read_connection()
    try:
        result = conn.execute(
            f"SELECT * FROM {table} WHERE date BETWEEN ? AND ? ORDER BY date, rank",
            [start_date, end_date],
        ).df()
    finally:
        conn.close()

    return result


# ═════════════════════════════════════════════════════════════════════════════
# Screener：动态条件选股
# ═════════════════════════════════════════════════════════════════════════════

_SAFE_OPS = {">", ">=", "<", "<=", "=", "!=", "<>", "between", "in"}


def query_screener(
    strategy: dict,
    date: str,
    filters: list[dict] | None = None,
    sort_field: str | None = None,
    sort_order: str = "desc",
    display_cols: list[str] | None = None,
    limit: int = 200,
) -> pd.DataFrame:
    """动态条件选股（screener 类型策略专用）。"""
    source = strategy["source"]
    adjustflag = strategy.get("adjustflag", "3")
    fields_def = {f["name"]: f for f in strategy.get("fields", [])}

    if filters is None:
        filters = strategy.get("default_filters", [])
    if sort_field is None:
        default_sort = strategy.get("default_sort", {})
        sort_field = default_sort.get("field", "pct_chg")
        sort_order = default_sort.get("order", "desc")
    if display_cols is None:
        display_cols = strategy.get("default_display", [f["name"] for f in strategy.get("fields", [])])

    # ── 拼 WHERE ──
    where_parts = ["d.date = ?", f"d.adjustflag = '{adjustflag}'", "d.tradestatus = '1'"]
    params: list = [date]

    for f in filters:
        field = f["field"]
        op = f["op"].lower().strip()
        value = f["value"]

        if field not in fields_def:
            logger.warning(f"screener: 未知字段 {field}，跳过")
            continue
        if op not in _SAFE_OPS:
            logger.warning(f"screener: 不安全操作符 {op}，跳过")
            continue

        prefix = _field_prefix(field, fields_def)

        if op == "between":
            lo, hi = value
            where_parts.append(f"{prefix}{field} BETWEEN ? AND ?")
            params.extend([lo, hi])
        elif op == "in":
            placeholders = ", ".join(["?"] * len(value))
            where_parts.append(f"{prefix}{field} IN ({placeholders})")
            params.extend(value)
        else:
            where_parts.append(f"{prefix}{field} {op} ?")
            params.append(value)

    # ── 拼 SELECT ──
    select_parts = []
    for col in display_cols:
        prefix = _field_prefix(col, fields_def)
        select_parts.append(f"{prefix}{col}")
    select_str = ", ".join(select_parts)

    # ── 拼 ORDER BY ──
    sort_prefix = _field_prefix(sort_field, fields_def)
    order_str = f"{sort_prefix}{sort_field} {'DESC' if sort_order == 'desc' else 'ASC'}"

    # ── 最终 SQL ──
    sql = f"SELECT {select_str} FROM {source} WHERE {' AND '.join(where_parts)} ORDER BY {order_str} LIMIT {limit}"

    logger.debug(f"screener SQL: {sql}")
    logger.debug(f"screener params: {params}")

    conn = dt.get_read_connection()
    try:
        result = conn.execute(sql, params).df()
    except Exception as e:
        logger.error(f"screener 执行失败: {e}\nSQL: {sql}\nparams: {params}")
        raise
    finally:
        conn.close()

    return result


def _field_prefix(field: str, fields_def: dict) -> str:
    """判断字段属于 daily_bar(d) 还是 indicators(i)，返回表前缀。"""
    indicator_cols = {
        "EMA5", "EMA10", "EMA20", "EMA60",
        "MACD_DIF", "MACD_DEA",
        "KDJ_K", "KDJ_D",
        "OBV",
        "BOLL_UP", "BOLL_MID", "BOLL_DOWN",
        "ATR",
    }
    if field in indicator_cols:
        return "i."
    return "d."


# ═════════════════════════════════════════════════════════════════════════════
# 策略元信息
# ═════════════════════════════════════════════════════════════════════════════

def strategy_info(name: str) -> dict | None:
    """获取策略的元信息（不触发计算）。"""
    strategy = get_strategy(name)
    if strategy is None:
        return None

    result = {**strategy}
    result["last_date"] = _get_last_date(name)

    # screener 类型没有持久化表
    if strategy.get("type") == "screener":
        result["data_status"] = "实时查询（无持久表）"
        result["rows"] = None
        result["date_range"] = None
        return result

    # 固定策略：查表状态
    table = strategy.get("table")
    if not table:
        return {**result, "data_status": "无策略表", "rows": 0, "date_range": None}

    conn = dt.get_read_connection()
    try:
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
            [table],
        ).fetchall()
        if not tables:
            return {**result, "data_status": "表不存在", "rows": 0, "date_range": None}

        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        r = conn.execute(
            f"SELECT MIN(date), MAX(date) FROM {table}"
        ).fetchone()
        date_range = f"{r[0]} ~ {r[1]}" if r[0] else None
    except Exception as e:
        return {**result, "data_status": f"查询失败: {e}", "rows": 0, "date_range": None}
    finally:
        conn.close()

    return {
        **result,
        "data_status": "ok" if count > 0 else "无数据",
        "rows": count,
        "date_range": date_range,
    }


# ═════════════════════════════════════════════════════════════════════════════
# 模块自测
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    print("\n===== 已加载策略 =====")
    for s in list_strategies():
        print(f"  {s['name']} — {s.get('description', '')}")
        print(f"    table: {s['table']}, pk: {s.get('primary_key')}")

    print("\n===== 策略元信息 =====")
    info = strategy_info("market_cap_rank")
    if info:
        print(f"  name: {info['name']}")
        print(f"  table: {info['table']}")
        print(f"  last_date: {info.get('last_date', '')}")
        print(f"  data_status: {info['data_status']}")
        print(f"  rows: {info['rows']}")
        print(f"  date_range: {info['date_range']}")
