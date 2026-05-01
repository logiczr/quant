"""
strategy.py — 策略引擎 + 计算函数

职责：
  - 加载 strategies/ 目录下的 JSON 策略定义
  - 注册 Python 计算函数（_COMPUTE_FUNCS 注册表）
  - 查询时：检查 last_date → 有缓存直接返回 → 无则计算写表 → 更新 last_date
  - 支持两种计算模式：Python 函数 / 纯 SQL
  - Screener：动态条件选股（type=screener，无持久化）

策略 JSON 格式：
  {
    "name": "策略名",
    "description": "描述",
    "table": "strategy_xxx",
    "columns": [
      {"name": "code", "type": "VARCHAR", "not_null": true},
      {"name": "date", "type": "DATE", "not_null": true},
      ...
    ],
    "primary_key": ["code", "date"],
    "compute": {"type": "python", "function": "func_name"},
    "write_sql": "仅供参考，引擎不读取",
    "read_sql": "SELECT * FROM strategy_xxx WHERE date = ? ORDER BY rank",
    "params": { ... },
    "last_date": ""
  }

依赖：
  pip install duckdb pandas
"""

from __future__ import annotations

import json
import logging
import os
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
# 策略目录 + 注册表
# ─────────────────────────────────────────────────────────────────────────────

_STRATEGIES_DIR = Path(__file__).parent / "strategy"

# 策略名 → Python 计算函数
# 注册方式：@register_compute("策略名")
_COMPUTE_FUNCS: dict[str, Callable] = {}


def register_compute(name: str):
    """装饰器：注册策略计算函数。"""
    def decorator(fn: Callable) -> Callable:
        _COMPUTE_FUNCS[name] = fn
        logger.debug(f"注册计算函数: {name} → {fn.__name__}")
        return fn
    return decorator


# ═════════════════════════════════════════════════════════════════════════════
# 加载策略定义（JSON）
# ═════════════════════════════════════════════════════════════════════════════

def list_strategies() -> list[dict]:
    """扫描 strategies/ 目录，返回所有策略定义列表。"""
    strategies = []
    if not _STRATEGIES_DIR.exists():
        logger.warning(f"策略目录不存在: {_STRATEGIES_DIR}")
        return strategies

    for f in sorted(_STRATEGIES_DIR.glob("*.json")):
        if f.name.startswith("_"):
            continue
        try:
            with open(f, encoding="utf-8") as fp:
                s = json.load(fp)
            s["_file"] = f.name
            strategies.append(s)
        except Exception as e:
            logger.error(f"加载策略文件失败 {f.name}: {e}")

    logger.info(f"已加载 {len(strategies)} 个策略定义")
    return strategies


def get_strategy(name: str) -> dict | None:
    """按策略名获取定义。"""
    for s in list_strategies():
        if s["name"] == name:
            return s
    return None


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


def ensure_strategy_table(strategy: dict) -> None:
    """根据 columns + primary_key 建表（IF NOT EXISTS），不删旧表。"""
    table = strategy.get("table")
    if not table:
        return

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
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# last_date 管理（读写 JSON 文件）
# ═════════════════════════════════════════════════════════════════════════════

def _update_last_date(strategy: dict, date: str) -> None:
    """计算完毕后，更新 JSON 文件中的 last_date 字段。"""
    name = strategy["name"]
    filepath = _STRATEGIES_DIR / strategy.get("_file", f"{name}.json")

    if not filepath.exists():
        logger.warning(f"策略文件不存在: {filepath}")
        return

    try:
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)
        data["last_date"] = date
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.debug(f"策略 {name} last_date 更新为 {date}")
    except Exception as e:
        logger.error(f"更新 last_date 失败: {e}")


# ═════════════════════════════════════════════════════════════════════════════
# 执行计算
# ═════════════════════════════════════════════════════════════════════════════

def _compute_python(strategy: dict, date: str) -> int:
    """调用注册的 Python 计算函数，写入策略表。"""
    name = strategy["name"]
    func = _COMPUTE_FUNCS.get(name)

    if func is None:
        logger.error(f"策略 {name} 没有注册计算函数")
        return 0

    params = strategy.get("params", {})

    # 函数签名: func(date: str, **kwargs) -> pd.DataFrame
    result_df = func(date=date, **params)

    if result_df is None or result_df.empty:
        logger.warning(f"策略 {name} 计算返回空数据 (date={date})")
        return 0

    # 写入策略表
    table = strategy["table"]
    conn = dt.get_connection()
    try:
        ensure_strategy_table(strategy)
        conn.execute(f"DELETE FROM {table} WHERE date = ?", [date])
        conn.execute(f"INSERT INTO {table} SELECT * FROM result_df")
        count = len(result_df)
        logger.info(f"策略 {name} 写入 {count} 条数据 (date={date})")
        return count
    finally:
        conn.close()


def _compute_sql(strategy: dict, date: str) -> int:
    """执行 SQL 语句计算策略结果并写入。"""
    compute = strategy["compute"]
    sql_template = compute["sql"]
    table = strategy["table"]

    # 替换 {table} 占位符
    sql = sql_template.replace("{table}", table)

    conn = dt.get_connection()
    try:
        ensure_strategy_table(strategy)
        conn.execute(f"DELETE FROM {table} WHERE date = ?", [date])
        conn.execute(f"INSERT INTO {table} {sql}", [date])
        count = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE date = ?", [date]
        ).fetchone()[0]
        logger.info(f"策略 {strategy['name']} SQL计算写入 {count} 条 (date={date})")
        return count
    finally:
        conn.close()


def compute_strategy(strategy: dict, date: str) -> int:
    """执行策略计算，返回写入条数。"""
    compute_type = strategy["compute"]["type"]

    if compute_type == "python":
        return _compute_python(strategy, date)
    elif compute_type == "sql":
        return _compute_sql(strategy, date)
    else:
        logger.error(f"未知计算类型: {compute_type}")
        return 0


# ═════════════════════════════════════════════════════════════════════════════
# 核心入口：查询策略数据
# ═════════════════════════════════════════════════════════════════════════════

def query_strategy(
    name: str,
    date: str,
    force_compute: bool = False,
) -> pd.DataFrame:
    """
    查询策略结果。

    流程：
      1. force_compute=True → 跳过缓存，直接计算
      2. last_date == date → 查表返回缓存
      3. 否则 → 计算 → 写表 → 更新 last_date → 查表返回
    """
    strategy = get_strategy(name)
    if strategy is None:
        raise ValueError(f"策略不存在: {name}")

    table = strategy.get("table")
    if not table:
        raise ValueError(f"策略 {name} 缺少 table 定义")

    # 判断是否需要计算
    need_compute = force_compute
    if not need_compute:
        last_date = strategy.get("last_date", "")
        if last_date != date:
            need_compute = True

    if need_compute:
        logger.info(f"策略 {name} 开始计算 (date={date})...")
        count = compute_strategy(strategy, date)
        if count == 0:
            logger.warning(f"策略 {name} 计算结果为空")
            return pd.DataFrame()
        # 计算成功，更新 last_date
        _update_last_date(strategy, date)

    # 查表返回
    read_sql = strategy.get("read_sql")
    if not read_sql:
        read_sql = f"SELECT * FROM {table} WHERE date = ? ORDER BY rank"

    conn = dt.get_read_connection()
    try:
        result = conn.execute(read_sql, [date]).df()
    finally:
        conn.close()

    return result


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
        _update_last_date(strategy, end_date)

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
# 策略计算函数
# ═════════════════════════════════════════════════════════════════════════════
# 签名：func(date: str, **kwargs) -> pd.DataFrame
# date 是 K线数据的入口，其余参数为策略调节旋钮
# 返回的 DataFrame 列结构必须与对应策略 JSON 的 columns 一致
# ═════════════════════════════════════════════════════════════════════════════


# ── 流通市值排行 ────────────────────────────────────────────────────────────

@register_compute("market_cap_rank")
def market_cap_rank(date: str) -> pd.DataFrame:
    """
    流通市值排行策略。

    公式：流通市值(亿) = close × volume × 100 / turn / 1e8
      - close: 收盘价（元）
      - volume: 成交量（股）
      - turn: 换手率（百分比数值，如 2.5 表示 2.5%）
      - ×100: 每手100股，还原总流通股数 = volume / turn% × 100
    """
    conn = dt.get_read_connection()
    try:
        df = conn.execute(
            """
            SELECT code, code_name, close, volume, turn
            FROM daily_bar
            WHERE date = ? AND adjustflag = '3' AND tradestatus = '1'
            """,
            [date],
        ).df()
    finally:
        conn.close()

    if df.empty:
        return pd.DataFrame(columns=["code", "code_name", "date", "flow_cap", "rank"])

    df = df[df["turn"] > 0].copy()

    df["flow_cap"] = (df["close"] * df["volume"] * 100 / df["turn"] / 1e8).round(4)

    df = df.sort_values("flow_cap", ascending=False)
    df["date"] = date
    df["rank"] = range(1, len(df) + 1)

    return df[["code", "code_name", "date", "flow_cap", "rank"]]


@register_compute("market_cap_growth")
def market_cap_growth(date: str) -> pd.DataFrame:
    """
    市值增长策略。

    逻辑：
      1. 取指定 date 和前一个交易日的 daily_bar
      2. 分别计算两天的流通市值 = close × volume × 100 / turn / 1e8
      3. cap_change = 今日市值 - 昨日市值
      4. 按 cap_change 降序排名
    """
    conn = dt.get_read_connection()
    try:
        # 取当天数据
        df_today = conn.execute(
            """
            SELECT code, code_name, close, volume, turn
            FROM daily_bar
            WHERE date = ? AND adjustflag = '3' AND tradestatus = '1'
            """,
            [date],
        ).df()

        if df_today.empty:
            return pd.DataFrame(columns=["code", "code_name", "date", "flow_cap", "cap_change", "rank"])

        # 取前一个交易日
        prev_date_row = conn.execute(
            """
            SELECT DISTINCT date FROM daily_bar
            WHERE date < ? AND adjustflag = '3'
            ORDER BY date DESC LIMIT 1
            """,
            [date],
        ).fetchone()

        if not prev_date_row:
            logger.warning(f"market_cap_growth: {date} 前无交易日数据")
            return pd.DataFrame(columns=["code", "code_name", "date", "flow_cap", "cap_change", "rank"])

        prev_date = str(prev_date_row[0])

        # 取前一日数据（不限 tradestatus，停牌也算市值）
        df_prev = conn.execute(
            """
            SELECT code, close, volume, turn
            FROM daily_bar
            WHERE date = ? AND adjustflag = '3' AND turn > 0
            """,
            [prev_date],
        ).df()
    finally:
        conn.close()

    # 过滤换手率异常
    df_today = df_today[df_today["turn"] > 0].copy()

    # 计算今日市值
    df_today["flow_cap"] = (df_today["close"] * df_today["volume"] * 100 / df_today["turn"] / 1e8).round(4)

    # 计算昨日市值
    df_prev["prev_flow_cap"] = (df_prev["close"] * df_prev["volume"] * 100 / df_prev["turn"] / 1e8).round(4)

    # 合并，计算变化
    df = df_today.merge(df_prev[["code", "prev_flow_cap"]], on="code", how="inner")
    df["cap_change"] = (df["flow_cap"] - df["prev_flow_cap"]).round(4)

    # 排名
    df = df.sort_values("cap_change", ascending=False)
    df["date"] = date
    df["rank"] = range(1, len(df) + 1)

    return df[["code", "code_name", "date", "flow_cap", "cap_change", "rank"]]


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

    print("\n===== 已注册计算函数 =====")
    for name, fn in _COMPUTE_FUNCS.items():
        print(f"  {name} → {fn.__name__}")

    print("\n===== 策略元信息 =====")
    info = strategy_info("market_cap_rank")
    if info:
        print(f"  name: {info['name']}")
        print(f"  table: {info['table']}")
        print(f"  last_date: {info.get('last_date', '')}")
        print(f"  data_status: {info['data_status']}")
        print(f"  rows: {info['rows']}")
        print(f"  date_range: {info['date_range']}")

