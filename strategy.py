"""
strategy.py — 策略引擎 + 计算函数

职责：
  - 加载 strategies/ 目录下的 JSON 策略定义
  - 注册 Python 计算函数（_COMPUTE_FUNCS 注册表）
  - 查询时：检查策略表有无数据 → 无则触发计算 → 返回结果
  - 支持两种计算模式：Python 函数 / 纯 SQL
  - Screener：动态条件选股（type=screener，无持久化）

策略 JSON 格式（compute 不再需要 module/function）：
  {
    "name": "策略名",
    "description": "描述",
    "version": 1,
    "table": "strategy_xxx",
    "schema": "CREATE TABLE IF NOT EXISTS strategy_xxx (...)",
    "dependencies": ["daily_bar"],
    "compute": {
      "type": "python" | "sql" | "none"
    },
    "query": "SELECT * FROM strategy_xxx WHERE date = ? ORDER BY rank",
    "params": { ... }
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

_STRATEGIES_DIR = Path(__file__).parent / "strategies"

# 策略名 → Python 计算函数
# 注册方式：@register_compute("策略名") 或 _COMPUTE_FUNCS["策略名"] = func
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
    """
    扫描 strategies/ 目录，返回所有策略定义列表。

    返回:
        list[dict]，每个元素是一个策略 JSON 的完整内容
    """
    strategies = []
    if not _STRATEGIES_DIR.exists():
        logger.warning(f"策略目录不存在: {_STRATEGIES_DIR}")
        return strategies

    for f in sorted(_STRATEGIES_DIR.glob("*.json")):
        if f.name.startswith("_"):
            continue  # 跳过模板文件
        try:
            with open(f, encoding="utf-8") as fp:
                s = json.load(fp)
            s["_file"] = f.name  # 记录来源文件
            strategies.append(s)
        except Exception as e:
            logger.error(f"加载策略文件失败 {f.name}: {e}")

    logger.info(f"已加载 {len(strategies)} 个策略定义")
    return strategies


def get_strategy(name: str) -> dict | None:
    """
    按策略名获取定义。

    参数:
        name: 策略名（JSON 中的 name 字段）

    返回:
        dict 或 None
    """
    for s in list_strategies():
        if s["name"] == name:
            return s
    return None


# ═════════════════════════════════════════════════════════════════════════════
# 策略表初始化
# ═════════════════════════════════════════════════════════════════════════════

def ensure_strategy_table(strategy: dict) -> None:
    """根据策略 schema 建表。若版本升级则删旧表重建。"""
    schema = strategy.get("schema")
    if not schema:
        return
    table = strategy.get("table", "")
    version = strategy.get("version", 1)

    conn = dt.get_connection()
    try:
        # 检查表是否存在
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
            [table],
        ).fetchall()

        if tables:
            # 表已存在，检查版本
            try:
                ver_row = conn.execute(
                    f"SELECT strategy_version FROM {table}_meta"
                ).fetchone()
                stored_version = ver_row[0] if ver_row else 0
            except Exception:
                stored_version = 0

            if stored_version < version:
                # 版本升级，删旧表重建
                logger.warning(f"策略 {strategy['name']} 版本升级 {stored_version} → {version}，重建表")
                conn.execute(f"DROP TABLE IF EXISTS {table}")
                conn.execute(f"DROP TABLE IF EXISTS {table}_meta")
            else:
                # 版本一致，不需要重建
                return

        # 建表
        conn.execute(schema)
        # 建版本元信息表
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table}_meta (strategy_version INTEGER)"
        )
        conn.execute(f"DELETE FROM {table}_meta")
        conn.execute(f"INSERT INTO {table}_meta VALUES ({version})")
        logger.debug(f"策略表 {table} 已就绪 (v{version})")
    finally:
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# 检查策略数据是否存在
# ═════════════════════════════════════════════════════════════════════════════

def _has_data(strategy: dict, date: str) -> bool:
    """检查策略表是否有指定日期的数据。"""
    table = strategy["table"]
    conn = dt.get_read_connection()
    try:
        # 先看表存不存在
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
            [table],
        ).fetchall()
        if not tables:
            return False
        # 查有无该日期数据
        result = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE date = ?", [date]
        ).fetchone()
        return result[0] > 0 if result else False
    except Exception:
        return False
    finally:
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# 执行计算
# ═════════════════════════════════════════════════════════════════════════════

def _compute_python(strategy: dict, date: str) -> int:
    """调用注册的 Python 计算函数，写入策略表。"""
    name = strategy["name"]
    func = _COMPUTE_FUNCS.get(name)

    if func is None:
        logger.error(f"策略 {name} 没有注册计算函数（_COMPUTE_FUNCS 中无此 key）")
        return 0

    params = strategy.get("params", {})

    # 函数签名: func(date, params) -> pd.DataFrame
    result_df = func(date=date, params=params)

    if result_df is None or result_df.empty:
        logger.warning(f"策略 {name} 计算返回空数据")
        return 0

    # 写入策略表
    table = strategy["table"]
    conn = dt.get_connection()
    try:
        # 确保表存在（含版本检查）
        ensure_strategy_table(strategy)
        # 先删该日期旧数据（避免重复）
        conn.execute(f"DELETE FROM {table} WHERE date = ?", [date])
        # 写入新数据
        conn.execute(f"INSERT INTO {table} SELECT * FROM result_df")
        count = len(result_df)
        logger.info(f"策略 {name} 写入 {count} 条数据 (date={date})")
        return count
    finally:
        conn.close()


def _compute_sql(strategy: dict, date: str) -> int:
    """执行 SQL 语句计算策略结果。"""
    compute = strategy["compute"]
    sql_template = compute["sql"]
    table = strategy["table"]

    # 替换占位符
    sql = sql_template.replace("{table}", table).replace("{date}", date)

    conn = dt.get_connection()
    try:
        conn.execute(strategy["schema"])
        conn.execute(f"DELETE FROM {table} WHERE date = ?", [date])
        conn.execute(sql, [date])
        count = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE date = ?", [date]
        ).fetchone()[0]
        logger.info(f"策略 {strategy['name']} SQL计算写入 {count} 条 (date={date})")
        return count
    finally:
        conn.close()


def compute_strategy(strategy: dict, date: str) -> int:
    """
    执行策略计算，返回写入条数。

    参数:
        strategy: 策略定义 dict
        date:     目标日期，格式 'YYYY-MM-DD'

    返回:
        写入条数
    """
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
    查询策略结果（透明计算：若缺失则自动触发）。

    参数:
        name:          策略名
        date:          目标日期，格式 'YYYY-MM-DD'
        force_compute: 强制重新计算（忽略已有数据）

    返回:
        pd.DataFrame
    """
    strategy = get_strategy(name)
    if strategy is None:
        raise ValueError(f"策略不存在: {name}")

    # 检查是否需要计算
    if not force_compute and _has_data(strategy, date):
        logger.debug(f"策略 {name} 已有 {date} 数据，直接查询")
    else:
        logger.info(f"策略 {name} 缺少 {date} 数据，开始计算...")
        count = compute_strategy(strategy, date)
        if count == 0:
            logger.warning(f"策略 {name} 计算结果为空")
            return pd.DataFrame()

    # 查询结果
    query_sql = strategy.get("query")
    if not query_sql:
        # 没有自定义 query，用默认
        query_sql = f"SELECT * FROM {strategy['table']} WHERE date = ? ORDER BY rank"

    conn = dt.get_read_connection()
    try:
        result = conn.execute(query_sql, [date]).df()
    finally:
        conn.close()

    return result


def query_strategy_range(
    name: str,
    start_date: str,
    end_date: str,
    force_compute: bool = False,
) -> pd.DataFrame:
    """
    查询策略在日期范围内的结果。

    参数:
        name:        策略名
        start_date:  起始日期
        end_date:    截止日期
        force_compute: 强制重新计算

    返回:
        pd.DataFrame
    """
    strategy = get_strategy(name)
    if strategy is None:
        raise ValueError(f"策略不存在: {name}")

    if force_compute:
        compute_strategy(strategy, end_date)

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

# 安全操作符白名单（防 SQL 注入）
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
    """
    动态条件选股（screener 类型策略专用）。

    参数:
        strategy:     screener 类型策略定义
        date:         目标日期
        filters:      筛选条件列表，每项 {"field": "xxx", "op": ">", "value": 0}
        sort_field:   排序字段
        sort_order:   "desc" 或 "asc"
        display_cols: 要展示的列名列表
        limit:        返回条数上限

    返回:
        pd.DataFrame
    """
    source = strategy["source"]
    adjustflag = strategy.get("adjustflag", "3")
    fields_def = {f["name"]: f for f in strategy.get("fields", [])}

    # 默认值
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

        # 安全校验
        if field not in fields_def:
            logger.warning(f"screener: 未知字段 {field}，跳过")
            continue
        if op not in _SAFE_OPS:
            logger.warning(f"screener: 不安全操作符 {op}，跳过")
            continue

        # 判断字段属于哪个表（d. 还是 i.）
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
    """
    获取策略的元信息（不触发计算）。

    返回:
        dict，包含 name, description, type, table, data_status 等
    """
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
            return {**strategy, "data_status": "表不存在", "rows": 0, "date_range": None}

        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        r = conn.execute(
            f"SELECT MIN(date), MAX(date) FROM {table}"
        ).fetchone()
        date_range = f"{r[0]} ~ {r[1]}" if r[0] else None
    except Exception as e:
        return {**strategy, "data_status": f"查询失败: {e}", "rows": 0, "date_range": None}
    finally:
        conn.close()

    return {
        **strategy,
        "data_status": "ok" if count > 0 else "无数据",
        "rows": count,
        "date_range": date_range,
    }


# ═════════════════════════════════════════════════════════════════════════════
# 策略计算函数
# ═════════════════════════════════════════════════════════════════════════════
# 签名统一：func(date: str, params: dict) -> pd.DataFrame
# 返回的 DataFrame 列结构必须与对应策略 JSON 的 schema 一致
# ═════════════════════════════════════════════════════════════════════════════


# ── 多因子评分 ──────────────────────────────────────────────────────────────

@register_compute("multi_factor_score")
def multi_factor_score(
    date: str,
    params: dict,
) -> pd.DataFrame:
    """
    多因子综合评分策略。

    逻辑：
      1. 从 daily_bar 取指定 date 的全市场数据
      2. 对每个因子按百分位排名（0~100）
      3. 加权求和得到综合评分
      4. 按评分排名

    参数 (params):
        factors:   参与评分的因子列表，如 ["pe_ttm", "pb_mrq", "turn"]
        weights:   对应权重，如 [0.4, 0.3, 0.3]
        ascending: 对应排序方向，True=越小越好（如PE），False=越大越好（如换手率）

    返回:
        DataFrame，列: code, code_name, date, score, rank
    """
    factors = params.get("factors", ["pe_ttm", "pb_mrq", "turn"])
    weights = params.get("weights", [1.0 / len(factors)] * len(factors))
    ascending = params.get("ascending", [True] * len(factors))

    # 查询当天全市场数据
    conn = dt.get_read_connection()
    try:
        cols = ", ".join(["code", "code_name"] + factors)
        df = conn.execute(
            f"""
            SELECT {cols}
            FROM daily_bar
            WHERE date = ?
              AND adjustflag = '3'
              AND tradestatus = '1'
            """,
            [date],
        ).df()
    finally:
        conn.close()

    if df.empty:
        logger.warning(f"multi_factor_score: {date} 无日线数据")
        return pd.DataFrame(columns=["code", "code_name", "date", "score", "rank"])

    # 过滤掉因子值为 NaN 的行
    df = df.dropna(subset=factors)

    # 计算百分位排名
    score = pd.Series(0.0, index=df.index)
    for factor, weight, asc in zip(factors, weights, ascending):
        rank_col = df[factor].rank(pct=True, ascending=asc) * 100
        score += rank_col * weight

    df["date"] = date
    df["score"] = score.round(2)
    df = df.sort_values("score", ascending=False)
    df["rank"] = range(1, len(df) + 1)

    return df[["code", "code_name", "date", "score", "rank"]]


# ── 动量突破 ────────────────────────────────────────────────────────────────

@register_compute("momentum_breakout")
def momentum_breakout(
    date: str,
    params: dict,
) -> pd.DataFrame:
    """
    动量突破策略。

    逻辑：
      1. 取指定 date 的收盘价和 N 日前收盘价，计算涨幅
      2. 取当日量比（成交量 / 5日均量）
      3. 筛选：涨幅 > 0 且 量比 > 阈值 的股票
      4. 按涨幅排名

    参数 (params):
        lookback_days:        回看天数，默认 20
        vol_ratio_threshold:  量比阈值，默认 1.5

    返回:
        DataFrame，列: code, code_name, date, n_day_chg, vol_ratio, close, rank
    """
    lookback = params.get("lookback_days", 20)
    vol_threshold = params.get("vol_ratio_threshold", 1.5)

    conn = dt.get_read_connection()
    try:
        # 取当天数据
        df_today = conn.execute(
            """
            SELECT code, code_name, close, volume, amount
            FROM daily_bar
            WHERE date = ? AND adjustflag = '3' AND tradestatus = '1'
            """,
            [date],
        ).df()

        if df_today.empty:
            return pd.DataFrame(
                columns=["code", "code_name", "date", "n_day_chg", "vol_ratio", "close", "rank"]
            )

        # 取 N 天前的收盘价
        past_date_row = conn.execute(
            """
            SELECT DISTINCT date FROM daily_bar
            WHERE date < ? AND adjustflag = '3'
            ORDER BY date DESC
            LIMIT 1 OFFSET ?
            """,
            [date, lookback - 1],
        ).fetchone()

        if not past_date_row:
            logger.warning(f"momentum_breakout: 找不到 {lookback} 天前的数据")
            return pd.DataFrame(
                columns=["code", "code_name", "date", "n_day_chg", "vol_ratio", "close", "rank"]
            )

        past_date = str(past_date_row[0])

        df_past = conn.execute(
            """
            SELECT code, close AS past_close
            FROM daily_bar
            WHERE date = ? AND adjustflag = '3'
            """,
            [past_date],
        ).df()

        # 取 5 日均量
        df_vol = conn.execute(
            """
            SELECT code, AVG(volume) AS vol_ma5
            FROM daily_bar
            WHERE date <= ? AND adjustflag = '3' AND tradestatus = '1'
              AND date >= (
                  SELECT DISTINCT date FROM daily_bar
                  WHERE date <= ? AND adjustflag = '3'
                  ORDER BY date DESC LIMIT 1 OFFSET 4
              )
            GROUP BY code
            """,
            [date, date],
        ).df()

    finally:
        conn.close()

    # 合并
    df = df_today.merge(df_past, on="code", how="inner")
    df = df.merge(df_vol, on="code", how="left")

    # 计算 N 日涨幅
    df["n_day_chg"] = ((df["close"] / df["past_close"]) - 1) * 100
    df["n_day_chg"] = df["n_day_chg"].round(2)

    # 计算量比
    df["vol_ratio"] = (df["volume"] / df["vol_ma5"]).round(2)

    # 筛选：涨幅 > 0 且 量比 > 阈值
    df = df[(df["n_day_chg"] > 0) & (df["vol_ratio"] >= vol_threshold)]

    # 排名
    df = df.sort_values("n_day_chg", ascending=False)
    df["date"] = date
    df["rank"] = range(1, len(df) + 1)

    return df[["code", "code_name", "date", "n_day_chg", "vol_ratio", "close", "rank"]]


# ── 资金进出推算 ────────────────────────────────────────────────────────────

@register_compute("capital_flow")
def capital_flow(
    date: str,
    params: dict,
) -> pd.DataFrame:
    """
    资金进出推算策略。

    核心逻辑：
      流通市值 + 实际资金净流入/出 + N日累计净流入占期初流值比。

    公式推导：
      流通市值(亿) = close × volume × 100 / turn / 1e8
        （收盘价口径，与股票软件一致）

      每日实际资金净流入(亿) = amount × (close - prev_close) / prev_close / 1e8
        涨跌幅加权：涨了钱进来，跌了钱出去，平盘≈0

      N日累计净流入占期初流值(%) = Σ(cap_daily) / flow_cap[t-N] × 100
        cap_ratio > 0 → N日内资金净流入
        cap_ratio < 0 → N日内资金净流出

    参数 (params):
        lookback_days: N日周期，默认 5

    返回:
        DataFrame，列: code, code_name, date, flow_cap, cap_daily, cap_ratio, close, turn
    """
    N = params.get("lookback_days", 5)

    conn = dt.get_read_connection()
    try:
        # 找目标日期及之前最近 N+1 个交易日
        date_rows = conn.execute(
            """
            SELECT DISTINCT date FROM daily_bar
            WHERE date <= ? AND adjustflag = '3'
            ORDER BY date DESC
            LIMIT ?
            """,
            [date, N + 1],
        ).fetchall()

        if not date_rows:
            logger.warning(f"capital_flow: {date} 附近无交易日数据")
            return pd.DataFrame(
                columns=["code", "code_name", "date", "flow_cap",
                         "cap_daily", "cap_ratio", "close", "turn"]
            )

        trading_dates = [str(r[0]) for r in reversed(date_rows)]  # 升序
        earliest = trading_dates[0]
        latest = trading_dates[-1]

        # 拉这 N+1 天的全市场数据
        df = conn.execute(
            """
            SELECT code, code_name, date, close, turn, amount, volume,
                   tradestatus
            FROM daily_bar
            WHERE date BETWEEN ? AND ?
              AND adjustflag = '3'
            ORDER BY code, date
            """,
            [earliest, latest],
        ).df()
    finally:
        conn.close()

    if df.empty:
        return pd.DataFrame(
            columns=["code", "code_name", "date", "flow_cap",
                     "cap_daily", "cap_ratio", "close", "turn"]
        )

    # 日期转字符串方便比较
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # 过滤停牌
    df = df[df["tradestatus"] == "1"].copy()

    # 过滤换手率异常
    df = df[df["turn"] > 0].copy()

    # 计算流通市值(亿): close(元) * volume(股) * 100 / turn(%) / 1e8 = 亿元
    df["flow_cap"] = (df["close"] * df["volume"] * 100 / df["turn"] / 1e8).round(4)

    # 按股票分组排序
    df = df.sort_values(["code", "date"])

    # 每日实际资金净流入(亿) = amount × 涨跌幅 / 1e8
    prev_close = df.groupby("code")["close"].shift(1)
    chg_pct = (df["close"] - prev_close) / prev_close
    df["cap_daily"] = (df["amount"] * chg_pct / 1e8).round(4)

    # N日累计净流入占期初流值(%) = rolling(N).sum(cap_daily) / flow_cap[t-N] * 100
    cum_flow = df.groupby("code")["cap_daily"].rolling(N, min_periods=N).sum()
    cum_flow = cum_flow.reset_index(level=0, drop=True)  # 对齐index
    flow_cap_N_ago = df.groupby("code")["flow_cap"].shift(N)
    df["cap_ratio"] = (cum_flow / flow_cap_N_ago * 100).round(4)

    # 只保留目标日期的行
    result = df[df["date"] == latest].copy()

    # NaN 表示没有足够历史，写入时自然变成 NULL
    result = result[["code", "code_name", "date", "flow_cap",
                     "cap_daily", "cap_ratio", "close", "turn"]]

    # 按 cap_ratio 降序（NaN 排末尾）
    result = result.sort_values("cap_ratio", ascending=False, na_position="last")

    logger.info(f"capital_flow: {latest} 共 {len(result)} 只股票，"
                f"其中 {result['cap_ratio'].notna().sum()} 只有 {N}日变化率")
    return result


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

    print("\n===== 策略列表 =====")
    for s in list_strategies():
        print(f"  {s['name']}: {s['description']}")

    print("\n===== 已注册计算函数 =====")
    for name, fn in _COMPUTE_FUNCS.items():
        print(f"  {name} → {fn.__name__}")

    print("\n===== 策略元信息 =====")
    info = strategy_info("multi_factor_score")
    if info:
        print(f"  name: {info['name']}")
        print(f"  table: {info['table']}")
        print(f"  data_status: {info['data_status']}")
        print(f"  rows: {info['rows']}")
        print(f"  date_range: {info['date_range']}")

    print("\n===== 执行策略计算 =====")
    df = query_strategy("multi_factor_score", "2026-04-28")
    if not df.empty:
        print(df.head(10).to_string())
    else:
        print("计算结果为空（可能当天非交易日或数据不足）")
