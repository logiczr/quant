"""BS区间策略

买线: EMA(CLOSE, 2)
卖线: EMA(SLOPE(CLOSE, 21)*20 + CLOSE, 42)
指导: EMA((EMA(CLOSE,4)+EMA(CLOSE,6)+EMA(CLOSE,12)+EMA(CLOSE,24))/4, 2)
界:   MA(CLOSE, 27)

金叉买: 买线上穿卖线
死叉卖: 卖线上穿买线
（指导/界仅作参考线，不产生买卖信号）

每次全量计算，从 _WARMUP_DAYS 天数据从头算起。
"""

import logging
import pathlib

import numpy as np
import pandas as pd

import duckdb_tools as dt

logger = logging.getLogger("strategy")

# 全量计算预热天数
# 卖线 = EMA(SLOPE(C,21)*20+C, 42)，SLOPE 需 21 天，EMA(42) 需约 193 天才充分收敛
# 21 + 193 = 214，取 250 留余量（约 1 年交易日）
_WARMUP_DAYS = 250


STRATEGY = {
    "description": "BS区间：买线上穿卖线为金叉买，卖线上穿买线为死叉卖",
    "columns": [
        {"name": "code", "type": "VARCHAR", "not_null": True},
        {"name": "code_name", "type": "VARCHAR"},
        {"name": "date", "type": "DATE", "not_null": True},
        {"name": "buy_line", "type": "DOUBLE"},       # 买线 EMA(C,2)
        {"name": "sell_line", "type": "DOUBLE"},       # 卖线
        {"name": "guide", "type": "DOUBLE"},           # 指导
        {"name": "boundary", "type": "DOUBLE"},        # 界
        {"name": "signal", "type": "INTEGER"},         # 1=金叉买 -1=死叉卖 0=无
        {"name": "signal_type", "type": "VARCHAR"},    # 信号来源描述
    ],
    "primary_key": ["code", "date"],
    "params": {},
    "param_ui": {
        "push_signals": {
            "type": "button",
            "label": "📢 推送到钉钉",
            "help": "将当日 BS 区间信号推送到钉钉群",
            "endpoint": "/notify/push_signals",
        },
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# 辅助函数
# ─────────────────────────────────────────────────────────────────────────────

def _slope(series: pd.Series, n: int) -> pd.Series:
    """
    线性回归斜率（滚动窗口），与通达信 SLOPE 一致。
    xi = 0,1,...,n-1 ；yi = close 滚动窗口值。
    """
    y = series.values.astype(float)
    result = np.full(len(y), np.nan)
    if len(y) < n:
        return pd.Series(result, index=series.index)

    x = np.arange(n, dtype=float)
    x_mean = x.mean()
    x_ss = ((x - x_mean) ** 2).sum()

    for i in range(n - 1, len(y)):
        y_win = y[i - n + 1 : i + 1]
        y_mean = y_win.mean()
        result[i] = ((x - x_mean) * (y_win - y_mean)).sum() / x_ss

    return pd.Series(result, index=series.index)


def _ema(series: pd.Series, n: int) -> pd.Series:
    """
    指数移动平均 EMA，与通达信一致。
    公式：EMA_t = α * price_t + (1-α) * EMA_{t-1}，α = 2/(N+1)
    初始值取 SMA，之后递推。
    """
    return series.ewm(span=n, adjust=False).mean()


def _ma(series: pd.Series, n: int) -> pd.Series:
    """
    简单移动平均 MA，与通达信一致。
    """
    return series.rolling(window=n, min_periods=1).mean()


def _cross_up(a: pd.Series, b: pd.Series) -> pd.Series:
    """
    上穿判断（金叉）：前一天 a < b，当天 a > b → True。
    与通达信 CROSS(A,B) 一致，严格小于。
    """
    return (a.shift(1) < b.shift(1)) & (a > b)


def _ensure_table():
    """确保策略表存在，schema 不匹配则重建"""
    table = STRATEGY.get("table")
    if not table:
        name = pathlib.Path(__file__).stem
        table = f"strategy_{name}"
        STRATEGY["table"] = table

    columns = STRATEGY["columns"]
    primary_key = STRATEGY.get("primary_key", ["code", "date"])
    expected_cols = {c["name"] for c in columns}

    conn = dt.get_connection()
    try:
        existing = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [table],
        ).fetchall()
        if existing:
            existing_cols = {row[0] for row in existing}
            if existing_cols != expected_cols:
                logger.warning(
                    f"策略表 {table} schema 不匹配，重建（旧:{existing_cols} 期望:{expected_cols}）"
                )
                conn.execute(f"DROP TABLE {table}")
            else:
                return

        col_defs = []
        for c in columns:
            s = f"{c['name']} {c['type']}"
            if c.get("not_null"):
                s += " NOT NULL"
            col_defs.append(s)
        col_str = ", ".join(col_defs)
        pk_str = ", ".join(primary_key)
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({col_str}, PRIMARY KEY({pk_str}))")
        logger.info(f"策略表 {table} 已创建")
    finally:
        conn.close()


def _build_signal(golden_buy, dead_sell):
    """根据交叉状态，返回 (signal, signal_type)"""
    if golden_buy:
        return 1, "买线上穿卖线"
    elif dead_sell:
        return -1, "卖线上穿买线"
    else:
        return 0, ""


def _write_day(table: str, day_results: list[dict]) -> None:
    """将一天的计算结果写入策略表（UPSERT by code+date）"""
    if not day_results:
        return
    df = pd.DataFrame(day_results)
    conn = dt.get_connection()
    try:
        for _, row in df.iterrows():
            conn.execute(
                f"DELETE FROM {table} WHERE code = ? AND date = ?",
                [row["code"], row["date"]],
            )
        conn.execute(f"INSERT INTO {table} SELECT * FROM df")
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# 全量计算
# ─────────────────────────────────────────────────────────────────────────────

def _compute_full(date: str) -> pd.DataFrame:
    """全量计算：从 _WARMUP_DAYS 天数据从头算起。"""
    trade_dates = dt.get_trade_dates(end_date=date)
    if not trade_dates:
        return _empty_result()

    lookback_dates = trade_dates[-_WARMUP_DAYS:]
    start_date = lookback_dates[0]

    conn = dt.get_read_connection()
    try:
        df = conn.execute(
            """
            SELECT code, code_name, date, close
            FROM daily_bar
            WHERE date BETWEEN ? AND ?
              AND adjustflag = '3'
              AND tradestatus = '1'
            ORDER BY code, date
            """,
            [start_date, date],
        ).df()
    finally:
        conn.close()

    if df.empty:
        return _empty_result()

    results: list[dict] = []
    for code, group in df.groupby("code"):
        group = group.sort_values("date").reset_index(drop=True)
        if len(group) < 50:
            continue

        close = group["close"].astype(float)

        # 买线: EMA(CLOSE, 2)
        buy_line = _ema(close, 2)

        # 卖线: EMA(SLOPE(CLOSE, 21)*20 + CLOSE, 42)
        slope_21 = _slope(close, 21)
        sell_base = slope_21 * 20 + close
        sell_line = _ema(sell_base, 42)

        # 指导: EMA((EMA(C,4)+EMA(C,6)+EMA(C,12)+EMA(C,24))/4, 2)
        ema4  = _ema(close, 4)
        ema6  = _ema(close, 6)
        ema12 = _ema(close, 12)
        ema24 = _ema(close, 24)
        guide_base = (ema4 + ema6 + ema12 + ema24) / 4
        guide = _ema(guide_base, 2)

        # 界: MA(CLOSE, 27)
        boundary = _ma(close, 27)

        last = len(group) - 1
        code_name = group["code_name"].iloc[last]

        # 交叉检测（严格小于，与通达信 CROSS 一致）
        # 金叉：买线上穿卖线
        # 死叉：卖线上穿买线
        golden_buy = bool(_cross_up(buy_line, sell_line).iloc[last])
        dead_sell = bool(_cross_up(sell_line, buy_line).iloc[last])

        signal, signal_type = _build_signal(golden_buy, dead_sell)

        results.append({
            "code": code,
            "code_name": code_name,
            "date": date,
            "buy_line": round(float(buy_line.iloc[last]), 4),
            "sell_line": round(float(sell_line.iloc[last]), 4),
            "guide": round(float(guide.iloc[last]), 4),
            "boundary": round(float(boundary.iloc[last]), 4),
            "signal": signal,
            "signal_type": signal_type,
        })

    if not results:
        return _empty_result()

    result_df = pd.DataFrame(results)
    result_df["_sort"] = result_df["signal"].map({1: 0, -1: 1, 0: 2})
    result_df = result_df.sort_values(["_sort", "code"]).drop(columns=["_sort"]).reset_index(drop=True)
    return result_df


# ─────────────────────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────────────────────

def compute(date: str, **kwargs) -> pd.DataFrame:
    """BS区间策略计算入口，全量计算。"""
    _ensure_table()

    result = _compute_full(date)
    if not result.empty:
        golden = (result["signal"] == 1).sum()
        dead = (result["signal"] == -1).sum()
        logger.info(f"BS区间策略完成（全量）：{len(result)} 只，金叉 {golden} 只，死叉 {dead} 只")
    return result


def _empty_result() -> pd.DataFrame:
    cols = [c["name"] for c in STRATEGY["columns"]]
    return pd.DataFrame(columns=cols)


def query_signals(date: str) -> pd.DataFrame:
    """
    查询某天的金叉/死叉信号。

    流程：
      1. 先从策略表查，有则直接返回 signal != 0 的记录
      2. 无则调用 compute(date) 全量计算，将结果写入策略表，再查表
    """
    _ensure_table()
    table = STRATEGY.get("table")

    # 先查表
    conn = dt.get_read_connection()
    try:
        df = conn.execute(
            f"SELECT * FROM {table} WHERE date = ? AND signal != 0 ORDER BY signal, code",
            [date],
        ).df()
    finally:
        conn.close()

    if not df.empty:
        return df

    # 表中无当天数据，全量计算
    result = compute(date)
    if result.empty:
        return result

    # 写入策略表
    _write_day(table, result.to_dict("records"))

    # 再查表
    conn = dt.get_read_connection()
    try:
        df = conn.execute(
            f"SELECT * FROM {table} WHERE date = ? AND signal != 0 ORDER BY signal, code",
            [date],
        ).df()
    finally:
        conn.close()

    return df
