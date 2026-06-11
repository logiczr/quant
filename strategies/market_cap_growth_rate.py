"""市值增长率策略"""

import logging

import pandas as pd
import duckdb_tools as dt

logger = logging.getLogger("strategy.market_cap_growth_rate")

STRATEGY = {
    "description": "市值增长率策略：(期末市值 - 期初市值) / 期初市值，从高到低排列",
    "columns": [
        {"name": "code", "type": "VARCHAR", "not_null": True},
        {"name": "code_name", "type": "VARCHAR"},
        {"name": "date", "type": "DATE", "not_null": True},
        {"name": "period_days", "type": "INTEGER", "not_null": True},
        {"name": "start_cap", "type": "DOUBLE"},
        {"name": "end_cap", "type": "DOUBLE"},
        {"name": "growth_rate", "type": "DOUBLE"},
        {"name": "rank", "type": "INTEGER"},
    ],
    "primary_key": ["code", "date", "period_days"],
    "params": {"period_days": 20},
    "param_ui": {
        "period_days": {
            "type": "number",
            "min": 1,
            "max": 250,
            "step": 5,
            "label": "回溯周期（交易日）",
            "help": "从期末往前回溯 N 个交易日作为期初",
        },
    },
    "_说明": {
        "period_days": "回溯交易日天数，默认20（约1个月）",
        "start_cap": "期初流通市值（亿元）",
        "end_cap": "期末流通市值（亿元）",
        "growth_rate": "(期末市值 - 期初市值) / 期初市值 × 100（%）",
        "rank": "按 growth_rate 降序排名",
    },
}


def compute(date: str, period_days: int = 20) -> pd.DataFrame:
    """
    市值增长率策略。

    逻辑：
      1. 取期末 date 当天的 daily_bar，用 turn > 0 的数据反推流通股数
      2. 往前回溯 period_days 个交易日作为期初
      3. 用期初流通股数 × 期初 close = 期初市值
         用期初流通股数 × 期末 close = 期末市值（流通股数短期内不变）
      4. growth_rate = (期末市值 - 期初市值) / 期初市值 × 100
      5. 按 growth_rate 降序排名
    """
    conn = dt.get_read_connection()
    try:
        # 取期末当天数据
        df_end = conn.execute(
            """
            SELECT code, code_name, close, volume, turn
            FROM daily_bar
            WHERE date = ? AND adjustflag = '3' AND tradestatus = '1'
            """,
            [date],
        ).df()

        if df_end.empty:
            return pd.DataFrame(
                columns=["code", "code_name", "date", "period_days", "start_cap", "end_cap", "growth_rate", "rank"]
            )

        # 找期初日期：往前 period_days 个交易日
        start_date_row = conn.execute(
            """
            SELECT DISTINCT date FROM daily_bar
            WHERE date < ? AND adjustflag = '3'
            ORDER BY date DESC
            LIMIT 1 OFFSET ?
            """,
            [date, period_days - 1],
        ).fetchone()

        if not start_date_row:
            logger.warning(f"market_cap_growth_rate: {date} 往前 {period_days} 个交易日无数据")
            return pd.DataFrame(
                columns=["code", "code_name", "date", "period_days", "start_cap", "end_cap", "growth_rate", "rank"]
            )

        start_date = str(start_date_row[0])

        # 取期初数据
        df_start = conn.execute(
            """
            SELECT code, close, volume, turn
            FROM daily_bar
            WHERE date = ? AND adjustflag = '3' AND turn > 0
            """,
            [start_date],
        ).df()
    finally:
        conn.close()

    # 用期初数据反推流通股数
    df_start["flow_shares"] = df_start["volume"] * 100 / df_start["turn"]
    df_start["start_cap"] = (df_start["flow_shares"] * df_start["close"] / 1e8).round(4)

    # 合并：期初流通股数 × 期末 close = 期末市值
    df = df_end.merge(df_start[["code", "flow_shares", "start_cap"]], on="code", how="inner")
    df = df[df["turn"] > 0].copy()  # 过滤换手率异常
    df["end_cap"] = (df["flow_shares"] * df["close"] / 1e8).round(4)

    # 计算增长率
    df["growth_rate"] = ((df["end_cap"] - df["start_cap"]) / df["start_cap"] * 100).round(4)

    # 排名
    df = df.sort_values("growth_rate", ascending=False)
    df["date"] = date
    df["period_days"] = period_days
    df["rank"] = range(1, len(df) + 1)

    return df[["code", "code_name", "date", "period_days", "start_cap", "end_cap", "growth_rate", "rank"]]
