"""市值增长策略"""

import logging

import pandas as pd
import duckdb_tools as dt

logger = logging.getLogger("strategy.market_cap_growth")

STRATEGY = {
    "description": "市值增长策略：今日流通市值 - 昨日流通市值，从大到小排列",
    "columns": [
        {"name": "code", "type": "VARCHAR", "not_null": True},
        {"name": "code_name", "type": "VARCHAR"},
        {"name": "date", "type": "DATE", "not_null": True},
        {"name": "flow_cap", "type": "DOUBLE"},
        {"name": "cap_change", "type": "DOUBLE"},
        {"name": "rank", "type": "INTEGER"},
    ],
    "primary_key": ["code", "date"],
    "params": {},
    "param_ui": {},
    "_说明": {
        "flow_cap": "今日流通市值（亿元）",
        "cap_change": "今日市值 - 昨日市值（亿元），正值=增长，负值=缩水",
        "rank": "按 cap_change 降序排名",
    },
}


def compute(date: str) -> pd.DataFrame:
    """
    市值增长策略。

    逻辑：
      1. 取指定 date 和前一个交易日的 daily_bar
      2. 用昨日数据反推流通股数 = volume × 100 / turn
      3. 今日流通市值 = 流通股数 × 今日close / 1e8
      4. 昨日流通市值 = 流通股数 × 昨日close / 1e8
      5. cap_change = 今日市值 - 昨日市值
         （流通股数取昨日值，避免 volume/turn 日间波动导致股数不一致）

    注意：
      之前版本分别用两天各自的 volume/turn 反推流通股数，
      导致同一股票两天股数不同，出现"股价跌但市值涨"的异常。
    """
    conn = dt.get_read_connection()
    try:
        # 取当天数据
        df_today = conn.execute(
            """
            SELECT code, code_name, close
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

        # 取前一日数据：close + 流通股数反推
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

    # 用昨日数据反推流通股数（短期内流通股数不变）
    df_prev["flow_shares"] = df_prev["volume"] * 100 / df_prev["turn"]

    # 计算昨日市值
    df_prev["prev_flow_cap"] = (df_prev["flow_shares"] * df_prev["close"] / 1e8).round(4)

    # 合并：用昨日流通股数 × 今日close 计算今日市值
    df = df_today.merge(df_prev[["code", "flow_shares", "prev_flow_cap"]], on="code", how="inner")
    df["flow_cap"] = (df["flow_shares"] * df["close"] / 1e8).round(4)

    # 计算变化
    df["cap_change"] = (df["flow_cap"] - df["prev_flow_cap"]).round(4)

    # 排名
    df = df.sort_values("cap_change", ascending=False)
    df["date"] = date
    df["rank"] = range(1, len(df) + 1)

    return df[["code", "code_name", "date", "flow_cap", "cap_change", "rank"]]
