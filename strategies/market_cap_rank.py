"""流通市值排行策略"""

import pandas as pd
import duckdb_tools as dt

STRATEGY = {
    "description": "流通市值排行（close × volume × 100 / turn）",
    "columns": [
        {"name": "code", "type": "VARCHAR", "not_null": True},
        {"name": "code_name", "type": "VARCHAR"},
        {"name": "date", "type": "DATE", "not_null": True},
        {"name": "flow_cap", "type": "DOUBLE"},
        {"name": "rank", "type": "INTEGER"},
    ],
    "primary_key": ["code", "date"],
    "params": {},
    "param_ui": {},
}


def compute(date: str) -> pd.DataFrame:
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
