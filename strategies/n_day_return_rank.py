"""N日涨跌幅度排行策略"""

import logging
import pandas as pd
import duckdb_tools as dt

logger = logging.getLogger("strategy")


STRATEGY = {
    "description": "N日涨跌幅度排行（累计涨跌幅）",
    "columns": [
        {"name": "code", "type": "VARCHAR", "not_null": True},
        {"name": "code_name", "type": "VARCHAR"},
        {"name": "date", "type": "DATE", "not_null": True},
        {"name": "n_days", "type": "INTEGER", "not_null": True},  # N日参数，避免不同参数缓存串数据
        {"name": "return_n", "type": "DOUBLE"},  # N日累计涨跌幅（%）
        {"name": "rank", "type": "INTEGER"},
    ],
    "primary_key": ["code", "date", "n_days"],  # n_days加入主键
    "params": {
        "n_days": 5,
    },
    "param_ui": {
        "n_days": {
            "label": "N日",
            "min_value": 1,
            "max_value": 60,
            "step": 1,
            "help": "计算最近N个交易日的累计涨跌幅",
        },
    },
}


def _ensure_table():
    """确保策略表存在，schema不匹配则重建"""
    table = STRATEGY.get("table")
    if not table:
        import pathlib
        name = pathlib.Path(__file__).stem
        table = f"strategy_{name}"
        STRATEGY["table"] = table

    columns = STRATEGY["columns"]
    primary_key = STRATEGY.get("primary_key", ["code", "date"])

    # 期望的列名集合
    expected_cols = {c["name"] for c in columns}

    conn = dt.get_connection()
    try:
        # 检查表是否存在以及schema是否匹配
        existing = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [table],
        ).fetchall()
        if existing:
            existing_cols = {row[0] for row in existing}
            if existing_cols != expected_cols:
                logger.warning(f"策略表 {table} schema不匹配，重建表（旧列:{existing_cols}，期望:{expected_cols}）")
                conn.execute(f"DROP TABLE {table}")
            else:
                logger.debug(f"策略表 {table} 已存在且schema匹配")
                return

        # 建表
        col_defs = []
        for c in columns:
            s = f"{c['name']} {c['type']}"
            if c.get("not_null"):
                s += " NOT NULL"
            col_defs.append(s)

        col_str = ", ".join(col_defs)
        pk_str = ", ".join(primary_key)
        ddl = f"CREATE TABLE IF NOT EXISTS {table} ({col_str}, PRIMARY KEY({pk_str}))"
        conn.execute(ddl)
        logger.info(f"策略表 {table} 已创建")
    finally:
        conn.close()


def compute(date: str, n_days: int = 5) -> pd.DataFrame:
    """
    N日涨跌幅度排行策略。

    计算逻辑：
    1. 找到date之前最近N个交易日（全局），确定日期范围
    2. 用日期范围直接过滤，避免昂贵的ROW_NUMBER窗口函数
    3. 在SQL中用product()聚合计算累计涨跌幅
    4. 按涨跌幅排序，给出rank
    """
    _ensure_table()

    logger.info(f"开始计算N日涨跌幅排行：参考日期={date}, n_days={n_days}")

    empty_result = pd.DataFrame(columns=["code", "code_name", "date", "n_days", "return_n", "rank"])

    conn = dt.get_read_connection()
    try:
        # 1. 找到全局最近N个交易日的日期范围（一次查询同时拿end_date和start_date）
        date_range = conn.execute(
            """
            SELECT MIN(date) as start_date, MAX(date) as end_date
            FROM (
                SELECT DISTINCT date
                FROM daily_bar
                WHERE date <= ?
                  AND adjustflag = '3'
                  AND tradestatus = '1'
                  AND pct_chg IS NOT NULL
                ORDER BY date DESC
                LIMIT ?
            )
            """,
            [date, n_days],
        ).fetchone()

        if not date_range or not date_range[0]:
            logger.warning(f"在{date}之前没有找到任何有效数据")
            return empty_result

        start_date, end_date = str(date_range[0]), str(date_range[1])
        logger.info(f"日期范围：{start_date} ~ {end_date}（{n_days}个交易日）")

        # 2. 一步SQL完成：过滤 + 累计涨跌幅计算 + 过滤数据不足的股票 + 排序
        result_df = conn.execute(
            """
            SELECT
                code,
                code_name,
                round((product(1 + pct_chg / 100.0) - 1) * 100, 4) as return_n,
                ? as date,
                ? as n_days
            FROM daily_bar
            WHERE date BETWEEN ? AND ?
              AND adjustflag = '3'
              AND tradestatus = '1'
              AND pct_chg IS NOT NULL
            GROUP BY code, code_name
            HAVING count(*) >= ?
            ORDER BY return_n DESC
            """,
            [end_date, n_days, start_date, end_date, n_days],
        ).df()

        logger.info(f"计算完成：{len(result_df)}只股票（数据不足已跳过）")

    finally:
        conn.close()

    if result_df.empty:
        logger.warning("计算结果为空")
        return empty_result

    # 3. 排名
    result_df["rank"] = range(1, len(result_df) + 1)

    logger.info(f"N日涨跌幅度排行计算完成，共{len(result_df)}只股票")
    return result_df[["code", "code_name", "date", "n_days", "return_n", "rank"]]
