"""
duckdb_tools.py — 数据库管理模块

职责：
  - 管理 DuckDB 数据库连接（单例）
  - 提供 stock_info / daily_bar / minute_bar 三张表的增删改查
  - 实现「缺失数据透明补拉」：上层调用 get_daily / get_minute 时，
    若本地数据不足，自动调用 data_tools 补拉后再返回，调用方无感知
  - 不依赖任何配置文件或上层业务模块（可选择性传入 db_path）

依赖：
  pip install duckdb pandas

数据库表结构：
  stock_info  — A 股基础信息，每日开盘前全量刷新
  daily_bar   — 日线 K 线（date + code + adjustflag 三字段联合主键）
  minute_bar  — 分钟线 K 线（datetime + code + frequency + adjustflag 联合主键）
"""

from __future__ import annotations

import logging
import os
import threading
from datetime import date, datetime, timedelta
from typing import Optional

import duckdb # type: ignore
import pandas as pd

from data_tools import (
    fetch_daily_single,
    fetch_minute,
    fetch_stock_list,
    fetch_index_list,
    DAILY_COLUMNS,
    INDEX_DAILY_COLUMNS,
    MINUTE_COLUMNS,
)

# ─────────────────────────────────────────────────────────────────────────────
# 日志
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("db")

# ─────────────────────────────────────────────────────────────────────────────
# 默认路径
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "stock_data.duckdb"
)


def _open(db_path: str, readonly: bool = False) -> duckdb.DuckDBPyConnection:
    """打开 DuckDB 连接（短连接，调用方负责关闭）。"""
    global _tables_initialized
    conn = duckdb.connect(db_path, read_only=False)
    return conn


def get_connection(db_path: str = _DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection:
    """
    获取 DuckDB 读写连接（短连接模式）。

    注意：调用方应在操作完成后及时释放（依赖 context manager 或 GC），
    不要长期持有，以免多进程写锁冲突。
    """
    return _open(db_path, readonly=False)


def get_read_connection(db_path: str = _DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection:
    """
    获取 DuckDB 只读连接（不持有写锁，可多进程并发）。
    """
    return _open(db_path, readonly=True)

# ─────────────────────────────────────────────────────────────────────────────
# 建表 DDL
# ─────────────────────────────────────────────────────────────────────────────

_DDL_STOCK_INFO = """
CREATE TABLE IF NOT EXISTS stock_info (
    code        VARCHAR PRIMARY KEY,   -- 股票代码，如 sh.600519
    code_name   VARCHAR,               -- 股票名称
    ipo_date    DATE,                  -- 上市日期
    out_date    DATE,                  -- 退市日期（在市为 NULL）
    type        VARCHAR,               -- 证券类型
    status      VARCHAR,               -- 上市状态
    updated_at  DATE
);
"""

_DDL_DAILY_BAR = """
CREATE TABLE IF NOT EXISTS daily_bar (
    date        DATE    NOT NULL,      -- 交易日期
    code        VARCHAR NOT NULL,      -- 股票代码
    code_name   VARCHAR,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    preclose    DOUBLE,
    volume      BIGINT,
    amount      DOUBLE,
    adjustflag  VARCHAR NOT NULL,      -- 1=后复权 2=前复权 3=不复权
    turn        DOUBLE,                -- 换手率 %
    tradestatus VARCHAR,               -- 1=正常 0=停牌
    pct_chg     DOUBLE,                -- 涨跌幅 %
    is_st       VARCHAR,               -- 是否 ST
    pe_ttm      DOUBLE,
    ps_ttm      DOUBLE,
    pcf_ncf_ttm DOUBLE,
    pb_mrq      DOUBLE,
    PRIMARY KEY (date, code, adjustflag)
);
"""

_DDL_MINUTE_BAR = """
CREATE TABLE IF NOT EXISTS minute_bar (
    date        DATE    NOT NULL,
    time        VARCHAR NOT NULL,      -- 格式 HH:MM:SS
    code        VARCHAR NOT NULL,
    code_name   VARCHAR,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      BIGINT,
    amount      DOUBLE,
    adjustflag  VARCHAR NOT NULL,
    frequency   VARCHAR NOT NULL,      -- '5' / '15' / '30' / '60'
    PRIMARY KEY (date, time, code, frequency, adjustflag)
);
"""

_DDL_INDEX_DAILY_BAR = """
CREATE TABLE IF NOT EXISTS index_daily_bar (
    date        DATE    NOT NULL,      -- 交易日期
    code        VARCHAR NOT NULL,      -- 指数代码，如 sh.000001
    code_name   VARCHAR,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    preclose    DOUBLE,
    volume      BIGINT,
    amount      DOUBLE,
    adjustflag  VARCHAR NOT NULL,      -- 指数一般用 '3' 不复权
    pct_chg     DOUBLE,                -- 涨跌幅 %
    PRIMARY KEY (date, code, adjustflag)
);
"""


def _ensure_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """建立四张核心表（如已存在则跳过）。"""
    for ddl in (_DDL_STOCK_INFO, _DDL_DAILY_BAR, _DDL_MINUTE_BAR, _DDL_INDEX_DAILY_BAR):
        conn.execute(ddl)
    logger.debug("数据库表结构已就绪")


# ─────────────────────────────────────────────────────────────────────────────
# 内部辅助：列名映射（BaoStock 原始列 → 数据库列）
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_daily_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    将 data_tools.fetch_daily 返回的 DataFrame 列名和类型
    对齐到 daily_bar 表结构。
    """
    rename_map = {
        "pctChg":     "pct_chg",
        "isST":       "is_st",
        "peTTM":      "pe_ttm",
        "psTTM":      "ps_ttm",
        "pcfNcfTTM":  "pcf_ncf_ttm",
        "pbMRQ":      "pb_mrq",
    }
    df = df.rename(columns=rename_map)

    # 类型转换：字符串 → 数值
    numeric_cols = [
        "open", "high", "low", "close", "preclose",
        "volume", "amount", "turn",
        "pct_chg", "pe_ttm", "ps_ttm", "pcf_ncf_ttm", "pb_mrq",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "volume" in df.columns:
        df["volume"] = df["volume"].astype("Int64")

    # date 转为 Python date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    return df


def _normalize_minute_df(df: pd.DataFrame, frequency: str) -> pd.DataFrame:
    """将 fetch_minute 返回的 DataFrame 对齐到 minute_bar 表结构。"""
    numeric_cols = ["open", "high", "low", "close", "volume", "amount"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "volume" in df.columns:
        df["volume"] = df["volume"].astype("Int64")

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    df["frequency"] = frequency
    return df


# ─────────────────────────────────────────────────────────────────────────────
# ── CRUD：stock_info ──────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

def upsert_stock_info(
    df: pd.DataFrame | None = None,
    db_path: str = _DEFAULT_DB_PATH,
) -> int:
    """
    刷新 stock_info 表（全量 UPSERT）。

    若未传入 df，则自动调用 data_tools.fetch_stock_list() 从 BaoStock 拉取。

    参数:
        df:      包含 stock_info 字段的 DataFrame；为 None 时自动拉取。
        db_path: 数据库路径

    返回:
        写入条数

    使用示例::

        count = upsert_stock_info()
        print(f"已刷新 {count} 条股票信息")
    """
    if df is None:
        logger.info("stock_info: 开始从 BaoStock 拉取股票列表 ...")
        df = fetch_stock_list()

    if df.empty:
        logger.warning("stock_info: 输入数据为空，跳过写入")
        return 0

    # 列名对齐
    rename_map = {
        "ipoDate": "ipo_date",
        "outDate": "out_date",
    }
    df = df.rename(columns=rename_map)

    # 日期列转换
    df["ipo_date"] = pd.to_datetime(df.get("ipo_date", pd.NaT), errors="coerce").dt.date #type: ignore
    df["out_date"] = pd.to_datetime(df["out_date"], errors="coerce").dt.date #type: ignore
    df["updated_at"] = pd.to_datetime(datetime.now(), errors="coerce")

    conn = get_connection(db_path)

    # 使用 DuckDB INSERT OR REPLACE（UPSERT by PRIMARY KEY）
    conn.execute("DELETE FROM stock_info")
    conn.execute("INSERT INTO stock_info SELECT * FROM df") 

    count = conn.execute("SELECT COUNT(*) FROM stock_info").fetchone()[0] #type: ignore
    logger.info(f"stock_info 刷新完毕，当前 {count} 条")
    conn.close()
    return count


def upsert_index_info(
    db_path: str = _DEFAULT_DB_PATH,
) -> int:
    """
    将指数列表写入 stock_info（type='2'），供 get_daily 查找 code_name。

    使用 INSERT OR REPLACE by PRIMARY KEY，不影响已有的股票条目。
    """
    logger.info("开始刷新 index_info ...")
    df = fetch_index_list()
    if df.empty:
        logger.warning("index_info: 指数列表为空，跳过")
        return 0

    rename_map = {
        "ipoDate": "ipo_date",
        "outDate": "out_date",
    }
    df = df.rename(columns=rename_map)
    df["ipo_date"] = pd.to_datetime(df.get("ipo_date", pd.NaT), errors="coerce").dt.date  # type: ignore
    df["out_date"] = pd.to_datetime(df["out_date"], errors="coerce").dt.date  # type: ignore
    df["updated_at"] = pd.to_datetime(datetime.now(), errors="coerce")

    conn = get_connection(db_path)
    # 只删除旧指数条目，保留股票条目
    conn.execute("DELETE FROM stock_info WHERE type = '2'")
    conn.execute("INSERT INTO stock_info SELECT * FROM df")

    count = conn.execute("SELECT COUNT(*) FROM stock_info WHERE type = '2'").fetchone()[0]  # type: ignore
    logger.info(f"index_info 刷新完毕，当前 {count} 只指数")
    conn.close()
    return count


def get_stock_info(
    code: str | None = None,
    db_path: str = _DEFAULT_DB_PATH,
) -> pd.DataFrame:
    """
    查询 stock_info 表。

    参数:
        code:    股票代码过滤（如 'sh.600519'）；为 None 则返回全部。
        db_path: 数据库路径

    返回:
        pd.DataFrame

    使用示例::

        df = get_stock_info('sh.600519')
        print(df)
    """
    conn = get_read_connection(db_path)
    if code:
        return conn.execute("SELECT * FROM stock_info WHERE code = ?", [code]).df()
    res = conn.execute("SELECT * FROM stock_info ORDER BY code").df()
    conn.close()
    return res


def delete_stock_info(
    code: str,
    db_path: str = _DEFAULT_DB_PATH,
) -> int:
    """
    按股票代码删除 stock_info 记录。

    返回:
        0
    """
    conn = get_connection(db_path)
    if code == 'ALL':
        conn.execute("DELETE FROM stock_info")
        logger.info("stock_info: 全表已删除")
    else:
        conn.execute("DELETE FROM stock_info WHERE code = ?", [code])
        logger.info(f"stock_info: 删除 {code}")
    conn.close()
    return 0

# ─────────────────────────────────────────────────────────────────────────────
# ── CRUD：daily_bar ───────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

_CHUNK_SIZE = 50_000   # 单次 INSERT 最大条数，防内存溢出


def insert_daily(
    df: pd.DataFrame,
    db_path: str = _DEFAULT_DB_PATH,
) -> int:
    """
    将日线 DataFrame 写入 daily_bar（UPSERT：主键冲突则覆盖）。

    参数:
        df:      data_tools.fetch_daily / fetch_daily_single 返回的 DataFrame，
                 或已经过 _normalize_daily_df 处理的 DataFrame。
        db_path: 数据库路径

    返回:
        实际写入（含覆盖）条数
    """
    if df.empty:
        logger.warning("insert_daily: 传入 DataFrame 为空，跳过")
        return 0

    df = _normalize_daily_df(df.copy())
    conn = get_connection(db_path)

    total = 0
    for start in range(0, len(df), _CHUNK_SIZE):
        chunk = df.iloc[start: start + _CHUNK_SIZE]
        conn.execute(
            """
            INSERT OR REPLACE INTO daily_bar
            SELECT
                date, code, code_name,
                open, high, low, close, preclose,
                volume, amount, adjustflag, turn,
                tradestatus, pct_chg, is_st,
                pe_ttm, ps_ttm, pcf_ncf_ttm, pb_mrq
            FROM chunk
            """
        )
        total += len(chunk)
        logger.debug(f"insert_daily: 已写入 {total}/{len(df)} 条")

    logger.info(f"insert_daily: 共写入 {total} 条日线数据")
    conn.close()
    return total

def get_daily(
    code: str,
    start_date: str,
    end_date: str,
    adjustflag: str = "3",
    db_path: str = _DEFAULT_DB_PATH,
    auto_fetch: bool = True,
) -> pd.DataFrame:
    """
    查询日线数据（带透明补拉）。

    查询流程::

        1. 查询本地 daily_bar，检查 [start_date, end_date] 内最早/最晚日期
        2. 若本地数据完整（首尾日期均覆盖目标区间）→ 直接返回
        3. 若存在缺口（含完全没有数据）→ 自动调用 data_tools.fetch_daily_single
           补拉缺失区间 → 写入 daily_bar → 重新查询后返回
        4. auto_fetch=False 时跳过步骤 3，直接返回现有数据

    参数:
        code:        股票代码，如 ``'sh.600519'``
        code_name:   股票名称（拉取时需要）
        start_date:  起始日期，格式 ``'YYYY-MM-DD'``
        end_date:    截止日期，格式 ``'YYYY-MM-DD'``
        adjustflag:  复权方式（'1'=后复权，'2'=前复权，'3'=不复权）
        db_path:     数据库路径
        auto_fetch:  是否允许自动补拉（默认 True）

    返回:
        pd.DataFrame，列结构同 daily_bar 表

    使用示例::

        df = get_daily('sh.600519', '贵州茅台', '2025-01-01', '2026-04-17')
        print(df.tail())
    """ 
    stk_info = get_stock_info(code)
    conn = get_read_connection(db_path)
    if stk_info.empty:
        code_name = code  # 指数等可能不在 stock_info，用 code 兜底
    else:
        code_name = stk_info['code_name'].values[0]

    def _query_local() -> pd.DataFrame:
        return conn.execute(
            """
            SELECT * FROM daily_bar
            WHERE code = ?
              AND adjustflag = ?
              AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            [code, adjustflag, start_date, end_date],
        ).df()

    local_df = _query_local()

    if auto_fetch:
        need_fetch_start: str | None = None
        need_fetch_end:   str | None = None

        if local_df.empty:
            # 完全没有数据，拉全段
            need_fetch_start = start_date
            need_fetch_end   = end_date
            logger.info(
                f"[Lazy Pull] {code} 本地无数据，拉取 [{start_date} ~ {end_date}]"
            )
        else:
            local_min = str(pd.to_datetime(local_df["date"].min()))[:10]
            local_max = str(pd.to_datetime(local_df["date"].max()))[:10]

            logger.warning(f"[Lazy Pull] {code} 本地数据范围: [{local_min} ~ {local_max}]")
                
            gaps = []
            if local_min > start_date:
                gaps.append(f"前缺: {start_date} ~ {local_min}")
                need_fetch_start = start_date
                need_fetch_end   = local_min
            if local_max < end_date:
                gaps.append(f"后缺: {local_max} ~ {end_date}")
                # 若同时有前后缺口，合并为整段重拉（避免多次登录登出）
                need_fetch_start = need_fetch_start or local_max
                need_fetch_end   = end_date

            if gaps:
                logger.info(
                    f"[Lazy Pull] {code} 本地数据缺口: {'; '.join(gaps)}，"
                    f"补拉 [{need_fetch_start} ~ {need_fetch_end}]"
                )

        if need_fetch_start and need_fetch_end:
            logger.warning(
                f"[Lazy Pull] {code} 补拉 [{need_fetch_start} ~ {need_fetch_end}]"
            )
            fetched = fetch_daily_single(
                code=code,
                code_name=code_name,
                start_date=need_fetch_start,
                end_date=need_fetch_end,
                adjustflag=adjustflag,
            )
            if not fetched.empty:
                insert_daily(fetched, db_path=db_path)
            else:
                logger.warning(f"[Lazy Pull] {code} 补拉返回空数据")
        else:
            logger.warning(f"[Don't Pull] {code} 本地数据完整，直接返回")
    local_df = _query_local()
    conn.close()
    return local_df


def update_daily(
    df: pd.DataFrame,
    db_path: str = _DEFAULT_DB_PATH,
) -> int:
    """
    更新日线数据（等价于 UPSERT：有则覆盖，无则插入）。

    直接委托 insert_daily，因为 insert_daily 内部使用 INSERT OR REPLACE。

    参数:
        df:      需要更新的日线 DataFrame
        db_path: 数据库路径

    返回:
        更新/插入条数
    """
    return insert_daily(df, db_path=db_path)


def delete_daily(
    code: str,
    start_date: str | None = None,
    end_date:   str | None = None,
    adjustflag: str  = '3',
    db_path: str = _DEFAULT_DB_PATH,
) -> None:
    """
    删除日线数据。

    参数:
        code:       股票代码（必填，禁止无限制全表删除）
        start_date: 起始日期（可选，不传则不限下界）
        end_date:   截止日期（可选，不传则不限上界）
        adjustflag: 复权方式过滤（可选）
        db_path:    数据库路径

    使用示例::

        # 删除茅台 2025 年全年数据
        delete_daily('sh.600519', start_date='2025-01-01', end_date='2025-12-31')
    """
    conditions = ["code = ?"]
    params: list = [code]

    if start_date:
        conditions.append("date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("date <= ?")
        params.append(end_date)
    
    if adjustflag:
        conditions.append("adjustflag = ?")
        params.append(adjustflag)

    sql = f"DELETE FROM daily_bar WHERE {' AND '.join(conditions)}"
    conn = get_connection(db_path)
    conn.execute(sql, params)
    conn.close()
    logger.info(
        f"delete_daily: {code} [{start_date}~{end_date}] adjustflag={adjustflag} 已删除"
    )


def query_daily(
    sql: str,
    params: list | None = None,
    db_path: str = _DEFAULT_DB_PATH,
) -> pd.DataFrame:
    """
    执行任意 SELECT SQL 并返回 DataFrame（高级查询入口）。

    参数:
        sql:     完整 SELECT 语句，支持 DuckDB 全部语法
        params:  参数化查询参数列表（防 SQL 注入）
        db_path: 数据库路径

    返回:
        pd.DataFrame

    使用示例::

        df = query_daily(
            "SELECT code, date, close FROM daily_bar "
            "WHERE date = ? AND adjustflag = '1' "
            "ORDER BY close DESC LIMIT 10",
            params=['2026-04-17']
        )
    """
    conn = get_read_connection(db_path)
    res = conn.execute(sql, params or []).df()
    return res


# ─────────────────────────────────────────────────────────────────────────────
# ── CRUD：index_daily_bar ────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_index_daily_df(df: pd.DataFrame) -> pd.DataFrame:
    """将 data_tools.fetch_index_daily 返回的 DataFrame 对齐到 index_daily_bar 表结构。"""
    rename_map = {
        "pctChg": "pct_chg",
    }
    df = df.rename(columns=rename_map)

    numeric_cols = ["open", "high", "low", "close", "preclose", "volume", "amount", "pct_chg"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "volume" in df.columns:
        df["volume"] = df["volume"].astype("Int64")

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    return df


def insert_index_daily(
    df: pd.DataFrame,
    db_path: str = _DEFAULT_DB_PATH,
) -> int:
    """
    将指数日线 DataFrame 写入 index_daily_bar（UPSERT：主键冲突则覆盖）。

    参数:
        df:      data_tools.fetch_index_daily 返回的 DataFrame
        db_path: 数据库路径

    返回:
        实际写入（含覆盖）条数
    """
    if df.empty:
        logger.warning("insert_index_daily: 传入 DataFrame 为空，跳过")
        return 0

    df = _normalize_index_daily_df(df.copy())
    conn = get_connection(db_path)

    total = 0
    for start in range(0, len(df), _CHUNK_SIZE):
        chunk = df.iloc[start: start + _CHUNK_SIZE]
        conn.execute(
            """
            INSERT OR REPLACE INTO index_daily_bar
            SELECT
                date, code, code_name,
                open, high, low, close, preclose,
                volume, amount, adjustflag, pct_chg
            FROM chunk
            """
        )
        total += len(chunk)
        logger.debug(f"insert_index_daily: 已写入 {total}/{len(df)} 条")

    logger.info(f"insert_index_daily: 共写入 {total} 条指数日线数据")
    conn.close()
    return total


def get_index_daily(
    code: str,
    start_date: str,
    end_date: str,
    adjustflag: str = "3",
    db_path: str = _DEFAULT_DB_PATH,
) -> pd.DataFrame:
    """
    查询指数日线数据（纯读，不带透明补拉，指数靠 daemon 定时拉取）。

    参数:
        code:        指数代码，如 ``'sh.000001'``
        start_date:  起始日期，格式 ``'YYYY-MM-DD'``
        end_date:    截止日期，格式 ``'YYYY-MM-DD'``
        adjustflag:  复权方式（指数一般用 '3' 不复权）
        db_path:     数据库路径

    返回:
        pd.DataFrame，列结构同 index_daily_bar 表
    """
    conn = get_read_connection(db_path)
    res = conn.execute(
        """
        SELECT * FROM index_daily_bar
        WHERE code = ?
          AND adjustflag = ?
          AND date BETWEEN ? AND ?
        ORDER BY date
        """,
        [code, adjustflag, start_date, end_date],
    ).df()
    conn.close()
    return res


# ─────────────────────────────────────────────────────────────────────────────
# ── CRUD：minute_bar ──────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

def insert_minute(
    df: pd.DataFrame,
    frequency: str,
    db_path: str = _DEFAULT_DB_PATH,
) -> int:
    """
    将分钟线 DataFrame 写入 minute_bar（UPSERT）。

    参数:
        df:        data_tools.fetch_minute 返回的 DataFrame
        frequency: K线周期 ('5' / '15' / '30' / '60')
        db_path:   数据库路径

    返回:
        写入条数
    """
    if df.empty:
        logger.warning("insert_minute: 传入 DataFrame 为空，跳过")
        return 0

    df = _normalize_minute_df(df.copy(), frequency)
    conn = get_connection(db_path)

    total = 0
    for start in range(0, len(df), _CHUNK_SIZE):
        chunk = df.iloc[start: start + _CHUNK_SIZE]
        conn.execute(
            """
            INSERT OR REPLACE INTO minute_bar
            SELECT
                date, time, code, code_name,
                open, high, low, close,
                volume, amount, adjustflag, frequency
            FROM chunk
            """
        )
        total += len(chunk)

    logger.info(f"insert_minute: 共写入 {total} 条 {frequency}min 分钟线数据")
    return total


def get_minute(
    code: str,
    start_date: str,
    end_date: str,
    frequency: str = "5",
    adjustflag: str = "1",
    db_path: str = _DEFAULT_DB_PATH,
    auto_fetch: bool = True,
) -> pd.DataFrame:
    """
    查询分钟线数据（带透明补拉，逻辑同 get_daily）。

    参数:
        code:        股票代码
        start_date:  起始日期，格式 ``'YYYY-MM-DD'``
        end_date:    截止日期，格式 ``'YYYY-MM-DD'``
        frequency:   K线周期 ('5' / '15' / '30' / '60')
        adjustflag:  复权方式
        db_path:     数据库路径
        fetch_config: FetchConfig
        auto_fetch:  是否允许自动补拉

    返回:
        pd.DataFrame，列结构同 minute_bar 表

    注意:
        BaoStock 分钟线最多支持近 3 个月，请勿传入过早的 start_date。

    使用示例::

        df = get_minute('sh.600519', '贵州茅台',
                        '2026-04-14', '2026-04-17', frequency='5')
        print(df.head())
    """
    conn = get_connection(db_path)
    stk_info = get_stock_info(code)
    if stk_info.empty:
        code_name = code  # 兜底
    else:
        code_name = stk_info['code_name'].values[0]
    def _query_local() -> pd.DataFrame:
        return conn.execute(
            """
            SELECT * FROM minute_bar
            WHERE code = ?
              AND frequency = ?
              AND adjustflag = ?
              AND date BETWEEN ? AND ?
            ORDER BY date, time
            """,
            [code, frequency, adjustflag, start_date, end_date],
        ).df()

    local_df = _query_local()

    if auto_fetch:
        need_fetch = False

        if local_df.empty:
            need_fetch = True
            logger.info(
                f"[Lazy Pull] {code} {frequency}min 本地无数据，"
                f"拉取 [{start_date} ~ {end_date}]"
            )
        else:
            local_min = str(pd.to_datetime(local_df["date"].min()))[:10]
            local_max = str(pd.to_datetime(local_df["date"].max()))[:10]

            if local_min <= start_date and local_max >= end_date:
                logger.info(f"[Don't Pull] {code} 本地数据完整，直接返回")
                return local_df

            if local_min > start_date or local_max < end_date:
                need_fetch = True
                logger.info(
                    f"[Lazy Pull] {code} {frequency}min 本地数据不完整 "
                    f"({local_min}~{local_max})，重拉 [{start_date}~{end_date}]"
                )

        if need_fetch:
            single_row = pd.DataFrame([{"code": code, "code_name": code_name}])
            fetched = fetch_minute(
                stock_list=single_row,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                adjustflag=adjustflag,
            )
            if not fetched.empty:
                insert_minute(fetched, frequency=frequency, db_path=db_path)
                local_df = _query_local()
            else:
                logger.warning(f"[Lazy Pull] {code} {frequency}min 补拉返回空数据")

    return local_df


def delete_minute(
    code: str,
    start_date: str | None = None,
    end_date:   str | None = None,
    frequency:  str | None = None,
    adjustflag: str | None = None,
    db_path: str = _DEFAULT_DB_PATH,
) -> None:
    """
    删除分钟线数据。

    参数规则同 delete_daily：code 为必填，其余均为可选过滤条件。
    """
    conditions = ["code = ?"]
    params: list = [code]

    if start_date:
        conditions.append("date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("date <= ?")
        params.append(end_date)
    if frequency:
        conditions.append("frequency = ?")
        params.append(frequency)
    if adjustflag:
        conditions.append("adjustflag = ?")
        params.append(adjustflag)

    sql = f"DELETE FROM minute_bar WHERE {' AND '.join(conditions)}"
    conn = get_connection(db_path)
    conn.execute(sql, params)
    logger.info(
        f"delete_minute: {code} [{start_date}~{end_date}] "
        f"freq={frequency} adjustflag={adjustflag} 已删除"
    )


def query_minute(
    sql: str,
    params: list | None = None,
    db_path: str = _DEFAULT_DB_PATH,
) -> pd.DataFrame:
    """
    执行任意 SELECT SQL 查询分钟线（高级入口，同 query_daily）。
    """
    conn = get_connection(db_path)
    return conn.execute(sql, params or []).df()


# ─────────────────────────────────────────────────────────────────────────────
# ── 通用工具 ──────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

def table_stats(db_path: str = _DEFAULT_DB_PATH) -> pd.DataFrame:
    """
    返回三张核心表的行数与最新数据日期，供监控/调试使用。

    返回:
        pd.DataFrame，示例::

            table       rows   min_date    max_date
            daily_bar   5000   2024-01-01  2026-04-17
            minute_bar  12000  2026-03-01  2026-04-17
            stock_info  5200   NaN         NaN

    使用示例::

        print(table_stats())
    """
    conn = get_read_connection(db_path)
    rows = []

    for tbl, date_col in [
        ("daily_bar", "date"),
        ("minute_bar", "date"),
        ("stock_info", None),
    ]:
        count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        if date_col:
            r = conn.execute(
                f"SELECT MIN({date_col}), MAX({date_col}) FROM {tbl}"
            ).fetchone()
            min_d, max_d = r[0], r[1]
        else:
            min_d = max_d = None
        rows.append({"table": tbl, "rows": count, "min_date": min_d, "max_date": max_d})
    return pd.DataFrame(rows)




# ─────────────────────────────────────────────────────────────────────────────
# 模块自测
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    # ── 测试 1：建表 + 初始化连接 ──────────────────────────
    print("\n===== 测试 1: 连接 & 建表 =====")
    conn = get_connection()
    print("连接成功，表结构已就绪")

    # ── 测试 3：Lazy Pull 日线（首次必然触发拉取） ─────────
    print("\n===== 测试 3: get_daily (Lazy Pull) =====")
    df_daily = get_daily(
        code="sh.600519",
        start_date="2026-04-01",
        end_date="2026-04-17",
        adjustflag="1",
    )
    print(f"日线行数: {len(df_daily)}")
    print(df_daily.tail(3).to_string())

    # ── 测试 4：再次查询（应命中本地缓存，不触发拉取） ────
    print("\n===== 测试 4: get_daily (缓存命中) =====")
    df_cached = get_daily(
        code="sh.600519",
        start_date="2026-04-01",
        end_date="2026-04-17",
        adjustflag="1",
    )
    print(f"缓存命中，行数: {len(df_cached)}")

    # ── 测试 5：delete_daily ───────────────────────────────
    print("\n===== 测试 5: delete_daily =====")
    delete_daily("sh.600519", start_date="2026-04-17", end_date="2026-04-17")
    print("已删除 2026-04-17 单日数据")

    # ── 测试 6：高级 SQL 查询 ──────────────────────────────
    print("\n===== 测试 6: query_daily =====")
    df_top = query_daily(
        "SELECT code, date, close, pct_chg FROM daily_bar "
        "WHERE adjustflag = '1' ORDER BY pct_chg DESC LIMIT 5"
    )
    print(df_top.to_string())

    # ── 测试 7：table_stats ────────────────────────────────
    print("\n===== 测试 7: table_stats =====")
    print(table_stats().to_string())

    print("\n所有测试完成")
