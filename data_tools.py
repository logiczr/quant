"""
data_tools.py — 数据获取模块

职责：
  - 与 BaoStock 数据源交互（登录/登出/拉取）
  - 提供股票列表查询、日线批量拉取、分钟线批量拉取三个核心接口
  - 不依赖任何数据库、配置文件、上层业务模块（完全解耦）
  - 所有函数返回标准 pd.DataFrame，调用方自行决定存储/处理方式

依赖：
  pip install baostock pandas
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import  Optional

import baostock as bs # type: ignore
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# 日志
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("fetch")

# ─────────────────────────────────────────────────────────────────────────────
# 常量
# ─────────────────────────────────────────────────────────────────────────────

# 日线请求字段（与 BaoStock API 保持一致）
DAILY_FIELDS = (
    "date,code,open,high,low,close,preclose,"
    "volume,amount,adjustflag,turn,tradestatus,"
    "pctChg,isST,peTTM,psTTM,pcfNcfTTM,pbMRQ"
)

# 指数日线请求字段（指数无 turn/tradestatus/isST/估值字段，但有 pctChg）
INDEX_DAILY_FIELDS = (
    "date,code,open,high,low,close,preclose,"
    "volume,amount,adjustflag,pctChg"
)

# 指数日线标准列顺序（含 code_name，由 fetch_index_daily 注入）
INDEX_DAILY_COLUMNS = [
    "date", "code", "code_name", "open", "high", "low", "close", "preclose",
    "volume", "amount", "adjustflag", "pctChg",
]

# 分钟线请求字段
MINUTE_FIELDS = "date,time,code,open,high,low,close,volume,amount,adjustflag"

# 日线标准列顺序（含 code_name，由本模块在 row_data 中注入）
DAILY_COLUMNS = [
    "date", "code", "code_name","open", "high", "low", "close", "preclose",
    "volume", "amount", "adjustflag", "turn","tradestatus", "pctChg", "isST",
    "peTTM", "psTTM", "pcfNcfTTM", "pbMRQ",
]

# 分钟线标准列顺序
MINUTE_COLUMNS = [
    "date", "time", "code", "code_name","open", "high", "low", "close",
    "volume", "amount", "adjustflag",
]

# ─────────────────────────────────────────────────────────────────────────────
# BaoStock 会话上下文管理器
# ─────────────────────────────────────────────────────────────────────────────

@contextmanager
def baostock_session(socket_timeout: float = 15.0):
    """
    BaoStock 登录/登出上下文管理器。

    用法::

        with baostock_session():
            rs = bs.query_history_k_data_plus(...)

    保证无论是否发生异常，都会执行 logout()，避免会话泄漏。

    参数:
        socket_timeout: 底层 socket 超时秒数。
            BaoStock 用裸 socket 通信，默认无超时，TCP 断开后会永远阻塞。
            设置超时后，任何 socket 操作超过此时间将抛出 socket.timeout 异常，
            被 _query_with_retry 捕获后计入失败，不再卡死。
    """
    import socket as _socket

    old_timeout = _socket.getdefaulttimeout()
    _socket.setdefaulttimeout(socket_timeout)
    try:
        login_result = bs.login()
        if login_result.error_code != "0":
            raise RuntimeError(
                f"BaoStock 登录失败: [{login_result.error_code}] {login_result.error_msg}"
            )
        logger.debug(f"BaoStock 登录成功 (socket_timeout={socket_timeout}s)")
        try:
            yield
        finally:
            bs.logout()
            logger.debug("BaoStock 已登出")
    finally:
        # 恢复原始 socket 超时设置，避免影响其他模块
        _socket.setdefaulttimeout(old_timeout)


# ─────────────────────────────────────────────────────────────────────────────
# 辅助：BaoStock 重连
# ─────────────────────────────────────────────────────────────────────────────

def _reconnect_baostock() -> bool:
    """断开并重新登录 BaoStock，返回是否成功"""
    try:
        bs.logout()
    except Exception:
        pass
    try:
        login_result = bs.login()
        if login_result.error_code == "0":
            logger.info("BaoStock 重连成功")
            return True
    except Exception:
        pass
    logger.error("BaoStock 重连失败")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# 辅助：带重试的 BaoStock 请求
# ─────────────────────────────────────────────────────────────────────────────

def _query_with_retry(
    query_fn,
    code: str,
    max_retries: int = 2,
) -> Optional[object]:
    """
    带重试的 BaoStock 请求。遇到 socket 异常立即重连，不在死连接上浪费重试。

    参数:
        query_fn:     无参可调用，返回 BaoStock ResultData
        code:         股票代码（日志用）
        max_retries:  最大重试次数

    返回:
        ResultData 或 None
    """
    reconnected = False

    for attempt in range(1, max_retries + 1):
        try:
            rs = query_fn()
        except (OSError, TimeoutError) as e:
            # TCP 已断，在死连接上重试无意义
            logger.warning(f"  [{code}] 网络异常: {type(e).__name__}")
            if not reconnected:
                if _reconnect_baostock():
                    reconnected = True
                    continue  # 用新连接进入下一轮重试
                break  # 重连也失败，放弃
            continue  # 已重连过仍异常，继续重试

        if rs.error_code == "0":
            return rs  # 成功返回结果
        # 业务层错误，正常重试
        logger.warning(f"  [{code}] 第 {attempt}/{max_retries} 次失败: {rs.error_msg}")

    return None


# ─────────────────────────────────────────────────────────────────────────────
# 公开接口 1：获取 A 股股票列表
# ─────────────────────────────────────────────────────────────────────────────

def fetch_stock_list() -> pd.DataFrame:
    """
    查询全市场 A 股在市股票列表。

    功能:
        - 调用 ``bs.query_stock_basic()`` 获取所有证券
        - 只保留类型 type == '1'（A 股）
        - 过滤已退市（outDate != ''）的股票

    返回:
        pd.DataFrame，包含以下字段::

            code        股票代码（如 sh.600000）
            code_name   股票名称
            ipoDate     上市日期
            outDate     退市日期（在市为空字符串）
            type        证券类型（此处均为 '1'）
            status      上市状态

    异常:
        RuntimeError: 登录失败或接口调用失败时抛出

    使用示例::

        df = fetch_stock_list()
        print(df.head())
    """
    logger.info("开始获取 A 股股票列表 ...")

    with baostock_session():
        rs = bs.query_stock_basic()
        if rs.error_code != "0":
            raise RuntimeError(
                f"query_stock_basic 失败: [{rs.error_code}] {rs.error_msg}"
            )

        rows = []
        while rs.next():
            rows.append(rs.get_row_data())

    if not rows:
        logger.warning("query_stock_basic 返回空结果")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=rs.fields)

    # 只保留 A 股（type == '1'）且未退市（outDate 为空）
    df = df[(df["type"] == "1") & (df["outDate"] == "")].copy()
    df = df.reset_index(drop=True)

    logger.info(f"A 股股票列表获取完毕，共 {len(df)} 只在市股票")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 公开接口 1b：获取指数列表
# ─────────────────────────────────────────────────────────────────────────────

def fetch_index_list() -> pd.DataFrame:
    """
    查询 BaoStock 全市场指数列表。

    功能:
        - 调用 ``bs.query_stock_basic()`` 获取所有证券
        - 只保留类型 type == '2'（指数）
        - 过滤已退市（outDate != ''）的指数

    返回:
        pd.DataFrame，结构同 fetch_stock_list()
    """
    logger.info("开始获取指数列表 ...")

    with baostock_session():
        rs = bs.query_stock_basic()
        if rs.error_code != "0":
            raise RuntimeError(
                f"query_stock_basic 失败: [{rs.error_code}] {rs.error_msg}"
            )

        rows = []
        while rs.next():
            rows.append(rs.get_row_data())

    if not rows:
        logger.warning("query_stock_basic 返回空结果")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=rs.fields)
    df = df[(df["type"] == "2") & (df["outDate"] == "")].copy()
    df = df.reset_index(drop=True)

    logger.info(f"指数列表获取完毕，共 {len(df)} 只指数")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 公开接口 2b：批量拉取指数日线
# ─────────────────────────────────────────────────────────────────────────────

def fetch_index_daily(
    index_list: pd.DataFrame,
    start_date: str,
    end_date: str,
    adjustflag: str = "3",
) -> pd.DataFrame:
    """
    批量拉取多只指数的日线数据。

    与 fetch_daily 类似，但使用 INDEX_DAILY_FIELDS，
    缺失字段填充空字符串以对齐 DAILY_COLUMNS 结构。

    参数:
        index_list:  指数列表 DataFrame（至少包含 ``code`` 和 ``code_name`` 列）
        start_date:  起始日期
        end_date:    截止日期
        adjustflag:  复权方式（指数一般用 '3' 不复权）

    返回:
        pd.DataFrame，列结构参见 INDEX_DAILY_COLUMNS
    """
    total_indices = len(index_list)
    logger.info(
        f"开始批量拉取指数日线: {total_indices} 只指数 "
        f"[{start_date} ~ {end_date}]  adjustflag={adjustflag}"
    )

    total_list: list[pd.DataFrame] = []
    cache_rows: list[list] = []
    flush_size = 10000

    success_count = 0
    fail_count = 0

    with baostock_session():
        for idx, row in index_list.iterrows():
            code: str = row["code"]
            code_name: str = row.get("code_name", "")

            rs = _query_with_retry(
                query_fn=lambda: bs.query_history_k_data_plus(
                    code, INDEX_DAILY_FIELDS,
                    start_date=start_date, end_date=end_date,
                    frequency="d", adjustflag=adjustflag,
                ),
                code=code,
                max_retries=2,
            )

            if rs is None:
                fail_count += 1
                continue

            while rs.next():  # type: ignore
                raw = rs.get_row_data()  # type: ignore
                # raw: date, code, open, high, low, close, preclose,
                #       volume, amount, adjustflag, pctChg
                aligned = [
                    raw[0],   # date
                    raw[1],   # code
                    code_name,  # code_name
                    raw[2],   # open
                    raw[3],   # high
                    raw[4],   # low
                    raw[5],   # close
                    raw[6],   # preclose
                    raw[7],   # volume
                    raw[8],   # amount
                    raw[9],   # adjustflag
                    raw[10],  # pctChg
                ]
                cache_rows.append(aligned)

                if len(cache_rows) >= flush_size:
                    total_list.append(
                        pd.DataFrame(cache_rows, columns=INDEX_DAILY_COLUMNS)
                    )
                    cache_rows = []

            success_count += 1
            logger.info(f"  [{idx+1}/{total_indices}] {code} {code_name} 完成")

    if cache_rows:
        total_list.append(pd.DataFrame(cache_rows, columns=INDEX_DAILY_COLUMNS))

    if not total_list:
        logger.warning("fetch_index_daily: 未获取到任何数据")
        return pd.DataFrame(columns=INDEX_DAILY_COLUMNS)

    result = pd.concat(total_list, axis=0, ignore_index=True, copy=False)  # type: ignore
    logger.info(
        f"fetch_index_daily 完成: 成功 {success_count} 只，失败 {fail_count} 只，"
        f"共 {len(result)} 条记录"
    )
    return result


# ─────────────────────────────────────────────────────────────────────────────
# 公开接口 2：批量拉取日线数据
# ─────────────────────────────────────────────────────────────────────────────

def fetch_daily(
    stock_list: pd.DataFrame,
    start_date: str,
    end_date: str,
    adjustflag: str | None = None,
) -> pd.DataFrame:
    """
    批量拉取多只股票的日线 K 线数据。

    参数:
        stock_list:  股票列表 DataFrame（至少包含 ``code`` 和 ``code_name`` 列），
                     通常直接传入 :func:`fetch_stock_list` 的返回值。
                     也可以传入只有一行的 DataFrame 以拉取单只股票。
        start_date:  起始日期，格式 ``'YYYY-MM-DD'``
        end_date:    截止日期，格式 ``'YYYY-MM-DD'``
        adjustflag:  复权方式（'1'=后复权，'2'=前复权，'3'=不复权）；
                     为 None 时使用 config.default_adjustflag。

    返回:
        pd.DataFrame，列顺序参见模块常量 ``DAILY_COLUMNS``::

            date, code, code_name, open, high, low, close, preclose,
            volume, amount, adjustflag, turn, tradestatus,
            pctChg, isST, peTTM, psTTM, pcfNcfTTM, pbMRQ

        - 停牌日（tradestatus == '0'）保留在结果中，由调用方决定是否过滤。
        - 若某只股票全部重试失败，该股票不出现在结果中（不中断整批任务）。

    异常:
        RuntimeError: BaoStock 登录失败时抛出。

    使用示例::

        stocks = fetch_stock_list()
        df = fetch_daily(stocks, '2025-01-01', '2026-04-14',
                         adjustflag='1')
        print(df.shape)
    """
    adjustflag = adjustflag

    total_stocks = len(stock_list)
    logger.info(
        f"开始批量拉取日线: {total_stocks} 只股票 "
        f"[{start_date} ~ {end_date}]  adjustflag={adjustflag}"
    )

    total_list: list[pd.DataFrame] = []
    cache_rows: list[list] = []
    flush_size = 10000

    success_count = 0
    fail_count = 0
    t_total_fetch = 0.0
    t_total_parse = 0.0

    with baostock_session():
        for i, (_, row) in enumerate(stock_list.iterrows()):
            code: str = row["code"]
            code_name: str = row.get("code_name", "")

            # ── 发起查询（带重试 + 自动重连） ─────────────────────
            t0 = time.time()
            rs = _query_with_retry(
                query_fn=lambda : bs.query_history_k_data_plus(code,DAILY_FIELDS,start_date=start_date,end_date=end_date,frequency="d",adjustflag=adjustflag),
                code=code,
                max_retries=2,
            )
            t1 = time.time()

            if rs is None:
                # 失败后 logout → 等 10s → login → 重试一次
                fail_count += 1
                logger.warning(f"  [{i + 1}/{total_stocks}] {code} {code_name}: 拉取失败，logout 后重连重试...")
                try:
                    bs.logout()
                except Exception:
                    pass
                time.sleep(10)
                try:
                    login_result = bs.login()
                    if login_result.error_code != "0":
                        logger.error(f"BaoStock 重连登录失败: [{login_result.error_code}] {login_result.error_msg}")
                        continue
                except Exception as e:
                    logger.error(f"BaoStock 重连登录异常: {e}")
                    continue
                logger.info("BaoStock 重连成功，重试拉取")
                rs = _query_with_retry(
                    query_fn=lambda : bs.query_history_k_data_plus(code,DAILY_FIELDS,start_date=start_date,end_date=end_date,frequency="d",adjustflag=adjustflag),
                    code=code,
                    max_retries=2,
                )
                t1 = time.time()
                if rs is None:
                    logger.warning(f"  [{i + 1}/{total_stocks}] {code} {code_name}: 重连后仍然失败，跳过")
                    continue

            # ── 解析结果行 ────────────────────────────────────────
            while rs.next(): # type: ignore
                raw = rs.get_row_data() # type: ignore
                # raw 顺序：date,code,open,high,...（无 code_name）
                # 在 code（index=1）之后插入 code_name（index=2）
                raw.insert(2, code_name)
                cache_rows.append(raw)

                # 达到刷新阈值，写入 total_list 并清空缓存
                if len(cache_rows) >= flush_size:
                    total_list.append(
                        pd.DataFrame(cache_rows, columns=DAILY_COLUMNS)
                    )
                    cache_rows = []

            t2 = time.time()
            t_total_parse += t2 - t1
            t_total_fetch += t1 - t0
            success_count += 1
            logger.warning(
                f"  [{i + 1}/{total_stocks}] {code} {code_name}: "
                f"fetch={t1-t0:.3f}s  parse={t2-t1:.3f}s"
            )

    # ── 刷入剩余缓存 ────────────────────────────────────────────
    if cache_rows:
        total_list.append(pd.DataFrame(cache_rows, columns=DAILY_COLUMNS))

    if not total_list:
        logger.warning("fetch_daily: 未获取到任何数据，返回空 DataFrame")
        return pd.DataFrame(columns=DAILY_COLUMNS)

    result = pd.concat(total_list, axis=0, ignore_index=True, copy=False) # type: ignore

    logger.info(
        f"fetch_daily 完成: 成功 {success_count} 只，失败 {fail_count} 只，"
        f"共 {len(result)} 条记录，"
        f"累计 fetch={t_total_fetch:.1f}s  parse={t_total_parse:.1f}s"
    )
    return result


# ─────────────────────────────────────────────────────────────────────────────
# 公开接口 3：批量拉取分钟线数据
# ─────────────────────────────────────────────────────────────────────────────

def fetch_minute(
    stock_list: pd.DataFrame,
    start_date: str,
    end_date: str,
    frequency: str = "5",
    adjustflag: str | None = None,
) -> pd.DataFrame:
    """
    批量拉取多只股票的分钟线 K 线数据。

    参数:
        stock_list:  股票列表 DataFrame（至少包含 ``code`` 和 ``code_name`` 列）
        start_date:  起始日期，格式 ``'YYYY-MM-DD'``
        end_date:    截止日期，格式 ``'YYYY-MM-DD'``
        frequency:   K 线周期，支持 ``'5'`` / ``'15'`` / ``'30'`` / ``'60'``（分钟）
        adjustflag:  复权方式；为 None 时使用 config.default_adjustflag。

    返回:
        pd.DataFrame，列顺序参见模块常量 ``MINUTE_COLUMNS``::

            date, time, code, code_name,
            open, high, low, close, volume, amount, adjustflag

    注意:
        - BaoStock 分钟线最多支持近 3 个月数据，请勿跨度过大。
        - 分钟线数据量大，建议分批拉取并及时持久化。

    使用示例::

        stocks = fetch_stock_list().head(10)
        df = fetch_minute(stocks, '2026-04-01', '2026-04-14',
                          frequency='5', adjustflag='1')
        print(df.shape)
    """
    adjustflag = adjustflag

    _valid_freq = {"5", "15", "30", "60"}
    if frequency not in _valid_freq:
        raise ValueError(
            f"frequency 参数非法: '{frequency}'，支持: {_valid_freq}"
        )

    total_stocks = len(stock_list)
    logger.info(
        f"开始批量拉取 {frequency}min 分钟线: {total_stocks} 只股票 "
        f"[{start_date} ~ {end_date}]  adjustflag={adjustflag}"
    )

    total_list: list[pd.DataFrame] = []
    cache_rows: list[list] = []
    flush_size = 10000

    success_count = 0
    fail_count = 0
    t_total_fetch = 0.0
    t_total_parse = 0.0

    with baostock_session():
        for idx, row in stock_list.iterrows():
            code: str = row["code"]
            code_name: str = row.get("code_name", "")

            t0 = time.time()
            rs = _query_with_retry(
                query_fn=lambda : bs.query_history_k_data_plus(
                    code,
                    MINUTE_FIELDS,
                    start_date=start_date,
                    end_date=end_date,
                    frequency=frequency,
                    adjustflag=adjustflag,
                ),
                code=code,
            )
            t1 = time.time()

            if rs is None:
                fail_count += 1
                continue

            while rs.next(): # type: ignore
                raw = rs.get_row_data() # type: ignore
                # raw 顺序：date,time,code,open,...（无 code_name）
                # 在 code（index=2）之后插入 code_name（index=3）
                raw.insert(3, code_name)
                cache_rows.append(raw)

                if len(cache_rows) >= flush_size:
                    total_list.append(
                        pd.DataFrame(cache_rows, columns=MINUTE_COLUMNS)
                    )
                    cache_rows = []
            t2 = time.time()
            t_total_parse += t2 - t1
            t_total_fetch += t1 - t0

            success_count += 1
            logger.debug(
                f"  [{idx+1}/{total_stocks}] {code} {code_name}: " # type: ignore
                f"fetch={t1-t0:.3f}s parse={t2-t1:.3f}s"
            )

    if cache_rows:
        total_list.append(pd.DataFrame(cache_rows, columns=MINUTE_COLUMNS))

    if not total_list:
        logger.warning("fetch_minute: 未获取到任何数据，返回空 DataFrame")
        return pd.DataFrame(columns=MINUTE_COLUMNS)

    result = pd.concat(total_list, axis=0, ignore_index=True, copy=False) # type: ignore

    logger.info(
        f"fetch_minute 完成: 成功 {success_count} 只，失败 {fail_count} 只，"
        f"共 {len(result)} 条记录"
        f"累计 fetch={t_total_fetch:.1f}s  parse={t_total_parse:.1f}s"
    )
    return result


# ─────────────────────────────────────────────────────────────────────────────
# 公开接口 4：单只股票日线（便捷函数，供按需拉取场景使用）
# ─────────────────────────────────────────────────────────────────────────────

def fetch_daily_single(
    code: str,
    code_name: str,
    start_date: str,
    end_date: str,
    adjustflag: str | None = None,
) -> pd.DataFrame:
    """
    拉取单只股票的日线数据（Lazy Pull 场景专用便捷接口）。

    内部复用 :func:`fetch_daily`，传入只含一行的 stock_list。

    参数:
        code:        股票代码，如 ``'sh.600519'``
        code_name:   股票名称，如 ``'贵州茅台'``
        start_date:  起始日期
        end_date:    截止日期
        adjustflag:  复权方式

    返回:
        pd.DataFrame（同 fetch_daily 结构），若无数据则返回空 DataFrame。

    使用示例::

        df = fetch_daily_single('sh.600519', '贵州茅台',
                                '2025-01-01', '2026-04-14',
                                adjustflag='1')
        print(df.tail())
    """
    single_row = pd.DataFrame([{"code": code, "code_name": code_name}])
    return fetch_daily(stock_list=single_row,start_date=start_date,end_date=end_date,adjustflag=adjustflag,)


def fetch_trade_dates(start_date: str, end_date: str) -> pd.DataFrame:
    """
    从 BaoStock 获取指定区间的交易日历。

    参数:
        start_date: "YYYY-MM-DD"
        end_date:   "YYYY-MM-DD"

    返回:
        pd.DataFrame，列: ["date", "is_trading_day"]
        is_trading_day == "1" 表示交易日
    """
    with baostock_session():
        rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
        data_list = []
        while rs.error_code == '0' and rs.next():
            data_list.append(rs.get_row_data())
        df = pd.DataFrame(data_list, columns=rs.fields)
        return df



# ─────────────────────────────────────────────────────────────────────────────
# 模块自测（直接运行此文件时执行）
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    # ── 测试 1：股票列表 ─────────────────────────────────────
    print("\n===== 测试 1: fetch_stock_list =====")
    stocks = fetch_stock_list()
    print(f"A 股在市数量: {len(stocks)}")
    print(stocks.head(3).to_string())

    # ── 测试 2：单只股票日线（贵州茅台，前复权，近 10 天）────
    print("\n===== 测试 2: fetch_daily_single =====")
    df_single = fetch_daily_single(
        code="sh.600519",
        code_name="贵州茅台",
        start_date="2026-04-01",
        end_date="2026-04-14",
        adjustflag="1",
    )
    print(f"贵州茅台日线: {len(df_single)} 条")
    print(df_single.to_string())

    # ── 测试 3：前 5 只股票批量日线 ─────────────────────────
    print("\n===== 测试 3: fetch_daily (前5只) =====")
    df_batch = fetch_daily(
        stock_list=stocks.head(5),
        start_date="2026-04-07",
        end_date="2026-04-14",
        adjustflag="1",
    )
    print(f"批量日线: {len(df_batch)} 条，股票数: {df_batch['code'].nunique()}")
    print(df_batch.head(5).to_string())

    # ── 测试 4：前 3 只股票 5 分钟线 ────────────────────────
    print("\n===== 测试 4: fetch_minute (前3只, 5min) =====")
    df_min = fetch_minute(
        stock_list=stocks.head(3),
        start_date="2026-04-14",
        end_date="2026-04-14",
        frequency="5",
        adjustflag="1",
    )
    print(f"5分钟线: {len(df_min)} 条")
    print(df_min.head(5).to_string())

