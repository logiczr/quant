"""
daemon_client.py — 守护进程 HTTP 客户端

Streamlit 侧通过此模块与 db_daemon 通信，实现前后端分离。
所有数据库访问都通过 daemon API，streamlit 不直连 DuckDB。
"""

from __future__ import annotations

import logging
import os

import pandas as pd
import requests

logger = logging.getLogger("daemon_client")

DAEMON_URL = os.environ.get("DAEMON_URL", "http://127.0.0.1:8502")
TIMEOUT = 10       # 普通查询
TIMEOUT_SLOW = 60  # 可能触发补拉/计算的查询


# ═══════════════════════════════════════════════════════════════════════════════
# 守护进程管理
# ═══════════════════════════════════════════════════════════════════════════════

def is_alive() -> bool:
    """守护进程是否在线。"""
    try:
        return requests.get(f"{DAEMON_URL}/health", timeout=TIMEOUT).ok
    except Exception:
        return False


def get_status() -> dict:
    """获取所有任务状态 {task_id: status}。"""
    try:
        return requests.get(f"{DAEMON_URL}/status", timeout=TIMEOUT).json()
    except Exception:
        return {}


def get_jobs() -> list:
    """获取已注册的定时任务列表。"""
    try:
        return requests.get(f"{DAEMON_URL}/jobs", timeout=TIMEOUT).json()
    except Exception:
        return []


def run_now(task_id: str, params: dict | None = None) -> dict:
    """手动触发指定任务，可传 params 字典作为任务函数的 kwargs。"""
    try:
        resp = requests.post(
            f"{DAEMON_URL}/run_now/{task_id}",
            json=params,
            timeout=TIMEOUT,
        )
        return resp.json()
    except Exception as e:
        logger.warning(f"[Client] 触发任务失败: {task_id} - {e}")
        return {"success": False, "error": str(e)}


def get_last_fetch() -> dict:
    """获取最近一次收盘拉取的结果。"""
    try:
        return requests.get(f"{DAEMON_URL}/last_fetch", timeout=TIMEOUT).json()
    except Exception as e:
        logger.warning(f"[Client] 获取 last_fetch 失败: {e}")
        return {}


# ═══════════════════════════════════════════════════════════════════════════════
# 数据查询
# ═══════════════════════════════════════════════════════════════════════════════

def table_stats() -> pd.DataFrame:
    """侧边栏：数据库核心表统计"""
    try:
        resp = requests.get(f"{DAEMON_URL}/query/table_stats", timeout=TIMEOUT)
        resp.raise_for_status()
        return pd.DataFrame(resp.json()["data"])
    except Exception as e:
        logger.warning(f"[Client] table_stats 失败: {e}")
        return pd.DataFrame()


def check_daily_integrity(
    target_date: str | None = None, adjustflag: str = "3"
) -> pd.DataFrame:
    """日线数据完整性检查：找出在市但无数据的股票"""
    try:
        params = {}
        if target_date:
            params["target_date"] = target_date
        params["adjustflag"] = adjustflag
        resp = requests.get(
            f"{DAEMON_URL}/query/check_daily_integrity",
            params=params,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        return pd.DataFrame(resp.json()["data"])
    except Exception as e:
        logger.warning(f"[Client] check_daily_integrity 失败: {e}")
        return pd.DataFrame()


# ─── 交易日历 ───

def get_trade_dates(start_date: str | None = None, end_date: str | None = None) -> list[str]:
    """交易日历查询"""
    try:
        params = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        resp = requests.get(f"{DAEMON_URL}/query/trade_dates", params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()["data"]
    except Exception as e:
        logger.warning(f"[Client] get_trade_dates 失败: {e}")
        return []


def is_trade_date(date: str) -> bool:
    """判断某日期是否为交易日"""
    try:
        resp = requests.get(f"{DAEMON_URL}/query/is_trade_date", params={"date": date}, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json().get("is_trade_date", False)
    except Exception as e:
        logger.warning(f"[Client] is_trade_date 失败: {e}")
        return False


def find_nearest_trade_date(date: str, direction: str = "backward") -> str | None:
    """找到离指定日期最近的交易日"""
    try:
        resp = requests.get(
            f"{DAEMON_URL}/query/nearest_trade_date",
            params={"date": date, "direction": direction},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("date")
    except Exception as e:
        logger.warning(f"[Client] find_nearest_trade_date 失败: {e}")
        return None


def trigger_fill_trade_calendar() -> None:
    """触发后端无感补全日历（近 2 年）"""
    try:
        resp = requests.post(
            f"{DAEMON_URL}/query/fill_trade_calendar",
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"[Client] trigger_fill_trade_calendar 失败（非致命）: {e}")


def get_market_overview() -> dict:
    """大盘概览：最新交易日 + 三大指数 + 涨跌幅分布"""
    try:
        resp = requests.get(f"{DAEMON_URL}/query/market_overview", timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        data["pct_series"] = pd.Series(data["pct_series"])
        return data
    except Exception as e:
        logger.warning(f"[Client] get_market_overview 失败: {e}")
        return {"date": None, "indices": {}, "pct_series": pd.Series(dtype=float)}


def get_stock_info(code: str | None = None) -> pd.DataFrame:
    """股票/指数基础信息，code 为空时返回全部"""
    try:
        params = {}
        if code:
            params["code"] = code
        resp = requests.get(f"{DAEMON_URL}/query/stock_info", params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return pd.DataFrame(resp.json()["data"])
    except Exception as e:
        logger.warning(f"[Client] get_stock_info 失败: {e}")
        return pd.DataFrame()


def get_daily(
    code: str,
    start_date: str,
    end_date: str,
    adjustflag: str = "3",
    auto_fetch: bool = True,
) -> pd.DataFrame:
    """个股日线查询（daemon 端自动补拉）"""
    try:
        resp = requests.get(f"{DAEMON_URL}/query/daily", params={
            "code": code,
            "start_date": start_date,
            "end_date": end_date,
            "adjustflag": adjustflag,
            "auto_fetch": auto_fetch,
        }, timeout=TIMEOUT_SLOW)
        resp.raise_for_status()
        return pd.DataFrame(resp.json()["data"])
    except Exception as e:
        logger.warning(f"[Client] get_daily 失败: {e}")
        return pd.DataFrame()


def get_index_daily(
    code: str,
    start_date: str,
    end_date: str,
    adjustflag: str = "3",
) -> pd.DataFrame:
    """指数日线查询"""
    try:
        resp = requests.get(f"{DAEMON_URL}/query/index_daily", params={
            "code": code,
            "start_date": start_date,
            "end_date": end_date,
            "adjustflag": adjustflag,
        }, timeout=TIMEOUT)
        resp.raise_for_status()
        return pd.DataFrame(resp.json()["data"])
    except Exception as e:
        logger.warning(f"[Client] get_index_daily 失败: {e}")
        return pd.DataFrame()


def get_db_tables() -> list[dict]:
    """数据库维护：所有表名 + 行数 + 日期范围"""
    try:
        resp = requests.get(f"{DAEMON_URL}/query/db_tables", timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()["data"]
    except Exception as e:
        logger.warning(f"[Client] get_db_tables 失败: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# 策略查询
# ═══════════════════════════════════════════════════════════════════════════════

def list_strategies() -> list[dict]:
    """策略列表"""
    try:
        resp = requests.get(f"{DAEMON_URL}/strategy/list", timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()["data"]
    except Exception as e:
        logger.warning(f"[Client] list_strategies 失败: {e}")
        return []


def strategy_info(name: str) -> dict | None:
    """策略元信息"""
    try:
        resp = requests.get(f"{DAEMON_URL}/strategy/info", params={"name": name}, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"[Client] strategy_info 失败: {e}")
        return None


def query_strategy(name: str, date: str, force_compute: bool = False, **params) -> pd.DataFrame:
    """查询策略结果（含自动计算 + 写入）"""
    try:
        resp = requests.post(f"{DAEMON_URL}/strategy/query", json={
            "name": name,
            "date": date,
            "force_compute": force_compute,
            "params": params,
        }, timeout=TIMEOUT_SLOW)
        resp.raise_for_status()
        return pd.DataFrame(resp.json()["data"])
    except Exception as e:
        logger.warning(f"[Client] query_strategy 失败: {e}")
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════════
# 自选股
# ═══════════════════════════════════════════════════════════════════════════════

def list_groups() -> list[str]:
    """所有自选组合名称列表"""
    try:
        resp = requests.get(f"{DAEMON_URL}/watchlist/groups", timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()["data"]
    except Exception as e:
        logger.warning(f"[Client] list_groups 失败: {e}")
        return []


def create_group(name: str, description: str = "") -> bool:
    """创建自选组合"""
    try:
        resp = requests.post(
            f"{DAEMON_URL}/watchlist/group",
            json={"name": name, "description": description},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("success", False)
    except Exception as e:
        logger.warning(f"[Client] create_group 失败: {e}")
        return False


def delete_group(name: str) -> bool:
    """删除自选组合"""
    try:
        resp = requests.delete(
            f"{DAEMON_URL}/watchlist/group",
            params={"name": name},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("success", False)
    except Exception as e:
        logger.warning(f"[Client] delete_group 失败: {e}")
        return False


def get_group_summary() -> pd.DataFrame:
    """所有组合摘要"""
    try:
        resp = requests.get(f"{DAEMON_URL}/watchlist/summary", timeout=TIMEOUT)
        resp.raise_for_status()
        return pd.DataFrame(resp.json()["data"])
    except Exception as e:
        logger.warning(f"[Client] get_group_summary 失败: {e}")
        return pd.DataFrame()


def get_stock_codes(group: str) -> list[str]:
    """某组合的股票代码列表"""
    try:
        resp = requests.get(
            f"{DAEMON_URL}/watchlist/stock_codes",
            params={"group": group},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()["data"]
    except Exception as e:
        logger.warning(f"[Client] get_stock_codes 失败: {e}")
        return []


def get_group_info(group: str) -> dict:
    """组合完整信息"""
    try:
        resp = requests.get(
            f"{DAEMON_URL}/watchlist/group_info",
            params={"group": group},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"[Client] get_group_info 失败: {e}")
        return {}


def get_group_overview(group: str) -> pd.DataFrame:
    """组合内个股最新行情"""
    try:
        resp = requests.get(
            f"{DAEMON_URL}/watchlist/group_overview",
            params={"group": group},
            timeout=TIMEOUT_SLOW,
        )
        resp.raise_for_status()
        return pd.DataFrame(resp.json()["data"])
    except Exception as e:
        logger.warning(f"[Client] get_group_overview 失败: {e}")
        return pd.DataFrame()


def add_stock(group: str, code: str, note: str = "") -> bool:
    """向组合添加股票"""
    try:
        resp = requests.post(
            f"{DAEMON_URL}/watchlist/add_stock",
            json={"group": group, "code": code, "note": note},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("success", False)
    except Exception as e:
        logger.warning(f"[Client] add_stock 失败: {e}")
        return False


def remove_stock(group: str, code: str) -> bool:
    """从组合移除股票"""
    try:
        resp = requests.post(
            f"{DAEMON_URL}/watchlist/remove_stock",
            json={"group": group, "code": code},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("success", False)
    except Exception as e:
        logger.warning(f"[Client] remove_stock 失败: {e}")
        return False


def calc_group_index(group: str, start_date: str, end_date: str) -> pd.DataFrame:
    """计算组合等权指数"""
    try:
        resp = requests.get(
            f"{DAEMON_URL}/watchlist/calc_index",
            params={"group": group, "start_date": start_date, "end_date": end_date},
            timeout=TIMEOUT_SLOW,
        )
        resp.raise_for_status()
        return pd.DataFrame(resp.json()["data"])
    except Exception as e:
        logger.warning(f"[Client] calc_group_index 失败: {e}")
        return pd.DataFrame()


def calc_group_valuation(group: str) -> dict:
    """组合估值统计"""
    try:
        resp = requests.get(
            f"{DAEMON_URL}/watchlist/calc_valuation",
            params={"group": group},
            timeout=TIMEOUT_SLOW,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"[Client] calc_group_valuation 失败: {e}")
        return {}


def push_signals(date: str | None = None) -> dict:
    """推送 BS 区间信号到钉钉群。"""
    try:
        body = {}
        if date:
            body["date"] = date
        resp = requests.post(
            f"{DAEMON_URL}/notify/push_signals",
            json=body,
            timeout=TIMEOUT_SLOW,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"[Client] push_signals 失败: {e}")
        return {"status": "FAILED", "error": str(e)}
