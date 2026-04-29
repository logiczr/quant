"""
daemon_client.py — 守护进程 HTTP 客户端

Streamlit 侧通过此模块与 db_daemon 通信。
所有函数都是轻量 HTTP 调用，不会阻塞前端。
"""

from __future__ import annotations

import logging

import requests

logger = logging.getLogger("daemon_client")

DAEMON_URL = "http://127.0.0.1:8502"
TIMEOUT = 3


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


def run_now(task_id: str) -> dict:
    """手动触发指定任务。"""
    try:
        resp = requests.post(f"{DAEMON_URL}/run_now/{task_id}", timeout=TIMEOUT)
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
