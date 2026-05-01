"""
db_daemon.py — 数据库守护进程（统一任务管理 + HTTP API）

职责：
  - 后台执行任务（手动提交 + 定时调度）
  - 提供 FastAPI HTTP 接口，供前端查询状态和触发任务

架构：

    ┌──────────────────┐    HTTP     ┌──────────────────────┐
    │  Streamlit 前端    │  ────→     │  db_daemon            │
    │  (port 8501)      │            │  (port 8502)          │
    │                   │  ←────     │                       │
    │  展示 + 触发       │   JSON     │  TaskManager          │
    └──────────────────┘            │  ├── submit() 手动提交  │
                                    │  ├── schedule() 定时注册│
                                    │  └── status/ 查状态     │
                                    └──────────────────────┘

接口：
    GET  /health             健康检查
    GET  /status             所有任务当前状态
    GET  /jobs               已注册的定时任务列表
    POST /run_now/{task_id}  手动触发指定任务
    GET  /result/{task_id}   获取任务结果

启动：
    python db_daemon.py
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable

import duckdb_tools as dt
import pandas as pd
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

logger = logging.getLogger("db_daemon")


# ═══════════════════════════════════════════════════════════════════════════════
# 任务状态
# ═══════════════════════════════════════════════════════════════════════════════

class TaskStatus(str, Enum):
    PENDING  = "PENDING"
    RUNNING  = "RUNNING"
    DONE     = "DONE"
    FAILED   = "FAILED"
    SKIPPED  = "SKIPPED"


@dataclass
class TaskRecord:
    task_id: str
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: str | None = None


# ═══════════════════════════════════════════════════════════════════════════════
# TaskManager — 统一任务管理
# ═══════════════════════════════════════════════════════════════════════════════

class TaskManager:
    """
    统一后台任务管理器。

    - submit()  提交一次性任务，后台线程执行
    - schedule() 注册定时任务，APScheduler 调度
    - status / result / all_tasks  查询
    """

    def __init__(self) -> None:
        self._tasks: dict[str, TaskRecord] = {}
        self._lock = threading.Lock()
        self._scheduler = BackgroundScheduler(daemon=True)
        self._task_map: dict[str, Callable] = {}  # task_id → fn（手动触发用）

    # ── 启动 / 停止 ──────────────────────────────────────────────────────

    def start(self) -> None:
        self._scheduler.start()
        logger.info("[TaskManager] 已启动")

    def shutdown(self, wait: bool = True) -> None:
        self._scheduler.shutdown(wait=wait)
        logger.info("[TaskManager] 已停止")

    # ── 手动提交 ─────────────────────────────────────────────────────────

    def submit(self, task_id: str, fn: Callable, **kwargs) -> bool:
        """
        提交一次性任务到后台线程。
        同一 task_id 如果还在运行则跳过。
        """
        with self._lock:
            existing = self._tasks.get(task_id)
            if existing and existing.status == TaskStatus.RUNNING:
                logger.warning(f"[TaskManager] {task_id} 仍在运行，跳过")
                return False
            self._tasks[task_id] = TaskRecord(task_id=task_id)
            self._task_map[task_id] = fn

        thread = threading.Thread(
            target=self._run, args=(task_id, fn, kwargs),
            daemon=True, name=f"task-{task_id}",
        )
        thread.start()
        logger.info(f"[TaskManager] 提交任务 {task_id}")
        return True

    # ── 定时注册 ─────────────────────────────────────────────────────────

    def schedule(self, task_id: str, fn: Callable, cron: str, **kwargs) -> None:
        """
        注册定时任务。

        参数:
            task_id: 任务标识
            fn:      任务函数
            cron:    "HH:MM" 或 "day_of_week HH:MM" 如 "sun 06:00"
        """
        parts = cron.split()
        if len(parts) == 1:
            hour, minute = parts[0].split(":")
            trigger = CronTrigger(hour=int(hour), minute=int(minute))
        else:
            dow, time_str = parts
            hour, minute = time_str.split(":")
            trigger = CronTrigger(day_of_week=dow, hour=int(hour), minute=int(minute))

        self._task_map[task_id] = fn

        self._scheduler.add_job(
            self._run_scheduled,
            trigger=trigger,
            args=(task_id, fn, kwargs),
            id=task_id,
            replace_existing=True,
        )
        logger.info(f"[TaskManager] 注册定时任务 {task_id}: {cron}")

    # ── 内部执行 ─────────────────────────────────────────────────────────

    def _run(self, task_id: str, fn: Callable, kwargs: dict) -> None:
        with self._lock:
            self._tasks[task_id] = TaskRecord(task_id=task_id, status=TaskStatus.RUNNING)
        try:
            result = fn(**kwargs)
            with self._lock:
                rec = self._tasks[task_id]
                rec.status = TaskStatus.DONE
                rec.result = result
            logger.info(f"[TaskManager] {task_id} 完成")
        except Exception as e:
            with self._lock:
                rec = self._tasks[task_id]
                rec.status = TaskStatus.FAILED
                rec.error = str(e)
            logger.error(f"[TaskManager] {task_id} 失败: {e}")

    def _run_scheduled(self, task_id: str, fn: Callable, kwargs: dict) -> None:
        """APScheduler 回调，复用 _run。"""
        self._run(task_id, fn, kwargs)

    # ── 查询 ─────────────────────────────────────────────────────────────

    def status(self, task_id: str) -> TaskStatus | None:
        with self._lock:
            rec = self._tasks.get(task_id)
            return rec.status if rec else None

    def result(self, task_id: str) -> Any:
        with self._lock:
            rec = self._tasks.get(task_id)
            if rec and rec.status == TaskStatus.DONE:
                return rec.result
            return None

    def error(self, task_id: str) -> str | None:
        with self._lock:
            rec = self._tasks.get(task_id)
            if rec and rec.status == TaskStatus.FAILED:
                return rec.error
            return None

    def all_tasks(self) -> dict[str, str]:
        with self._lock:
            return {tid: rec.status.value for tid, rec in self._tasks.items()}

    def get_jobs(self) -> list[dict]:
        jobs = []
        for job in self._scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "next_run": str(job.next_run_time) if job.next_run_time else "未排期",
                "trigger": str(job.trigger),
            })
        return jobs

    def run_now(self, task_id: str, **kwargs) -> bool:
        """手动触发已注册的任务，可传 kwargs。"""
        fn = self._task_map.get(task_id)
        if fn is None:
            return False
        return self.submit(task_id, fn, **kwargs)


# ═══════════════════════════════════════════════════════════════════════════════
# 具体任务函数
# ═══════════════════════════════════════════════════════════════════════════════

_LAST_FETCH_PATH = Path(__file__).parent / "last_fetch.json"


def _load_last_fetch() -> dict:
    if not _LAST_FETCH_PATH.exists():
        return {}
    try:
        return json.loads(_LAST_FETCH_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, KeyError):
        return {}


def _save_last_fetch(data: dict) -> None:
    _LAST_FETCH_PATH.parent.mkdir(parents=True, exist_ok=True)
    data["updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _LAST_FETCH_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _batch_fetch_and_save(
    stock_list: pd.DataFrame,
    start_date: str,
    end_date: str,
    adjustflag: str = "3",
) -> pd.DataFrame:
    """批量拉取日线数据并写入 DuckDB（一次 login，批量写入）。"""
    from data_tools import fetch_daily
    try:
        fetched = fetch_daily(
            stock_list=stock_list,
            start_date=start_date,
            end_date=end_date,
            adjustflag=adjustflag,
        )
    except Exception as e:
        logger.error(f"fetch_daily 批量拉取失败: {e}")
        return pd.DataFrame()

    if fetched.empty:
        return pd.DataFrame()

    try:
        dt.insert_daily(fetched)
        logger.info(f"写入 {len(fetched)} 条日线数据")
    except Exception as e:
        logger.error(f"insert_daily 写入失败: {e}")

    return fetched




# ── 08:30 刷新股票列表 ──

def task_refresh_stock_info() -> dict:
    logger.info("开始刷新 stock_info ...")
    try:
        stock_count = dt.upsert_stock_info()
        index_count = dt.upsert_index_info()
        result = {"stock_count": stock_count, "index_count": index_count, "status": "DONE"}
        logger.info(f"stock_info 刷新完毕，{stock_count} 只股票 + {index_count} 只指数")
    except Exception as e:
        result = {"status": "FAILED", "error": str(e)}
        logger.error(f"stock_info 刷新失败: {e}")
    return result



# ── 17:00 收盘批次拉取 ──

def task_post_market_fetch(
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """拉取全市场股票日线数据（不含指数，指数由 task_fetch_index_daily 单独拉取）。"""
    logger.info("收盘拉取开始 ...")

    today = datetime.today().strftime("%Y-%m-%d")
    start_date = start_date or today
    end_date = end_date or today

    all_info = dt.get_stock_info()
    # 只拉股票（type != '2'），指数由独立任务处理
    if "type" in all_info.columns:
        stocks = all_info[all_info["type"] != "2"][["code", "code_name"]]
    else:
        stocks = all_info[["code", "code_name"]]
    total = len(stocks)
    logger.info(f"共 {total} 只股票，拉取 [{start_date} ~ {end_date}] 日线")

    fetched = _batch_fetch_and_save(stocks, start_date=start_date, end_date=end_date)

    fetched_codes = set(fetched["code"].unique()) if not fetched.empty else set()
    success_count = len(fetched_codes)
    failed_codes = [c for c in stocks["code"].tolist() if c not in fetched_codes]

    result = {
        "status": "DONE" if not failed_codes else "PARTIAL",
        "start_date": start_date,
        "end_date": end_date,
        "stock_count": total,
        "success_count": success_count,
        "failed_codes": failed_codes,
    }

    _save_last_fetch(result)
    logger.info(f"收盘拉取完毕: 成功 {success_count}/{total}")
    return result


# ── 17:30 指数日线拉取 ──

def task_fetch_index_daily(
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """拉取全市场指数日线数据，入库。"""
    from data_tools import fetch_index_daily, fetch_index_list
    logger.info("指数日线拉取开始 ...")

    today = datetime.today().strftime("%Y-%m-%d")
    start_date = start_date or today
    end_date = end_date or today


    # 从 BaoStock 拉取全市场指数列表
    index_list = fetch_index_list()
    if index_list.empty:
        logger.warning("指数列表为空，跳过拉取")
        return {"status": "SKIPPED", "reason": "指数列表为空"}

    logger.info(f"共 {len(index_list)} 只指数，拉取 [{start_date} ~ {end_date}] 日线")

    try:
        fetched = fetch_index_daily(
            index_list=index_list,
            start_date=start_date,
            end_date=end_date,
            adjustflag="3",
        )
    except Exception as e:
        logger.error(f"指数日线拉取失败: {e}")
        return {"status": "FAILED", "error": str(e)}

    if not fetched.empty:
        try:
            dt.insert_index_daily(fetched)
            logger.info(f"写入 {len(fetched)} 条指数日线数据")
        except Exception as e:
            logger.error(f"指数日线写入失败: {e}")
            return {"status": "FAILED", "error": str(e)}

    return {
        "status": "DONE",
        "start_date": start_date,
        "end_date": end_date,
        "index_count": len(index_list),
        "rows": len(fetched),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 初始化：创建 TaskManager + 注册定时任务 + FastAPI
# ═══════════════════════════════════════════════════════════════════════════════



tm: TaskManager | None = None

@asynccontextmanager
async def lifespan(app):
    global tm
    # ── startup ──
    tm = TaskManager()
    tm.schedule("refresh_stock_info", task_refresh_stock_info, "08:30")
    tm.schedule("post_market_fetch", task_post_market_fetch, "17:00")
    tm.schedule("fetch_index_daily", task_fetch_index_daily, "17:30")
    tm.start()
    logger.info("[Daemon] TaskManager 已启动，3 个定时任务已注册")
    yield
    # ── shutdown ──
    if tm:
        tm.shutdown()


app = FastAPI(title="Stock DB Daemon", lifespan=lifespan)
# ── HTTP 接口 ─────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/status")
def get_status():
    return tm.all_tasks()


@app.get("/jobs")
def get_jobs():
    return tm.get_jobs()


@app.post("/run_now/{task_id}")
def run_now(task_id: str, params: dict | None = None):
    ok = tm.run_now(task_id, **(params or {}))
    if not ok:
        raise HTTPException(status_code=409, detail=f"任务 {task_id} 不存在或已在运行")
    return {"success": True, "task_id": task_id}


@app.get("/result/{task_id}")
def get_result(task_id: str):
    r = tm.result(task_id)
    if r is None:
        err = tm.error(task_id)
        if err:
            return {"task_id": task_id, "status": "FAILED", "error": err}
        return {"task_id": task_id, "status": "pending", "result": None}
    return {"task_id": task_id, "status": "DONE", "result": r}

@app.get("/last_fetch")
def get_last_fetch():
    return _load_last_fetch()

# ── 入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    uvicorn.run(app, host="127.0.0.1", port=8502, log_level="info")
