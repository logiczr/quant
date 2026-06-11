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
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable

# 在导入 notify 之前加载 .env（notify 模块级别读取环境变量）
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

import duckdb_tools as dt
import data_tools as dta
import notify
import pandas as pd
import strategy as se
import uvicorn
import watchlist_tools as wt
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
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
        self._task_map: dict[str, Callable] = {}
        self._task_meta: dict[str, dict] = {}       # {on_done, on_fail}
        self._event_handlers: dict[str, list[Callable]] = {}

    # ── 启动 / 停止 ──────────────────────────────────────────────────────

    def start(self) -> None:
        self._scheduler.start()
        logger.info("[TaskManager] 已启动")

    def shutdown(self, wait: bool = True) -> None:
        self._scheduler.shutdown(wait=wait)
        logger.info("[TaskManager] 已停止")

    # ── 事件系统 ────────────────────────────────────────────────────────

    def on(self, event: str, handler: Callable) -> None:
        """订阅事件。handler 接收 dict 参数。"""
        self._event_handlers.setdefault(event, []).append(handler)
        logger.info(f"[TaskManager] 订阅事件 {event}")

    def _emit(self, event: str, data: dict | None = None) -> None:
        """发布事件（同步调用所有订阅者）。"""
        handlers = self._event_handlers.get(event, [])
        if not handlers:
            return
        logger.info(f"[TaskManager] 发布事件 {event} ({len(handlers)} 个订阅者)")
        for handler in handlers:
            try:
                handler(data or {})
            except Exception as e:
                logger.error(f"[TaskManager] 事件处理失败 {event}: {e}")

    # ── 手动提交 ─────────────────────────────────────────────────────────

    def submit(self, task_id: str, fn: Callable,
               on_done: str | None = None,
               on_fail: str | None = None,
               **kwargs) -> bool:
        """
        提交一次性任务到后台线程。
        同一 task_id 如果还在运行则跳过。

        参数:
            on_done: 完成后发布的事件名
            on_fail: 失败后发布的事件名
        """
        with self._lock:
            existing = self._tasks.get(task_id)
            if existing and existing.status == TaskStatus.RUNNING:
                logger.warning(f"[TaskManager] {task_id} 仍在运行，跳过")
                return False
            self._tasks[task_id] = TaskRecord(task_id=task_id)
            self._task_map[task_id] = fn
            self._task_meta[task_id] = {"on_done": on_done, "on_fail": on_fail}

        thread = threading.Thread(
            target=self._run, args=(task_id, fn, kwargs),
            daemon=True, name=f"task-{task_id}",
        )
        thread.start()
        logger.info(f"[TaskManager] 提交任务 {task_id}")
        return True

    # ── 定时注册 ─────────────────────────────────────────────────────────

    def schedule(self, task_id: str, fn: Callable, cron: str,
                 on_done: str | None = None,
                 on_fail: str | None = None,
                 **kwargs) -> None:
        """
        注册定时任务。

        参数:
            task_id: 任务标识
            fn:      任务函数
            cron:    "HH:MM" 或 "day_of_week HH:MM" 如 "sun 06:00"
            on_done: 完成后发布的事件名
            on_fail: 失败后发布的事件名
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
        self._task_meta[task_id] = {"on_done": on_done, "on_fail": on_fail}

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
        meta = self._task_meta.get(task_id, {})
        with self._lock:
            self._tasks[task_id] = TaskRecord(task_id=task_id, status=TaskStatus.RUNNING)
        try:
            result = fn(**kwargs)
            with self._lock:
                self._tasks[task_id].status = TaskStatus.DONE
                self._tasks[task_id].result = result
            logger.info(f"[TaskManager] {task_id} 完成")
            if meta.get("on_done"):
                self._emit(meta["on_done"], {"task_id": task_id, "result": result})
        except Exception as e:
            with self._lock:
                self._tasks[task_id].status = TaskStatus.FAILED
                self._tasks[task_id].error = str(e)
            logger.error(f"[TaskManager] {task_id} 失败: {e}")
            if meta.get("on_fail"):
                self._emit(meta["on_fail"], {"task_id": task_id, "error": str(e)})

    def _run_scheduled(self, task_id: str, fn: Callable, kwargs: dict) -> None:
        """APScheduler 回调。从 _task_meta 取 on_done/on_fail 后提交。"""
        meta = self._task_meta.get(task_id, {})
        self.submit(task_id, fn, on_done=meta.get("on_done"), on_fail=meta.get("on_fail"), **kwargs)

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
        """手动触发已注册的任务，可传 kwargs。自动携带 on_done/on_fail。"""
        fn = self._task_map.get(task_id)
        if fn is None:
            return False
        meta = self._task_meta.get(task_id, {})
        return self.submit(task_id, fn, on_done=meta.get("on_done"), on_fail=meta.get("on_fail"), **kwargs)


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

        # 同步更新交易日历（用 BaoStock 交易日历接口）
        try:
            # 取 daily_bar 已有数据的日期范围
            conn = dt.get_read_connection()
            try:
                r = conn.execute("SELECT MIN(date), MAX(date) FROM daily_bar").fetchone()
            finally:
                conn.close()
            if r and r[0]:
                cal_df = dta.fetch_trade_dates(str(r[0]), str(r[1]))
                if not cal_df.empty:
                    # BaoStock 返回列: calendar_date, is_trading_day
                    cal_df["is_trading"] = cal_df["is_trading_day"].apply(lambda x: x == "1")
                    cal_df = cal_df[["calendar_date", "is_trading"]].rename(
                        columns={"calendar_date": "date"}
                    )
                    dt.upsert_trade_calendar(cal_df)
        except Exception as cal_e:
            logger.warning(f"交易日历刷新失败（非致命）: {cal_e}")

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

def _setup_tasks(tm: TaskManager) -> None:
    """注册事件订阅和定时任务。"""
    # 事件链
    tm.on("daily_bar.fetched", lambda _: [
        tm.submit("fetch_index_daily", task_fetch_index_daily,
                  on_done="index_daily.fetched"),
        tm.submit("push_bs_zone_signals", notify.push_signals),
    ])

    # 定时入口
    tm.schedule("refresh_stock_info", task_refresh_stock_info, "08:30",
                on_done="stock_info.refreshed")
    tm.schedule("post_market_fetch", task_post_market_fetch, "17:40",
                on_done="daily_bar.fetched")

@asynccontextmanager
async def lifespan(app):
    global tm
    # ── startup ──
    tm = TaskManager()
    _setup_tasks(tm)
    tm.start()
    # 启动钉钉 Stream 连接（后台线程，用于接收群内指令）
    stream_thread = threading.Thread(target=notify.start_dingtalk_stream, daemon=True)
    stream_thread.start()
    logger.info("[Daemon] TaskManager 已启动，2 个定时入口 + 事件驱动；钉钉 Stream 已启动")
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


# ── Extract 调度状态 ──

@app.get("/extract/events")
def extract_events():
    """查看已注册的事件订阅"""
    result = {}
    for event, handlers in tm._event_handlers.items():
        result[event] = [f"{h.__name__}" for h in handlers]
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 数据查询 API（前后端分离：streamlit 只通过这些接口获取数据）
# ═══════════════════════════════════════════════════════════════════════════════

def _df_to_records(df: pd.DataFrame) -> list:
    """DataFrame → JSON 安全的 list[dict]。（仅用于非 Response 返回的场景）"""
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")


def _df_response(df: pd.DataFrame, key: str = "data") -> Response:
    """DataFrame → 预序列化 JSON Response。

    跳过 to_dict() + FastAPI JSON 编码两步，直接用 pandas C 实现的 to_json()
    序列化后返回 Response，大幅降低大 DataFrame 的序列化耗时。
    """
    if df is None or df.empty:
        return Response(content=f'{{"{key}":[]}}', media_type="application/json")
    records_json = df.to_json(orient="records", date_format="iso")
    return Response(content=f'{{"{key}":{records_json}}}', media_type="application/json")


# ── 数据查询 ──

@app.get("/query/trade_dates")
def query_trade_dates(start_date: str | None = None, end_date: str | None = None):
    """交易日历查询"""
    try:
        dates = dt.get_trade_dates(start_date, end_date)
        return {"data": dates}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/query/is_trade_date")
def query_is_trade_date(date: str):
    """判断某日期是否为交易日"""
    return {"is_trade_date": dt.is_trade_date(date)}


@app.get("/query/nearest_trade_date")
def query_nearest_trade_date(date: str, direction: str = "backward"):
    """找到离指定日期最近的交易日"""
    result = dt.find_nearest_trade_date(date, direction)
    return {"date": result}


@app.post("/query/fill_trade_calendar")
def query_fill_trade_calendar():
    """无感补全日历：从 BaoStock 拉取近 2 年数据写入 trade_calendar"""
    try:
        dt._auto_fill_trade_calendar()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/query/table_stats")
def query_table_stats():
    """侧边栏：数据库核心表统计"""
    try:
        df = dt.table_stats()
        return _df_response(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/query/check_daily_integrity")
def query_check_daily_integrity(target_date: str | None = None, adjustflag: str = "3"):
    """日线数据完整性检查：找出在市但无数据的股票"""
    try:
        df = dt.check_daily_integrity(target_date=target_date, adjustflag=adjustflag)
        return _df_response(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/query/market_overview")
def query_market_overview():
    """大盘概览：最新交易日 + 三大指数 + 涨跌幅分布"""
    try:
        overview = dt.get_market_overview()
        pct = overview["pct_series"]
        return {
            "date": overview["date"],
            "indices": overview["indices"],
            "pct_series": pct.tolist() if hasattr(pct, "tolist") else list(pct),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/query/stock_info")
def query_stock_info(code: str | None = None):
    """股票/指数基础信息，code 为空时返回全部"""
    try:
        df = dt.get_stock_info(code)
        return _df_response(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/query/daily")
def query_daily(
    code: str,
    start_date: str,
    end_date: str,
    adjustflag: str = "3",
    auto_fetch: bool = True,
):
    """个股日线查询（daemon 端自动补拉）"""
    try:
        df = dt.get_daily(
            code=code,
            start_date=start_date,
            end_date=end_date,
            adjustflag=adjustflag,
            auto_fetch=auto_fetch,
        )
        return _df_response(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/query/index_daily")
def query_index_daily(
    code: str,
    start_date: str,
    end_date: str,
    adjustflag: str = "3",
):
    """指数日线查询"""
    try:
        df = dt.get_index_daily(
            code=code,
            start_date=start_date,
            end_date=end_date,
            adjustflag=adjustflag,
        )
        return _df_response(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/query/db_tables")
def query_db_tables():
    """数据库维护：所有表名 + 行数 + 日期范围"""
    try:
        conn = dt.get_read_connection()
        tables_df = conn.execute(
            "SELECT table_name FROM duckdb_tables() ORDER BY table_name"
        ).df()
        conn.close()

        rows_list = []
        conn = dt.get_read_connection()
        for _, tbl_row in tables_df.iterrows():
            tbl_name = tbl_row["table_name"]
            count = 0
            date_range = "—"
            try:
                count = conn.execute(f'SELECT COUNT(*) FROM "{tbl_name}"').fetchone()[0]
            except Exception:
                pass
            try:
                r = conn.execute(
                    f'SELECT MIN(date), MAX(date) FROM "{tbl_name}"'
                ).fetchone()
                if r[0] is not None:
                    date_range = f"{r[0]} ~ {r[1]}"
            except Exception:
                pass
            rows_list.append({
                "table_name": tbl_name,
                "row_count": count,
                "date_range": date_range,
            })
        conn.close()
        return {"data": rows_list}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── 策略查询 ──

@app.get("/strategy/list")
def strategy_list():
    """策略列表"""
    try:
        return {"data": se.list_strategies()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/strategy/info")
def strategy_info(name: str):
    """策略元信息"""
    try:
        info = se.strategy_info(name)
        if info is None:
            raise HTTPException(status_code=404, detail=f"策略不存在: {name}")
        return info
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/strategy/query")
def strategy_query(body: dict):
    """查询策略结果（含自动计算 + 写入）"""
    try:
        name = body.get("name")
        date = body.get("date")
        force_compute = body.get("force_compute", False)
        params = body.get("params", {})
        df = se.query_strategy(name, date=date, force_compute=force_compute, **params)
        return _df_response(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── 自选股 ──

@app.get("/watchlist/groups")
def watchlist_groups():
    """所有自选组合名称列表"""
    try:
        return {"data": wt.list_groups()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/watchlist/group")
def watchlist_create_group(body: dict):
    """创建自选组合"""
    name = body.get("name", "").strip()
    desc = body.get("description", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="组合名称不能为空")
    wt.create_group(name, desc)
    return {"success": True}


@app.delete("/watchlist/group")
def watchlist_delete_group(name: str):
    """删除自选组合"""
    wt.delete_group(name)
    return {"success": True}


@app.get("/watchlist/summary")
def watchlist_summary():
    """所有组合摘要"""
    try:
        df = wt.get_group_summary()
        return _df_response(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/watchlist/stock_codes")
def watchlist_stock_codes(group: str):
    """某组合的股票代码列表"""
    try:
        return {"data": wt.get_stock_codes(group)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/watchlist/group_info")
def watchlist_group_info(group: str):
    """组合完整信息"""
    try:
        return wt.get_group_info(group)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/watchlist/group_overview")
def watchlist_group_overview(group: str):
    """组合内个股最新行情"""
    try:
        df = wt.get_group_overview(group)
        return _df_response(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/watchlist/add_stock")
def watchlist_add_stock(body: dict):
    """向组合添加股票"""
    wt.add_stock(body.get("group", ""), body.get("code", ""), body.get("note", ""))
    return {"success": True}


@app.post("/watchlist/remove_stock")
def watchlist_remove_stock(body: dict):
    """从组合移除股票"""
    wt.remove_stock(body.get("group", ""), body.get("code", ""))
    return {"success": True}


@app.get("/watchlist/calc_index")
def watchlist_calc_index(group: str, start_date: str, end_date: str):
    """计算组合等权指数"""
    try:
        df = wt.calc_group_index(group, start_date, end_date)
        return _df_response(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/watchlist/calc_valuation")
def watchlist_calc_valuation(group: str):
    """组合估值统计"""
    try:
        return wt.calc_group_valuation(group)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── 推送 ──

@app.post("/notify/push_signals")
def notify_push_signals(body: dict | None = None):
    """推送 BS 区间信号（钉钉互动卡片）"""
    try:
        date = (body or {}).get("date")
        result = notify.push_signals(date)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── 入口 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    uvicorn.run(app, host="0.0.0.0", port=8502, log_level="info")
