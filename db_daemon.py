"""
db_daemon.py — 数据库守护进程

独立常驻进程，职责：
  - 运行 APScheduler 定时任务（数据拉取、指标计算等）
  - 提供 FastAPI HTTP 接口，供 Streamlit 查询状态和手动触发

启动方式：
    python db_daemon.py
    # 默认监听 127.0.0.1:8502

架构：

    ┌──────────────────┐    HTTP     ┌──────────────────┐
    │  Streamlit 前端    │  ────→     │  db_daemon 守护    │
    │  (port 8501)      │            │  (port 8502)      │
    │                   │  ←────     │                   │
    │  展示 + 触发       │   JSON     │  调度器 + DB写入    │
    └──────────────────┘            └──────────────────┘

接口：
    GET  /health             健康检查
    GET  /status             所有任务当前状态
    GET  /jobs               已注册的定时任务列表
    POST /run_now/{task_id}  手动触发指定任务

依赖：
    pip install fastapi uvicorn apscheduler baostock duckdb
"""

from __future__ import annotations

import atexit
import logging

import uvicorn
from fastapi import FastAPI, HTTPException

from scheduler import UpdaterScheduler

logger = logging.getLogger("db_daemon")

# ─── 调度器（模块级单例，import 时即启动） ───

sched = UpdaterScheduler()
sched.start()
atexit.register(sched.shutdown)
logger.info("[Daemon] 调度器已启动")

app = FastAPI(title="Stock DB Daemon")


# ─── 接口 ───

@app.get("/health")
def health():
    """健康检查。"""
    return {"status": "ok"}


@app.get("/status")
def get_status():
    """所有任务当前状态 {task_id: status}。"""
    return sched.status


@app.get("/jobs")
def get_jobs():
    """已注册的定时任务列表。"""
    return sched.get_jobs()


@app.post("/run_now/{task_id}")
def run_now(task_id: str):
    """手动触发指定任务（后台线程执行，立即返回）。"""
    ok = sched.run_now(task_id)
    if not ok:
        raise HTTPException(status_code=409, detail=f"任务 {task_id} 不存在或已在运行")
    return {"success": True, "task_id": task_id}


# ─── 入口 ───

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    uvicorn.run(app, host="127.0.0.1", port=8502, log_level="info")
