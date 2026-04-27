from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import threading
import json
from datetime import datetime
from pathlib import Path
import logging
import duckdb_tools as dt
import data_tools as datat


_LAST_FETCH_PATH = Path(__file__).parent / "last_fetch.json"
logger = logging.getLogger("scheduler")
logger.setLevel(logging.WARNING)

def _load_last_fetch() -> dict:
    """加载 last_fetch.json。"""
    if not _LAST_FETCH_PATH.exists():
        return {}
    try:
        return json.loads(_LAST_FETCH_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, KeyError):
        return {}

def _save_last_fetch(data: dict) -> None:
    """保存 last_fetch.json。"""
    _LAST_FETCH_PATH.parent.mkdir(parents=True, exist_ok=True)
    data["updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _LAST_FETCH_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

def task_refresh_stock_info() -> dict:
    """
    08:30 — 全量刷新 stock_info 表。

    返回:
        {"stock_count": N, "status": "DONE"}
    """
    logger.info("[Scheduler] 开始刷新 stock_info ...")
    try:
        count = dt.upsert_stock_info(None)
        result = {"stock_count": count, "status": "DONE"}
        logger.info(f"[Scheduler] stock_info 刷新完毕，{count} 只股票")
    except Exception as e:
        result = {"status": "FAILED", "error": str(e)}
        logger.error(f"[Scheduler] stock_info 刷新失败: {e}")
    return result

def task_post_market_fetch() -> dict:
    """
    17:00 — 收盘批次拉取。

    遍历全市场股票，调用 get_daily 触发 Lazy Pull 补拉今日数据。
    最多 3 轮重试（间隔 5/10/15 分钟）。
    """
    logger.info("[Scheduler] 收盘批次拉取开始 ...")

    today = datetime.today().strftime("%Y-%m-%d")
    stocks = dt.get_stock_info()[["code"]]
    total = len(stocks)
    failed_codes = []
    success_count = 0

    logger.info(f"[Scheduler] 共 {total} 只股票待拉取")

    df = datat.fetch_daily(stocks,start_date='2026-04-20',end_date=today,adjustflag='3')
    #注意此处的date起始日期需要在本层实现
    dt.insert_daily(df)
    success_count = len(df)
    

    result = {
        "status": "DONE" if not failed_codes else "PARTIAL",
        "last_success": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stock_count": total,
        "success_count": success_count,
        "failed_codes": failed_codes,
        "retry_count": 0,
    }

    # 有失败的安排重试
    if failed_codes:
        logger.warning(f"[Scheduler] {len(failed_codes)} 只股票拉取失败，将重试")
        result["retry_count"] = 1
        result = _retry_fetch(failed_codes, today, result)

    _save_last_fetch(result)
    logger.info(
        f"[Scheduler] 收盘拉取完毕: 成功 {success_count}/{total}，"
        f"失败 {len(result.get('failed_codes', []))}"
    )
    return result

def _retry_fetch(failed_codes: list[str], today: str, result: dict) -> dict:
    """
    重试失败股票（同步版本，在调度线程中直接执行）。
    最多 3 轮，间隔 5/10/15 分钟。
    """
    import time

    max_retries = 3
    intervals = [300, 600, 900]  # 5分钟, 10分钟, 15分钟

    for retry in range(1, max_retries + 1):
        logger.info(f"[Scheduler] 第 {retry} 轮重试，等待 {intervals[retry-1]//60} 分钟 ...")
        time.sleep(intervals[retry - 1])

        still_failed = []
        stocks = dt.get_stock_info()[["code", "code_name"]]

        for code in failed_codes:
            row = stocks[stocks["code"] == code]
            name = row["code_name"].values[0] if not row.empty else ""
            try:
                df = dt.get_daily(code=code, start_date=today,
                                  end_date=today, adjustflag="3", auto_fetch=True)
                if not df.empty:
                    logger.info(f"[Scheduler] 重试成功: {code}")
                else:
                    still_failed.append(code)
            except Exception as e:
                still_failed.append(code)
                logger.error(f"[Scheduler] 重试失败: {code} - {e}")

        result["retry_count"] = retry
        failed_codes = still_failed

        if not failed_codes:
            result["status"] = "DONE"
            break

    result["failed_codes"] = failed_codes
    return result


class UpdaterScheduler:
    """
    基于 APScheduler 的定时任务调度器。

    - start() 注册所有定时任务并启动后台线程，立即返回
    - 所有任务在独立线程执行，不阻塞 Streamlit
    - status_summary() 供前端轮询展示状态
    """

    def __init__(self) -> None:
        self._scheduler = BackgroundScheduler(daemon=True)
        self._status: dict[str, str] = {}
        self._lock = threading.Lock()

    def start(self) -> None:
        """注册所有定时任务并启动调度器。"""
        # 08:30 刷新股票列表
        self._scheduler.add_job(
            self._wrap_task("refresh_stock_info", task_refresh_stock_info),
            CronTrigger(hour=8, minute=30),
            id="refresh_stock_info",
            replace_existing=True,
        )

        self._scheduler.add_job(
            self._wrap_task("task_post_market_fetch", task_post_market_fetch),
            CronTrigger(hour=18, minute=00),
            id="market_fetch",
            replace_existing=True,
        )

        self._scheduler.start()
        logger.warning("[Scheduler] 调度器已启动，2 个定时任务已注册")

    def shutdown(self, wait: bool = True) -> None:
        """停止调度器。"""
        self._scheduler.shutdown(wait=wait)
        logger.info("[Scheduler] 调度器已停止")

    def _wrap_task(self, task_id: str, fn: callable) -> callable:
        """
        包装任务函数：执行前设置状态为 RUNNING，执行后设置状态。
        """
        def _run():
            with self._lock:
                self._status[task_id] = "RUNNING"
            try:
                result = fn()
                with self._lock:
                    self._status[task_id] = result.get("status", "DONE")
            except Exception as e:
                with self._lock:
                    self._status[task_id] = "FAILED"
                logger.error(f"[Scheduler] 任务 {task_id} 异常: {e}")

        return _run

    # ── 状态查询 ──────────────────────────────────────────────────────────

    @property
    def status(self) -> dict[str, str]:
        """所有任务状态摘要 {task_id: status}。"""
        with self._lock:
            return dict(self._status)

    def status_summary(self) -> str:
        """可读的状态摘要，供 Streamlit 展示。"""
        with self._lock:
            lines = []
            for tid, st in self._status.items():
                lines.append(f"{tid}: {st}")
            return "\n".join(lines) if lines else "暂无任务执行记录"

    def get_jobs(self) -> list[dict]:
        """获取所有已注册的定时任务信息。"""
        jobs = []
        for job in self._scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "next_run": str(job.next_run_time) if job.next_run_time else "未排期",
                "trigger": str(job.trigger),
            })
        return jobs

    # ── 手动触发 ──────────────────────────────────────────────────────────

    def run_now(self, task_id: str) -> bool:
        """
        手动触发指定任务（在后台线程中执行，不阻塞调用方）。

        参数:
            task_id: 任务 ID，同注册时的 id

        返回:
            True = 触发成功，False = 任务不存在或已在运行
        """
        task_map = {
            "refresh_stock_info": task_refresh_stock_info,
            "market_fetch": task_post_market_fetch
        }

        fn = task_map.get(task_id)
        if fn is None:
            logger.warning(f"[Scheduler] 未知任务: {task_id}")
            return False

        # 检查是否已在运行
        with self._lock:
            if self._status.get(task_id) == "RUNNING":
                logger.warning(f"[Scheduler] 任务 {task_id} 已在运行，跳过")
                return False

        thread = threading.Thread(
            target=self._wrap_task(task_id, fn),
            daemon=True,
            name=f"manual-{task_id}",
        )
        thread.start()
        logger.warning(f"[Scheduler] 手动触发任务: {task_id}")
        return True


if __name__ == '__main__':
    u = UpdaterScheduler()
    u.start()
    while True:
        import time
        time.sleep(5)
        print(u.get_jobs())
