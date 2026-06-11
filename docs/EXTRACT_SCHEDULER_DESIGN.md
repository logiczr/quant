# ETL Extract 任务编排设计

## 一、问题背景

当前 daemon 的 TaskManager 是"无状态独立调度"模型——4 个定时任务各自在独立线程中执行，互不感知。这在实际运行中暴露了严重问题：

### BaoStock 全局单连接约束

BaoStock 要求同一进程只能维持一个 login 状态。第二次 login 会踢掉前一个连接，导致正在执行的任务崩溃。

```
17:40  股票拉取启动 → BaoStock login → 遍历 5000 只股票中...
18:10  指数拉取启动 → BaoStock login → 踢掉股票拉取的连接
       → 股票拉取的下一个 query 报错 → 崩溃
       → 指数拉取自身也可能 login 失败
       → 两个任务都 FAILED
```

18:30 的 `push_bs_zone_signals` 也调 BaoStock，同样会冲突。

### 当前 TaskManager 的局限性

| 问题 | 表现 |
|------|------|
| **无资源互斥** | 多个任务同时抢 BaoStock 连接，互相踩踏 |
| **无依赖编排** | 指数拉取不等股票拉取完成就启动 |
| **无失败重试** | 任务失败就是 FAILED，不会自动重跑 |
| **无状态持久化** | 重启后所有状态丢失 |
| **同 ID 覆盖** | 手动触发覆盖正在运行的任务记录 |

### 未来更复杂：多数据源

引入多源数据后，约束更多：

| 数据源 | 约束 |
|--------|------|
| BaoStock | 全局单连接，不能并发 login |
| Tushare | 积分限制，每分钟最多 X 次请求 |
| AKShare | 频率限制，高频会被临时封 IP |
| 东方财富 | 反爬，需要控制请求间隔 |

这些约束不是"加个锁"能解决的——需要一个任务编排系统来管理共享资源、调度顺序、控制速率。

## 二、需求分析

### Extract 阶段的所有任务

```
定时任务（日终批次）：
  1. 刷新股票列表（BaoStock）         08:30
  2. 拉取全市场股票日线（BaoStock）    17:40
  3. 拉取全市场指数日线（BaoStock）    18:10
  4. 推送 BS 区间信号（BaoStock）      18:30

未来新增：
  5. 拉取财务数据（Tushare）           季度/按需
  6. 拉取龙虎榜（东方财富）            日终
  7. 拉取新闻/公告（东方财富）          日终
  8. 拉取复权因子（Tushare）           日终
  9. 历史数据回填（混合数据源）         按需/一次性
```

### 任务的共同特征

1. **都需要网络 I/O**：调外部 API，耗时不确定（秒级到分钟级）
2. **都有共享资源约束**：同一数据源的连接不能并发
3. **有依赖关系**：股票日线拉完后才能跑依赖日线数据的策略
4. **有优先级**：日终批量 > 回填；交互触发 > 定时触发
5. **需要限速**：不同数据源有不同的请求频率上限

## 三、设计目标

1. **资源互斥**：同一数据源同一时间只允许一个任务使用
2. **任务依赖**：支持"A 完成后再执行 B"
3. **速率控制**：不同数据源有不同的请求频率限制
4. **失败重试**：任务失败后自动重试（可配置次数和间隔）
5. **可观测**：任务状态、进度、耗时可查询
6. **优先级**：交互触发的任务优先于定时批量任务

## 四、方案：在现有 TaskManager 上增量改造

不重写，在现有 `TaskManager` 上逐步加三个能力：**资源互斥 → 事件驱动 → 速率控制**。

### 改造 1：资源互斥

在 `submit()` 中增加 `resources` 参数，调度时检查资源是否空闲。

```python
class TaskManager:
    def __init__(self) -> None:
        # ... 现有字段不变 ...
        self._resources: dict[str, threading.Semaphore] = {}  # 新增

    def register_resource(self, name: str, max_concurrent: int = 1):
        """注册资源，限制并发数"""
        self._resources[name] = threading.Semaphore(max_concurrent)

    def submit(self, task_id: str, fn: Callable, resources: list[str] | None = None, **kwargs) -> bool:
        """
        提交任务。
        resources: 声明需要的资源列表，如 ["baostock"]
        """
        with self._lock:
            existing = self._tasks.get(task_id)
            if existing and existing.status == TaskStatus.RUNNING:
                return False
            self._tasks[task_id] = TaskRecord(task_id=task_id, resources=resources or [])
            self._task_map[task_id] = fn

        thread = threading.Thread(
            target=self._run_with_resources,
            args=(task_id, fn, resources or [], kwargs),
            daemon=True, name=f"task-{task_id}",
        )
        thread.start()
        return True

    def _run_with_resources(self, task_id, fn, resources, kwargs):
        """先获取所有资源锁，再执行任务，最后释放"""
        acquired = []
        try:
            # 按固定顺序获取，避免死锁
            for r in sorted(resources):
                self._resources[r].acquire()
                acquired.append(r)
                logger.info(f"[TaskManager] {task_id} 获取资源 {r}")

            self._run(task_id, fn, kwargs)  # 复用现有执行逻辑

        finally:
            # 反向释放
            for r in reversed(acquired):
                self._resources[r].release()
                logger.info(f"[TaskManager] {task_id} 释放资源 {r}")
```

**效果**：BaoStock 声明为 `max_concurrent=1`，股票拉取和指数拉取不会同时执行。指数拉取会阻塞等 `acquire()`，股票拉取 `release()` 后立即启动。

**改动量**：`TaskManager` 加约 20 行，现有任务函数加 `resources=["baostock"]` 参数。

### 改造 2：事件驱动

在 `TaskRecord` 中增加 `on_done`/`on_fail` 事件名，任务完成时自动发布事件。

```python
class TaskManager:
    def __init__(self) -> None:
        # ... 现有字段不变 ...
        self._event_handlers: dict[str, list[Callable]] = {}  # 新增

    def on(self, event: str, handler: Callable):
        """订阅事件。handler 接收 dict 参数"""
        self._event_handlers.setdefault(event, []).append(handler)

    def _emit(self, event: str, data: dict | None = None):
        """发布事件"""
        for handler in self._event_handlers.get(event, []):
            try:
                handler(data or {})
            except Exception as e:
                logger.error(f"事件处理失败 {event}: {e}")

    def _run(self, task_id: str, fn: Callable, kwargs: dict) -> None:
        """现有执行逻辑，末尾加事件发布"""
        with self._lock:
            self._tasks[task_id] = TaskRecord(task_id=task_id, status=TaskStatus.RUNNING)
        try:
            result = fn(**kwargs)
            with self._lock:
                rec = self._tasks[task_id]
                rec.status = TaskStatus.DONE
                rec.result = result
            logger.info(f"[TaskManager] {task_id} 完成")
            # 新增：发布 on_done 事件
            if rec.on_done:
                self._emit(rec.on_done, {"task_id": task_id, "result": result})
        except Exception as e:
            with self._lock:
                rec = self._tasks[task_id]
                rec.status = TaskStatus.FAILED
                rec.error = str(e)
            logger.error(f"[TaskManager] {task_id} 失败: {e}")
            # 新增：发布 on_fail 事件
            if rec.on_fail:
                self._emit(rec.on_fail, {"task_id": task_id, "error": str(e)})
```

`TaskRecord` 增加两个字段：

```python
@dataclass
class TaskRecord:
    task_id: str
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: str | None = None
    on_done: str | None = None    # 新增
    on_fail: str | None = None    # 新增
    resources: list[str] = field(default_factory=list)  # 新增
```

`submit()` 增加 `on_done`/`on_fail` 参数：

```python
def submit(self, task_id, fn, resources=None, on_done=None, on_fail=None, **kwargs):
    ...
    self._tasks[task_id] = TaskRecord(
        task_id=task_id, resources=resources or [],
        on_done=on_done, on_fail=on_fail,
    )
```

**改动量**：`TaskRecord` 加 3 个字段，`_run()` 末尾加 4 行事件发布，新增 `on()`/`_emit()` 约 10 行。

### 改造 3：日终 ETL 编排

在 `lifespan` 中用事件驱动替代固定时间调度：

```python
@asynccontextmanager
async def lifespan(app):
    global tm
    tm = TaskManager()

    # 注册资源
    tm.register_resource("baostock", max_concurrent=1)
    tm.register_resource("tushare", max_concurrent=1)
    tm.register_resource("eastmoney", max_concurrent=1)

    # ── 事件订阅：定义任务链 ──

    # 股票列表刷新后补交易日历
    tm.on("stock_info.refreshed", lambda _: tm.submit(
        "fill_trade_calendar", task_fill_trade_calendar,
        resources=["baostock"],
    ))

    # 股票日线拉完后，并行启动三个下游
    def on_daily_fetched(_):
        tm.submit("fetch_index_daily", task_fetch_index_daily,
                  resources=["baostock"], on_done="index_daily.fetched")
        tm.submit("push_bs_zone_signals", notify.push_signals,
                  resources=[])  # 不需要 BaoStock，立即执行
        # 未来：tm.submit("fetch_financial", task_fetch_financial, resources=["tushare"])

    tm.on("daily_bar.fetched", on_daily_fetched)

    # ── 定时任务：只注册入口任务 ──

    tm.schedule("refresh_stock_info", task_refresh_stock_info, "08:30",
                resources=["baostock"], on_done="stock_info.refreshed")
    tm.schedule("post_market_fetch", task_post_market_fetch, "17:40",
                resources=["baostock"], on_done="daily_bar.fetched")
    # 注意：fetch_index_daily 不再单独定时，由事件触发
    # 注意：push_bs_zone_signals 不再单独定时，由事件触发

    tm.start()
    ...
```

**关键变化**：

| 之前 | 之后 |
|------|------|
| 4 个独立定时任务 | 2 个定时入口 + 2 个事件触发任务 |
| fetch_index_daily 固定 18:10 | 股票拉完自动启动，不用等固定时间 |
| push_bs_zone_signals 固定 18:30 | 股票拉完立即启动（不需要 BaoStock） |
| BaoStock 并发冲突 | 资源互斥保证串行 |

### 改造 4（可选）：速率控制

对 Tushare 等有频率限制的数据源，在资源基础上加令牌桶：

```python
class RateLimitedResource:
    def __init__(self, name: str, max_concurrent: int = 1,
                 rate_limit: int | None = None):
        self.semaphore = threading.Semaphore(max_concurrent)
        self.rate_limit = rate_limit  # 每分钟最大请求数
        self._tokens = rate_limit
        self._last_refill = time.time()

    def acquire(self):
        self.semaphore.acquire()
        self._wait_for_token()

    def release(self):
        self.semaphore.release()

    def consume(self):
        """任务执行中每次调 API 前调用"""
        self._wait_for_token()

    def _wait_for_token(self):
        if self.rate_limit is None:
            return
        self._refill()
        while self._tokens <= 0:
            time.sleep(1)
            self._refill()
        self._tokens -= 1

    def _refill(self):
        now = time.time()
        elapsed = now - self._last_refill
        new_tokens = int(elapsed / 60 * self.rate_limit)
        if new_tokens > 0:
            self._tokens = min(self._tokens + new_tokens, self.rate_limit)
            self._last_refill = now
```

这是最复杂的部分，但也是可选的——当前只有 BaoStock 一个数据源，`max_concurrent=1` 就够了。等引入 Tushare 时再加。

### 改造总结

| 步骤 | 改动 | 行数 | 依赖 |
|------|------|------|------|
| 1. 资源互斥 | TaskManager + TaskRecord + register_resource | ~30 行 | 无 |
| 2. 事件驱动 | TaskRecord + on/emit + submit 参数 | ~20 行 | 无 |
| 3. 日终编排 | lifespan 中重新编排定时+事件 | ~20 行 | 步骤1+2 |
| 4. 速率控制 | RateLimitedResource | ~30 行 | 步骤1，可选 |

总计约 100 行新代码，完全在现有 TaskManager 上增量修改，不需要新调度器。

## 五、方案 C 详细设计

### 1. 资源定义

```python
@dataclass
class Resource:
    name: str                    # 资源名，如 "baostock", "tushare"
    max_concurrent: int = 1      # 最大并发数（BaoStock=1, Tushare 可 >1）
    rate_limit: int | None = None  # 每分钟最大请求数
    rate_remaining: int = 0       # 当前分钟剩余配额
    rate_reset_at: float = 0.0     # 配额重置时间

    @property
    def is_available(self) -> bool:
        """资源是否可用（空闲 + 有配额）"""
        ...
```

预定义资源：

| 资源 | max_concurrent | rate_limit | 说明 |
|------|---------------|------------|------|
| `baostock` | 1 | 无硬限制 | 全局单连接 |
| `tushare` | 1 | 200/min（根据积分） | 积分限制 |
| `akshare` | 1 | 30/min | 反爬限制 |
| `eastmoney` | 1 | 60/min | 反爬限制 |

### 2. 任务定义

```python
@dataclass
class ExtractTask:
    task_id: str                     # 唯一 ID
    fn: Callable                     # 执行函数
    resources: list[str]             # 需要的资源列表，如 ["baostock"]
    priority: int = 5                # 优先级，1 最高，9 最低
    depends_on: list[str] | None = None  # 依赖的任务 ID 列表
    max_retries: int = 2             # 最大重试次数
    retry_count: int = 0             # 当前已重试次数
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: str | None = None
    created_at: datetime = field(default_factory=datetime.now)
    started_at: datetime | None = None
    finished_at: datetime | None = None
```

### 3. 调度器

```python
class ExtractScheduler:
    def __init__(self):
        self._resources: dict[str, Resource] = {}
        self._queue: list[ExtractTask] = []     # 优先级队列
        self._running: dict[str, ExtractTask] = {}  # 正在执行的任务
        self._completed: dict[str, ExtractTask] = {}  # 已完成的任务（历史）
        self._lock = threading.Lock()
        self._thread: threading.Thread  # 调度线程

    def register_resource(self, resource: Resource):
        """注册资源池"""
        ...

    def submit(self, task: ExtractTask) -> str:
        """提交任务，返回 task_id"""
        ...

    def _schedule_loop(self):
        """调度主循环（单线程）"""
        while not self._stopped:
            with self._lock:
                # 1. 找到优先级最高且资源可用的任务
                for task in sorted(self._queue, key=lambda t: t.priority):
                    if self._is_ready(task) and self._resources_available(task):
                        self._queue.remove(task)
                        self._allocate(task)
                        self._start(task)
                        break
                # 2. 检查已完成的任务，释放资源
                for task_id, task in list(self._running.items()):
                    if task.status in (TaskStatus.DONE, TaskStatus.FAILED):
                        self._release(task)
                        if task.status == TaskStatus.FAILED and task.retry_count < task.max_retries:
                            task.retry_count += 1
                            task.status = TaskStatus.PENDING
                            self._queue.append(task)  # 重新入队
                        else:
                            self._completed[task_id] = task
                        del self._running[task_id]
            time.sleep(0.5)  # 调度间隔

    def _is_ready(self, task: ExtractTask) -> bool:
        """检查依赖是否都已完成"""
        if not task.depends_on:
            return True
        return all(
            dep_id in self._completed and self._completed[dep_id].status == TaskStatus.DONE
            for dep_id in task.depends_on
        )

    def _resources_available(self, task: ExtractTask) -> bool:
        """检查所需资源是否都可用"""
        return all(self._resources[r].is_available for r in task.resources)

    def _allocate(self, task: ExtractTask):
        """分配资源"""
        for r in task.resources:
            self._resources[r].max_concurrent -= 1

    def _release(self, task: ExtractTask):
        """释放资源"""
        for r in task.resources:
            self._resources[r].max_concurrent += 1

    def _start(self, task: ExtractTask):
        """在新线程中执行任务"""
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now()
        self._running[task.task_id] = task
        thread = threading.Thread(target=self._run_task, args=(task,), daemon=True)
        thread.start()

    def _run_task(self, task: ExtractTask):
        """执行任务（在工作线程中）"""
        try:
            result = task.fn()
            task.status = TaskStatus.DONE
            task.result = result
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = str(e)
        task.finished_at = datetime.now()
```

### 4. 事件驱动 + 依赖编排

之前的设计用 `depends_on` 静态声明依赖，调度器轮询检查。这可行但不够灵活——事件驱动更自然：任务完成后主动发出事件，监听该事件的下游任务被自动激活。

#### 事件总线

```python
class EventBus:
    """进程内事件总线，发布-订阅模式"""

    def __init__(self):
        self._handlers: dict[str, list[Callable]] = {}

    def subscribe(self, event: str, handler: Callable):
        """订阅事件"""
        self._handlers.setdefault(event, []).append(handler)

    def publish(self, event: str, data: dict | None = None):
        """发布事件（同步调用所有订阅者）"""
        for handler in self._handlers.get(event, []):
            try:
                handler(data or {})
            except Exception as e:
                logger.error(f"事件处理失败 {event}: {e}")
```

#### 事件定义

日终 ETL 涉及的事件：

```python
# 事件名约定：{对象}.{动作}
EVENTS = {
    "stock_info.refreshed":  "股票列表刷新完成",
    "daily_bar.fetched":     "股票日线拉取完成",
    "index_daily.fetched":   "指数日线拉取完成",
    "bs_zone.computed":      "BS区间信号计算完成",
    "strategy.computed":     "策略计算完成",
    "trade_calendar.filled": "交易日历补全完成",
    "extract.failed":        "拉取任务失败",
}
```

#### 日终 ETL 编排（事件驱动）

```
08:30 定时触发 refresh_stock_info
         │
         ├── 事件 "stock_info.refreshed" ──→ 补全交易日历
         │
         └── (完成)

17:40 定时触发 post_market_fetch
         │
         ├── 事件 "daily_bar.fetched" ──┬──→ 提交 fetch_index_daily（需 BaoStock）
         │                              ├──→ 提交 compute_bs_zone + 推送钉钉（无需外部资源）
         │                              └──→ 提交 fetch_financial（需 Tushare，可并行）
         │
         │   fetch_index_daily 完成 ──→ 事件 "index_daily.fetched"
         │                                    │
         │                                    └──→ (其他依赖指数数据的策略)
         │
         │   fetch_financial 完成 ──→ 事件 "financial.fetched"
         │                                    │
         │                                    └──→ DB 写入清洗后的财务数据
         │
         └── (全部完成) ──→ 事件 "etl.eod_done" ──→ 钉钉/企业微信推送"日终ETL完成"
```

**关键点**：股票日线拉完后，指数拉取和财务数据拉取**可以并行**——它们用不同的数据源（BaoStock vs Tushare），资源池互不冲突。

#### 代码示例

```python
event_bus = EventBus()
scheduler = ExtractScheduler(event_bus)

# ── 08:30 定时 ──
scheduler.schedule_cron("08:30", ExtractTask(
    task_id="refresh_stock_info",
    fn=task_refresh_stock_info,
    resources=["baostock"],
    on_done="stock_info.refreshed",  # 完成后发出的事件
))

# ── 事件：股票列表刷新后补交易日历 ──
event_bus.subscribe("stock_info.refreshed", lambda _: scheduler.submit(ExtractTask(
    task_id="fill_trade_calendar",
    fn=task_fill_trade_calendar,
    resources=["baostock"],
    priority=5,
)))

# ── 17:40 定时 ──
scheduler.schedule_cron("17:40", ExtractTask(
    task_id="post_market_fetch",
    fn=task_post_market_fetch,
    resources=["baostock"],
    on_done="daily_bar.fetched",
))

# ── 事件：股票日线拉完后，并行启动三个下游 ──
def on_daily_fetched(data):
    # 指数拉取（需 BaoStock，和股票拉取串行但无需等固定时间）
    scheduler.submit(ExtractTask(
        task_id="fetch_index_daily",
        fn=task_fetch_index_daily,
        resources=["baostock"],
        on_done="index_daily.fetched",
    ))
    # BS区间信号计算+钉钉推送（无需外部资源，纯计算+读DB+调钉钉API）
    scheduler.submit(ExtractTask(
        task_id="push_bs_zone_signals",
        fn=notify.push_signals,
        resources=[],                # 不需要 BaoStock！
        on_done="bs_zone.pushed",
    ))
    # 财务拉取（需 Tushare，和上面两个并行）
    scheduler.submit(ExtractTask(
        task_id="fetch_financial",
        fn=task_fetch_financial,
        resources=["tushare"],
        on_done="financial.fetched",
    ))

event_bus.subscribe("daily_bar.fetched", on_daily_fetched)

# ── 事件：全部完成 ──
def on_all_done(data):
    notify.push_text("日终ETL全部完成")
event_bus.subscribe("bs_zone.pushed", on_all_done)
```

#### 事件驱动 vs 静态依赖

| 维度 | 静态 `depends_on` | 事件驱动 |
|------|-------------------|----------|
| 依赖声明 | 任务创建时硬编码 | 运行时动态订阅，可增减 |
| 并行分支 | 不支持（depends_on 是"全部完成后"） | 天然支持（一个事件触发多个订阅者） |
| 条件触发 | 所有依赖完成就触发 | 可以在事件处理函数里加条件逻辑 |
| 解耦程度 | 任务需要知道上游的 task_id | 任务只需知道事件名，不需要知道谁触发 |
| 可扩展性 | 新增下游需要改上游 | 新增下游只需 subscribe，不改任何现有代码 |

事件驱动更灵活，特别适合"一个任务完成后触发多个并行分支"的场景。

#### ExtractTask 增加 on_done/on_fail

```python
@dataclass
class ExtractTask:
    task_id: str
    fn: Callable
    resources: list[str]
    priority: int = 5
    on_done: str | None = None      # 完成后发出的事件名
    on_fail: str | None = None      # 失败后发出的事件名
    max_retries: int = 2
    retry_count: int = 0
    status: TaskStatus = TaskStatus.PENDING
    # ...
```

调度器在任务完成/失败后自动 `event_bus.publish(task.on_done, {"task_id": task.task_id, "result": task.result})`

### 5. 懒拉取如何适配

懒拉取本质上也是 Extract 任务——需要 BaoStock 资源，完成后触发 DB 写入。

```python
def get_daily(code, start, end, auto_fetch=True):
    # 1. 先查 DB
    df = read_from_db(...)
    if not df.empty:
        return df
    # 2. 查 _pending_writes 缓存
    if key in _pending_writes:
        return _pending_writes[key]
    # 3. 提交 Extract 任务（高优先级，插队）
    task_id = scheduler.submit(ExtractTask(
        task_id=f"lazy_fetch_{code}_{start}_{end}",
        fn=lambda: fetch_daily_single(code, start, end),
        resources=["baostock"],
        priority=1,  # 最高优先级
        on_done="lazy_fetch.done",
    ))
    # 4. 同步等待结果
    return scheduler.wait_for(task_id, timeout=60)
```

`scheduler.wait_for()` 阻塞等待任务完成，内部用 Event 信号量实现，不是轮询：

```python
class ExtractScheduler:
    def wait_for(self, task_id: str, timeout: float = 60) -> Any:
        """同步等待任务完成，返回结果。超时返回 None。"""
        event = threading.Event()
        self._waiters[task_id] = event
        # 订阅一次性事件：该任务完成后通知
        event.wait(timeout=timeout)
        task = self._completed.get(task_id) or self._running.get(task_id)
        if task and task.status == TaskStatus.DONE:
            return task.result
        return None
```

如果 BaoStock 被批量拉取占用，懒拉取会在队列里等——但它的优先级最高，批量任务释放资源后第一个就轮到它。最多等一个 chunk 的时间（<500ms 如果按 chunk 拆分拉取）。

### 6. 速率控制

对于 Tushare 等有明确频率限制的数据源，资源池需要跟踪配额：

```python
class Resource:
    rate_limit: int | None = None  # 每分钟最大请求数
    rate_remaining: int = 0
    rate_reset_at: float = 0.0     # 配额重置时间

    @property
    def is_available(self) -> bool:
        if self.max_concurrent <= 0:
            return False
        if self.rate_limit is not None:
            now = time.time()
            if now >= self.rate_reset_at:
                # 新的一分钟，重置配额
                self.rate_remaining = self.rate_limit
                self.rate_reset_at = now + 60
            if self.rate_remaining <= 0:
                return False
        return True

    def consume_rate(self):
        """每次 API 调用前消费一个配额"""
        if self.rate_limit is not None:
            self.rate_remaining -= 1
```

任务执行时，每次调 API 前调用 `resource.consume_rate()`。调度器在分配任务前检查 `is_available`。

### 7. 重试策略

```python
@dataclass
class ExtractTask:
    max_retries: int = 2
    retry_delay: float = 30.0  # 重试间隔秒数

    # 调度器在任务失败后：
    # 1. 如果 retry_count < max_retries：延迟 retry_delay 秒后重新入队
    # 2. 如果已达到最大重试次数：标记 FAILED，不再重试
```

可区分失败类型：
- **网络超时**：可重试
- **认证失败**（API Key 无效）：不重试，人工介入
- **限频 429**：等待后重试（需要从响应头读 retry-after）
- **数据格式异常**：不重试，人工介入

### 8. 可观测性

```
GET /extract/status          → 所有任务状态
GET /extract/resources       → 资源池状态（BUSY/IDLE、剩余配额）
GET /extract/task/{task_id}  → 单个任务详情（状态、耗时、错误）
POST /extract/submit         → 手动提交任务
POST /extract/cancel/{id}    → 取消排队中的任务
```

### 9. 与 DB 写入队列的关系

Extract Scheduler 负责拉取，DB Write Queue 负责写入。两者串联：

```
ExtractScheduler                DB Write Queue
     │                               │
     │  拉取完成                       │  写入完成
     ├─→ DataFrame                    │
     │    ├─→ 文件落盘（Raw 层）        │
     │    └─→ 创建 DBWriteTask ──────→ │ DB Worker 消费
     │                               │
     │  释放资源（BaoStock 等）          │
     ▼                               ▼
```

Extract 任务完成后：
1. DataFrame 落盘为文件（Raw 层）
2. 创建 DBWriteTask 入写队列
3. 释放资源（让下一个 Extract 任务启动）

拉取和写入完全解耦。Extract 任务的资源是"BaoStock/Tushare 连接"，DB Write 的资源是"DuckDB 写锁"——两个队列管理两种不同的资源约束。

## 六、需要你回答的问题

| # | 问题 | 影响 |
|---|------|------|
| 1 | 懒拉取触发时，用户愿意等多久？同步等还是异步返回？ | 决定懒拉取在调度器中的处理方式 |
| 2 | 日终 ETL 的依赖链是怎样的？股票→指数→推送，还有其他依赖吗？ | 决定 DAG 的形状 |
| 3 | 不同数据源的频率限制具体是多少？ | 决定 rate_limit 配置 |
| 4 | 任务失败后是否需要人工确认才能重试？ | 决定重试策略 |
| 5 | 是否需要支持定时触发的替代（如"收盘后自动检测并触发"而非固定时间）？ | 决定触发方式 |
