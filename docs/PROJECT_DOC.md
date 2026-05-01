# 量化交易系统 — 项目文档

> 版本：v0.4  
> 更新：2026-05-01  
> 状态：开发中

---

## 一、项目概况

基于 **BaoStock + DuckDB + Streamlit + FastAPI** 的 A 股量化分析平台。三层架构：数据层（拉取 + 存储 + 指标计算）、策略层（JSON 定义 + 引擎调度）、展示层（守护进程 + Streamlit 前端）。

### 核心能力

| 能力 | 说明 |
|------|------|
| 数据拉取 | BaoStock 全市场日线/分钟线，带重试退避 |
| 本地存储 | DuckDB 嵌入式数据库，3 张核心表 |
| 透明补拉 | 查询时自动检测缺口 → 补拉 → 返回，上层无感知 |
| 技术指标 | 14 类指标计算（MACD/KDJ/BOLL/RSI/ATR 等），支持金叉死叉信号 |
| 策略引擎 | JSON 定义策略，Python/SQL 双计算模式，last_date 缓存管理 |
| 动态选股 | Screener 类型策略，UI 拼条件 → 自动生成 SQL → 实时查询 |
| 守护进程 | FastAPI 后台进程（lifespan 管理），定时拉数据，前端通过 HTTP 查状态 |

---

## 二、文件结构与职责

```
/home/logiczr/quant/
├── data_tools.py          # 数据获取（BaoStock 交互，完全解耦）
├── duckdb_tools.py         # 数据库管理（DuckDB CRUD + 透明补拉）
├── index_tools.py          # 技术指标计算（14 类指标）
├── strategy.py             # 策略引擎 + 计算函数 + 注册器
├── scheduler.py            # 旧版调度器（已被 db_daemon 替代，保留备用）
├── db_daemon.py            # 守护进程（TaskManager + FastAPI HTTP）
├── daemon_client.py        # HTTP 客户端（Streamlit 侧调用守护进程）
├── streamlit_app.py        # Streamlit 前端（5 个页面）
├── stra.py                 # 实验脚本（市值计算，不参与主流程）
├── stock_data.duckdb       # DuckDB 数据库文件（~57MB）
├── last_fetch.json         # 最近一次收盘拉取记录
├── strategy/               # 策略 JSON 定义目录
│   ├── market_cap_rank.json    # 流通市值排行
│   └── market_cap_growth.json  # 市值增长排行
├── requirements.txt        # Python 依赖
├── test_datatools.py       # BaoStock 行业数据测试
├── test_duckdb.py          # DuckDB 工具测试
├── test_index.py           # 指标计算测试
└── venv/                   # Python 虚拟环境
```

---

## 三、模块详细说明

### 3.1 data_tools.py — 数据获取

**职责**：与 BaoStock 交互，提供股票列表、日线、分钟线三个核心接口。不依赖任何数据库或上层模块。

| 接口 | 签名 | 说明 |
|------|------|------|
| `fetch_stock_list` | `() -> pd.DataFrame` | 查询全市场 A 股在市股票列表 |
| `fetch_daily` | `(stock_list, start_date, end_date, adjustflag) -> pd.DataFrame` | 批量拉取多只股票日线 |
| `fetch_daily_single` | `(code, code_name, start_date, end_date, adjustflag) -> pd.DataFrame` | 单只股票日线（便捷接口） |
| `fetch_minute` | `(stock_list, start_date, end_date, frequency, adjustflag) -> pd.DataFrame` | 批量拉取分钟线（5/15/30/60分钟） |
| `baostock_session` | `() -> Generator` | BaoStock 登录/登出上下文管理器 |

**内部机制**：
- `_query_with_retry`：单只股票查询重试封装（默认 2 次重试）
- 批量拉取时使用流式写入（flush_size=10000），避免大 DataFrame 内存溢出
- 日线返回列：`date, code, code_name, open, high, low, close, preclose, volume, amount, adjustflag, turn, tradestatus, pctChg, isST, peTTM, psTTM, pcfNcfTTM, pbMRQ`
- 分钟线返回列：`date, time, code, code_name, open, high, low, close, volume, amount, adjustflag`

**依赖**：baostock, pandas

---

### 3.2 duckdb_tools.py — 数据库管理

**职责**：管理 DuckDB 三张核心表，实现「缺失数据透明补拉」。

#### 核心表结构

| 表名 | 主键 | 说明 |
|------|------|------|
| `stock_info` | code | A 股基础信息，每日开盘前全量刷新 |
| `daily_bar` | date + code + adjustflag | 日线 K 线（含成交量/成交额/换手率/估值等） |
| `minute_bar` | date + time + code + frequency + adjustflag | 分钟线 K 线 |

#### 主要接口

| 接口 | 说明 |
|------|------|
| `get_connection(db_path)` | 获取读写连接（短连接模式） |
| `get_read_connection(db_path)` | 获取只读连接 |
| `upsert_stock_info(df, db_path)` | 全量刷新 stock_info（DELETE + INSERT） |
| `get_stock_info(code, db_path)` | 查询 stock_info |
| `delete_stock_info(code, db_path)` | 删除 stock_info（code='ALL' 全删） |
| `insert_daily(df, db_path)` | 日线数据写入（INSERT OR REPLACE，分块 50000 条） |
| `get_daily(code, start_date, end_date, ...)` | **核心接口**：查询日线 + 透明补拉 |
| `delete_daily(code, ...)` | 删除日线数据 |
| `query_daily(sql, params, db_path)` | 执行任意 SELECT SQL 查询日线 |
| `insert_minute(df, frequency, db_path)` | 分钟线写入 |
| `get_minute(code, ...)` | 查询分钟线 + 透明补拉 |
| `delete_minute(code, ...)` | 删除分钟线数据 |
| `query_minute(sql, params, db_path)` | 执行任意 SELECT SQL 查询分钟线 |
| `table_stats(db_path)` | 核心表行数与日期范围 |

#### 透明补拉机制

`get_daily` / `get_minute` 内部自动检测本地数据是否覆盖查询区间：
1. 本地无数据 → 补拉全段
2. 本地数据范围不足（前缺/后缺） → 补拉缺口区间
3. 本地数据完整 → 直接返回
4. `auto_fetch=False` 时跳过补拉

#### 数据库路径

默认 `项目根目录/stock_data.duckdb`，尚不支持环境变量覆盖。

```python
_DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "stock_data.duckdb"
)
```

**连接管理**：短连接模式，每次操作开新连接用完关闭。

**依赖**：duckdb, pandas, data_tools

---

### 3.3 index_tools.py — 技术指标计算

**职责**：基于本地 DuckDB 数据计算各类技术指标，支持单股和批量。

| 指标 | 函数 | 输出列 |
|------|------|--------|
| MACD | `calc_macd` | DIF, DEA, MACD, MACD_SIGNAL |
| KDJ | `calc_kdj` | KDJ_K, KDJ_D, KDJ_J, KDJ_SIGNAL |
| BOLL | `calc_boll` | BOLL_MID, BOLL_UP, BOLL_DOWN, BOLL_WIDTH, BOLL_PCT |
| RSI | `calc_rsi` | RSI6, RSI12, RSI24 |
| CCI | `calc_cci` | CCI |
| WR | `calc_wr` | WR14, WR28 |
| ATR | `calc_atr` | TR, ATR, ATR_PCT |
| MA | `calc_ma` | MA5, MA10, MA20, MA60 |
| EMA | `calc_ema` | EMA5, EMA10, EMA20, EMA60 |
| OBV | `calc_obv` | OBV, OBV_CHG |
| VOL_MA | `calc_vol_ma` | VOL_MA5/10/20, VOL_RATIO |
| DMA | `calc_dma` | DMA_DIF, DMA_AMA |
| VR | `calc_vr` | VR |
| HV | `calc_hv` | HV_DAILY, HV_ANN |

#### 全局配置：IndicatorConfig

所有指标参数通过 `IndicatorConfig` dataclass 统一管理，默认值均为业界常用参数：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| macd_fast / macd_slow / macd_signal | 12 / 26 / 9 | MACD 三参数 |
| kdj_n / kdj_m1 / kdj_m2 | 9 / 3 / 3 | KDJ 三参数 |
| boll_n / boll_k | 20 / 2.0 | BOLL 周期和倍数 |
| rsi_periods | (6, 12, 24) | RSI 多周期 |
| ma_periods | (5, 10, 20, 60) | MA/EMA 周期 |
| vol_ma_periods | (5, 10, 20) | 量均线周期 |
| vr_period | 24 | VR 周期 |
| dma_short / dma_long | 10 / 50 | DMA 短长周期 |

#### 统一入口

| 接口 | 说明 |
|------|------|
| `calc_indicators(df, indicators, cfg)` | 单股统一入口，indicators 传列表或 'all' |
| `calc_batch(codes, start_date, end_date, ...)` | 多股批量计算，返回 dict[str, DataFrame] |

**金叉死叉信号**：MACD_SIGNAL / KDJ_SIGNAL（1=金叉，-1=死叉，0=无信号）

**依赖**：duckdb, numpy, pandas, duckdb_tools

---

### 3.4 strategy.py — 策略引擎 + 计算函数

**职责**：策略注册器 + JSON 策略加载 + last_date 缓存管理 + 计算调度 + Screener 动态选股。

#### 核心接口

| 接口 | 说明 |
|------|------|
| `list_strategies()` | 扫描 strategy/ 目录，返回所有策略定义 |
| `get_strategy(name)` | 按名获取策略定义 |
| `ensure_strategy_table(strategy)` | 按 columns + primary_key 建表（IF NOT EXISTS，不删旧表） |
| `compute_strategy(strategy, date)` | 执行策略计算，返回写入条数 |
| `query_strategy(name, date, force_compute)` | **核心入口**：last_date 缓存判断 → 计算或查表 |
| `query_strategy_range(name, start_date, end_date)` | 日期范围查询 |
| `query_screener(strategy, date, filters, ...)` | 动态条件选股 |
| `strategy_info(name)` | 获取策略元信息（不触发计算） |
| `register_compute(name)` | 装饰器，注册 Python 计算函数 |

#### 策略 JSON 格式

```json
{
  "name": "market_cap_rank",
  "description": "流通市值排行（close × volume × 100 / turn）",
  "table": "strategy_market_cap_rank",
  "columns": [
    {"name": "code",      "type": "VARCHAR", "not_null": true},
    {"name": "code_name", "type": "VARCHAR"},
    {"name": "date",      "type": "DATE",    "not_null": true},
    {"name": "flow_cap",  "type": "DOUBLE"},
    {"name": "rank",      "type": "INTEGER"}
  ],
  "primary_key": ["code", "date"],
  "compute": {"type": "python", "function": "market_cap_rank"},
  "write_sql": "仅供参考，引擎不读取",
  "read_sql": "SELECT * FROM strategy_market_cap_rank WHERE date = ? ORDER BY rank",
  "params": {},
  "last_date": "2026-04-29"
}
```

| 字段 | 职责 | 引擎读取？ |
|------|------|-----------|
| `name` | 标识，匹配 `@register_compute` 注册名 | ✅ |
| `description` | 前端展示 | ✅ |
| `table` | 建表/查表/写表 | ✅ |
| `columns` | 表结构，engine 拼 CREATE TABLE | ✅ |
| `primary_key` | 主键，建表 + 去重 | ✅ |
| `compute` | 计算方式：python 调注册函数，sql 直接执行 compute.sql | ✅ |
| `write_sql` | 写表逻辑 | ❌ 纯阅读，引擎用代码模板 |
| `read_sql` | 查表 SQL，不写用默认模板 | ✅ |
| `params` | 参数覆盖，`func(date, **params)` | ✅ |
| `last_date` | 缓存判断，计算完自动更新 | ✅ |

#### 查询流程

```
query_strategy("market_cap_rank", date)
  │
  ├── JSON 里 last_date == date ?
  │     ├── ✅ 是 → 查表返回（read_sql）
  │     └── ❌ 不是（或为空）↓
  │
  └── 计算函数(date, **params) → DataFrame
        │
        ├── 结果为空 → 返回空 DataFrame
        │
        └── 有结果 → DELETE + INSERT 写表
                      → 更新 JSON 的 last_date
                      → 查表返回
```

#### 函数签名规范

```python
@register_compute("策略名")
def 策略名(date: str, 可选参数: 类型 = 默认值, ...) -> pd.DataFrame:
    # date → K线数据的入口
    # 其余参数 → 策略调节旋钮，默认值只在签名里写
    # 返回 DataFrame 列结构必须与 JSON columns 一致
```

引擎调用：`func(date=date, **strategy.get("params", {}))`

#### 已注册的计算函数

| 策略名 | 函数 | 逻辑 | 参数 |
|--------|------|------|------|
| `market_cap_rank` | `market_cap_rank` | 流通市值排行：close×volume×100/turn/1e8 | 无 |
| `market_cap_growth` | `market_cap_growth` | 今日市值 - 昨日市值，按 cap_change 降序 | 无 |

**流通市值公式**：`flow_cap(亿) = close × volume × 100 / turn / 1e8`
- close: 收盘价（元），volume: 成交量（股），turn: 换手率（百分比数值）
- ×100: 还原流通股数 = volume / (turn/100)

**Screener 动态选股**：`type="screener"` 的策略不做持久化，前端拼筛选条件 → 自动生成 SQL → 实时查询 daily_bar + indicators。`_field_prefix` 自动判断字段归属 daily_bar（d.）还是 indicators（i.）。

**安全机制**：`_SAFE_OPS` 白名单限制操作符（`>`, `>=`, `<`, `<=`, `=`, `!=`, `<>`, `between`, `in`），防止 SQL 注入。

**依赖**：duckdb, pandas, duckdb_tools

---

### 3.5 scheduler.py — 旧版调度器

**职责**：基于 APScheduler 的定时任务调度器（已被 `db_daemon.py` 的 TaskManager 替代，保留备用）。

#### 与 db_daemon.py 的区别

| 对比项 | scheduler.py | db_daemon.py |
|--------|-------------|-------------|
| 调度器 | `UpdaterScheduler` 类 | `TaskManager` 类 + FastAPI |
| HTTP API | ❌ 无 | ✅ 5 个接口 |
| 前端交互 | ❌ 无 | ✅ daemon_client |
| 重试机制 | ✅ 3轮（5/10/15分钟） | ❌ 无（单次执行） |
| 收盘拉取时间 | 18:00 | 17:00 |
| 状态 | 保留备用 | **当前使用** |

**依赖**：apscheduler, duckdb_tools, data_tools

---

### 3.6 db_daemon.py — 守护进程

**职责**：后台执行定时任务 + 提供 FastAPI HTTP 接口。使用 lifespan 上下文管理器管理 TaskManager 生命周期。

#### TaskManager 类

| 方法 | 说明 |
|------|------|
| `submit(task_id, fn)` | 提交一次性任务（后台线程执行） |
| `schedule(task_id, fn, cron)` | 注册定时任务，cron 格式 `"HH:MM"` 或 `"day_of_week HH:MM"` |
| `status(task_id)` | 查询任务状态 |
| `result(task_id)` | 获取任务结果 |
| `error(task_id)` | 获取任务错误信息 |
| `all_tasks()` | 所有任务状态 |
| `get_jobs()` | 已注册定时任务列表 |
| `run_now(task_id)` | 手动触发已注册任务 |

#### 任务状态枚举

`PENDING` → `RUNNING` → `DONE` / `FAILED` / `SKIPPED`

#### 定时任务

| 任务 | task_id | 时间 | 逻辑 |
|------|---------|------|------|
| 刷新股票列表 | `refresh_stock_info` | 08:30 | 全量更新 stock_info |
| 收盘批次拉取 | `post_market_fetch` | 17:00 | 全市场日线批量拉取 + 写入 |

#### Lifespan 管理

```python
@asynccontextmanager
async def lifespan(app):
    global tm
    # ── startup ──
    tm = TaskManager()
    tm.schedule("refresh_stock_info", task_refresh_stock_info, "08:30")
    tm.schedule("post_market_fetch", task_post_market_fetch, "17:00")
    tm.start()
    yield
    # ── shutdown ──
    if tm:
        tm.shutdown()

app = FastAPI(title="Stock DB Daemon", lifespan=lifespan)
```

好处：`import db_daemon` 不会启动调度器，只有 uvicorn 真正跑起来时才初始化 TaskManager。

#### HTTP API（127.0.0.1:8502）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 健康检查 |
| GET | `/status` | 所有任务当前状态 |
| GET | `/jobs` | 已注册定时任务列表 |
| POST | `/run_now/{task_id}` | 手动触发指定任务 |
| GET | `/result/{task_id}` | 获取任务结果 |
| GET | `/last_fetch` | 获取最近一次收盘拉取的结果 |

**依赖**：uvicorn, apscheduler, fastapi, duckdb_tools, data_tools（延迟导入）

---

### 3.7 daemon_client.py — HTTP 客户端

**职责**：Streamlit 侧通过 HTTP 与守护进程通信。

| 接口 | 说明 |
|------|------|
| `is_alive()` | 守护进程是否在线 |
| `get_status()` | 获取所有任务状态 |
| `get_jobs()` | 获取定时任务列表 |
| `run_now(task_id)` | 手动触发任务 |
| `get_last_fetch()` | 获取最近拉取记录 |

**Daemon URL**：当前硬编码 `http://127.0.0.1:8502`，超时 3 秒。

```python
DAEMON_URL = "http://127.0.0.1:8502"
TIMEOUT = 3
```

**依赖**：requests

---

### 3.8 streamlit_app.py — 前端

**职责**：5 页面看板，通过 daemon_client 与守护进程通信，直接调用 duckdb_tools/index_tools/strategy 获取数据。

| 页面 | 功能 |
|------|------|
| 📊 排行榜 | 量比/涨幅/换手率排行（需手动点击计算） |
| 🔍 个股查询 | 输入代码 → 日线 + 技术指标 + 收盘价折线图 |
| 📋 股票列表 | 全市场股票搜索浏览 + 手动更新 |
| 📈 因子分析 | 策略选择 → 查询结果 |
| 🔧 数据库维护 | 守护进程状态 + 手动触发定时任务 + 最近拉取记录 |

**侧边栏**：数据库状态（table_stats）+ 守护进程在线状态

**注意**：排行榜页面日期当前硬编码为 `2026-04-01 ~ 2026-04-20`，待改为动态日期。

**依赖**：streamlit, pandas, duckdb_tools, index_tools, daemon_client, strategy

---

### 3.9 stra.py — 实验脚本

**职责**：市值计算实验代码，不参与主流程。

功能：遍历全市场股票，计算流通市值、市值变化、变化率，按不同维度排序输出。

---

## 四、模块依赖关系

```
                    ┌─────────────────────┐
                    │   streamlit_app.py   │ (port 8501)
                    └──┬──────┬──────┬────┘
                       │      │      │
              ┌────────┘      │      └──────────┐
              ▼               ▼                  ▼
     ┌──────────────┐  ┌───────────┐    ┌──────────────┐
     │ daemon_client │  │ strategy  │    │ index_tools  │
     │  (HTTP)      │  │   .py     │    │    .py       │
     └──────┬───────┘  └─────┬─────┘    └──────┬───────┘
            │                │                  │
            │ HTTP           │                  │
            ▼                ▼                  ▼
     ┌──────────────┐  ┌──────────────────────────────┐
     │  db_daemon   │  │        duckdb_tools           │
     │  (port 8502) │  │          .py                  │
     └──────┬───────┘  └──────────────┬───────────────┘
            │                         │
    ┌───────┼────────┐                │
    ▼       ▼        ▼                ▼
 duckdb   index    scheduler    ┌───────────┐
 _tools   _tools*    .py        │ data_tools │
    │                           │    .py     │
    │                           └───────────┘
    └──────────┬─────────────────────┘
               ▼
         ┌───────────┐
         │  DuckDB   │
         │ 数据库文件 │
         └───────────┘

* db_daemon 通过 duckdb_tools 间接使用 data_tools
  scheduler.py 直接导入 data_tools（旧版）
```

**分层说明**：

| 层 | 模块 | 说明 |
|----|------|------|
| 数据层 | `data_tools` → `duckdb_tools` → `index_tools` | 拉取 → 存储 → 指标计算 |
| 策略层 | `strategy.py`（注册器 + 计算函数 + 引擎） ← JSON 策略定义 | 单模块包含注册/计算/查询 |
| 调度层 | `db_daemon` / `scheduler` | 守护进程（当前） / 旧版调度器（备用） |
| 展示层 | `daemon_client` + `streamlit_app` | HTTP 客户端 + 前端 |

---

## 五、第三方依赖

| 库 | 用途 |
|----|------|
| baostock | A 股数据源（日线/分钟线/股票列表） |
| duckdb | 嵌入式列式数据库 |
| pandas | 数据处理核心 |
| numpy | 指标计算（数组运算） |
| streamlit | Web 前端框架 |
| fastapi | 守护进程 HTTP API |
| uvicorn | ASGI 服务器 |
| apscheduler | 定时任务调度 |
| requests | HTTP 客户端（daemon_client） |

---

## 六、BaoStock 数据字段踩坑

| 字段 | 文档说明 | 实际情况 | 影响 |
|------|----------|----------|------|
| `amount` | 千元 | **元**（通过 `amount/close ≈ volume` 验证） | 流值反推公式不能用千元换算 |
| `turn` | 换手率 | 百分比数值（2.5 = 2.5%），不是小数 | 反推需要 ×100 |
| 流通市值反推 | — | 必须用 `close × volume × 100 / turn` | 用 amount 反推是 VWAP 口径，偏差 1~3% |
| 市值变化 vs 资金流入 | — | 市值变化 ≠ 资金流入 | 资金净流入 = amount × 涨跌幅 |

---

## 七、进程架构

```
本地开发（手动启动两个进程）：

┌──────────────────────────────────────┐
│  终端 1: python db_daemon.py         │
│  → FastAPI, port 8502               │
│  → 2 个定时任务（08:30 / 17:00）      │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│  终端 2: streamlit run streamlit_app │
│  → Streamlit, port 8501             │
│  → 直接 import duckdb_tools, etc.   │
│  → 通过 daemon_client 查守护进程状态  │
└──────────────────────────────────────┘

数据流：
  Streamlit ──HTTP──→ db_daemon（状态查询/任务触发）
  Streamlit ──import──→ duckdb_tools → data_tools → BaoStock
```

---

## 八、待完善项

| 优先级 | 内容 | 状态 |
|--------|------|------|
| P0 | daemon_client 支持 `DAEMON_URL` 环境变量覆盖 | 待实现 |
| P0 | db_daemon 支持 `host="0.0.0.0"` 配置（Docker 部署需要） | 待实现 |
| P1 | Streamlit 排行榜日期改为动态（当前硬编码） | 待修复 |
| P1 | Streamlit 排行榜性能优化（全市场 calc_batch 太慢） | 待优化 |
| P1 | 日线 K 线图（当前只有收盘价折线） | 待实现 |
| P2 | indicators 表持久化（EMA/MACD/KDJ 等历史依赖型指标） | 待实现 |
| P2 | 策略结果缓存失效策略（收盘判断 + stale 状态） | 待设计 |
| P2 | Docker 部署（Dockerfile + docker-compose） | 待实现 |
| P2 | 一键启动脚本（start_all.py） | 待实现 |
| P3 | test_duckdb.py 中 `check_all_daily_gaps` 函数不存在 | 待修复/实现 |
