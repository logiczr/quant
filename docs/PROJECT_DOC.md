# 量化交易系统 — 项目文档

> 版本：v1.0  
> 更新：2026-05-10  
> 状态：已部署

---

## 一、项目概况

基于 **BaoStock + DuckDB + Streamlit + FastAPI** 的 A 股量化分析平台。采用**前后端分离**架构：所有数据库访问由 daemon 后端统一管理，Streamlit 前端通过 HTTP API 获取数据，绝不直连 DuckDB。

### 核心能力

| 能力 | 说明 |
|------|------|
| 数据拉取 | BaoStock 全市场股票/指数日线 + 分钟线，带重试退避 |
| 本地存储 | DuckDB 嵌入式数据库，4 张核心表（stock_info / daily_bar / index_daily_bar / minute_bar） |
| 透明补拉 | 查询时自动检测缺口 → 补拉 → 返回，上层无感知（仅 daemon 侧执行） |
| 大盘概览 | 三大指数实时行情 + 全市场涨跌分布（自动识别最新交易日，跳过节假日） |
| 技术指标 | 14 类指标计算（MACD/KDJ/BOLL/RSI/ATR 等），支持金叉死叉信号 |
| 策略引擎 | Python 模块定义策略，引擎自动发现/调度，last_date 存 DuckDB |
| 动态选股 | Screener 类型策略，UI 拼条件 → 自动生成 SQL → 实时查询 |
| 自选股管理 | 组合创建/删除/添加移除股票，等权指数计算，板块估值统计 |
| 守护进程 | FastAPI 后台进程，20+ 数据查询 API + 4 个定时任务 + 手动触发 |
| 钉钉推送 | 基于 dingtalk-stream SDK 推送到钉钉群（互动机器人），支持手动/定时推送 |
| Docker 部署 | docker-compose 双容器部署（daemon + streamlit），支持本地构建离线镜像传输 |
| 回测引擎 | backtrader 集成（设计中，详见 [BACKTEST_DESIGN.md](BACKTEST_DESIGN.md)） |

---

## 二、文件结构与职责

```
/home/logiczr/quant/
├── data_tools.py          # 数据获取（BaoStock 交互，完全解耦）
├── duckdb_tools.py        # 数据库管理（DuckDB CRUD + 透明补拉 + 大盘概览 + 连接管理）
├── index_tools.py         # 技术指标计算（14 类指标）
├── watchlist_tools.py     # 自选股管理（组合 CRUD + 指数计算 + 估值统计）
├── notify.py              # 钉钉推送（bingtalk-stream SDK，信号推送 + 群内指令）
├── strategy.py            # 策略引擎（发现 + 调度 + 缓存 + Screener）
├── strategies/            # 策略模块目录（每个 .py 一个策略）
│   ├── __init__.py
│   ├── bs_zone.py              # BS 区间策略（金叉买/死叉卖）
│   ├── market_cap_rank.py      # 流通市值排行
│   ├── market_cap_growth.py    # 市值增长排行
│   ├── market_cap_growth_rate.py # 市值增长率
│   ├── ff3_factor.py           # Fama-French 三因子
│   └── ff3_regression.py       # FF3 因子回归分析
├── db_daemon.py           # 守护进程（TaskManager + FastAPI HTTP，20+ 查询 API + 4 定时任务）
├── daemon_client.py       # HTTP 客户端（Streamlit 侧所有数据查询通过此模块）
├── streamlit_app.py       # Streamlit 前端（7 个页面，零直连数据库）
├── scheduler.py           # 旧版调度器（已被 db_daemon 替代，保留备用）
├── stra.py                # 实验脚本（市值计算，不参与主流程）
├── stock_data.duckdb      # DuckDB 数据库文件（~57MB）
├── last_fetch.json        # 最近一次收盘拉取记录
├── Dockerfile             # Docker 镜像定义
├── docker-compose.yml     # 双容器部署配置（daemon + streamlit）
├── docs/                  # 项目文档
│   ├── PROJECT_DOC.md          # 本文件
│   └── BACKTEST_DESIGN.md      # 回测引擎设计文档
├── requirements.txt       # Python 依赖
├── test_datatools.py      # BaoStock 行业数据测试
├── test_duckdb.py         # DuckDB 工具测试
├── test_index.py          # 指标计算测试
└── venv/                  # Python 虚拟环境
```

---

## 三、架构设计

### 3.1 前后端分离架构

**核心原则**：Streamlit 前端**绝不直连 DuckDB**，所有数据查询通过 HTTP API → daemon → DuckDB。

```
┌──────────────────────┐    HTTP     ┌──────────────────────────┐
│  Streamlit 前端        │  ────→     │  db_daemon (port 8502)     │
│  (port 8501)          │            │                            │
│                       │            │  20+ 查询 API              │
│  只 import:           │  ←────     │  3 个定时任务               │
│  - daemon_client      │   JSON     │  TaskManager               │
│  - index_tools        │            │                            │
│                       │            │  直接访问 DuckDB            │
│  不 import:           │            │  import:                   │
│  - duckdb_tools ✗     │            │  - duckdb_tools            │
│  - strategy ✗        │            │  - strategy                │
│  - watchlist_tools ✗  │            │  - watchlist_tools         │
└──────────────────────┘            │  - data_tools              │
                                    └─────────┬────────────────┘
                                              │
                                              ▼
                                        ┌──────────┐
                                        │  DuckDB   │
                                        │  数据库    │
                                        └──────────┘
```

**为什么这样设计**：
- DuckDB **不支持多进程并发访问同一文件**（即使一个读一个写也不行）
- Docker 部署中 daemon 和 streamlit 是两个独立容器/进程
- 前后端分离确保 daemon 是唯一 DB 拥有者，无并发冲突

### 3.2 Docker 部署架构

```yaml
services:
  daemon:                    # 后端：拥有 DB，提供 HTTP API
    command: python db_daemon.py
    ports: ["8502:8502"]
    environment:
      - QUANT_DB_PATH=/quant3/data/stock_data.duckdb
    volumes:
      - quant3-data:/quant3/data    # 唯一挂载数据卷

  streamlit:                 # 前端：零 DB 访问，纯 HTTP 调用
    command: streamlit run streamlit_app.py --server.address 0.0.0.0 --server.port 8501
    ports: ["8501:8501"]
    environment:
      - DAEMON_URL=http://daemon:8502   # Docker 内部 DNS
    # 无 volume，无 QUANT_DB_PATH

volumes:
  quant3-data:               # 持久化数据卷
```

**关键设计**：
- streamlit 容器**不挂载数据卷**，不设 `QUANT_DB_PATH`，从物理上杜绝直连 DB
- `DAEMON_URL` 环境变量指定 daemon 地址，Docker 内用服务名 `http://daemon:8502`，本地用 `http://127.0.0.1:8502`
- daemon uvicorn 绑定 `0.0.0.0`（非 `127.0.0.1`），确保容器间可达

### 3.3 DuckDB 连接管理

DuckDB 不允许同一进程对同一文件同时存在 `read_only=True` 和 `read_only=False` 的连接。连接管理策略：

| 进程 | 策略 | 说明 |
|------|------|------|
| daemon | 所有连接 `read_only=False` | `get_read_connection()` 在非 readonly 模式下也返回读写连接，避免混用 |
| streamlit | 不访问 DB | 通过 HTTP API，无 DuckDB 连接 |

```python
def get_read_connection(db_path):
    """非只读模式下统一返回 read_write 连接，避免混用"""
    return _open(db_path, readonly=is_readonly_mode())
```

---

## 四、模块详细说明

### 4.1 data_tools.py — 数据获取

**职责**：与 BaoStock 交互，提供股票列表、指数列表、日线、指数日线、分钟线五个核心接口。不依赖任何数据库或上层模块。

| 接口 | 签名 | 说明 |
|------|------|------|
| `fetch_stock_list` | `() -> pd.DataFrame` | 查询全市场 A 股在市股票列表（type='1'） |
| `fetch_index_list` | `() -> pd.DataFrame` | 查询全市场指数列表（type='2'） |
| `fetch_daily` | `(stock_list, start_date, end_date, adjustflag) -> pd.DataFrame` | 批量拉取多只股票日线 |
| `fetch_daily_single` | `(code, code_name, start_date, end_date, adjustflag) -> pd.DataFrame` | 单只股票日线（便捷接口） |
| `fetch_index_daily` | `(index_list, start_date, end_date, adjustflag) -> pd.DataFrame` | 批量拉取多只指数日线 |
| `fetch_minute` | `(stock_list, start_date, end_date, frequency, adjustflag) -> pd.DataFrame` | 批量拉取分钟线（5/15/30/60分钟） |
| `baostock_session` | `() -> Generator` | BaoStock 登录/登出上下文管理器 |

**内部机制**：
- `_query_with_retry`：单只股票查询重试封装（默认 2 次重试）
- 批量拉取时使用流式写入（flush_size=10000），避免大 DataFrame 内存溢出
- 进度显示使用 `enumerate` 生成连续计数，不受 DataFrame 过滤后行号不连续影响

**依赖**：baostock, pandas

---

### 4.2 duckdb_tools.py — 数据库管理

**职责**：管理 DuckDB 四张核心表，实现「缺失数据透明补拉」，提供大盘概览数据。

#### 核心表结构

| 表名 | 主键 | 说明 |
|------|------|------|
| `stock_info` | code | A 股 + 指数基础信息（type 列区分：'1'=股票，'2'=指数），每日开盘前全量刷新 |
| `daily_bar` | date + code + adjustflag | 股票日线 K 线（含成交量/成交额/换手率/估值等） |
| `index_daily_bar` | date + code + adjustflag | 指数日线 K 线（无 turn/tradestatus/估值字段，有 pct_chg） |
| `minute_bar` | date + time + code + frequency + adjustflag | 分钟线 K 线 |

#### 主要接口

| 接口 | 说明 |
|------|------|
| `get_connection(db_path)` | 获取读写连接（短连接模式） |
| `get_read_connection(db_path)` | 获取只读连接（非 readonly 模式下统一返回读写连接，避免 DuckDB 同进程连接配置冲突） |
| **stock_info** | |
| `upsert_stock_info(df, db_path)` | 全量刷新 stock_info 中股票部分（DELETE + INSERT） |
| `upsert_index_info(db_path)` | 刷新 stock_info 中指数部分（DELETE type='2' + INSERT），不影响股票条目 |
| `get_stock_info(code, db_path)` | 查询 stock_info（含股票+指数） |
| `delete_stock_info(code, db_path)` | 删除 stock_info（code='ALL' 全删） |
| **daily_bar** | |
| `insert_daily(df, db_path)` | 股票日线数据写入（INSERT OR REPLACE，分块 50000 条，try/finally 保关闭） |
| `get_daily(code, start_date, end_date, ...)` | **核心接口**：查询股票日线 + 透明补拉 |
| `delete_daily(code, ...)` | 删除股票日线数据 |
| `query_daily(sql, params, db_path)` | 执行任意 SELECT SQL 查询日线 |
| **index_daily_bar** | |
| `insert_index_daily(df, db_path)` | 指数日线数据写入（INSERT OR REPLACE，分块 50000 条） |
| `get_index_daily(code, start_date, end_date, ...)` | 查询指数日线（纯读，不带透明补拉） |
| **minute_bar** | |
| `insert_minute(df, frequency, db_path)` | 分钟线写入 |
| `get_minute(code, ...)` | 查询分钟线 + 透明补拉 |
| `delete_minute(code, ...)` | 删除分钟线数据 |
| `query_minute(sql, params, db_path)` | 执行任意 SELECT SQL 查询分钟线 |
| **概览 & 统计** | |
| `get_market_overview(db_path)` | 大盘概览（三大指数行情 + 全市场涨跌分布） |
| `table_stats(db_path)` | 核心表行数与日期范围 |

#### 数据库路径

默认 `项目根目录/stock_data.duckdb`，支持环境变量 `QUANT_DB_PATH` 覆盖：

```python
_DEFAULT_DB_PATH = os.environ.get(
    "QUANT_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "stock_data.duckdb"),
)
```

**连接管理**：短连接模式，每次操作开新连接用完关闭。所有写操作使用 `try/finally` 确保连接释放。

**依赖**：duckdb, pandas, data_tools

---

### 4.3 index_tools.py — 技术指标计算

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

**依赖**：duckdb, numpy, pandas, duckdb_tools

---

### 4.4 strategy.py — 策略引擎

**职责**：策略发现 + 调度 + 缓存管理 + Screener 动态选股。纯框架，不含任何计算逻辑。

#### 核心接口

| 接口 | 说明 |
|------|------|
| `list_strategies()` | 扫描 strategies/ 目录，返回所有策略定义 |
| `get_strategy(name)` | 按名获取策略定义 |
| `get_compute_fn(name)` | 按名获取计算函数 |
| `ensure_strategy_table(strategy)` | 按 columns + primary_key 建表 |
| `compute_strategy(strategy, date)` | 执行策略计算，返回写入条数 |
| `write_strategy_result(name, result_df)` | 将 DataFrame 写入策略表（upsert by primary_key） |
| `query_strategy(name, date, force_compute, **params_override)` | **核心入口**：缓存判断 → 计算或查表 |
| `query_screener(strategy, date, filters, ...)` | 动态条件选股 |
| `strategy_info(name)` | 获取策略元信息（不触发计算） |

#### 策略模块约定

每个策略文件导出 `STRATEGY` 字典 + `compute(date, **kwargs)` 函数。框架自动注入 `name`（文件名去 .py）和 `table`（`strategy_` + name）。

`param_ui` 字段支持的控件类型：

| type | 渲染为 | 示例 |
|------|--------|------|
| `"number"` | `st.number_input` | `{"type": "number", "min": 0, "max": 100, "label": "阈值"}` |
| `"select"` | `st.selectbox` | `{"type": "select", "options": ["monthly", "weekly"], "label": "频率"}` |
| `"text"` | `st.text_input` | `{"type": "text", "default": "", "label": "搜索"}` |
| `"button"` | `st.button` | `{"type": "button", "label": "📢 推送到钉钉", "help": "...", "endpoint": "/notify/push_signals"}` |

`type: "button"` 的值不会传给 `compute()`，点击后通过 `endpoint` 调用 daemon API。

**依赖**：duckdb, pandas, duckdb_tools

---

### 4.5 watchlist_tools.py — 自选股管理

**职责**：自选股组合的 CRUD + 等权指数计算 + 板块估值统计。

| 接口 | 说明 |
|------|------|
| `list_groups()` | 所有自选组合名称列表 |
| `create_group(name, description)` | 创建自选组合 |
| `delete_group(name)` | 删除自选组合 |
| `get_group_summary()` | 所有组合摘要（名称 + 股票数 + 创建时间） |
| `get_stock_codes(group)` | 某组合的股票代码列表 |
| `get_group_info(group)` | 组合完整信息 |
| `get_group_overview(group)` | 组合内个股最新行情 |
| `add_stock(group, code, note)` | 向组合添加股票 |
| `remove_stock(group, code)` | 从组合移除股票 |
| `calc_group_index(group, start_date, end_date)` | 计算组合等权指数 |
| `calc_group_valuation(group)` | 组合估值统计（PE/PB 均值/中位） |

**数据存储**：自选股数据存储在 DuckDB `_watchlist_groups` 和 `_watchlist_stocks` 两张表中。

**依赖**：duckdb, pandas, duckdb_tools

---

### 4.6 db_daemon.py — 守护进程

**职责**：后台执行定时任务 + 提供 FastAPI HTTP 接口（20+ 数据查询 API）。使用 lifespan 上下文管理器管理 TaskManager 生命周期。

#### TaskManager 类

| 方法 | 说明 |
|------|------|
| `submit(task_id, fn, **kwargs)` | 提交一次性任务（后台线程执行） |
| `schedule(task_id, fn, cron, **kwargs)` | 注册定时任务，cron 格式 `"HH:MM"` 或 `"day_of_week HH:MM"` |
| `run_now(task_id, **kwargs)` | 手动触发已注册任务 |
| `status(task_id)` / `result(task_id)` / `error(task_id)` | 查询任务状态/结果/错误 |
| `all_tasks()` / `get_jobs()` | 所有任务状态 / 定时任务列表 |

#### 定时任务

| 任务 | task_id | 时间 | 逻辑 |
|------|---------|------|------|
| 刷新股票+指数列表 | `refresh_stock_info` | 08:30 | 全量更新 stock_info（股票+指数） |
| 收盘股票日线拉取 | `post_market_fetch` | 17:40 | 全市场股票日线批量拉取 + 写入 |
| 指数日线拉取 | `fetch_index_daily` | 18:10 | 全市场指数日线拉取 + 写入 |
| BS 区间信号推送 | `push_bs_zone_signals` | 18:30 | 计算当日 BS 区间策略信号，推送到钉钉群（先死叉后金叉，两条卡片消息） |

#### HTTP API（0.0.0.0:8502）

**任务管理**：

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 健康检查 |
| GET | `/status` | 所有任务当前状态 |
| GET | `/jobs` | 已注册定时任务列表 |
| POST | `/run_now/{task_id}` | 手动触发指定任务，body 可传 `params: dict` |
| GET | `/result/{task_id}` | 获取任务结果 |
| GET | `/last_fetch` | 获取最近一次收盘拉取的结果 |

**数据查询**（前后端分离核心）：

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/query/table_stats` | 侧边栏：数据库核心表统计 |
| GET | `/query/market_overview` | 大盘概览：三大指数 + 涨跌分布 |
| GET | `/query/stock_info` | 股票/指数基础信息（支持 code 过滤） |
| GET | `/query/daily` | 个股日线查询（daemon 端自动补拉） |
| GET | `/query/index_daily` | 指数日线查询 |
| GET | `/query/db_tables` | 数据库所有表名 + 行数 + 日期范围 |

**策略查询**：

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/strategy/list` | 策略列表 |
| GET | `/strategy/info` | 策略元信息 |
| POST | `/strategy/query` | 查询策略结果（含自动计算 + 写入） |

**自选股**：

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/watchlist/groups` | 所有自选组合名称 |
| POST | `/watchlist/group` | 创建自选组合 |
| DELETE | `/watchlist/group` | 删除自选组合 |
| GET | `/watchlist/summary` | 所有组合摘要 |
| GET | `/watchlist/stock_codes` | 某组合的股票代码列表 |
| GET | `/watchlist/group_info` | 组合完整信息 |
| GET | `/watchlist/group_overview` | 组合内个股最新行情 |
| POST | `/watchlist/add_stock` | 向组合添加股票 |
| POST | `/watchlist/remove_stock` | 从组合移除股票 |
| GET | `/watchlist/calc_index` | 计算组合等权指数 |
| GET | `/watchlist/calc_valuation` | 组合估值统计 |

**依赖**：uvicorn, apscheduler, fastapi, duckdb_tools, strategy, watchlist_tools, notify, data_tools（延迟导入）

**钉钉 Stream**：daemon 启动时会在后台线程启动 DingTalk Stream 连接，接收群内 @机器人 的指令（如"信号"、"群ID"等），不依赖此连接也能正常推送。

---

### 4.6.1 notify.py — 钉钉推送

**职责**：基于 `dingtalk-stream` SDK，推送 BS 区间策略信号到钉钉群，支持群内 @机器人 交互指令。

#### 核心接口

| 接口 | 说明 |
|------|------|
| `push_signals(date=None)` | **核心推送接口**：计算 BS 区间信号，先推死叉卡片，再推金叉卡片。默认当天 |
| `start_dingtalk_stream()` | 启动钉钉 Stream 长连接（后台线程），接收群内 @机器人 指令 |

#### 配置（环境变量）

```bash
export DINGTALK_CLIENT_ID="ding..."               # 应用 AppKey / robotCode
export DINGTALK_CLIENT_SECRET="..."               # 应用 AppSecret
export DINGTALK_OPEN_CONVERSATION_ID="cid..."     # 目标群的 openConversationId
```

也可以在项目根目录创建 `.env` 文件（已加入 `.gitignore`），daemon 启动时自动加载。docker-compose 通过 `${VAR:-}` 语法引用。

#### 群内指令（需 Stream 连接）

| 指令 | 回复 |
|------|------|
| @机器人 **群ID** / **群号** | 返回 openConversationId |
| @机器人 **信号** / **金叉** / **死叉** | 分两条卡片返回当日金叉和死叉信号 |

#### HTTP API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/notify/push_signals` | 手动推送当天信号，body 可选 `{"date": "2025-05-19"}` |

**依赖**：dingtalk-stream, strategies/bs_zone

---

### 4.7 daemon_client.py — HTTP 客户端

**职责**：Streamlit 侧通过此模块与 db_daemon 通信，实现前后端分离。**所有数据库访问都通过 daemon API，Streamlit 不直连 DuckDB。**

#### 连接配置

```python
DAEMON_URL = os.environ.get("DAEMON_URL", "http://127.0.0.1:8502")
TIMEOUT = 10       # 普通查询
TIMEOUT_SLOW = 60   # 可能触发补拉/计算的查询
```

- 本地开发：默认 `http://127.0.0.1:8502`
- Docker 部署：通过 `DAEMON_URL=http://daemon:8502` 环境变量覆盖

#### 完整接口

| 接口 | 说明 | 对应 daemon API |
|------|------|-----------------|
| **守护进程管理** | | |
| `is_alive()` | 守护进程是否在线 | GET /health |
| `get_status()` | 获取所有任务状态 | GET /status |
| `get_jobs()` | 获取定时任务列表 | GET /jobs |
| `run_now(task_id, params)` | 手动触发任务 | POST /run_now/{task_id} |
| `get_last_fetch()` | 获取最近拉取记录 | GET /last_fetch |
| **数据查询** | | |
| `table_stats()` | 数据库核心表统计 | GET /query/table_stats |
| `get_market_overview()` | 大盘概览 | GET /query/market_overview |
| `get_stock_info(code)` | 股票/指数基础信息 | GET /query/stock_info |
| `get_daily(code, start_date, end_date, ...)` | 个股日线查询 | GET /query/daily |
| `get_index_daily(code, start_date, end_date, ...)` | 指数日线查询 | GET /query/index_daily |
| `get_db_tables()` | 数据库表概览 | GET /query/db_tables |
| **策略查询** | | |
| `list_strategies()` | 策略列表 | GET /strategy/list |
| `strategy_info(name)` | 策略元信息 | GET /strategy/info |
| `query_strategy(name, date, **params)` | 查询策略结果 | POST /strategy/query |
| **自选股** | | |
| `list_groups()` | 所有组合名称 | GET /watchlist/groups |
| `create_group(name, desc)` | 创建组合 | POST /watchlist/group |
| `delete_group(name)` | 删除组合 | DELETE /watchlist/group |
| `get_group_summary()` | 所有组合摘要 | GET /watchlist/summary |
| `get_stock_codes(group)` | 某组合股票代码 | GET /watchlist/stock_codes |
| `get_group_info(group)` | 组合完整信息 | GET /watchlist/group_info |
| `get_group_overview(group)` | 组合个股行情 | GET /watchlist/group_overview |
| `add_stock(group, code, note)` | 添加股票 | POST /watchlist/add_stock |
| `remove_stock(group, code)` | 移除股票 | POST /watchlist/remove_stock |
| `calc_group_index(group, start, end)` | 计算等权指数 | GET /watchlist/calc_index |
| `calc_group_valuation(group)` | 组合估值统计 | GET /watchlist/calc_valuation |
| **推送** | | |
| `push_signals(date=None)` | 推送 BS 区间信号到钉钉 | POST /notify/push_signals |

**容错**：所有函数在连接失败时返回空数据（空 DataFrame / 空列表 / None），不抛异常。

**依赖**：requests, pandas

---

### 4.8 streamlit_app.py — 前端

**职责**：7 页面看板，**只通过 daemon_client 获取数据**，不直连数据库。

| 页面 | 功能 | 数据来源 |
|------|------|----------|
| 📊 大盘概览 | 三大指数行情卡片 + 涨跌分布图 | `dc.get_market_overview()` |
| ⭐ 自选股 | 组合管理 + 等权指数 + 估值 + 成分股行情 | `dc.list_groups()` / `dc.calc_group_index()` 等 |
| 🔍 个股查询 | 日线 + 技术指标 + 收盘价折线图 | `dc.get_daily()` + `it.calc_indicators()` |
| 📋 股票列表 | 全市场股票搜索浏览 + 刷新数据源 | `dc.get_stock_info()` |
| 📑 指数列表 | 全市场指数搜索浏览 + 刷新数据源 | `dc.get_stock_info()` |
| 📈 因子分析 | 策略选择 → 参数配置 → 查询结果 → 手动推送（BS Zone） | `dc.list_strategies()` / `dc.query_strategy()` / `dc.push_signals()` |
| 🔧 数据库维护 | 定时任务状态 + 手动触发 + 最近拉取记录 + 表概览 | `dc.get_jobs()` / `dc.run_now()` 等 |

**侧边栏**：数据库状态 `dc.table_stats()` + 守护进程在线状态 `dc.is_alive()`

**依赖**：streamlit, pandas, plotly, numpy, daemon_client, index_tools

**不依赖**：duckdb_tools, strategy, watchlist_tools（前端零直连数据库）

---

## 五、模块依赖关系

```
┌────────────────────────────────────┐
│       streamlit_app.py (8501)      │
│  import: daemon_client, index_tools│
│  不 import: duckdb_tools ✗         │
└──────────────┬─────────────────────┘
               │ HTTP
               ▼
┌────────────────────────────────────┐
│        db_daemon.py (8502)         │
│  import: duckdb_tools, strategy,   │
│          watchlist_tools, notify,   │
│          data_tools (延迟导入)      │
└──────────────┬─────────────────────┘
               │
       ┌───────┼───────────┬────────────┐
       ▼       ▼           ▼            ▼
┌──────────┐ ┌────────┐ ┌──────────┐ ┌────────┐
│ DuckDB    │ │strategy│ │watchlist │ │notify  │
│ 数据库    │ │  .py   │ │_tools.py │ │  .py   │
└──────────┘ └───┬────┘ └────┬─────┘ └───┬────┘
                 │           │           │
                 ▼           ▼           ▼
            ┌─────────────────────────────────┐
            │         duckdb_tools.py         │
            └────────┬────────────────────────┘
                     │
                     ▼
              ┌──────────────┐
              │ data_tools.py │
              │ (BaoStock)   │
              └──────────────┘
```

---

## 六、第三方依赖

| 库 | 用途 |
|----|------|
| baostock | A 股数据源（股票/指数日线/分钟线/列表） |
| duckdb | 嵌入式列式数据库 |
| pandas | 数据处理核心 |
| numpy | 指标计算（数组运算） |
| streamlit | Web 前端框架 |
| plotly | 图表（环状图/直方图） |
| fastapi | 守护进程 HTTP API |
| uvicorn | ASGI 服务器 |
| apscheduler | 定时任务调度 |
| requests | HTTP 客户端（daemon_client） |
| dingtalk-stream | 钉钉 Stream 模式 SDK（推送 + 接收群内消息） |
| backtrader | 回测引擎（待集成） |

---

## 七、BaoStock 数据字段踩坑

| 字段 | 文档说明 | 实际情况 | 影响 |
|------|----------|----------|------|
| `amount` | 千元 | **元**（通过 `amount/close ≈ volume` 验证） | 流值反推公式不能用千元换算 |
| `turn` | 换手率 | 百分比数值（2.5 = 2.5%），不是小数 | 反推需要 ×100 |
| 流通市值反推 | — | 必须用 `close × volume × 100 / turn` | 用 amount 反推是 VWAP 口径，偏差 1~3% |
| 市值变化 vs 资金流入 | — | 市值变化 ≠ 资金流入 | 资金净流入 = amount × 涨跌幅 |
| 指数日线字段 | — | 无 turn/tradestatus/isST/估值字段，但有 pctChg | 指数需单独建表 `index_daily_bar` |

---

## 八、部署指南

### 本地开发

```bash
# 终端 1：启动守护进程
cd /home/logiczr/quant
python db_daemon.py

# 终端 2：启动 Streamlit
cd /home/logiczr/quant
streamlit run streamlit_app.py
```

### Docker 部署（本地构建 → NAS）

**方式一：直接构建（NAS 需要联网）**

```bash
cd /docker/quant3
docker-compose up -d --build
```

**方式二：离线部署（本地构建镜像，传输到 NAS 加载）**

适合 NAS 网络条件不佳的情况，在本地电脑完成构建和依赖下载，镜像导出后上传到 NAS 直接运行。

```bash
# ① 本地电脑：构建镜像
cd /path/to/quant
docker build -t quant3:latest .

# ② 导出镜像为 tar 文件
docker save quant3:latest -o quant3.tar

# ③ 传输到 NAS（任选一种方式）
scp quant3.tar your-nas:/docker/quant3/
# 或通过 SMB/NFS/WebDAV 拷贝

# ④ NAS 上：加载镜像
ssh your-nas
cd /docker/quant3
docker load -i quant3.tar

# ⑤ 启动服务
docker-compose up -d
```

> **注意**：`docker-compose.yml` 中需将 `build: .` 替换为 `image: quant3:latest`，否则 `docker-compose up` 会尝试重新构建。
>
> 每次代码更新后重复 ①~④ 即可更新到最新版本。

**访问地址**：
- Streamlit 前端：`http://<NAS-IP>:8501`
- Daemon API：`http://<NAS-IP>:8502`

---

## 九、相关文档

| 文档 | 路径 | 说明 |
|------|------|------|
| 项目主文档 | `docs/PROJECT_DOC.md` | 本文件 |
| 回测引擎设计 | `docs/BACKTEST_DESIGN.md` | backtrader 集成方案 |

---

## 十、待完善项

| 优先级 | 内容 | 状态 |
|--------|------|------|
| P0 | 回测引擎实现（backtest_engine.py + 3 个示例策略 + Streamlit 页面） | 设计完成，待实现 |
| P1 | 日线 K 线图（当前只有收盘价折线） | 待实现 |
| P1 | `fetch_index_daily` 中 logger 每条记录输出（仿照 fetch_daily） | 待完善 |
| P2 | indicators 表持久化（EMA/MACD/KDJ 等历史依赖型指标） | 待实现 |
| P2 | 策略结果缓存失效策略（收盘判断 + stale 状态） | 待设计 |
| P2 | 回测结果持久化（backtest_result 表） | 待实现 |
| P3 | test_duckdb.py 中 `check_all_daily_gaps` 函数不存在 | 待修复/实现 |
| P3 | 回测与 strategy.py 打通（选股池 → 批量回测） | 远期 |
| P3 | 多股组合回测（Portfolio 级别） | 远期 |
