# CODEBUDDY.md

This file provides guidance to CodeBuddy Code when working with code in this repository.

## Project Overview

A-stock quantitative analysis platform built on **BaoStock + DuckDB + Streamlit + FastAPI**. Chinese A-share market data pipeline with three layers: data (fetch + storage + indicators), strategy (Python modules + engine), and presentation (daemon + Streamlit frontend). Front-back separation: Streamlit talks to FastAPI daemon via HTTP; indicators are calculated client-side.

## Commands

### Environment Setup
```bash
source venv/bin/activate
pip install -r requirements.txt
```

### Run the Application (two processes)
```bash
# Terminal 1: Start the daemon (FastAPI on port 8502)
python db_daemon.py

# Terminal 2: Start the frontend (Streamlit on port 8501)
streamlit run streamlit_app.py
```

### Docker Deployment
```bash
docker-compose up -d
# daemon: port 8502, owns DB volume
# streamlit: port 8501, no DB access (uses DAEMON_URL)
```

### Run Tests
No formal test framework. Standalone scripts requiring BaoStock network + DuckDB:
```bash
python test_duckdb.py      # DuckDB CRUD + lazy pull tests
python test_datatools.py   # BaoStock industry data fetch test
```

### Run Individual Modules (self-test mode)
Each core module has `if __name__ == "__main__"` blocks:
```bash
python data_tools.py       # Fetch stock list + daily + minute data
python duckdb_tools.py     # DB connection, lazy pull, CRUD ops
python index_tools.py      # MACD/KDJ/BOLL calculations
python strategy.py         # Strategy registry + compute
```

## Architecture

### Module Dependency Graph
```
streamlit_app.py ──┬── daemon_client.py ──HTTP──→ db_daemon.py (port 8502)
                   │                              ├── notify.py ──DingTalk──→ bs_zone signals
                   │                              ├── wx_notify.py ──WeChat──→ bs_zone signals
                   │                              └── watchlist_tools.py ──JSON──→ data/watchlist.json
                   ├── index_tools.py ──→ duckdb_tools.py ──→ data_tools.py ──→ BaoStock
                   └── strategy.py ────→ duckdb_tools.py
```
Note: Streamlit imports both `daemon_client` AND `index_tools`. Indicators are calculated client-side, not via the daemon.

### Three-Layer Design

**Data Layer** (fetch → store → indicators):
- `data_tools.py` — BaoStock API wrapper. Stateless, no DB dependency. Key functions: `fetch_stock_list()`, `fetch_daily()`, `fetch_daily_single()`, `fetch_minute()`, `fetch_index_list()`, `fetch_index_daily()`, `fetch_trade_dates()`. Uses `baostock_session()` context manager with `socket_timeout` (default 15s — BaoStock raw sockets have no default timeout, can block indefinitely). Includes retry logic (`_query_with_retry`, max 2 retries). `fetch_daily()` has additional reconnect: on failure → `logout() → sleep(10) → login() → retry once`.
- `duckdb_tools.py` — DuckDB CRUD + **transparent lazy pull**. When `get_daily()`/`get_minute()` is called, checks if local data covers the requested date range; if not, auto-fetches from BaoStock and caches. Connection model: short-lived (open/close per operation). Five core tables (see schema below). Includes trade calendar system: `get_trade_dates()`, `is_trade_date()`, `find_nearest_trade_date()`, `check_daily_integrity()`. Auto-fills calendar from BaoStock when empty. Supports `DB_READONLY` mode (skip writes, skip strategy computation).
- `index_tools.py` — 14 technical indicators (MACD, KDJ, BOLL, RSI, CCI, WR, ATR, MA, EMA, OBV, VOL_MA, DMA, VR, HV). Configurable via `IndicatorConfig` dataclass. Unified entry: `calc_indicators(df, indicators="all")` and `calc_batch(codes, ...)` for multi-stock.

**Strategy Layer**:
- `strategy.py` — Strategy engine (discovery, caching, query dispatch). Auto-discovers modules in `strategies/`. Each exports `STRATEGY` dict (metadata) + `compute()` function. Key flow: `query_strategy(name, date)` → check cache in `_strategy_meta` → if miss, call `compute(date, **params)` → write to DB → return DataFrame. Also supports `screener` type (dynamic SQL, no persistence). Skips computation in `DB_READONLY` mode.
- `strategies/*.py` — 7 strategy modules. Each exports `STRATEGY = {description, columns, primary_key, params, param_ui}` and `def compute(date, **kwargs) -> pd.DataFrame`. `param_ui` defines controls: `"number"`, `"select"`, `"text"`, `"button"`. Strategy table schema: `bs_zone.py` and `n_day_return_rank.py` implement `_ensure_table()` for schema migration (drop + recreate on mismatch); other strategies use `ensure_strategy_table()` which only creates if not exists.
  - `bs_zone.py` — Buy/sell lines (EMA + SLOPE), guide/boundary lines. 250-day warmup. Has `query_signals()` for push notifications.
  - `market_cap_rank.py` — Flow cap ranking. No params.
  - `market_cap_growth.py` — Uses previous day's flow shares to avoid "price drops but cap rises" anomaly.
  - `market_cap_growth_rate.py` — Parametric: `period_days` (default 20).
  - `ff3_factor.py` — Fama-French 3-factor. 2x3 independent sort (Size x BM). Requires `sh.000300` in `index_daily_bar` for MKT factor.
  - `ff3_regression.py` — OLS regression on FF3 factors. Supports individual stocks and watchlist groups as targets.
  - `n_day_return_rank.py` — Uses DuckDB `product()` for cumulative return.

**Presentation Layer**:
- `db_daemon.py` — FastAPI daemon (port 8502) with `TaskManager` (APScheduler + event-driven orchestration). Two cron entry points: `refresh_stock_info` (08:30), `post_market_fetch` (17:40); downstream tasks triggered via `on_done`/`on_fail` events instead of independent cron schedules. Event flow: `post_market_fetch` → `daily_bar.fetched` → (fetch_index_daily + push_bs_zone_signals in parallel). TaskManager supports `on(event, handler)` for subscription and `_emit(event, data)` for publishing. `_setup_tasks()` registers cron entries + event subscriptions at startup. No resource mutex needed — event ordering guarantees BaoStock tasks never overlap. Loads `.env` via `python-dotenv` at startup. Uses lifespan context manager (importing the module doesn't start scheduler). Performance: `_df_response()` uses `df.to_json()` + pre-serialized JSON Response instead of FastAPI's default serialization.
- `daemon_client.py` — HTTP client for Streamlit → daemon. Uses `DAEMON_URL` env var (default `http://127.0.0.1:8502`), timeout 3s. Error pattern: all functions catch exceptions and return empty data (empty DataFrame / empty list / None / False), never raises to UI.
- `streamlit_app.py` — 7-page dashboard: Market Overview, Watchlist, Stock Query, Stock List, Index List, Factor Analysis (strategy), DB Maintenance.
- `notify.py` — DingTalk notifications via `dingtalk-stream` SDK. Pushes BS Zone signals as interactive cards. DingTalk Stream handler (`BSZoneBotHandler`) supports commands: "群ID"/"群号" returns conversation ID, "信号"/"金叉"/"死叉" returns BS Zone signals. Credentials from env vars.
- `wx_notify.py` — WeChat enterprise push for BS Zone signals. Two channels: intelligent bot (aibot SDK, WebSocket) and group bot webhook (HTTP POST). Auto-splits long messages to platform limits (18KB / 3.8KB).
- `watchlist_tools.py` — Self-selected portfolio management. CRUD for groups/stocks, equal-weight index calculation (base 1000), valuation stats (PE/PB mean/median). Storage: `data/watchlist.json` (not DuckDB). Key functions: `list_groups()`, `create_group()`, `add_stock()`, `calc_group_index()`, `calc_group_valuation()`, `get_group_overview()`.

### Daemon HTTP API

**Task Management**:
- `GET /health` — Health check
- `GET /status` — All task statuses
- `GET /jobs` — Scheduled task list
- `POST /run_now/{task_id}` — Manually trigger task (body: optional `params: dict`)
- `GET /result/{task_id}` — Task result
- `GET /last_fetch` — Last post-market fetch result
- `GET /extract/events` — Registered event subscriptions

**Data Queries**:
- `GET /query/trade_dates` — Trade calendar
- `GET /query/is_trade_date?date=` — Check trading day
- `GET /query/nearest_trade_date?date=&direction=` — Find nearest trading day
- `POST /query/fill_trade_calendar` — Auto-fill calendar from BaoStock
- `GET /query/table_stats` — Core table stats
- `GET /query/check_daily_integrity?target_date=&adjustflag=` — Daily data integrity
- `GET /query/market_overview` — Market overview (3 indices + distribution)
- `GET /query/stock_info?code=` — Stock/index info
- `GET /query/daily?code=&start_date=&end_date=&adjustflag=&auto_fetch=` — Stock daily (with auto-fetch)
- `GET /query/index_daily?code=&start_date=&end_date=&adjustflag=` — Index daily
- `GET /query/db_tables` — All DB tables + row counts

**Strategy Queries**:
- `GET /strategy/list` — Strategy list
- `GET /strategy/info?name=` — Strategy metadata
- `POST /strategy/query` — Query strategy (body: name, date, force_compute, params)

**Watchlist**:
- `GET /watchlist/groups` — Group names
- `POST /watchlist/group` — Create group (body: name, description)
- `DELETE /watchlist/group?name=` — Delete group
- `GET /watchlist/summary` — All groups summary
- `GET /watchlist/stock_codes?group=` — Stocks in group
- `GET /watchlist/group_info?group=` — Full group info
- `GET /watchlist/group_overview?group=` — Latest quotes for group stocks
- `POST /watchlist/add_stock` — Add stock (body: group, code, note)
- `POST /watchlist/remove_stock` — Remove stock (body: group, code)
- `GET /watchlist/calc_index?group=&start_date=&end_date=` — Equal-weight index
- `GET /watchlist/calc_valuation?group=` — Group valuation stats

**Notifications**:
- `POST /notify/push_signals` — Push BS Zone signals to DingTalk

### DuckDB Schema
| Table | Primary Key | Notes |
|-------|-------------|-------|
| `stock_info` | code | Full refresh (DELETE + INSERT) |
| `daily_bar` | date + code + adjustflag | UPSERT (INSERT OR REPLACE) |
| `minute_bar` | date + time + code + frequency + adjustflag | UPSERT |
| `index_daily_bar` | date + code + adjustflag | Index daily bars (fewer fields than daily_bar — no turn/tradestatus/valuation) |
| `trade_calendar` | date | Trading calendar (date + is_trading boolean). Auto-filled from BaoStock. |
| `strategy_*` | defined per module | Auto-created from STRATEGY columns |
| `_strategy_meta` | name | Strategy metadata (last_date cache) |

Database file: `stock_data.duckdb` (path overridden by `QUANT_DB_PATH` env var).

### Environment Variables
| Variable | Default | Used By | Purpose |
|----------|---------|---------|---------|
| `QUANT_DB_PATH` | `./stock_data.duckdb` | duckdb_tools.py | Override DB file location (Docker) |
| `DAEMON_URL` | `http://127.0.0.1:8502` | daemon_client.py | Daemon address (Docker: `http://daemon:8502`) |
| `DB_READONLY` | (not set) | duckdb_tools.py, strategy.py | Set `1`/`true`/`yes` for read-only mode |
| `DINGTALK_CLIENT_ID` | (empty) | notify.py | DingTalk AppKey |
| `DINGTALK_CLIENT_SECRET` | (empty) | notify.py | DingTalk AppSecret |
| `DINGTALK_OPEN_CONVERSATION_ID` | (empty) | notify.py | DingTalk target group ID |

### Docker Architecture
Two containers via `docker-compose.yml`:
- `daemon`: owns DB volume (`/quant3/data`), port 8502, sets `QUANT_DB_PATH`
- `streamlit`: no DB volume mount, port 8501, sets `DAEMON_URL=http://daemon:8502`
- Streamlit container physically cannot access DuckDB directly — all data goes through daemon HTTP API.

### Key Conventions
- Stock codes follow BaoStock format: `sh.600519`, `sz.002149` (exchange prefix + code)
- `adjustflag`: `"1"` = backward adjustment, `"2"` = forward, `"3"` = no adjustment
- Column naming: BaoStock camelCase (`pctChg`, `isST`, `peTTM`) → DB snake_case (`pct_chg`, `is_st`, `pe_ttm`)
- Indicator output columns use UPPER_CASE: `DIF`, `DEA`, `MACD_SIGNAL`, `KDJ_K`, `BOLL_UP`, etc.
- All dates in `YYYY-MM-DD` format
- `stra.py` is an experimental script, not part of the main flow
- `last_fetch.json` tracks last post-market fetch result with timestamp

### BaoStock Data Gotchas
- `amount` is in yuan (not thousands as docs claim)
- `turn` is a percentage number (2.5 = 2.5%), not a decimal
- Market cap formula: `close × volume × 100 / turn / 1e8`
- Minute-line data limited to recent 3 months
- BaoStock uses raw sockets with no default timeout — always use `baostock_session(socket_timeout=15.0)`
