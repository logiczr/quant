# CODEBUDDY.md

This file provides guidance to CodeBuddy Code when working with code in this repository.

## Project Overview

A-stock quantitative analysis platform built on **BaoStock + DuckDB + Streamlit + FastAPI**. Chinese A-share market data pipeline with three layers: data (fetch + storage + indicators), strategy (JSON-defined + engine), and presentation (daemon + Streamlit frontend).

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

### Run Tests
No formal test framework is configured. Tests are standalone scripts that require BaoStock network access and the DuckDB database:
```bash
python test_duckdb.py      # DuckDB CRUD + lazy pull tests
python test_index.py       # Indicator calculation tests
python test_datatools.py   # BaoStock industry data fetch test
```

### Run Individual Modules (self-test mode)
Each core module has a `if __name__ == "__main__"` block for ad-hoc testing:
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
                   ├── strategy.py ────→ duckdb_tools.py ────→ data_tools.py ──→ BaoStock
                   └── index_tools.py ──→ duckdb_tools.py
```

### Three-Layer Design

**Data Layer** (fetch → store → indicators):
- `data_tools.py` — BaoStock API wrapper. Stateless, no DB dependency. Key functions: `fetch_stock_list()`, `fetch_daily()`, `fetch_daily_single()`, `fetch_minute()`. Uses `baostock_session()` context manager for login/logout. Includes retry logic (`_query_with_retry`, max 2 retries).
- `duckdb_tools.py` — DuckDB CRUD + **transparent lazy pull**. When `get_daily()`/`get_minute()` is called, it checks if local data covers the requested date range; if not, auto-fetches from BaoStock and caches to DB. Connection model: short-lived connections (open/close per operation). Three core tables: `stock_info`, `daily_bar`, `minute_bar`.
- `index_tools.py` — 14 technical indicators (MACD, KDJ, BOLL, RSI, CCI, WR, ATR, MA, EMA, OBV, VOL_MA, DMA, VR, HV). Configurable via `IndicatorConfig` dataclass. Unified entry: `calc_indicators(df, indicators="all")` and `calc_batch(codes, ...)` for multi-stock.

**Strategy Layer**:
- `strategy.py` — Engine + compute functions. Strategies defined as JSON files in `strategy/` directory. Two compute modes: `python` (calls `@register_compute` decorated functions) and `sql` (executes SQL directly). Key flow: `query_strategy(name, date)` checks `last_date` in JSON → if stale, runs compute → writes to strategy table → updates `last_date` in JSON → returns result. Also supports `screener` type (dynamic SQL generation, no persistence).
- `strategy/*.json` — Strategy definitions with `name`, `table`, `columns`, `primary_key`, `compute`, `read_sql`, `params`, `last_date`.

**Presentation Layer**:
- `db_daemon.py` — FastAPI daemon (port 8502) with `TaskManager` (APScheduler). Two scheduled tasks: `refresh_stock_info` (08:30), `post_market_fetch` (17:00). Uses lifespan context manager so importing the module doesn't start the scheduler.
- `daemon_client.py` — Thin HTTP client for Streamlit to talk to the daemon. Hardcoded `http://127.0.0.1:8502`, timeout 3s.
- `streamlit_app.py` — 5-page dashboard: Market Overview, Stock Query, Stock List, Factor Analysis (strategy), DB Maintenance.

### DuckDB Schema
| Table | Primary Key | Notes |
|-------|-------------|-------|
| `stock_info` | code | Full refresh (DELETE + INSERT) |
| `daily_bar` | date + code + adjustflag | UPSERT (INSERT OR REPLACE) |
| `minute_bar` | date + time + code + frequency + adjustflag | UPSERT |
| `strategy_*` | defined per JSON | Auto-created from JSON columns |

Database file: `stock_data.duckdb` (same directory as scripts).

### Key Conventions
- Stock codes follow BaoStock format: `sh.600519`, `sz.002149` (exchange prefix + code)
- `adjustflag`: `"1"` = backward adjustment, `"2"` = forward, `"3"` = no adjustment
- Column naming: BaoStock camelCase (`pctChg`, `isST`, `peTTM`) → DB snake_case (`pct_chg`, `is_st`, `pe_ttm`)
- Indicator output columns use UPPER_CASE: `DIF`, `DEA`, `MACD_SIGNAL`, `KDJ_K`, `BOLL_UP`, etc.
- All dates in `YYYY-MM-DD` format
- `stra.py` is an experimental script, not part of the main flow

### BaoStock Data Gotchas
- `amount` is in yuan (not thousands as docs claim)
- `turn` is a percentage number (2.5 = 2.5%), not a decimal
- Market cap formula: `close × volume × 100 / turn / 1e8`
- Minute-line data limited to recent 3 months
