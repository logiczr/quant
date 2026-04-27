import pandas as pd
import duckdb_tools as dt
import index_tools as it


if __name__ == "__main__":
    info = dt.get_daily('sz.002149','2026-01-06','2026-04-20',auto_fetch=True)
    info = it.calc_macd(info)
    print(info)
