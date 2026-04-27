import data_tools
import baostock as bs # type: ignore
import logging
import sys

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    ls = data_tools.fetch_stock_list()
    ls = ls[:10]
    res = data_tools.fetch_daily(stock_list=ls, start_date="2026-02-01", end_date="2026-04-17",adjustflag="3")
    print(res)
    res2 = data_tools.fetch_minute(stock_list=ls, start_date="2026-02-15", end_date="2026-04-17",frequency="5",adjustflag="3")
    print(res2)