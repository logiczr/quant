import duckdb_tools
import data_tools
import logging
import sys

def test1():
    ls = data_tools.fetch_stock_list()
    ls = ls[0:10]
    print(ls)
    data = data_tools.fetch_daily(ls, start_date='2026-01-01', end_date='2026-04-01',adjustflag='3')
    print(data)
    r = duckdb_tools.insert_daily(data)
    #a = duckdb_tools.upsert_stock_info(ls)
    #print(a)

def test2():
    info = duckdb_tools.get_daily('sh.600000','2025-01-02','2026-04-01',auto_fetch=True)
    print(info)

def test_dbstats():
    st = duckdb_tools.table_stats()
    print(st)

def test_stockinfo():
    #data = data_tools.fetch_stock_list()
    #a = duckdb_tools.upsert_stock_info(None) #type: ignore
    info = duckdb_tools.get_stock_info()
    #print(info)
    return info

def test_del():
    duckdb_tools.delete_daily('sh.600000')

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    duckdb_tools.check_all_daily_gaps('2010-01-01')
    