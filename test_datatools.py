import data_tools
import baostock as bs # type: ignore
import pandas as pd
import logging
import sys

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    lg = bs.login()
    rs = bs.query_stock_industry()
    industry_list = []
    while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
        industry_list.append(rs.get_row_data())
    result = pd.DataFrame(industry_list, columns=rs.fields)
    result.to_csv("./res.csv", encoding="gbk", index=False)
    print(result)
    bs.logout()