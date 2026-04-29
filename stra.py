import duckdb_tools as dt
import pandas as pd
import time
def caps(df):
    res = df['close']*df['volume']*100 / df['turn']/100000000
    return res

def cap_change(df,leng = 1):
    dif = df[len(df) - 1] - df[len(df) - leng - 1]
    return dif

def ratio(df,dif,leng = 1):
    return dif/df[len(df) - leng - 1]

if __name__ == '__main__':
    info = dt.get_stock_info()
    print(info)
    fin = []
    t1 = time.time()
    for _, stk in info.iterrows():
        df = dt.get_daily(str(stk['code']),start_date='2026-04-21',end_date='2026-04-28')
        if len(df) < 4:
            continue
        res = caps(df)
        dif = cap_change(res,leng = 4)
        fin.append([stk['code_name'],dif,ratio(res,dif,leng=4),res[len(res) - 1]])
    t2 = time.time()
    print(t2-t1)
    df =pd.DataFrame(fin,columns=['code_name','v','r','caps'])
    df.sort_values(by='v',ascending=False,inplace=True)
    print(df)
    df.sort_values(by='r',ascending=False,inplace=True)
    print(df)
    df.sort_values(by='caps',ascending=False,inplace=True)
    print(df)



    
    