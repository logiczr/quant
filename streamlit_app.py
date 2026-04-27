"""
股票看板 — Streamlit 应用
快速启动：streamlit run h:/WB_User/streamlit_app.py
"""

import streamlit as st
import pandas as pd
import sys
from pathlib import Path
import logging

# 把上层目录加到路径，方便直接 import
sys.path.insert(0, str(Path(__file__).parent))

import duckdb_tools as dt
import index_tools as it
import daemon_client as dc


# ─── 页面配置 ───
st.set_page_config(
    page_title="股票看板",
    page_icon="📈",
    layout="wide",
)

if dc.is_alive():
    st.sidebar.success("🟢 守护进程在线")
else:
    st.sidebar.warning("🔴 守护进程离线")


# ─── 侧边栏：数据库状态 ───
st.sidebar.title("数据库状态")
try:
    stats = dt.table_stats()
    st.sidebar.dataframe(stats, use_container_width=True)
except Exception as e:
    st.sidebar.error(f"数据库连接失败: {e}")

# ─── 主导航 ───
page = st.sidebar.radio(
    "功能",
    ["📊 排行榜", "🔍 个股查询", "📋 股票列表","📈 因子分析","🔧 数据库维护"],
)

# ─── 1. 排行榜 ───
if page == "📊 排行榜":
    st.title("排行榜")
    
    rank_type = st.selectbox(
        "排行类型",
        ["量比", "日涨幅", "3日涨幅", "5日涨幅", "换手率"],
    )

    top_n = st.slider("显示数量", 10, 100, 20)

    # 拉全市场股票列表
    @st.cache_data
    def load_all_stocks():
        df = dt.get_stock_info()[["code", "code_name"]]
        return df

    stocks = load_all_stocks()

    # 批量计算近期数据（最近 N 天）
    #today = pd.Timestamp.today().strftime("%Y-%m-%d")
    #start = (pd.Timestamp.today() - pd.Timedelta(days=60)).strftime("%Y-%m-%d")
    today = '2026-04-20'
    start = '2026-04-01'
    if st.button("计算", type="primary"):
        with st.spinner("正在计算指标，请稍候..."):
            result = it.calc_batch(
                codes=stocks["code"].tolist(),
                start_date=start,
                end_date=today,
                indicators=["vol_ma", "macd"],
                adjustflag="3",
            )

        rows = []
        for code, df in result.items():
            if df.empty:
                continue
            row = df.iloc[-1]  # 取最新一天
            rows.append({
                "代码": code,
                "名称": stocks[stocks["code"] == code]["code_name"].values[0],
                "收盘价": row.get("close"),
                "日涨幅%": row.get("pct_chg"),
                "量比": row.get("VOL_RATIO"),
                "换手率%": row.get("turn"),
                "MACD_DIF": row.get("DIF"),
                "MACD_DEA": row.get("DEA"),
            })

        rank_df = pd.DataFrame(rows)

    # 过滤掉涨跌幅为空的（停牌股）
        rank_df = rank_df.dropna(subset=["日涨幅%"])

    # 按排行类型排序
        if rank_type == "量比":
            rank_df = rank_df.sort_values("量比", ascending=False).head(top_n)
            rank_df = rank_df.rename(columns={"量比": "量比（当日/5日均量）"})
    
        rank_df = rank_df.reset_index(drop=True)
        rank_df.index = rank_df.index + 1
        rank_df.index.name = "排名"

        st.dataframe(
            rank_df,
            width='stretch',
            height=600,
        )
 

# ─── 2. 个股查询 ───
elif page == "🔍 个股查询":
    st.title("个股查询")
    col1,col2 = st.columns(2)
    with col1:
        code = st.text_input("股票代码", "sh.600000")
        start = st.date_input("起始日期", pd.Timestamp.today() - pd.Timedelta(days=120))
        
    with col2:
        indicators = st.multiselect(
        "选择指标",
        ["macd", "kdj", "boll", "rsi", "cci", "wr", "atr", "ma", "ema", "obv", "vol_ma", "dma", "vr", "hv"],
        default=["macd", "kdj", "boll"],
    )
        end = st.date_input("截止日期", pd.Timestamp.today())

    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    if st.button("查询", type="primary"):
        with st.spinner("加载数据中..."):
            try:
                df = dt.get_daily(
                    code=code,
                    start_date=start_str,
                    end_date=end_str,
                    adjustflag="3",
                    auto_fetch=True,
                )

                if df.empty:
                    st.warning("未找到数据，请检查代码是否正确")
                else:
                    # 基本信息
                    info = dt.get_stock_info(code)
                    if not info.empty:
                        st.caption(
                            f"**{info['code_name'].values[0]}** | "
                            f"上市日期: {info['ipo_date'].values[0]} | "
                            f"共 {len(df)} 条行情"
                        )

                    # 计算指标
                    if indicators:
                        df = it.calc_indicators(df, indicators=indicators)

                    # 格式化日期列
                    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

                    st.dataframe(
                        df,
                        use_container_width=True,
                        height=500,
                    )

                    # 简单 K 线（收盘价折线）
                    st.line_chart(df.set_index("date")[["open","close"]])

            except Exception as e:
                st.error(f"查询失败: {e}")

# ─── 3. 股票列表 ───
elif page == "📋 股票列表":
    
    st.title("股票列表")

    @st.cache_data
    def load_stocks():
        return dt.get_stock_info()

    if st.button("更新", type="primary"):
        with st.spinner("加载数据中..."):
            try:
                st.info(f"开始更新")
                dt.delete_stock_info('ALL')
                dt.upsert_stock_info(None) #type: ignore
                load_stocks.clear()
                st.success(f"更新成功")
            except Exception as e:
                st.error(f"更新失败: {e}")


    df = load_stocks()
    st.caption(f"共 {len(df)} 只股票")

    # 搜索过滤
    search = st.text_input("搜索代码或名称")
    if search:
        df = df[
            df["code"].str.contains(search, na=False) |
            df["code_name"].str.contains(search, na=False)
        ]

    st.dataframe(df, use_container_width=True, height=600)

elif page == "🔧 数据库维护":
    pass
    st.title("数据库维护")
    st.subheader("定时任务")
    jobs = dc.get_jobs()
    if jobs:
        jobs_df = pd.DataFrame(jobs)
        st.dataframe(jobs_df, use_container_width=True, hide_index=True)
    else:
        st.info("暂无已注册的定时任务")

    # ── 任务状态 ──
    st.subheader("任务状态")
    status = dc.get_status()
    if status:
        status_df = pd.DataFrame(
            [{"任务": k, "状态": v} for k, v in status.items()]
        )
        st.dataframe(status_df, use_container_width=True, hide_index=True)
    else:
        st.info("暂无执行记录")


    st.subheader("手动触发")
    task_options = {
        "refresh_stock_info": "🔄 刷新股票列表 (08:30)",
        "market_fetch": "📊 收盘批次拉取 (18:00)",
    }

    cols = st.columns(len(task_options))
    for idx, (task_id, label) in enumerate(task_options.items()):
        with cols[idx]:
            if st.button(label, key=f"btn_{task_id}"):
                result = dc.run_now(task_id)
                if result.get("success"):
                    st.toast(f"✅ {task_id} 已触发")
                else:
                    st.toast(f"❌ {task_id} 触发失败：{result.get('detail', result.get('error', '未知'))}")


