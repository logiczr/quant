"""
股票看板 — Streamlit 应用
快速启动：streamlit run h:/WB_User/streamlit_app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path
import logging

# 把上层目录加到路径，方便直接 import
sys.path.insert(0, str(Path(__file__).parent))

import duckdb_tools as dt
import index_tools as it
import daemon_client as dc
import strategy as se

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
if st.sidebar.button('链接守护进程'):
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
    ["📊 大盘概览", "🔍 个股查询", "📋 股票列表","📈 因子分析","🔧 数据库维护"],
)

# ─── 1. 大盘概览 ───
if page == "📊 大盘概览":
    st.title("大盘概览")

    # ── 指数卡片 ──
    INDEX_CODES = {
        "上证指数": "sh.000001",
        "深证成指": "sz.399001",
        "创业板指": "sz.399006",
    }

    # TODO: 后端接好后替换为 DuckDB 直读
    MOCK_INDEX_DATA = {
        "上证指数": {"close": 3245.68, "preclose": 3217.12, "amount": 4231e8},
        "深证成指": {"close": 10876.32, "preclose": 10918.47, "amount": 5128e8},
        "创业板指": {"close": 2134.56, "preclose": 2119.33, "amount": 2345e8},
    }
    MOCK_LATEST_DATE = "2026-04-30"

    # 三栏指数卡片
    cols = st.columns(3)
    for idx, (name, code) in enumerate(INDEX_CODES.items()):
        with cols[idx]:
            d = MOCK_INDEX_DATA[name]
            close_val = d["close"]
            preclose = d["preclose"]
            delta_val = close_val - preclose
            pct = delta_val / preclose * 100
            amount_str = f"{d['amount'] / 1e8:.0f}亿"
            st.metric(
                label=f"{name}  `{code}`",
                value=f"{close_val:,.2f}",
                delta=f"{delta_val:+,.2f}  ({pct:+.2f}%)",
            )
            st.caption(f"成交额: {amount_str}")

    st.divider()

    # ── 涨跌分布 ──
    # TODO: 后端接好后替换为 DuckDB 直读
    st.subheader(f"涨跌分布  ({MOCK_LATEST_DATE})")

    # 模拟全市场涨跌幅分布
    np.random.seed(42)
    _n = 5200
    _pct = np.concatenate([
        np.random.uniform(0, 5, 1845),       # 涨0~5%
        np.random.uniform(-5, 0, 1567),      # 跌0~5%
        np.zeros(312),                         # 平盘
        np.random.uniform(5, 9.9, 89),        # 涨>5%
        np.random.uniform(-9.9, -5, 98),      # 跌>5%
        np.random.uniform(9.9, 10.1, 12),     # 涨停
        np.random.uniform(-10.1, -9.9, 3),    # 跌停
    ])
    pct = pd.Series(_pct)
    total = len(pct)

    # ── 分区统计 ──
    BINS = [
        ("涨停",   lambda x: x >= 9.9,           "#8B0000"),
        ("涨>5%",  lambda x: (x >= 5) & (x < 9.9), "#FF4444"),
        ("涨0~5%", lambda x: (x > 0) & (x < 5),    "#FFAAAA"),
        ("平盘",   lambda x: x == 0,                "#999999"),
        ("跌0~5%", lambda x: (x < 0) & (x > -5),   "#A8D8A8"),
        ("跌>5%",  lambda x: (x <= -5) & (x > -9.9), "#44CC44"),
        ("跌停",   lambda x: x <= -9.9,             "#006400"),
    ]

    bin_data = []
    for label, cond, color in BINS:
        count = int(pct.loc[cond(pct)].count())
        bin_data.append({
            "区间": label,
            "数量": count,
            "占比": f"{count / total * 100:.1f}%" if total > 0 else "0%",
            "颜色": color,
        })
    bin_df = pd.DataFrame(bin_data)

    # ── 环状图 + 直方图 并排 ──
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        # 环状图
        fig_donut = px.pie(
            bin_df,
            values="数量",
            names="区间",
            color="区间",
            color_discrete_map={row["区间"]: row["颜色"] for _, row in bin_df.iterrows()},
            hole=0.55,
        )
        fig_donut.update_traces(
            textinfo="label+value",
            textfont_size=18,
            hovertemplate="%{label}: %{value}只 (%{percent})<extra></extra>",
        )
        fig_donut.update_layout(
            showlegend=True,
            height=420,
            margin=dict(t=10, b=10, l=10, r=10),
            annotations=[dict(
                text=f"{total}<br>只",
                x=0.5, y=0.5,
                font_size=20,
                showarrow=False,
            )],
        )
        st.plotly_chart(fig_donut, use_container_width=True)

    with chart_col2:
        # 直方图：1% 一档
        hist_bins = list(range(-11, 12))
        hist_counts, hist_edges = np.histogram(pct, bins=hist_bins)
        hist_labels = [f"{hist_edges[i]:+.0f}%~{hist_edges[i+1]:+.0f}%" for i in range(len(hist_counts))]
        hist_colors = [
            "#FFAAAA" if hist_edges[i] >= 0 else "#A8D8A8"
            for i in range(len(hist_counts))
        ]

        fig_hist = go.Figure(data=[go.Bar(
            x=hist_labels,
            y=hist_counts,
            marker_color=hist_colors,
            hovertemplate="%{x}: %{y}只<extra></extra>",
        )])
        fig_hist.update_layout(
            xaxis_title="涨跌幅",
            yaxis_title="家数",
            margin=dict(t=50, b=50, l=30, r=10),
            xaxis_tickangle=-45,
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    # ── 分区统计表 ──
    st.dataframe(
        bin_df[["区间", "数量", "占比"]],
        use_container_width=True,
        hide_index=True,
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
                    auto_fetch=False,
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
        if dc.is_alive():
            result = dc.run_now("refresh_stock_info")
            if result.get("success"):
                st.toast("✅ 股票列表刷新已触发，稍后刷新页面")
                load_stocks.clear()
            else:
                st.toast(f"❌ 触发失败：{result.get('detail', result.get('error', '未知'))}")
        else:
            st.warning("守护进程离线，无法触发刷新")


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
        "post_market_fetch": "📊 收盘批次拉取 (18:00)",
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
    
    st.subheader("最近拉取记录")
    last_fetch = dc.get_last_fetch()
    if last_fetch:
        # 把 failed_codes 列表转成可读字符串
        display = {}
        for k, v in last_fetch.items():
            if k == "failed_codes":
                display[k] = f"{len(v)} 只" if v else "无"
            else:
                display[k] = v
        st.dataframe(
            pd.DataFrame([{"字段": k, "值": v} for k, v in display.items()]),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("暂无拉取记录")


elif page == "📈 因子分析":
    st.title("策略看板")

    # 加载策略列表
    strategies = se.list_strategies()
    if not strategies:
        st.warning("strategy/ 目录下没有策略定义文件")
        st.stop()

    # 策略选择
    strategy_names = [s["name"] for s in strategies]
    strategy_labels = [f"{s['name']} — {s.get('description', '')}" for s in strategies]
    selected_idx = st.selectbox(
        "选择策略",
        range(len(strategy_labels)),
        format_func=lambda i: strategy_labels[i],
    )
    selected = strategies[selected_idx]

    # 日期选择
    query_date = st.date_input("查询日期", pd.Timestamp.today())
    date_str = query_date.strftime("%Y-%m-%d")

    # 策略信息
    info = se.strategy_info(selected["name"])
    if info:
        if info.get("type") == "screener":
            st.caption(
                f"类型: 动态选股 | "
                f"状态: {info['data_status']}"
            )
        else:
            st.caption(
                f"策略表: `{info['table']}` | "
                f"状态: {info['data_status']} | "
                f"数据量: {info['rows']} 行 | "
                f"日期范围: {info.get('date_range', '无')}"
            )

    if st.button("查询"):
        df = se.query_strategy(
            selected["name"],
            date=date_str
        )
        st.dataframe(df, use_container_width=True, height=600)
    else:
        st.info("请先选择日期")