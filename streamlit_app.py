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

import daemon_client as dc
import index_tools as it


@st.cache_data(ttl=60)
def _align_trade_date(d: pd.Timestamp) -> pd.Timestamp:
    """将日期对齐到最近的交易日（向后取最近），结果缓存 1 分钟"""
    try:
        nearest = dc.find_nearest_trade_date(d.strftime("%Y-%m-%d"), direction="backward")
        if nearest:
            return pd.Timestamp(nearest)
        # 返回 None 说明本地无日历数据，触发补拉后重试一次
        logging.warning("本地无交易日历，触发补拉...")
        dc.trigger_fill_trade_calendar()
        nearest = dc.find_nearest_trade_date(d.strftime("%Y-%m-%d"), direction="backward")
        if nearest:
            return pd.Timestamp(nearest)
    except Exception:
        pass
    return d


# 预计算对齐后的"今天"，供全文件默认日期使用
_TODAY_RAW = pd.Timestamp.today().normalize()
_TODAY_TD = _align_trade_date(_TODAY_RAW)

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
    stats = dc.table_stats()
    st.sidebar.dataframe(stats, use_container_width=True)
except Exception as e:
    st.sidebar.error(f"数据库连接失败: {e}")

# ─── 主导航 ───
page = st.sidebar.radio(
    "功能",
    ["📊 大盘概览", "⭐ 自选股", "🔍 个股查询", "📋 股票列表", "📑 指数列表","📈 因子分析","🔧 数据库维护"],
)

# ─── 1. 大盘概览 ───
if page == "📊 大盘概览":
    st.title("大盘概览")

    # ── 指数代码 → 名称映射 ──
    INDEX_NAMES = {
        "sh.000001": "上证指数",
        "sz.399001": "深证成指",
        "sz.399006": "创业板指",
    }
    INDEX_CODES_ORDER = ["sh.000001", "sz.399001", "sz.399006"]

    overview = dc.get_market_overview()
    latest_date = overview["date"]
    indices = overview["indices"]
    pct = overview["pct_series"]

    if latest_date is None:
        st.warning("暂无行情数据，请先在「数据库维护」中拉取日线数据")
        st.stop()

    # ── 指数卡片 ──
    cols = st.columns(3)
    for idx, code in enumerate(INDEX_CODES_ORDER):
        with cols[idx]:
            d = indices.get(code)
            name = INDEX_NAMES[code]
            if d:
                close_val = d["close"]
                preclose = d["preclose"]
                delta_val = close_val - preclose
                pct_val = d["pct_chg"]
                amount_str = f"{d['amount'] / 1e8:.0f}亿" if d["amount"] else "—"
                st.metric(
                    label=f"{name}  `{code}`",
                    value=f"{close_val:,.2f}",
                    delta=f"{delta_val:+,.2f}  ({pct_val:+.2f}%)",
                )
                st.caption(f"成交额: {amount_str}")
            else:
                st.metric(label=f"{name}  `{code}`", value="—")

    st.divider()

    # ── 涨跌分布 ──
    st.subheader(f"涨跌分布  ({latest_date})")

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
            textfont_size=12,
            hovertemplate="%{label}: %{value}只 (%{percent})<extra></extra>",
        )
        fig_donut.update_layout(
            showlegend=True,
            height=425,
            margin=dict(t=10, b=10, l=10, r=10),
            annotations=[dict(
                text=f"{total}<br>只",
                x=0.5, y=0.5,
                font_size=15,
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

# ─── 2. 自选股 ───
elif page == "⭐ 自选股":

    # ── 子页面状态管理 ──
    if "watchlist_page" not in st.session_state:
        st.session_state.watchlist_page = "list"
    if "watchlist_group" not in st.session_state:
        st.session_state.watchlist_group = None

    # ══════════════════════════════════════════════════════════
    # 组合列表页
    # ══════════════════════════════════════════════════════════
    if st.session_state.watchlist_page == "list":
        st.title("⭐ 自选股")

        # ── 新建组合 ──
        with st.expander("➕ 新建组合"):
            col1, col2 = st.columns([2, 3])
            with col1:
                new_name = st.text_input("组合名称", key="wl_new_name")
            with col2:
                new_desc = st.text_input("描述（可选）", key="wl_new_desc")
            if st.button("创建", key="wl_create_btn"):
                if not new_name.strip():
                    st.warning("组合名称不能为空")
                elif new_name in dc.list_groups():
                    st.warning(f"组合 '{new_name}' 已存在")
                else:
                    dc.create_group(new_name.strip(), new_desc.strip())
                    st.toast(f"✅ 组合 '{new_name}' 已创建")
                    st.rerun()

        # ── 组合列表 ──
        groups = dc.get_group_summary()
        if groups.empty:
            st.info("还没有自选组合，点击上方「新建组合」开始")
        else:
            for _, row in groups.iterrows():
                gname = row["name"]
                with st.container(border=True):
                    col1, col2, col3, col4 = st.columns([3, 3, 1, 1])
                    with col1:
                        st.subheader(f"📁 {gname}")
                        st.caption(row.get("description", ""))
                    with col2:
                        st.write(f"{row['stock_count']} 只 · 创建于 {row.get('created_at', '')}")
                        # 实时获取最新涨跌幅
                        codes = dc.get_stock_codes(gname)
                        if codes:
                            try:
                                overview_df = dc.get_group_overview(gname)
                                if not overview_df.empty and "pct_chg" in overview_df.columns:
                                    avg_pct = overview_df["pct_chg"].mean()
                                    st.metric("今日等权涨跌", f"{avg_pct:+.2f}%")
                                else:
                                    st.write("暂无行情")
                            except Exception:
                                st.write("暂无行情")
                    with col3:
                        if st.button("查看", key=f"wl_view_{gname}"):
                            st.session_state.watchlist_page = "detail"
                            st.session_state.watchlist_group = gname
                            st.rerun()
                    with col4:
                        if st.button("删除🗑", key=f"wl_del_{gname}"):
                            dc.delete_group(gname)
                            st.toast(f"✅ 组合 '{gname}' 已删除")
                            st.rerun()

    # ══════════════════════════════════════════════════════════
    # 组合详情页
    # ══════════════════════════════════════════════════════════
    elif st.session_state.watchlist_page == "detail":
        gname = st.session_state.watchlist_group
        if gname is None or gname not in dc.list_groups():
            st.session_state.watchlist_page = "list"
            st.rerun()

        info = dc.get_group_info(gname)

        # ── 顶部导航 ──
        if st.button("← 返回自选股列表"):
            st.session_state.watchlist_page = "list"
            st.session_state.watchlist_group = None
            st.rerun()

        st.title(f"📁 {gname}")
        if info.get("description"):
            st.caption(info["description"])

        codes = dc.get_stock_codes(gname)
        if not codes:
            st.info("组合内暂无股票，点击下方「添加股票」开始")

            # ── 添加股票 ──
            with st.expander("➕ 添加股票"):
                add_code = st.text_input("股票代码", "sh.600519", key="wl_add_code")
                add_note = st.text_input("备注（可选）", key="wl_add_note")
                if st.button("添加", key="wl_add_btn"):
                    dc.add_stock(gname, add_code.strip(), add_note.strip())
                    st.toast(f"✅ {add_code} 已加入")
                    st.rerun()
        else:
            # ── 指数卡片 ──
            col_date1, col_date2 = st.columns(2)
            with col_date1:
                index_start = st.date_input(
                    "起始日期",
                    _align_trade_date(_TODAY_TD - pd.Timedelta(days=120)),
                    key="wl_idx_start",
                )
            with col_date2:
                index_end = st.date_input(
                    "截止日期",
                    _TODAY_TD,
                    key="wl_idx_end",
                )

            # 对齐到交易日
            idx_start_str = _align_trade_date(pd.Timestamp(index_start)).strftime("%Y-%m-%d")
            idx_end_str = _align_trade_date(pd.Timestamp(index_end)).strftime("%Y-%m-%d")

            try:
                index_data = dc.calc_group_index(
                    gname,
                    idx_start_str,
                    idx_end_str,
                )
                if not index_data.empty:
                    latest = index_data.iloc[-1]
                    st.metric(
                        f"{gname}指数",
                        f"{latest['index_value']:.2f}",
                        f"{latest['daily_return']:+.2f}%",
                    )

                    # ── 指数走势图 ──
                    st.subheader("📈 指数走势")
                    chart_df = index_data.copy()
                    chart_df["date"] = pd.to_datetime(chart_df["date"])
                    st.line_chart(chart_df.set_index("date")["index_value"])
                else:
                    st.warning("暂无足够数据计算指数")
            except Exception as e:
                st.warning(f"指数计算失败: {e}")

            st.divider()

            # ── 估值统计 ──
            st.subheader("📊 板块估值")
            try:
                val = dc.calc_group_valuation(gname)
                val_col1, val_col2, val_col3, val_col4 = st.columns(4)
                val_col1.metric("PE_TTM 均值", f"{val['pe_mean']}" if val['pe_mean'] else "—")
                val_col2.metric("PE_TTM 中位", f"{val['pe_median']}" if val['pe_median'] else "—")
                val_col3.metric("PB 均值", f"{val['pb_mean']}" if val['pb_mean'] else "—")
                val_col4.metric("PB 中位", f"{val['pb_median']}" if val['pb_median'] else "—")
                st.caption(f"有效成分: {val['valid_count']}/{val['total_count']}（PE_TTM > 0）")
            except Exception as e:
                st.warning(f"估值计算失败: {e}")

            st.divider()

            # ── 成分股行情 ──
            st.subheader("📋 成分股")
            try:
                overview_df = dc.get_group_overview(gname)
                if not overview_df.empty:
                    # 格式化显示
                    display_df = overview_df.copy()
                    if "pct_chg" in display_df.columns:
                        display_df["pct_chg"] = display_df["pct_chg"].apply(
                            lambda x: f"{x:+.2f}%" if pd.notna(x) else "—"
                        )
                    if "amount" in display_df.columns:
                        display_df["amount"] = display_df["amount"].apply(
                            lambda x: f"{x/1e8:.2f}亿" if pd.notna(x) and x > 0 else "—"
                        )
                    if "volume" in display_df.columns:
                        display_df["volume"] = display_df["volume"].apply(
                            lambda x: f"{x/1e4:.0f}万" if pd.notna(x) and x > 0 else "—"
                        )
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
                else:
                    st.info("暂无行情数据")
            except Exception as e:
                st.warning(f"行情获取失败: {e}")

            # ── 添加/移除股票 ──
            col_add, col_remove = st.columns(2)
            with col_add:
                with st.expander("➕ 添加股票"):
                    add_code = st.text_input("股票代码", "sh.600519", key="wl_add_code2")
                    add_note = st.text_input("备注（可选）", key="wl_add_note2")
                    if st.button("添加", key="wl_add_btn2"):
                        dc.add_stock(gname, add_code.strip(), add_note.strip())
                        st.toast(f"✅ {add_code} 已加入")
                        st.rerun()

            with col_remove:
                if codes:
                    with st.expander("➖ 移除股票"):
                        rm_code = st.selectbox("选择股票", codes, key="wl_rm_code")
                        if st.button("移除", key="wl_rm_btn"):
                            dc.remove_stock(gname, rm_code)
                            st.toast(f"✅ {rm_code} 已移除")
                            st.rerun()

# ─── 3. 个股查询 ───
elif page == "🔍 个股查询":
    st.title("个股查询")
    col1,col2 = st.columns(2)
    with col1:
        code = st.text_input("股票代码", "sh.600000")
        start = st.date_input("起始日期", _align_trade_date(_TODAY_TD - pd.Timedelta(days=120)))
        
    with col2:
        indicators = st.multiselect(
        "选择指标",
        ["macd", "kdj", "boll", "rsi", "cci", "wr", "atr", "ma", "ema", "obv", "vol_ma", "dma", "vr", "hv"],
        default=["macd", "kdj", "boll"],
    )
        end = st.date_input("截止日期", _TODAY_TD)

    start_str = _align_trade_date(pd.Timestamp(start)).strftime("%Y-%m-%d")
    end_str = _align_trade_date(pd.Timestamp(end)).strftime("%Y-%m-%d")

    if st.button("查询", type="primary"):
        with st.spinner("加载数据中..."):
            try:
                df = dc.get_daily(
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
                    info = dc.get_stock_info(code)
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

    if st.button("🔄 刷新数据源", type="primary"):
        if dc.is_alive():
            result = dc.run_now("refresh_stock_info")
            if result.get("success"):
                st.toast("✅ 股票列表刷新已触发")
                st.rerun()
            else:
                st.toast(f"❌ 触发失败：{result.get('detail', result.get('error', '未知'))}")
        else:
            st.warning("守护进程离线，无法触发刷新")

    all_info = dc.get_stock_info()
    if "type" in all_info.columns:
        df = all_info[all_info["type"] == "1"].reset_index(drop=True)
    else:
        df = all_info
    st.caption(f"共 {len(df)} 只股票")

    # 搜索过滤
    search = st.text_input("搜索代码或名称")
    if search:
        df = df[
            df["code"].str.contains(search, na=False) |
            df["code_name"].str.contains(search, na=False)
        ]

    st.dataframe(df, use_container_width=True, height=600)

# ─── 4. 指数列表 ───
elif page == "📑 指数列表":
    st.title("指数列表")

    if st.button("🔄 刷新数据源", type="primary"):
        if dc.is_alive():
            result = dc.run_now("refresh_stock_info")
            if result.get("success"):
                st.toast("✅ 指数列表刷新已触发")
                st.rerun()
            else:
                st.toast(f"❌ 触发失败：{result.get('detail', result.get('error', '未知'))}")
        else:
            st.warning("守护进程离线，无法触发刷新")

    all_info = dc.get_stock_info()
    if "type" in all_info.columns:
        df = all_info[all_info["type"] == "2"].reset_index(drop=True)
    else:
        df = pd.DataFrame()
    st.caption(f"共 {len(df)} 只指数")

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

    # ── 日期选择 ──
    date_col1, date_col2 = st.columns(2)
    with date_col1:
        fetch_start = st.date_input("起始日期", pd.Timestamp.today(), key="fetch_start")
    with date_col2:
        fetch_end = st.date_input("截止日期", pd.Timestamp.today(), key="fetch_end")

    task_options = {
        "refresh_stock_info": "🔄 刷新股票列表 (08:30)",
        "post_market_fetch": "📊 股票日线拉取",
        "fetch_index_daily": "📈 指数日线拉取",
    }

    # 需要传日期的任务
    DATE_TASKS = {"post_market_fetch", "fetch_index_daily"}

    cols = st.columns(len(task_options))
    for idx, (task_id, label) in enumerate(task_options.items()):
        with cols[idx]:
            if st.button(label, key=f"btn_{task_id}"):
                params = {}
                if task_id in DATE_TASKS:
                    params = {
                        "start_date": fetch_start.strftime("%Y-%m-%d"),
                        "end_date": fetch_end.strftime("%Y-%m-%d"),
                    }
                result = dc.run_now(task_id, params=params or None)
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

    # ── 数据库表概览 ──
    st.subheader("数据库表概览")
    try:
        tables = dc.get_db_tables()
        if tables:
            rows_list = []
            for tbl in tables:
                rows_list.append({
                    "表名": tbl["table_name"],
                    "行数": f"{tbl['row_count']:,}",
                    "日期范围": tbl["date_range"],
                })
            st.dataframe(
                pd.DataFrame(rows_list),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("暂无表数据")
    except Exception as e:
        st.warning(f"数据库表查询失败: {e}")

    # ── 日线完整性检查 ──
    st.subheader("日线完整性检查")
    integrity_col1, integrity_col2 = st.columns([3, 1])
    with integrity_col1:
        integrity_date = st.date_input("检查日期", _TODAY_TD, key="integrity_date")
    with integrity_col2:
        st.markdown("##### ")  # 对齐按钮
        if st.button("🔍 检查缺失"):
            # 完整性检查不需要对齐到交易日，直接用用户选的日期
            date_str_integrity = pd.Timestamp(integrity_date).strftime("%Y-%m-%d")
            missing_df = dc.check_daily_integrity(target_date=date_str_integrity)
            if missing_df.empty:
                st.success(f"✅ {date_str_integrity} 数据完整，无缺失股票")
            else:
                st.warning(f"⚠️ 缺失 {len(missing_df)} 只股票")
                st.dataframe(missing_df, use_container_width=True, height=300)

elif page == "📈 因子分析":
    st.title("策略看板")

    # 加载策略列表
    strategies = dc.list_strategies()
    if not strategies:
        st.warning("strategies/ 目录下没有策略定义文件")
        st.stop()

    # 策略选择 + 日期选择分两列
    col_strategy, col_date = st.columns(2)
    with col_strategy:
        strategy_names = [s["name"] for s in strategies]
        strategy_labels = [f"{s['name']} — {s.get('description', '')}" for s in strategies]
        selected_idx = st.selectbox(
            "选择策略",
            range(len(strategy_labels)),
            format_func=lambda i: strategy_labels[i],
        )
    with col_date:
        query_date = st.date_input("查询日期", _TODAY_TD)
        date_str = _align_trade_date(pd.Timestamp(query_date)).strftime("%Y-%m-%d")

    selected = strategies[selected_idx]

    # ── 自动生成策略参数控件（与日期同行） ──
    param_values = {}
    param_ui = selected.get("param_ui", {})
    param_names = list(param_ui.keys())

    if param_names:
        param_cols = st.columns(len(param_names))
        for i, param_name in enumerate(param_names):
            ui_conf = param_ui[param_name]
            widget_type = ui_conf.get("type", "number")
            default_val = selected.get("params", {}).get(param_name)

            with param_cols[i]:
                if widget_type == "number":
                    param_values[param_name] = st.number_input(
                        ui_conf.get("label", param_name),
                        min_value=ui_conf.get("min"),
                        max_value=ui_conf.get("max"),
                        value=default_val,
                        step=ui_conf.get("step", 1),
                        help=ui_conf.get("help", ""),
                    )
                elif widget_type == "select":
                    param_values[param_name] = st.selectbox(
                        ui_conf.get("label", param_name),
                        options=ui_conf.get("options", []),
                        help=ui_conf.get("help", ""),
                    )
                elif widget_type == "text":
                    param_values[param_name] = st.text_input(
                        ui_conf.get("label", param_name),
                        value=ui_conf.get("default", ""),
                        help=ui_conf.get("help", ""),
                    )
                elif widget_type == "button":
                    param_values[param_name] = st.button(
                        ui_conf.get("label", param_name),
                        help=ui_conf.get("help", ""),
                        key=f"param_btn_{param_name}",
                    )

    # 策略信息
    info = dc.strategy_info(selected["name"])
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

    col_query, col_force = st.columns([1, 1])
    with col_query:
        btn_query = st.button("查询")
    with col_force:
        btn_force = st.button("🔄 强制重新计算")

    if btn_query or btn_force:
        force = btn_force
        query_params = {k: v for k, v in param_values.items()
                        if param_ui.get(k, {}).get("type") != "button"}
        df = dc.query_strategy(
            selected["name"],
            date=date_str,
            force_compute=force,
            **query_params,
        )
        if force:
            st.success("已强制重新计算并写入")
        st.dataframe(df, use_container_width=True, height=600)
    else:
        st.info("请先选择日期")

    # ── 处理 param_ui 中 button 点击 ──
    for param_name, val in param_values.items():
        if not val:
            continue
        ui_conf = param_ui.get(param_name, {})
        if ui_conf.get("type") != "button":
            continue
        endpoint = ui_conf.get("endpoint", "")
        if not endpoint:
            st.warning(f"'{param_name}' 未配置 endpoint")
            continue
        with st.spinner("执行中..."):
            result = dc.push_signals(date_str)
        if result.get("status") == "DONE":
            msg = f"✅ 推送成功！金叉 {result.get('golden', 0)} 只，死叉 {result.get('dead', 0)} 只"
            st.success(msg)
        elif result.get("status") == "SKIPPED":
            st.info("当日无信号，已跳过推送")
        else:
            st.error(f"❌ 推送失败: {result.get('error', '未知错误')}")