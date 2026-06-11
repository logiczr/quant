"""Fama-French 三因子策略"""

import logging

import pandas as pd
import duckdb_tools as dt

logger = logging.getLogger("strategy.ff3_factor")

STRATEGY = {
    "description": "Fama-French 三因子：MKT/SMB/HML，2×3 独立排序法",
    "columns": [
        {"name": "date", "type": "DATE", "not_null": True},
        {"name": "regroup_freq", "type": "VARCHAR", "not_null": True},
        {"name": "regroup_date", "type": "DATE"},
        {"name": "mkt", "type": "DOUBLE"},
        {"name": "smb", "type": "DOUBLE"},
        {"name": "hml", "type": "DOUBLE"},
        {"name": "sl_return", "type": "DOUBLE"},
        {"name": "sm_return", "type": "DOUBLE"},
        {"name": "sh_return", "type": "DOUBLE"},
        {"name": "bl_return", "type": "DOUBLE"},
        {"name": "bm_return", "type": "DOUBLE"},
        {"name": "bh_return", "type": "DOUBLE"},
        {"name": "stock_count", "type": "INTEGER"},
    ],
    "primary_key": ["date", "regroup_freq"],
    "params": {"regroup_freq": "monthly"},
    "param_ui": {
        "regroup_freq": {
            "type": "select",
            "options": ["monthly", "weekly"],
            "label": "重分组频率",
            "help": "每月末/每周末重新对股票排序分组",
        },
    },
    "_说明": {
        "MKT": "R_沪深300 - R_f，R_f 暂设为 0。需 index_daily_bar 中有 sh.000300 数据",
        "SMB": "Small Minus Big = 1/3(S/L+S/M+S/H) - 1/3(B/L+B/M+B/H)",
        "HML": "High Minus Low = 1/2(S/H+B/H) - 1/2(S/L+B/L)",
        "2×3 排序": {
            "Size": "流通市值（close × volume × 100 / turn），中位数切分 → S(小)/B(大)",
            "BM": "1/pb_mrq，30%/70% 分位切分 → L(低)/M(中)/H(高)",
            "6组合": "S/L, S/M, S/H, B/L, B/M, B/H",
        },
        "regroup_freq": "monthly=每月末重分组(标准做法), weekly=每周末重分组",
        "regroup_date": "实际使用的重分组日期（该日收盘后的市值/BM决定分组）",
        "sl_return 等六列": "6个组合的等权平均日收益率(%)，用于交叉检验",
        "组合收益计算": "对查询日期，取各组合内所有股票 pct_chg 的等权均值",
        "重分组逻辑": "查询日在M月 → 使用M-1月最后一个交易日的市值/BM排序；查询日在W周 → 使用W-1周最后一个交易日排序",
    },
}


def compute(date: str, regroup_freq: str = "monthly") -> pd.DataFrame:
    """
    Fama-French 三因子模型。

    MKT = R_沪深300 - R_f（R_f 暂设为 0）
    SMB = 1/3(S/L + S/M + S/H) - 1/3(B/L + B/M + B/H)
    HML = 1/2(S/H + B/H) - 1/2(S/L + B/L)

    2×3 独立排序：
      - Size: 流通市值中位数切 → S(小) / B(大)
      - BM: 1/pb_mrq 的 30%/70% 分位切 → L(低) / M(中) / H(高)

    重分组逻辑：
      - monthly: 查询日在 M 月 → 用 M-1 月最后交易日的市值/BM 排序
      - weekly:  查询日在 W 周 → 用 W-1 周最后交易日的市值/BM 排序
    """
    empty_cols = [
        "date", "regroup_freq", "regroup_date",
        "mkt", "smb", "hml",
        "sl_return", "sm_return", "sh_return",
        "bl_return", "bm_return", "bh_return",
        "stock_count",
    ]

    conn = dt.get_read_connection()
    try:
        # ── 1. MKT：沪深300 收益率（R_f = 0） ──
        mkt_row = conn.execute(
            """
            SELECT pct_chg FROM index_daily_bar
            WHERE code = 'sh.000300' AND date = ? AND adjustflag = '3'
            """,
            [date],
        ).fetchone()
        mkt = float(mkt_row[0]) if mkt_row and mkt_row[0] is not None else None

        # ── 2. 找重分组日期 ──
        # 查询日在当期（月/周）内 → 用上一期最后交易日的排序
        ts = pd.Timestamp(date)
        if regroup_freq == "monthly":
            # 当月1号的前一个交易日 = 上月末最后一个交易日
            period_start = ts.replace(day=1)
        else:  # weekly
            # 当周周一的前一个交易日 = 上周末最后一个交易日
            period_start = ts - pd.Timedelta(days=ts.weekday())

        regroup_row = conn.execute(
            """
            SELECT MAX(date) FROM daily_bar
            WHERE adjustflag = '3' AND date < ?
            """,
            [str(period_start.date())],
        ).fetchone()

        if not regroup_row or regroup_row[0] is None:
            logger.warning(f"ff3_factor: {date} 无法找到重分组日期 (freq={regroup_freq})")
            return pd.DataFrame(columns=empty_cols)

        regroup_date = str(regroup_row[0])

        # ── 3. 在重分组日取市值和BM ──
        df_group = conn.execute(
            """
            SELECT code, close, volume, turn, pb_mrq
            FROM daily_bar
            WHERE date = ? AND adjustflag = '3' AND tradestatus = '1'
              AND turn > 0 AND pb_mrq IS NOT NULL AND pb_mrq > 0
            """,
            [regroup_date],
        ).df()

        if df_group.empty:
            logger.warning(f"ff3_factor: 重分组日 {regroup_date} 无有效数据")
            return pd.DataFrame(columns=empty_cols)

        # ── 4. 计算流通市值和 BM ──
        df_group["flow_cap"] = df_group["close"] * df_group["volume"] * 100 / df_group["turn"] / 1e8
        df_group["bm"] = 1.0 / df_group["pb_mrq"]

        # ── 5. 2×3 排序 ──
        median_cap = df_group["flow_cap"].median()
        df_group["size_group"] = df_group["flow_cap"].apply(
            lambda x: "S" if x < median_cap else "B"
        )

        bm_30 = df_group["bm"].quantile(0.3)
        bm_70 = df_group["bm"].quantile(0.7)
        df_group["bm_group"] = df_group["bm"].apply(
            lambda x: "L" if x < bm_30 else ("H" if x >= bm_70 else "M")
        )
        df_group["portfolio"] = df_group["size_group"] + "/" + df_group["bm_group"]

        # ── 6. 取查询日的收益率 ──
        df_returns = conn.execute(
            """
            SELECT code, pct_chg
            FROM daily_bar
            WHERE date = ? AND adjustflag = '3' AND tradestatus = '1'
              AND pct_chg IS NOT NULL
            """,
            [date],
        ).df()

        # ── 7. 合并分组 + 收益 ──
        df = df_group[["code", "portfolio"]].merge(df_returns, on="code", how="inner")

        if df.empty:
            logger.warning(f"ff3_factor: {date} 无匹配的分组-收益数据")
            return pd.DataFrame(columns=empty_cols)

        # ── 8. 计算各组合等权收益 ──
        portfolio_returns = {}
        for p in ["S/L", "S/M", "S/H", "B/L", "B/M", "B/H"]:
            sub = df[df["portfolio"] == p]
            portfolio_returns[p] = float(sub["pct_chg"].mean()) if not sub.empty else 0.0

        # ── 9. 计算三因子 ──
        smb = (
            portfolio_returns["S/L"] + portfolio_returns["S/M"] + portfolio_returns["S/H"]
        ) / 3 - (
            portfolio_returns["B/L"] + portfolio_returns["B/M"] + portfolio_returns["B/H"]
        ) / 3
        hml = (
            portfolio_returns["S/H"] + portfolio_returns["B/H"]
        ) / 2 - (
            portfolio_returns["S/L"] + portfolio_returns["B/L"]
        ) / 2
    finally:
        conn.close()

    return pd.DataFrame([{
        "date": date,
        "regroup_freq": regroup_freq,
        "regroup_date": regroup_date,
        "mkt": round(mkt, 4) if mkt is not None else None,
        "smb": round(smb, 4),
        "hml": round(hml, 4),
        "sl_return": round(portfolio_returns["S/L"], 4),
        "sm_return": round(portfolio_returns["S/M"], 4),
        "sh_return": round(portfolio_returns["S/H"], 4),
        "bl_return": round(portfolio_returns["B/L"], 4),
        "bm_return": round(portfolio_returns["B/M"], 4),
        "bh_return": round(portfolio_returns["B/H"], 4),
        "stock_count": len(df),
    }])
