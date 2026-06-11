"""FF3 因子回归分析策略"""

import logging

import pandas as pd
import duckdb_tools as dt

logger = logging.getLogger("strategy.ff3_regression")

STRATEGY = {
    "description": "FF3因子回归分析：对个股/组合做 OLS 回归，输出 α 和 β",
    "columns": [
        {"name": "target", "type": "VARCHAR", "not_null": True},
        {"name": "date", "type": "DATE", "not_null": True},
        {"name": "window_days", "type": "INTEGER", "not_null": True},
        {"name": "regroup_freq", "type": "VARCHAR", "not_null": True},
        {"name": "alpha", "type": "DOUBLE"},
        {"name": "alpha_tstat", "type": "DOUBLE"},
        {"name": "beta_mkt", "type": "DOUBLE"},
        {"name": "beta_mkt_tstat", "type": "DOUBLE"},
        {"name": "beta_smb", "type": "DOUBLE"},
        {"name": "beta_smb_tstat", "type": "DOUBLE"},
        {"name": "beta_hml", "type": "DOUBLE"},
        {"name": "beta_hml_tstat", "type": "DOUBLE"},
        {"name": "r_squared", "type": "DOUBLE"},
        {"name": "residual_std", "type": "DOUBLE"},
        {"name": "sample_count", "type": "INTEGER"},
    ],
    "primary_key": ["target", "date", "window_days", "regroup_freq"],
    "params": {
        "target_type": "stock",
        "target_code": "sh.600519",
        "window_days": 60,
        "regroup_freq": "monthly",
    },
    "param_ui": {
        "target_type": {
            "type": "select",
            "options": ["stock", "watchlist"],
            "label": "分析对象",
            "help": "stock=个股, watchlist=自选组合",
        },
        "target_code": {
            "type": "text",
            "default": "sh.600519",
            "label": "股票代码/组合名称",
            "help": "个股填代码如 sh.600519，组合填名称如 spacex概念",
        },
        "window_days": {
            "type": "number",
            "min": 20,
            "max": 500,
            "step": 20,
            "label": "回归窗口（交易日）",
            "help": "用最近 N 个交易日的数据做 OLS 回归",
        },
        "regroup_freq": {
            "type": "select",
            "options": ["monthly", "weekly"],
            "label": "因子重分组频率",
            "help": "与 FF3 因子构建一致",
        },
    },
    "_说明": {
        "target": "分析对象标识：个股=代码(如sh.600519)，组合=名称(如spacex概念)",
        "alpha": "截距项，>0 表示存在三因子无法解释的超额收益（日度%）",
        "alpha_tstat": "alpha 的 t 统计量，|t|>2 则显著",
        "beta_mkt/smb/hml": "因子暴露，如 beta_smb>0 偏小盘，beta_hml>0 偏价值",
        "beta_*_tstat": "对应 β 的 t 统计量",
        "r_squared": "模型解释力度，0~1，越高说明三因子解释得越好",
        "residual_std": "残差标准差，个股特有波动大小",
        "sample_count": "实际参与回归的交易日数",
        "OLS": "y = Xβ + ε，β̂ = (XᵀX)⁻¹Xᵀy，t = β̂ / SE(β̂)",
    },
}


def compute(
    date: str,
    target_type: str = "stock",
    target_code: str = "sh.600519",
    window_days: int = 60,
    regroup_freq: str = "monthly",
) -> pd.DataFrame:
    """
    FF3 因子回归分析。

    对个股或自选组合的日收益率做 OLS 回归：
      R_i - R_f = α + β_MKT × MKT + β_SMB × SMB + β_HML × HML + ε
    R_f 暂设为 0。

    步骤：
      1. 取 [date - window_days, date] 范围的 FF3 因子数据
      2. 取同一时间窗口的个股/组合日收益率
      3. 对齐后做 OLS: β̂ = (XᵀX)⁻¹Xᵀy
      4. 计算 t 统计量、R² 等
    """
    empty_cols = [
        "target", "date", "window_days", "regroup_freq",
        "alpha", "alpha_tstat", "beta_mkt", "beta_mkt_tstat",
        "beta_smb", "beta_smb_tstat", "beta_hml", "beta_hml_tstat",
        "r_squared", "residual_std", "sample_count",
    ]

    conn = dt.get_read_connection()
    try:
        # ── 1. 确定日期范围 ──
        start_row = conn.execute(
            """
            SELECT DISTINCT date FROM daily_bar
            WHERE date <= ? AND adjustflag = '3'
            ORDER BY date DESC
            LIMIT 1 OFFSET ?
            """,
            [date, window_days - 1],
        ).fetchone()

        if not start_row:
            logger.warning(f"ff3_regression: {date} 往前 {window_days} 天无数据")
            return pd.DataFrame(columns=empty_cols)

        start_date = str(start_row[0])

        # ── 2. 取 FF3 因子数据 ──
        # 如果因子数据不足，自动补算
        factors_df = conn.execute(
            """
            SELECT date, mkt, smb, hml
            FROM strategy_ff3_factor
            WHERE date BETWEEN ? AND ? AND regroup_freq = ?
            ORDER BY date
            """,
            [start_date, date, regroup_freq],
        ).df()

        if len(factors_df) < 20:
            # 补算：找出缺失的交易日，用 ff3_factor 模块直接计算
            conn.close()

            # 直接 import ff3_factor 模块（同包，无循环依赖）
            from strategies.ff3_factor import compute as ff3_compute
            import strategy as se  # 延迟导入，避免循环

            conn2 = dt.get_read_connection()
            missing_dates = conn2.execute(
                """
                SELECT DISTINCT d.date FROM daily_bar d
                WHERE d.date BETWEEN ? AND ? AND d.adjustflag = '3' AND d.tradestatus = '1'
                  AND d.date NOT IN (
                    SELECT date FROM strategy_ff3_factor
                    WHERE regroup_freq = ?
                  )
                ORDER BY d.date
                """,
                [start_date, date, regroup_freq],
            ).df()
            conn2.close()

            if not missing_dates.empty:
                for _, row in missing_dates.iterrows():
                    result = ff3_compute(date=str(row["date"]), regroup_freq=regroup_freq)
                    if result is not None and not result.empty:
                        se.write_strategy_result("ff3_factor", result)
                logger.info(f"ff3_regression: 补算 {len(missing_dates)} 天因子数据")

            # 重新读取
            conn = dt.get_read_connection()
            factors_df = conn.execute(
                """
                SELECT date, mkt, smb, hml
                FROM strategy_ff3_factor
                WHERE date BETWEEN ? AND ? AND regroup_freq = ?
                ORDER BY date
                """,
                [start_date, date, regroup_freq],
            ).df()

        if factors_df.empty or len(factors_df) < 20:
            logger.warning(f"ff3_regression: 因子数据不足 ({len(factors_df)} 天)")
            return pd.DataFrame(columns=empty_cols)

        # ── 3. 取目标收益率 ──
        if target_type == "stock":
            returns_df = conn.execute(
                """
                SELECT date, pct_chg
                FROM daily_bar
                WHERE code = ? AND adjustflag = '3' AND tradestatus = '1'
                  AND date BETWEEN ? AND ? AND pct_chg IS NOT NULL
                ORDER BY date
                """,
                [target_code, start_date, date],
            ).df()
        else:  # watchlist
            # 从 watchlist JSON 读取组合成分，计算等权日收益率
            import watchlist_tools as wt
            codes = wt.get_stock_codes(target_code)
            if not codes:
                logger.warning(f"ff3_regression: 组合 '{target_code}' 无成分股")
                return pd.DataFrame(columns=empty_cols)

            placeholders = ",".join(["?"] * len(codes))
            returns_df = conn.execute(
                f"""
                SELECT date, AVG(pct_chg) as pct_chg
                FROM daily_bar
                WHERE code IN ({placeholders}) AND adjustflag = '3' AND tradestatus = '1'
                  AND date BETWEEN ? AND ? AND pct_chg IS NOT NULL
                GROUP BY date
                ORDER BY date
                """,
                codes + [start_date, date],
            ).df()

        if returns_df.empty or len(returns_df) < 20:
            logger.warning(f"ff3_regression: {target_code} 收益数据不足 ({len(returns_df)} 天)")
            return pd.DataFrame(columns=empty_cols)

        # ── 4. 对齐因子和收益 ──
        merged = factors_df.merge(returns_df[["date", "pct_chg"]], on="date", how="inner")
        if len(merged) < 20:
            logger.warning(f"ff3_regression: 对齐后数据不足 ({len(merged)} 天)")
            return pd.DataFrame(columns=empty_cols)

        # ── 5. OLS 回归 ──
        import numpy as np

        y = merged["pct_chg"].values  # T×1
        X = np.column_stack([
            np.ones(len(merged)),       # intercept (α)
            merged["mkt"].values,       # MKT
            merged["smb"].values,       # SMB
            merged["hml"].values,       # HML
        ])  # T×4

        # β̂ = (XᵀX)⁻¹Xᵀy
        beta_hat, *_ = np.linalg.lstsq(X, y, rcond=None)

        # 残差和统计量
        y_hat = X @ beta_hat
        resid = y - y_hat
        n, k = X.shape
        residual_var = np.sum(resid ** 2) / (n - k) if n > k else 0

        # 标准误 = sqrt(diag((XᵀX)⁻¹ × residual_var))
        try:
            xtx_inv = np.linalg.inv(X.T @ X)
            se = np.sqrt(np.diag(xtx_inv) * residual_var)
            t_stats = beta_hat / np.where(se != 0, se, np.nan)
        except np.linalg.LinAlgError:
            se = np.full(k, np.nan)
            t_stats = np.full(k, np.nan)

        # R² = 1 - SS_res / SS_tot
        ss_res = np.sum(resid ** 2)
        ss_tot = np.sum((y - y.mean()) ** 2)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    finally:
        conn.close()

    return pd.DataFrame([{
        "target": target_code,
        "date": date,
        "window_days": window_days,
        "regroup_freq": regroup_freq,
        "alpha": round(float(beta_hat[0]), 6),
        "alpha_tstat": round(float(t_stats[0]), 4),
        "beta_mkt": round(float(beta_hat[1]), 4),
        "beta_mkt_tstat": round(float(t_stats[1]), 4),
        "beta_smb": round(float(beta_hat[2]), 4),
        "beta_smb_tstat": round(float(t_stats[2]), 4),
        "beta_hml": round(float(beta_hat[3]), 4),
        "beta_hml_tstat": round(float(t_stats[3]), 4),
        "r_squared": round(float(r_squared), 4),
        "residual_std": round(float(np.sqrt(residual_var)), 4),
        "sample_count": len(merged),
    }])
