"""
index_tools.py — 技术指标计算模块

职责：
  - 基于本地 DuckDB 数据（调用 duckdb_tools）计算各类技术指标
  - 支持：MACD、KDJ、BOLL、RSI、CCI、WR、ATR、MA/EMA、OBV、VOL_MA、DMA、VR
  - 提供单股便捷接口和多股批量接口
  - 不直接依赖 baostock/data_tools，只依赖 duckdb_tools（数据从本地获取）

依赖：
  pip install duckdb pandas numpy

命名规范（统一后缀约定）：
  _S / _L / _SIGNAL   — MACD 三值 / KDJ 三线及信号
  _UP / _MID / _DOWN  — BOLL 三轨
  _W / _PCT           — ATR 原始 / 百分比标准化
  _BUY / _SELL        — 金叉死叉信号（1=金叉，-1=死叉，0=无信号）
  _HA / _HS           — 历史波动率 / 年化波动率
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Literal, Optional

import duckdb # type: ignore
import numpy as np
import pandas as pd

import duckdb_tools as dt

# ─────────────────────────────────────────────────────────────────────────────
# 日志
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("indicator")

# ─────────────────────────────────────────────────────────────────────────────
# 常量
# ─────────────────────────────────────────────────────────────────────────────

# 交易日集合（A股：工作日，不含节假日，由调用方传入或从 duckdb_tools 获取）
# 本模块内部不做交易日校验，依赖上层传入正确区间

# ─────────────────────────────────────────────────────────────────────────────
# 全局配置（可由调用方覆盖）
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class IndicatorConfig:
    """
    指标计算全局配置，可按需实例化后传入各计算函数。

    默认值均为业界常用参数，可直接使用。
    """

    # ── MACD ────────────────────────────────────────────────────────────────
    macd_fast:   int = 12   # 快线 EMA 周期
    macd_slow:   int = 26   # 慢线 EMA 周期
    macd_signal: int = 9    # DEA(Signal) 周期

    # ── KDJ ─────────────────────────────────────────────────────────────────
    kdj_n:      int = 9    # RSV 计算周期（K/D 初始值用）
    kdj_m1:     int = 3    # K 指数平滑
    kdj_m2:     int = 3    # D 指数平滑

    # ── BOLL ─────────────────────────────────────────────────────────────────
    boll_n:     int = 20   # 中轨 N 日均线周期
    boll_k:     float = 2.0 # 轨道宽度倍数

    # ── RSI ─────────────────────────────────────────────────────────────────
    rsi_periods: tuple[int, int, int] = (6, 12, 24)

    # ── CCI ─────────────────────────────────────────────────────────────────
    cci_period: int = 14

    # ── WR ──────────────────────────────────────────────────────────────────
    wr_periods: tuple[int, int] = (14, 28)

    # ── ATR ─────────────────────────────────────────────────────────────────
    atr_period: int = 14

    # ── MA / EMA ─────────────────────────────────────────────────────────────
    ma_periods: tuple[int, ...] = (5, 10, 20, 60)

    # ── OBV ─────────────────────────────────────────────────────────────────
    obv_ma_period: int = 20

    # ── VOL_MA ───────────────────────────────────────────────────────────────
    vol_ma_periods: tuple[int, int, int] = (5, 10, 20)

    # ── VR ──────────────────────────────────────────────────────────────────
    vr_period: int = 24

    # ── DMA ─────────────────────────────────────────────────────────────────
    dma_short: int = 10
    dma_long:  int = 50

    # ── 缓存相关 ─────────────────────────────────────────────────────────────
    # 结果 DataFrame 的 date 列是否强制转为 Python date（方便后续比较）
    normalize_date: bool = True

    # 缓存目录（未来扩展：指标结果缓存）；目前本模块不实现缓存，写入由调用方控制
    cache_dir: Optional[str] = None


# ═════════════════════════════════════════════════════════════════════════════
# 辅助函数
# ═════════════════════════════════════════════════════════════════════════════

def _ema(series: pd.Series, n: int) -> pd.Series:
    """
    计算指数移动平均（Exponential Moving Average）。

    使用 ta-lib / pandas 内置 ewm 实现，与通达信/Wind 一致。
    初始值取 SMA，之后递推。

    公式：EMA_t = α * price_t + (1-α) * EMA_{t-1}，
    α = 2/(N+1)
    """
    return series.ewm(span=n, adjust=False).mean()


def _smma(series: pd.Series, n: int) -> pd.Series:
    """
    平滑移动平均（Smoothed MA，SMMA），即"均线之王"等价实现。
    初始值取前 N 日 SMA。
    """
    result = series.copy()
    result.iloc[:n] = series.iloc[:n].mean()
    alpha = 1.0 / n
    for i in range(n, len(series)):
        result.iloc[i] = result.iloc[i - 1] + alpha * (series.iloc[i] - result.iloc[i - 1])
    return result


def _std(series: pd.Series, n: int) -> pd.Series:
    """滚动标准差（用于 BOLL）。"""
    return series.rolling(window=n, min_periods=1).std()


def _to_date(df: pd.DataFrame) -> pd.DataFrame:
    """将 date 列统一转为 Python date 对象。"""
    if "date" in df.columns and not pd.api.types.is_object_dtype(df["date"]):
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    return df


def _require_cols(df: pd.DataFrame, required: list[str], name: str) -> None:
    """校验 DataFrame 是否包含必需列。"""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[{name}] 缺少必需列: {missing}，当前列: {list(df.columns)}")


def _signal_cross_up(series_a: pd.Series, series_b: pd.Series) -> pd.Series:
    """
    金叉信号：series_a 从下方穿越 series_b 上方。
    前一天 series_a <= series_b，当天 series_a > series_b → 1，否则 0。
    """
    a = series_a.values
    b = series_b.values
    diff = np.diff(np.where(a[:-1] <= b[:-1], 0, 1) + np.where(a[1:] > b[1:], 1, 0))
    # diff == 1 表示今天金叉；用更直观方式
    up_today  = (a[1:] > b[1:])
    up_yest   = (a[:-1] > b[:-1])
    golden    = (~up_yest) & up_today
    return pd.Series(golden.astype(int), index=series_a.index[1:], name="buy_signal")


def _signal_cross_down(series_a: pd.Series, series_b: pd.Series) -> pd.Series:
    """
    死叉信号：series_a 从上方穿越 series_b 下方。
    前一天 series_a >= series_b，当天 series_a < series_b → -1，否则 0。
    """
    a = series_a.values
    b = series_b.values
    down_today = (a[1:] < b[1:])
    down_yest  = (a[:-1] < b[:-1])
    dead       = (~down_yest) & down_today
    return pd.Series((~dead).astype(int), index=series_a.index[1:], name="sell_signal")


# ═════════════════════════════════════════════════════════════════════════════
# 指标计算器
# ═════════════════════════════════════════════════════════════════════════════

# ── 1. MACD ─────────────────────────────────────────────────────────────────

def calc_macd(
    df: pd.DataFrame,
    cfg: Optional[IndicatorConfig] = None,
) -> pd.DataFrame:
    """
    计算 MACD（Moving Average Convergence & Divergence）。

    输出列：
      DIF      — 快线 EMA(fast) - EMA(slow)
      DEA      — Signal 线，EMA(DIF, signal_period)
      MACD     — 柱状图，2 * (DIF - DEA)
      MACD_SIGNAL — 金叉死叉信号（1=金叉，-1=死叉，0=无信号）
    """
    cfg = cfg or IndicatorConfig()
    _require_cols(df, ["close"], "calc_macd")

    result = df[["date", "code", "close"]].copy()
    if cfg.normalize_date:
        result = _to_date(result)

    close = result["close"]

    dif  = _ema(close, cfg.macd_fast) - _ema(close, cfg.macd_slow)
    dea  = _ema(dif, cfg.macd_signal)
    macd = 2.0 * (dif - dea)

    result["DIF"]  = dif.values
    result["DEA"]  = dea.values
    result["MACD"] = macd.values

    # 金叉死叉（DIF 上穿 DEA = 金叉，DIF 下穿 DEA = 死叉）
    buy  = _signal_cross_up(dif, dea)
    sell = _signal_cross_down(dif, dea)

    signal = pd.Series(0, index=result.index, name="MACD_SIGNAL")
    # 对齐：从 index[1:] 开始写入
    signal.iloc[1:] = buy.values + sell.values
    result["MACD_SIGNAL"] = signal.values

    logger.debug(f"[MACD] {len(result)} rows, fast={cfg.macd_fast} slow={cfg.macd_slow} signal={cfg.macd_signal}")
    return result


# ── 2. KDJ ──────────────────────────────────────────────────────────────────

def calc_kdj(
    df: pd.DataFrame,
    cfg: Optional[IndicatorConfig] = None,
) -> pd.DataFrame:
    """
    计算 KDJ（随机指标）。

    流程：
      1. RSV_n = (C_n - L_n) / (H_n - L_n) * 100
         其中 C=当日收盘价，L_n/H_n = 近 N 日最低/最高价
      2. K = M1/ M1+M2 * K_{t-1} + M2/ M1+M2 * RSV
         初始 K 取 SMA(KDJ_N)
      3. D = 同理对 K 做平滑
      4. J = 3*K - 2*D

    输出列：
      KDJ_K, KDJ_D, KDJ_J, KDJ_SIGNAL（KD 金叉死叉）
    """
    cfg = cfg or IndicatorConfig()
    _require_cols(df, ["close", "high", "low"], "calc_kdj")

    result = df[["date", "code", "close", "high", "low"]].copy()
    if cfg.normalize_date:
        result = _to_date(result)

    n  = cfg.kdj_n
    m1 = cfg.kdj_m1
    m2 = cfg.kdj_m2

    low_n  = result["low"].rolling(window=n, min_periods=1).min()
    high_n = result["high"].rolling(window=n, min_periods=1).max()

    rsv = (result["close"] - low_n) / (high_n - low_n + 1e-9) * 100.0

    # 初始化 K[0]、D[0] 为 50（或 SMA 前 n 日均值）
    k = pd.Series(50.0, index=result.index)
    d = pd.Series(50.0, index=result.index)

    for i in range(n, len(result)):
        k.iloc[i] = (m1 / (m1 + m2)) * k.iloc[i - 1] + (m2 / (m1 + m2)) * rsv.iloc[i]
        d.iloc[i] = (m1 / (m1 + m2)) * d.iloc[i - 1] + (m2 / (m1 + m2)) * k.iloc[i]

    j = 3.0 * k - 2.0 * d

    result["KDJ_K"] = k.values
    result["KDJ_D"] = d.values
    result["KDJ_J"] = j.values

    # 金叉死叉（K 上穿 D = 金叉，K 下穿 D = 死叉）
    buy  = _signal_cross_up(k, d)
    sell = _signal_cross_down(k, d)

    signal = pd.Series(0, index=result.index, name="KDJ_SIGNAL")
    signal.iloc[1:] = buy.values + sell.values
    result["KDJ_SIGNAL"] = signal.values

    logger.debug(f"[KDJ] {len(result)} rows, n={n} m1={m1} m2={m2}")
    return result


# ── 3. BOLL ──────────────────────────────────────────────────────────────────

def calc_boll(
    df: pd.DataFrame,
    cfg: Optional[IndicatorConfig] = None,
) -> pd.DataFrame:
    """
    计算布林带（Bollinger Bands）。

    输出列：
      BOLL_MID   — 中轨，N 日收盘价均线
      BOLL_UP    — 上轨，中轨 + K * σ
      BOLL_DOWN  — 下轨，中轨 - K * σ
      BOLL_WIDTH — 带宽，(UP - DOWN) / MID
      BOLL_PCT   — %B，(close - DOWN) / (UP - DOWN)
    """
    cfg = cfg or IndicatorConfig()
    _require_cols(df, ["close"], "calc_boll")

    result = df[["date", "code", "close"]].copy()
    if cfg.normalize_date:
        result = _to_date(result)

    close = result["close"]
    mid   = close.rolling(window=cfg.boll_n, min_periods=1).mean()
    std   = close.rolling(window=cfg.boll_n, min_periods=1).std()

    up   = mid + cfg.boll_k * std
    down = mid - cfg.boll_k * std
    width = (up - down) / (mid + 1e-9)
    pct   = (close - down) / (up - down + 1e-9)

    result["BOLL_MID"]  = mid.values
    result["BOLL_UP"]   = up.values
    result["BOLL_DOWN"] = down.values
    result["BOLL_WIDTH"] = width.values
    result["BOLL_PCT"]  = pct.values

    logger.debug(f"[BOLL] {len(result)} rows, n={cfg.boll_n} k={cfg.boll_k}")
    return result


# ── 4. RSI ──────────────────────────────────────────────────────────────────

def calc_rsi(
    df: pd.DataFrame,
    cfg: Optional[IndicatorConfig] = None,
) -> pd.DataFrame:
    """
    计算 RSI（相对强弱指数），支持多周期。

    公式：
      涨跌幅 > 0 → U = 涨幅，D = 0
      涨跌幅 < 0 → U = 0，D = |跌幅|
      RS = SMA(U) / SMA(D)
      RSI = 100 - 100 / (1 + RS)

    输出列：RSI6、RSI12、RSI24（分别对应 cfg.rsi_periods）
    """
    cfg = cfg or IndicatorConfig()
    _require_cols(df, ["close"], "calc_rsi")

    result = df[["date", "code", "close"]].copy()
    if cfg.normalize_date:
        result = _to_date(result)

    close = result["close"]
    delta = close.diff()

    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    for period in cfg.rsi_periods:
        avg_gain = gain.rolling(window=period, min_periods=1).mean()
        avg_loss = loss.rolling(window=period, min_periods=1).mean()
        rs  = avg_gain / (avg_loss + 1e-9)
        rsi = 100.0 - (100.0 / (1.0 + rs))
        result[f"RSI{period}"] = rsi.values

    logger.debug(f"[RSI] {len(result)} rows, periods={cfg.rsi_periods}")
    return result


# ── 5. CCI ─────────────────────────────────────────────────────────────────

def calc_cci(
    df: pd.DataFrame,
    cfg: Optional[IndicatorConfig] = None,
) -> pd.DataFrame:
    """
    计算 CCI（顺势指标）。

    公式：
      TP（典型价）= (H + L + C) / 3
      CCI_n = (TP - SMA(TP)) / (0.015 * MADev)
      MADev = TP 与 SMA(TP) 的平均绝对偏差

    输出列：CCI
    """
    cfg = cfg or IndicatorConfig()
    _require_cols(df, ["high", "low", "close"], "calc_cci")

    result = df[["date", "code", "high", "low", "close"]].copy()
    if cfg.normalize_date:
        result = _to_date(result)

    tp = (result["high"] + result["low"] + result["close"]) / 3.0
    sma_tp = tp.rolling(window=cfg.cci_period, min_periods=1).mean()
    mad = tp.rolling(window=cfg.cci_period, min_periods=1).apply(
        lambda x: np.abs(x - x.mean()).mean(), raw=False
    )
    cci = (tp - sma_tp) / (0.015 * mad + 1e-9)

    result["CCI"] = cci.values
    logger.debug(f"[CCI] {len(result)} rows, period={cfg.cci_period}")
    return result


# ── 6. WR（Williams %R）────────────────────────────────────────────────────

def calc_wr(
    df: pd.DataFrame,
    cfg: Optional[IndicatorConfig] = None,
) -> pd.DataFrame:
    """
    计算 Williams %R（威廉指标）。

    公式：WR_n = (H_n - C) / (H_n - L_n) * -100
    通常取 14 日（短期）和 28 日（中期）。

    输出列：WR14、WR28
    """
    cfg = cfg or IndicatorConfig()
    _require_cols(df, ["close", "high", "low"], "calc_wr")

    result = df[["date", "code", "close", "high", "low"]].copy()
    if cfg.normalize_date:
        result = _to_date(result)

    for period in cfg.wr_periods:
        high_n = result["high"].rolling(window=period, min_periods=1).max()
        low_n  = result["low"].rolling(window=period, min_periods=1).min()
        wr = -100.0 * (high_n - result["close"]) / (high_n - low_n + 1e-9)
        result[f"WR{period}"] = wr.values

    logger.debug(f"[WR] {len(result)} rows, periods={cfg.wr_periods}")
    return result


# ── 7. ATR ──────────────────────────────────────────────────────────────────

def calc_atr(
    df: pd.DataFrame,
    cfg: Optional[IndicatorConfig] = None,
) -> pd.DataFrame:
    """
    计算 ATR（Average True Range，真實波幅）。

    公式：
      TR_t = max(H_t - L_t, |H_t - C_{t-1}|, |L_t - C_{t-1}|)
      ATR_n = SMA(TR, n)

    输出列：
      TR       — 当日真实波幅
      ATR      — N 日 ATR 均值
      ATR_PCT  — ATR_PCT = ATR / close * 100（ATR 占收盘价比例，便于跨股票比较）
    """
    cfg = cfg or IndicatorConfig()
    _require_cols(df, ["high", "low", "close"], "calc_atr")

    result = df[["date", "code", "high", "low", "close"]].copy()
    if cfg.normalize_date:
        result = _to_date(result)

    prev_close = result["close"].shift(1)

    tr1 = result["high"] - result["low"]
    tr2 = (result["high"] - prev_close).abs()
    tr3 = (result["low"]  - prev_close).abs()
    tr  = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.rolling(window=cfg.atr_period, min_periods=1).mean()
    atr_pct = atr / result["close"] * 100.0

    result["TR"]      = tr.values
    result["ATR"]     = atr.values
    result["ATR_PCT"] = atr_pct.values

    logger.debug(f"[ATR] {len(result)} rows, period={cfg.atr_period}")
    return result


# ── 8. MA / EMA ──────────────────────────────────────────────────────────────

def calc_ma(
    df: pd.DataFrame,
    cfg: Optional[IndicatorConfig] = None,
) -> pd.DataFrame:
    """
    计算简单移动平均线（SMA）。

    输出列：MA5、MA10、MA20、MA60（由 cfg.ma_periods 控制）
    """
    cfg = cfg or IndicatorConfig()
    _require_cols(df, ["close"], "calc_ma")

    result = df[["date", "code", "close"]].copy()
    if cfg.normalize_date:
        result = _to_date(result)

    for period in cfg.ma_periods:
        ma = result["close"].rolling(window=period, min_periods=1).mean()
        result[f"MA{period}"] = ma.values

    logger.debug(f"[MA] {len(result)} rows, periods={cfg.ma_periods}")
    return result


def calc_ema(
    df: pd.DataFrame,
    cfg: Optional[IndicatorConfig] = None,
) -> pd.DataFrame:
    """
    计算指数移动平均线（EMA）。

    输出列：EMA5、EMA10、EMA20、EMA60
    """
    cfg = cfg or IndicatorConfig()
    _require_cols(df, ["close"], "calc_ema")

    result = df[["date", "code", "close"]].copy()
    if cfg.normalize_date:
        result = _to_date(result)

    for period in cfg.ma_periods:
        ema = _ema(result["close"], period)
        result[f"EMA{period}"] = ema.values

    logger.debug(f"[EMA] {len(result)} rows, periods={cfg.ma_periods}")
    return result


# ── 9. OBV ─────────────────────────────────────────────────────────────────

def calc_obv(
    df: pd.DataFrame,
    cfg: Optional[IndicatorConfig] = None,
) -> pd.DataFrame:
    """
    计算 OBV（能量潮，On Balance Volume）。

    公式：
      OBV_t = OBV_{t-1} + V_t       if C_t > C_{t-1}
            = OBV_{t-1} - V_t       if C_t < C_{t-1}
            = OBV_{t-1}              if C_t == C_{t-1}

    输出列：
      OBV      — 累计能量潮
      OBV_CHG  — OBV 日环比变化率 %
    """
    cfg = cfg or IndicatorConfig()
    _require_cols(df, ["close", "volume"], "calc_obv")

    result = df[["date", "code", "close", "volume"]].copy()
    if cfg.normalize_date:
        result = _to_date(result)

    close  = result["close"]
    volume = result["volume"].fillna(0).astype(float)

    direction = np.sign(close.diff())
    obv = (direction * volume).cumsum()
    obv.iloc[0] = volume.iloc[0]   # 第一天方向为 0，直接取成交量

    obv_chg = obv.pct_change() * 100.0

    result["OBV"]      = obv.values
    result["OBV_CHG"]  = obv_chg.values

    logger.debug(f"[OBV] {len(result)} rows")
    return result


# ── 10. VOL_MA（量均线 + 量比）───────────────────────────────────────────────

def calc_vol_ma(
    df: pd.DataFrame,
    cfg: Optional[IndicatorConfig] = None,
) -> pd.DataFrame:
    """
    计算成交量均线及量比。

    输出列：
      VOL_MA5 / VOL_MA10 / VOL_MA20 — 近 N 日均量
      VOL_RATIO                      — 量比（今日成交量 / 5 日均量）
    """
    cfg = cfg or IndicatorConfig()
    _require_cols(df, ["volume"], "calc_vol_ma")

    result = df[["date", "code", "volume"]].copy()
    if cfg.normalize_date:
        result = _to_date(result)

    for period in cfg.vol_ma_periods:
        result[f"VOL_MA{period}"] = (
            result["volume"].rolling(window=period, min_periods=1).mean().values
        )

    vol_ma5 = result["volume"] / (result["VOL_MA5"] + 1)
    result["VOL_RATIO"] = vol_ma5.values

    logger.debug(f"[VOL_MA] {len(result)} rows, periods={cfg.vol_ma_periods}")
    return result


# ── 11. DMA（差离值）───────────────────────────────────────────────────────

def calc_dma(
    df: pd.DataFrame,
    cfg: Optional[IndicatorConfig] = None,
) -> pd.DataFrame:
    """
    计算 DMA（Different of Moving Average，差离值）。

    公式：DIF = MA_short - MA_long
          AMA = EMA(DIF, short)  （Kaufman 自适应均线）

    输出列：
      DMA_DIF  — 短均与长均之差
      DMA_AMA  — 自适应均线
    """
    cfg = cfg or IndicatorConfig()
    _require_cols(df, ["close"], "calc_dma")

    result = df[["date", "code", "close"]].copy()
    if cfg.normalize_date:
        result = _to_date(result)

    ma_short = result["close"].rolling(window=cfg.dma_short, min_periods=1).mean()
    ma_long  = result["close"].rolling(window=cfg.dma_long,  min_periods=1).mean()
    dif      = ma_short - ma_long
    ama       = _ema(dif, cfg.dma_short)

    result["DMA_DIF"] = dif.values
    result["DMA_AMA"] = ama.values

    logger.debug(f"[DMA] {len(result)} rows, short={cfg.dma_short} long={cfg.dma_long}")
    return result


# ── 12. VR（成交量变异率）──────────────────────────────────────────────────

def calc_vr(
    df: pd.DataFrame,
    cfg: Optional[IndicatorConfig] = None,
) -> pd.DataFrame:
    """
    计算 VR（Volume Ratio，成交量变异率）。

    公式：
      上涨量 = Σ(成交量) where pct_chg > 0
      下跌量 = Σ(成交量) where pct_chg < 0
      持平量 = Σ(成交量) where pct_chg == 0
      VR = (上涨量 + 持平量/2) / (下跌量 + 持平量/2) * 100

    输出列：VR（24 日默认）
    """
    cfg = cfg or IndicatorConfig()
    _require_cols(df, ["volume", "pct_chg"], "calc_vr")

    result = df[["date", "code", "volume", "pct_chg"]].copy()
    if cfg.normalize_date:
        result = _to_date(result)

    pct  = result["pct_chg"].fillna(0)
    vol  = result["volume"].fillna(0).astype(float)

    up_vol   = vol.where(pct > 0, 0.0)
    down_vol = vol.where(pct < 0, 0.0)
    flat_vol = vol.where(pct == 0, 0.0)

    def vr_sum(n: int):
        up_s   = up_vol.rolling(window=n, min_periods=1).sum()
        down_s = down_vol.rolling(window=n, min_periods=1).sum()
        flat_s = flat_vol.rolling(window=n, min_periods=1).sum()
        return (up_s + flat_s * 0.5) / (down_s + flat_s * 0.5 + 1e-9) * 100.0

    result["VR"] = vr_sum(cfg.vr_period).values

    logger.debug(f"[VR] {len(result)} rows, period={cfg.vr_period}")
    return result


# ── 13. Historical Volatility（历史波动率）───────────────────────────────────

def calc_hv(
    df: pd.DataFrame,
    period: int = 20,
) -> pd.DataFrame:
    """
    计算历史波动率（Historical Volatility）。

    公式：
      日收益率 = ln(C_t / C_{t-1})
      HV = STD(收益率, period) * sqrt(252)  （年化）

    输出列：
      HV_DAILY — 日波动率（STD）
      HV_ANN   — 年化波动率
    """
    _require_cols(df, ["close"], "calc_hv")

    result = df[["date", "code", "close"]].copy()
    if "date" in result.columns and not pd.api.types.is_object_dtype(result["date"]):
        result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.date

    ret = np.log(result["close"] / result["close"].shift(1))
    hv_daily = ret.rolling(window=period, min_periods=1).std()
    hv_ann   = hv_daily * math.sqrt(252)

    result["HV_DAILY"] = hv_daily.values
    result["HV_ANN"]   = hv_ann.values

    logger.debug(f"[HV] {len(result)} rows, period={period}")
    return result


# ═════════════════════════════════════════════════════════════════════════════
# 统一入口
# ═════════════════════════════════════════════════════════════════════════════

def calc_indicators(
    df: pd.DataFrame,
    indicators: list[str] | Literal["all"] = "all",
    cfg: Optional[IndicatorConfig] = None,
) -> pd.DataFrame:
    """
    统一指标计算入口。

    参数:
        df:         原始行情 DataFrame，至少包含：date, code, close, high, low
        indicators: 要计算的指标列表，如 ['macd', 'kdj', 'boll']；
                    传 'all' 则计算全部指标。
        cfg:        IndicatorConfig 实例；为 None 时使用默认参数。

    返回:
        在原始 DataFrame 基础上追加各指标列。

    注意:
        - 传入 df 的 code 列应唯一（单只股票），多只股票请用 calc_batch。
        - 所有指标以列方式追加，返回列顺序与传入 indicators 一致。

    使用示例::

        # 单股计算
        df = dt.get_daily('sh.600519', '贵州茅台', '2025-01-01', '2026-04-17')
        result = calc_indicators(df, indicators=['macd', 'kdj', 'boll', 'rsi'])
        print(result[['date', 'close', 'DIF', 'KDJ_K', 'BOLL_MID']].tail())

        # 计算全部指标
        result_all = calc_indicators(df, indicators='all')
    """
    cfg = cfg or IndicatorConfig()

    if indicators == "all":
        indicators = [
            "macd", "kdj", "boll", "rsi", "cci", "wr",
            "atr", "ma", "ema", "obv", "vol_ma", "dma", "vr", "hv",
        ]
    else:
        indicators = [i.lower().strip() for i in indicators]

    # 基础列校验
    _require_cols(df, ["date", "code", "close"], "calc_indicators")
    if df.empty:
        logger.warning("calc_indicators: 传入空 DataFrame，直接返回")
        return df.copy()

    # 按依赖顺序计算（ma/ema 基础指标放前，组合指标放后）
    calc_order = ["ma", "ema", "atr", "macd", "kdj", "boll", "rsi", "cci",
                  "wr", "obv", "vol_ma", "dma", "vr", "hv"]
    ordered = [i for i in calc_order if i in indicators]

    result = df.copy()
    for name in ordered:
        calc_fn = _CALC_REGISTRY.get(name)
        if calc_fn is None:
            logger.warning(f"[calc_indicators] 未知指标: {name}，跳过")
            continue
        df_indicator = calc_fn(result, cfg=cfg)
        # 提取非基础列追加到 result
        extra_cols = [c for c in df_indicator.columns if c not in result.columns]
        result = pd.concat([result, df_indicator[extra_cols]], axis=1)

    logger.info(
        f"[calc_indicators] 完成，计算指标: {ordered}，"
        f"原始列数={len(df.columns)} → 结果列数={len(result.columns)}"
    )
    return result


# ── 计算函数注册表 ──────────────────────────────────────────────────────────

_CALC_REGISTRY: dict[str, callable] = {
    "macd":   calc_macd,
    "kdj":    calc_kdj,
    "boll":   calc_boll,
    "rsi":    calc_rsi,
    "cci":    calc_cci,
    "wr":     calc_wr,
    "atr":    calc_atr,
    "ma":     calc_ma,
    "ema":    calc_ema,
    "obv":    calc_obv,
    "vol_ma": calc_vol_ma,
    "dma":    calc_dma,
    "vr":     calc_vr,
    "hv":     calc_hv,
}


# ═════════════════════════════════════════════════════════════════════════════
# 高级：多股批量计算
# ═════════════════════════════════════════════════════════════════════════════

def calc_batch(
    codes: list[str],
    start_date: str,
    end_date: str,
    indicators: list[str] | Literal["all"] = "all",
    adjustflag: str = "1",
    db_path: str = dt._DEFAULT_DB_PATH,
    cfg: Optional[IndicatorConfig] = None,
) -> dict[str, pd.DataFrame]:
    """
    批量计算多只股票的技术指标。

    参数:
        codes:       股票代码列表，如 ['sh.600519', 'sh.600036']
        start_date:  起始日期
        end_date:    截止日期
        indicators:  要计算的指标列表，'all' 则全量
        adjustflag:  复权方式
        db_path:     DuckDB 数据库路径
        cfg:         IndicatorConfig

    返回:
        dict，key = code，value = 含指标的计算结果 DataFrame

    注意:
        此函数会自动从 DuckDB 获取数据（触发 Lazy Pull），
        内部按单只股票分组计算，不混在一起。

    使用示例::

        result = calc_batch(
            codes=['sh.600519', 'sh.600036'],
            code_names=['贵州茅台', '招商银行'],
            start_date='2025-01-01',
            end_date='2026-04-17',
            indicators=['macd', 'kdj', 'boll']
        )
        print(result['sh.600519'][['date', 'DIF', 'KDJ_K']].tail())
    """
    results: dict[str, pd.DataFrame] = {}

    for code in codes:
        df_raw = dt.get_daily(
            code=code,
            start_date=start_date,
            end_date=end_date,
            adjustflag=adjustflag,
            db_path=db_path,
        )
        if df_raw.empty:
            logger.warning(f"[calc_batch] {code} 数据为空，跳过")
            continue

        df_indicators = calc_indicators(df_raw, indicators=indicators, cfg=cfg)
        results[code] = df_indicators
        logger.info(f"[calc_batch] {code} 计算完成，共 {len(df_indicators)} 行")

    return results


# ═════════════════════════════════════════════════════════════════════════════
# 模块自测
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    print("\n===== 自测：calc_indicators（单股） =====")
    # 从 DuckDB 拉取数据（触发 Lazy Pull）
    df_raw = dt.get_daily(
        code="sh.600519",
        start_date="2025-01-01",
        end_date="2026-04-17",
        adjustflag="1",
    )
    print(f"原始日线: {len(df_raw)} 行")

    result = calc_indicators(
        df_raw,
        indicators=["macd", "kdj", "boll", "rsi", "atr", "ma", "ema", "obv", "vr"],
    )

    cols_show = [
        "date", "close", "DIF", "DEA", "MACD", "MACD_SIGNAL",
        "KDJ_K", "KDJ_D", "KDJ_J", "KDJ_SIGNAL",
        "BOLL_MID", "BOLL_UP", "BOLL_DOWN",
        "RSI6", "ATR", "MA5", "EMA5",
    ]
    print(result[cols_show].tail(5).to_string())

    print("\n===== 自测：calc_batch（多股） =====")
    batch = calc_batch(
        codes=["sh.600519", "sh.600036"],
        start_date="2026-03-01",
        end_date="2026-04-17",
        indicators=["macd", "kdj", "boll"],
    )
    for code, df in batch.items():
        last5 = df[["date", "DIF", "KDJ_K", "BOLL_MID"]].tail(3)
        print(f"\n{code}:")
        print(last5.to_string())

    print("\n全部测试完成")
