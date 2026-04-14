"""
技术指标计算模块。

提供 MACD / KDJ / RSI 等常用技术指标，基于 pandas Series 运算，
不依赖外部技术分析库。
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# MACD
# ---------------------------------------------------------------------------

def calc_macd(
    close: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> Dict[str, pd.Series]:
    """计算 MACD 指标。

    返回:
        dif: 快线 - 慢线 (EMA_fast - EMA_slow)
        dea: DIF 的 EMA(signal)
        macd: (DIF - DEA) * 2  （柱状图）
    """
    close = pd.to_numeric(close, errors="coerce")
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd_bar = (dif - dea) * 2
    return {"dif": dif, "dea": dea, "macd": macd_bar}


# ---------------------------------------------------------------------------
# KDJ
# ---------------------------------------------------------------------------

def calc_kdj(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    n: int = 9,
    m1: int = 3,
    m2: int = 3,
) -> Dict[str, pd.Series]:
    """计算 KDJ 指标。

    返回:
        k: K 值
        d: D 值
        j: J 值 = 3K - 2D
    """
    high = pd.to_numeric(high, errors="coerce")
    low = pd.to_numeric(low, errors="coerce")
    close = pd.to_numeric(close, errors="coerce")

    lowest_low = low.rolling(window=n, min_periods=1).min()
    highest_high = high.rolling(window=n, min_periods=1).max()

    rsv = ((close - lowest_low) / (highest_high - lowest_low).replace(0, np.nan)) * 100
    rsv = rsv.fillna(50)

    k = pd.Series(np.nan, index=close.index, dtype=float)
    d = pd.Series(np.nan, index=close.index, dtype=float)

    k.iloc[0] = 50.0
    d.iloc[0] = 50.0
    for i in range(1, len(rsv)):
        k.iloc[i] = ((m1 - 1.0) / m1) * k.iloc[i - 1] + (1.0 / m1) * rsv.iloc[i]
        d.iloc[i] = ((m2 - 1.0) / m2) * d.iloc[i - 1] + (1.0 / m2) * k.iloc[i]

    j = 3 * k - 2 * d
    return {"k": k, "d": d, "j": j}


# ---------------------------------------------------------------------------
# RSI
# ---------------------------------------------------------------------------

def calc_rsi(
    close: pd.Series,
    periods: Tuple[int, ...] = (6, 12, 24),
) -> Dict[str, pd.Series]:
    """计算 RSI 指标（支持多个周期）。

    返回:
        rsi_6, rsi_12, rsi_24 等
    """
    close = pd.to_numeric(close, errors="coerce")
    delta = close.diff()
    result = {}
    for period in periods:
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - 100 / (1 + rs)
        result[f"rsi_{period}"] = rsi
    return result


# ---------------------------------------------------------------------------
# 布林带 (BOLL)
# ---------------------------------------------------------------------------

def calc_boll(
    close: pd.Series,
    period: int = 20,
    num_std: float = 2.0,
) -> Dict[str, pd.Series]:
    """计算布林带。

    返回:
        mid: 中轨 (MA)
        upper: 上轨 (mid + num_std * std)
        lower: 下轨 (mid - num_std * std)
    """
    close = pd.to_numeric(close, errors="coerce")
    mid = close.rolling(window=period, min_periods=period).mean()
    std = close.rolling(window=period, min_periods=period).std()
    return {
        "mid": mid,
        "upper": mid + num_std * std,
        "lower": mid - num_std * std,
    }


# ---------------------------------------------------------------------------
# 便捷函数：为 DataFrame 一次计算全部指标
# ---------------------------------------------------------------------------

def enrich_with_indicators(
    df: pd.DataFrame,
    macd: bool = True,
    kdj: bool = True,
    rsi: bool = True,
    boll: bool = True,
) -> pd.DataFrame:
    """给含 OHLC 的 DataFrame 附加技术指标列，返回新 DataFrame（不修改原始）。"""
    out = df.copy()
    close = pd.to_numeric(out["close"], errors="coerce")

    if macd and "close" in out.columns:
        m = calc_macd(close)
        out["macd_dif"] = m["dif"]
        out["macd_dea"] = m["dea"]
        out["macd_bar"] = m["macd"]

    if kdj and all(c in out.columns for c in ("high", "low", "close")):
        k = calc_kdj(out["high"], out["low"], close)
        out["kdj_k"] = k["k"]
        out["kdj_d"] = k["d"]
        out["kdj_j"] = k["j"]

    if rsi and "close" in out.columns:
        r = calc_rsi(close)
        for key, series in r.items():
            out[key] = series

    if boll and "close" in out.columns:
        b = calc_boll(close)
        out["boll_mid"] = b["mid"]
        out["boll_upper"] = b["upper"]
        out["boll_lower"] = b["lower"]

    return out
