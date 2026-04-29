"""东方财富 stock/get 行情字段的数值规范化。

东财把价格字段编成"元×1000"的整数（贵州茅台这种 1000+ 元的也只到 1500000 量级），
本模块按阈值还原成浮点元。
"""
from __future__ import annotations

from typing import Any

import pandas as pd


def em_scalar(x: Any) -> float:
    """把东财字段安全转成 float；空值/破值统一返回 0.0。"""
    if x is None or x == "-":
        return 0.0
    try:
        if isinstance(x, float) and pd.isna(x):
            return 0.0
    except Exception:
        pass
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def em_price_yuan(x: Any) -> float:
    """东财价格字段还原。

    阈值用 10000 以避免误伤高价股（如贵州茅台 ~1500 元）：
    - |v| >= 10000 → 视为「元×1000」编码，除以 1000；
    - 否则按原值返回。
    """
    v = em_scalar(x)
    if v == 0.0:
        return 0.0
    if abs(v) >= 10000:
        return v / 1000.0
    return v
