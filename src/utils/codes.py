"""股票代码标准化 + 交易所/板块推断。

纯函数模块，无外部状态依赖。
"""
from __future__ import annotations

import re
from typing import Any

import pandas as pd


def norm_code(code: Any) -> str:
    """把任意输入转成 6 位的字符串代码。空值返回空字符串。"""
    s = str(code).strip()
    if not s or s.lower() == "nan":
        return ""
    return re.sub(r"\.0$", "", s).strip().zfill(6)


def norm_code_series(s: pd.Series) -> pd.Series:
    """对 Series 批量做 ``norm_code``，比逐元素 map 快。"""
    return (
        s.astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .str.zfill(6)
    )


def infer_sz_board(code: str) -> str:
    """推断深市股票所属板块：创业板 / 主板 / A 股。"""
    c = str(code).strip().zfill(6)
    if c.startswith(("300", "301")):
        return "创业板"
    if c.startswith(("000", "001", "002", "003")):
        return "深交所主板"
    return "深交所A股"


def infer_exchange(code: str) -> str:
    """根据代码首位推断交易所归属（不区分北交所，与现有调用方保持一致）。"""
    c = str(code).strip().zfill(6)
    return "上交所" if c.startswith(("5", "6", "9")) else "深交所"
