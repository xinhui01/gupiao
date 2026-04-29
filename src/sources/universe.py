"""A 股全市场列表构建（深 + 沪含科创板，不含北交所）。

使用 akshare 的官方交易所接口（``stock_info_sz_name_code`` / ``stock_info_sh_name_code``），
不走东方财富 clist 分页（易卡死）。
"""
from __future__ import annotations

import os
from typing import Callable, List, Optional

import pandas as pd

from src.utils.codes import infer_sz_board, norm_code_series


def use_em_full_spot_for_list() -> bool:
    """设为 1 / em / eastmoney 时仍走东方财富分页全表（易卡死，不推荐）。"""
    return os.environ.get("ASHARE_SCAN_LIST_SOURCE", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "em",
        "eastmoney",
        "efull",
    )


def build_a_share_universe(log: Optional[Callable[[str], None]] = None) -> pd.DataFrame:
    """深交所 + 上交所（含科创板）官方列表，不含北交所；少量 HTTP，无东方财富 clist 分页。"""
    import akshare as ak
    # 延迟 import：_retry_ak_call 留在 stock_data
    from stock_data import _retry_ak_call

    parts: List[pd.DataFrame] = []
    tasks = [
        (
            "深交所 A 股",
            lambda: ak.stock_info_sz_name_code(symbol="A股列表"),
            {"A股代码": "code", "A股简称": "name"},
            "深交所",
        ),
        (
            "上交所主板",
            lambda: ak.stock_info_sh_name_code(symbol="主板A股"),
            {"证券代码": "code", "证券简称": "name"},
            "上交所",
        ),
        (
            "科创板",
            lambda: ak.stock_info_sh_name_code(symbol="科创板"),
            {"证券代码": "code", "证券简称": "name"},
            "上交所",
        ),
    ]
    for label, fetch, cmap, exchange in tasks:
        try:
            raw = _retry_ak_call(fetch)
            if raw is None or getattr(raw, "empty", True):
                if log:
                    log(f"{label}: 无数据，跳过。")
                continue
            d = raw.rename(columns=cmap)[["code", "name"]].copy()
            d["code"] = norm_code_series(d["code"])
            d["exchange"] = exchange
            if label == "深交所 A 股":
                d["board"] = d["code"].map(infer_sz_board)
            elif label == "上交所主板":
                d["board"] = "上交所主板"
            else:
                d["board"] = "科创板"
            parts.append(d)
            if log:
                log(f"{label}: {len(d)} 只")
        except Exception as e:
            if log:
                log(f"{label} 失败: {e}（已跳过该段）")
    if not parts:
        return pd.DataFrame(columns=["code", "name", "exchange", "board"])
    out = pd.concat(parts, ignore_index=True)
    out = out.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    if log:
        log(f"合并去重后股票池共 {len(out)} 只。")
    return out[["code", "name", "exchange", "board"]]
