"""东方财富历史日线请求构造 + 响应解析。

纯函数，无状态依赖，可独立测试。
"""
from __future__ import annotations

import time
from typing import Any, Dict

import pandas as pd

from src.sources._jsonp import random_callback


def request_params(stock_code: str, start_date: str, end_date: str) -> Dict[str, str]:
    """构造东方财富 stock/kline/get 接口的请求参数。"""
    market_code = 1 if str(stock_code).startswith("6") else 0
    return {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": "101",
        "fqt": "0",
        "secid": f"{market_code}.{stock_code}",
        "beg": start_date,
        "end": end_date,
        "cb": random_callback(),
        "_": str(int(time.time() * 1000)),
    }


def parse_hist_json(stock_code: str, data_json: Dict[str, Any]) -> "pd.DataFrame":
    """解析东方财富 klines 响应为 DataFrame（中文列名，与 akshare 输出一致）。"""
    klines = (data_json.get("data") or {}).get("klines") or []
    if not klines:
        return pd.DataFrame()
    temp_df = pd.DataFrame([item.split(",") for item in klines])
    temp_df["股票代码"] = str(stock_code).strip().zfill(6)
    temp_df.columns = [
        "日期",
        "开盘",
        "收盘",
        "最高",
        "最低",
        "成交量",
        "成交额",
        "振幅",
        "涨跌幅",
        "涨跌额",
        "换手率",
        "股票代码",
    ]
    temp_df["日期"] = pd.to_datetime(temp_df["日期"], errors="coerce").dt.date
    for col in [
        "开盘",
        "收盘",
        "最高",
        "最低",
        "成交量",
        "成交额",
        "振幅",
        "涨跌幅",
        "涨跌额",
        "换手率",
    ]:
        temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")
    return temp_df[
        [
            "日期",
            "股票代码",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
        ]
    ]
