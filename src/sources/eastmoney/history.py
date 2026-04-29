"""东方财富历史日线抓取（直连 push2his，多镜像轮换 + 自适应总超时）。

入口：
- ``probe_mirror(url)``：用 000001 测试单个镜像是否能拉到 ~40 天数据。
- ``fetch_hist_frame(stock_code, days, start_date, end_date, mirror_urls=None, log=None)``：
  按镜像列表顺序尝试，遇限流/封禁立即终止；总耗时受 ``ASHARE_SCAN_HISTORY_TOTAL_TIMEOUT_SEC`` 约束。

镜像健康度状态由 ``src.network.host_health`` 统一管理，本模块只是上报成功/失败。
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Callable, List, Optional, Tuple

import pandas as pd

from src.config import env_float
from src.network.host_health import mark_failed, mark_ok
from src.sources.eastmoney import throttling as _throttling
from src.sources.eastmoney.history_parser import parse_hist_json, request_params
from src.sources.eastmoney.rate_limit import mirror_host_of
from src.sources.eastmoney.session import get_json
from src.sources.eastmoney.throttling import (
    EastmoneyRateLimitError,
    HistoryAccessSuspendedError,
)


HISTORY_MIRRORS = [
    "https://push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://82.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://40.push2his.eastmoney.com/api/qt/stock/kline/get",
]


def _connect_timeout_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_CONNECT_TIMEOUT_SEC", default=2.5, lo=0.5, hi=10.0)


def _read_timeout_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_READ_TIMEOUT_SEC", default=4.0, lo=1.0, hi=15.0)


def _total_timeout_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_TOTAL_TIMEOUT_SEC", default=12.0, lo=3.0, hi=60.0)


def probe_mirror(url: str) -> Tuple[bool, str]:
    """探测某个镜像是否健康；用 000001 + 最近 40 天作为测试请求。

    返回 ``(is_healthy, latest_date_or_reason)``。
    """
    probe_code = "000001"
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=40)).strftime("%Y%m%d")
    params = request_params(probe_code, start_date, end_date)
    _throttling.increment_diagnostic("probe_requests")
    try:
        data_json = get_json(
            url,
            params=params,
            timeout=(_connect_timeout_sec(), _read_timeout_sec()),
        )
        df = parse_hist_json(probe_code, data_json)
        if df.empty:
            mark_failed(url)
            return False, "empty"
        latest_series = df["日期"].dropna()
        latest_date = str(latest_series.iloc[-1]) if not latest_series.empty else "unknown"
        mark_ok(url)
        _throttling.increment_diagnostic("probe_success")
        return True, latest_date
    except HistoryAccessSuspendedError as e:
        mark_failed(url)
        _throttling.increment_diagnostic("probe_failures")
        return False, str(e)
    except EastmoneyRateLimitError as e:
        mark_failed(url)
        _throttling.increment_diagnostic("probe_failures")
        return False, str(e)
    except Exception as e:
        mark_failed(url)
        _throttling.increment_diagnostic("probe_failures")
        return False, str(e)


def fetch_hist_frame(
    stock_code: str,
    days: int,
    start_date: str,
    end_date: str,
    mirror_urls: Optional[List[str]] = None,
    log: Optional[Callable[[str], None]] = None,
) -> "pd.DataFrame":
    """直接抓东方财富历史日线，并在多个镜像间轮换。

    ``days`` 当前未在请求层使用（保留入参兼容旧调用），实际范围由 ``start_date / end_date`` 决定。
    """
    params = request_params(stock_code, start_date, end_date)
    mirrors = list(mirror_urls or HISTORY_MIRRORS)
    last_exception: Optional[BaseException] = None
    deadline = time.time() + _total_timeout_sec()

    for base_url in mirrors:
        host = mirror_host_of(base_url)
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        try:
            connect_timeout = min(_connect_timeout_sec(), max(0.5, remaining))
            read_timeout = min(_read_timeout_sec(), max(1.0, remaining))
            data_json = get_json(
                base_url,
                params=params,
                timeout=(connect_timeout, read_timeout),
            )
            df = parse_hist_json(stock_code, data_json)
            if df.empty:
                err = RuntimeError(f"{host} 返回空历史数据")
                last_exception = err
                mark_failed(base_url)
                if log:
                    log(f"历史 {stock_code} 镜像 {host} 返回空数据，切换下一个镜像。")
                continue
            mark_ok(base_url)
            return df
        except HistoryAccessSuspendedError as e:
            last_exception = e
            if log:
                log(f"历史 {stock_code} 暂停访问东方财富：{e}")
            break
        except EastmoneyRateLimitError as e:
            last_exception = e
            mark_failed(base_url)
            if log:
                log(f"历史 {stock_code} 触发东方财富限流保护：{e}")
            break
        except Exception as e:
            last_exception = e
            mark_failed(base_url)
            if log:
                log(f"历史 {stock_code} 镜像 {host} 失败: {e}，切换下一个镜像。")
    if last_exception is not None:
        raise last_exception
    return pd.DataFrame()
