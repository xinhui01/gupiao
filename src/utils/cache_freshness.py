"""日期 / 缓存新鲜度判断工具。

包含：
- ``today_ymd()``：当前 YYYY-MM-DD
- ``should_refresh_today_row(df, date_col)``：日终缓存是否需要在收盘前实时刷新
- ``estimate_last_trade_date()``：估算最近一个交易日（仅排除周末，不考虑节假日）
- ``is_history_cache_fresh(stock_code, min_rows, log)``：综合 row_count + latest_trade_date + refreshed_at 判断本地历史缓存是否新鲜可复用

策略要点：``should_refresh_today_row`` 与 ``is_history_cache_fresh`` 都把 15:30 作为"基本稳定"分界——
之前继续刷新今日数据，之后视作可复用。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable, Optional

import pandas as pd

from stock_store import load_history_meta as _load_history_meta_store


def today_ymd() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def should_refresh_today_row(df: Optional[pd.DataFrame], date_col: str = "date") -> bool:
    """是否需要主动刷新今日行：仅当当前时间 < 15:30 且最新行就是今天才返回 True。"""
    if df is None or df.empty or date_col not in df.columns:
        return False
    latest_date = str(df[date_col].iloc[-1]).strip()
    if latest_date != today_ymd():
        return False
    now = datetime.now()
    # 15:30 之后默认认为日线与日资金流已基本稳定，可直接复用缓存。
    return now.hour < 15 or (now.hour == 15 and now.minute < 30)


def estimate_last_trade_date() -> str:
    """估算最近一个交易日（不考虑节假日，仅排除周末）。

    周一~周五 15:30 前返回上一个交易日，15:30 后返回当天；
    周六/周日返回最近的周五。
    """
    now = datetime.now()
    today = now.date()
    weekday = today.weekday()  # 0=Mon ... 6=Sun
    if weekday == 5:  # Saturday
        return (today - timedelta(days=1)).strftime("%Y-%m-%d")
    if weekday == 6:  # Sunday
        return (today - timedelta(days=2)).strftime("%Y-%m-%d")
    # Weekday
    market_closed = now.hour > 15 or (now.hour == 15 and now.minute >= 30)
    if market_closed:
        return today.strftime("%Y-%m-%d")
    # Market not yet closed today → last trade date is previous working day
    if weekday == 0:  # Monday before close → Friday
        return (today - timedelta(days=3)).strftime("%Y-%m-%d")
    return (today - timedelta(days=1)).strftime("%Y-%m-%d")


def is_history_cache_fresh(
    stock_code: str,
    min_rows: int,
    log: Optional[Callable[[str], None]] = None,
) -> bool:
    """判断本地历史缓存是否足够新鲜，可以跳过网络请求。

    策略：
    1. 读取 history_meta 表中的 refreshed_at 和 latest_trade_date
    2. 如果 ``latest_trade_date >= estimate_last_trade_date()``，并且 ``row_count >= min_rows`` → 新鲜
    3. 如果 ``refreshed_at`` 在今天 15:30 之后 → 新鲜（当天收盘后已刷新过）
    """
    meta = _load_history_meta_store(stock_code)
    if meta is None:
        return False
    latest_td_raw = str(meta.get("latest_trade_date") or "").strip()
    row_count = int(meta.get("row_count") or 0)
    refreshed_at = str(meta.get("refreshed_at") or "").strip()
    if not latest_td_raw or row_count < min_rows:
        return False

    # 统一日期格式为 YYYY-MM-DD，避免字符串比较出错
    try:
        # 支持多种日期格式：2024-01-15, 20240115, 2024/01/15
        normalized = latest_td_raw.replace("/", "-").replace(".", "-")
        if len(normalized) == 8 and normalized.isdigit():
            latest_td = f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:]}"
        else:
            latest_td = normalized
    except Exception:
        latest_td = latest_td_raw

    estimated_last_td = estimate_last_trade_date()

    # 缓存的最新交易日 >= 估算的最近交易日 → 数据足够新
    if latest_td >= estimated_last_td:
        if log:
            log(f"历史 {stock_code} 缓存新鲜 (latest={latest_td} >= estimated={estimated_last_td}, rows={row_count})")
        return True

    # 今天已经刷新过（收盘后），即使 latest_trade_date 较旧也信任
    if refreshed_at:
        try:
            refreshed_dt = datetime.strptime(refreshed_at, "%Y-%m-%d %H:%M:%S")
            now = datetime.now()
            if refreshed_dt.date() == now.date() and (
                refreshed_dt.hour > 15 or (refreshed_dt.hour == 15 and refreshed_dt.minute >= 30)
            ):
                if log:
                    log(f"历史 {stock_code} 今日收盘后已刷新 (refreshed={refreshed_at}), 跳过网络请求")
                return True
        except (ValueError, TypeError):
            pass

    return False
