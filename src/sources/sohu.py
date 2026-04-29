"""搜狐财经历史 K 线源。

接口：``https://q.stock.sohu.com/hisHq``，JSONP 格式。
"""
from __future__ import annotations

import json as _json
import random
import threading
import time

import pandas as pd

from src.network.headers import USER_AGENT_POOL
from src.network.host_health import (
    cooldown_remaining,
    mark_failed,
    mark_ok,
    on_cooldown,
)
from src.sources._common import normalize_history_frame
from src.sources._jsonp import strip_wrapper as _strip_jsonp_wrapper


_REQUEST_LOCK = threading.Lock()
_NEXT_REQUEST_AT = 0.0
_MIN_INTERVAL = 0.8


def throttle() -> None:
    global _NEXT_REQUEST_AT
    while True:
        with _REQUEST_LOCK:
            now = time.time()
            wait = _NEXT_REQUEST_AT - now
            if wait <= 0:
                _NEXT_REQUEST_AT = now + _MIN_INTERVAL + random.uniform(0.1, 0.5)
                return
        time.sleep(min(wait, 0.5))


def stock_code(code: str) -> str:
    """搜狐用 cn_000001 或 cn_600000 的格式。"""
    c = str(code).strip().zfill(6)
    return f"cn_{c}"


def fetch_hist_frame(stock_code_in: str, start_date: str, end_date: str) -> "pd.DataFrame":
    """搜狐财经历史日线：JSONP 格式。"""
    import requests

    if on_cooldown("q.stock.sohu.com"):
        remain = cooldown_remaining("q.stock.sohu.com")
        raise RuntimeError(f"sohu host on cooldown ({int(remain)}s remaining)")

    sohu_code = stock_code(stock_code_in)
    s = start_date.replace("-", "")
    e = end_date.replace("-", "")
    url = f"https://q.stock.sohu.com/hisHq"
    params = {
        "code": sohu_code,
        "start": s,
        "end": e,
        "stat": "1",
        "order": "D",
        "period": "d",
        "callback": f"historySearchHandler",
        "rt": f"jsonp{random.randint(1000, 9999)}",
    }

    last_error = None
    for attempt in range(3):
        try:
            throttle()
            resp = requests.get(
                url,
                params=params,
                timeout=(5, 12),
                headers={
                    "User-Agent": random.choice(USER_AGENT_POOL),
                    "Referer": "https://q.stock.sohu.com/",
                },
            )
            if resp.status_code != 200:
                last_error = RuntimeError(f"sohu HTTP {resp.status_code}")
                time.sleep(1.0 + random.uniform(0.5, 1.5))
                continue

            text = _strip_jsonp_wrapper(resp.text)
            data = _json.loads(text)

            if not isinstance(data, list) or not data:
                last_error = RuntimeError("sohu: empty or invalid response")
                continue

            hq = data[0].get("hq") or [] if isinstance(data[0], dict) else []
            if not hq:
                last_error = RuntimeError("sohu: no hq data")
                continue

            rows = []
            for item in hq:
                if not isinstance(item, list) or len(item) < 9:
                    continue
                rows.append({
                    "date": str(item[0]).strip(),
                    "open": item[1],
                    "close": item[2],
                    "change_amount": item[3],
                    "change_pct": str(item[4]).replace("%", ""),
                    "low": item[5],
                    "high": item[6],
                    "volume": item[7],
                    "amount": item[8],
                    "turnover_rate": item[9] if len(item) > 9 else None,
                })

            df = pd.DataFrame(rows)
            if df.empty:
                last_error = RuntimeError("sohu: empty parsed result")
                continue

            for col in ("open", "close", "high", "low", "volume", "amount", "change_pct", "change_amount", "turnover_rate"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace("%", "").str.replace(",", ""), errors="coerce")
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
            df = df.dropna(subset=["date", "close"])

            mark_ok("q.stock.sohu.com")
            return normalize_history_frame(df)
        except Exception as e:
            last_error = e
            time.sleep(1.5 * (attempt + 1) + random.uniform(0.3, 1.0))

    mark_failed("q.stock.sohu.com")
    if last_error is not None:
        raise last_error
    return pd.DataFrame()
