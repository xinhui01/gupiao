import os
import time
import re
import warnings
import threading
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable, TypeVar, Tuple
from datetime import datetime, timedelta

from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

from stock_store import (
    clear_history as clear_history_store,
    clear_scan_snapshots,
    clear_universe as clear_universe_store,
    load_fund_flow as load_fund_flow_store,
    load_history as load_history_store,
    load_universe as load_universe_store,
    save_fund_flow as save_fund_flow_store,
    save_history as save_history_store,
    save_universe as save_universe_store,
)

T = TypeVar("T")


def _history_request_concurrency() -> int:
    raw = os.environ.get("GUPPIAO_HISTORY_CONCURRENCY", "").strip()
    try:
        value = int(raw) if raw else 4
    except ValueError:
        value = 4
    return max(1, min(value, 8))


def _history_connect_timeout_sec() -> float:
    raw = os.environ.get("GUPPIAO_HISTORY_CONNECT_TIMEOUT_SEC", "").strip()
    try:
        value = float(raw) if raw else 2.5
    except ValueError:
        value = 2.5
    return max(0.5, min(value, 10.0))


def _history_read_timeout_sec() -> float:
    raw = os.environ.get("GUPPIAO_HISTORY_READ_TIMEOUT_SEC", "").strip()
    try:
        value = float(raw) if raw else 4.0
    except ValueError:
        value = 4.0
    return max(1.0, min(value, 15.0))


def _history_total_timeout_sec() -> float:
    raw = os.environ.get("GUPPIAO_HISTORY_TOTAL_TIMEOUT_SEC", "").strip()
    try:
        value = float(raw) if raw else 12.0
    except ValueError:
        value = 12.0
    return max(3.0, min(value, 60.0))


def _history_host_cooldown_sec() -> float:
    raw = os.environ.get("GUPPIAO_HISTORY_HOST_COOLDOWN_SEC", "").strip()
    try:
        value = float(raw) if raw else 180.0
    except ValueError:
        value = 180.0
    return max(10.0, min(value, 1800.0))


def _history_max_mirrors_per_stock() -> int:
    raw = os.environ.get("GUPPIAO_HISTORY_MAX_MIRRORS_PER_STOCK", "").strip()
    try:
        value = int(raw) if raw else 3
    except ValueError:
        value = 3
    return max(1, min(value, 8))


_HISTORY_REQUEST_SEMAPHORE = threading.BoundedSemaphore(_history_request_concurrency())
_EASTMONEY_HISTORY_MIRRORS = [
    "https://push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://1.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://7.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://28.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://33.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://45.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://58.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://72.push2his.eastmoney.com/api/qt/stock/kline/get",
]
_HISTORY_MIRROR_HEALTH: Dict[str, float] = {}
_HISTORY_MIRROR_HEALTH_LOCK = threading.Lock()

# 东方财富接口常校验 Referer / UA；缺省时易被直接断开连接
_EASTMONEY_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://quote.eastmoney.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "close",
}

# 拉取全市场列表分页时写入 GUI 日志（由 get_all_stocks 临时注册）
_list_download_log: Optional[Callable[[str], None]] = None


def _project_root() -> Path:
    return Path(__file__).resolve().parent


def _use_insecure_ssl() -> bool:
    if os.environ.get("GUPPIAO_INSECURE_SSL", "").strip().lower() in ("1", "true", "yes"):
        return True
    root = _project_root()
    return (root / "USE_INSECURE_SSL").is_file() or (root / ".gupiao_insecure_ssl").is_file()


def _use_bypass_proxy() -> bool:
    """不走 HTTP(S)_PROXY 等环境代理（避免公司代理对东方财富断开）。"""
    if os.environ.get("GUPPIAO_BYPASS_PROXY", "").strip().lower() in ("1", "true", "yes"):
        return True
    root = _project_root()
    return (root / "USE_BYPASS_PROXY").is_file() or (root / ".gupiao_bypass_proxy").is_file()


def _is_transient_network_error(exc: BaseException) -> bool:
    if isinstance(exc, (RequestsConnectionError, RequestsTimeout, OSError)):
        return True
    r = repr(exc)
    if "RemoteDisconnected" in r or "Connection aborted" in r:
        return True
    if "timed out" in r.lower():
        return True
    return False


def _is_name_resolution_error(exc: BaseException) -> bool:
    text = repr(exc)
    lowered = text.lower()
    return (
        "nameresolutionerror" in lowered
        or "failed to resolve" in lowered
        or "nodename nor servname provided" in lowered
        or "temporary failure in name resolution" in lowered
    )


def _retry_ak_call(fn: Callable[..., T], *args, retries: int = 5, base_delay: float = 1.2, **kwargs) -> T:
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt < retries - 1 and _is_transient_network_error(e):
                time.sleep(base_delay * (attempt + 1))
                continue
            raise


def _history_retry_ak_call(fn: Callable[..., T], *args, **kwargs) -> T:
    # 历史 K 线接口对并发和出口网络都更敏感，先串行闸门。
    # 具体的镜像轮换和短重试交给函数内部处理，避免外层再叠超长等待。
    with _HISTORY_REQUEST_SEMAPHORE:
        return fn(*args, **kwargs)


def _eastmoney_history_request_params(stock_code: str, start_date: str, end_date: str) -> Dict[str, str]:
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
    }


def _request_session_get_json(url: str, params: Dict[str, Any], timeout: Tuple[int, int]) -> Dict[str, Any]:
    import requests

    with requests.Session() as session:
        if _use_bypass_proxy():
            session.trust_env = False
            session.proxies = {"http": None, "https": None}
        req_kw: Dict[str, Any] = {
            "url": url,
            "params": params,
            "timeout": timeout,
            "headers": dict(_EASTMONEY_HEADERS),
        }
        if _use_insecure_ssl():
            req_kw["verify"] = False
        response = session.get(**req_kw)
        response.raise_for_status()
        return response.json()


def _history_mirror_host(url: str) -> str:
    text = re.sub(r"^https?://", "", str(url or "").strip())
    return text.split("/", 1)[0]


def _mark_history_mirror_failed(url: str) -> None:
    host = _history_mirror_host(url)
    if not host:
        return
    cooldown_until = time.time() + _history_host_cooldown_sec()
    with _HISTORY_MIRROR_HEALTH_LOCK:
        _HISTORY_MIRROR_HEALTH[host] = cooldown_until


def _mark_history_mirror_ok(url: str) -> None:
    host = _history_mirror_host(url)
    if not host:
        return
    with _HISTORY_MIRROR_HEALTH_LOCK:
        _HISTORY_MIRROR_HEALTH.pop(host, None)


def _history_mirror_on_cooldown(url: str, now: Optional[float] = None) -> bool:
    host = _history_mirror_host(url)
    if not host:
        return False
    current = time.time() if now is None else now
    with _HISTORY_MIRROR_HEALTH_LOCK:
        cooldown_until = _HISTORY_MIRROR_HEALTH.get(host, 0.0)
        if cooldown_until <= current:
            if host in _HISTORY_MIRROR_HEALTH:
                _HISTORY_MIRROR_HEALTH.pop(host, None)
            return False
        return True


def _prioritize_history_mirrors(
    mirror_urls: List[str],
    preferred_mirror: Optional[str] = None,
) -> List[str]:
    now = time.time()
    seen: set[str] = set()

    candidates = []
    if preferred_mirror:
        candidates.append(preferred_mirror)
    candidates.extend(mirror_urls)

    healthy: List[str] = []
    cooling: List[str] = []
    for url in candidates:
        clean = str(url or "").strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        if _history_mirror_on_cooldown(clean, now):
            cooling.append(clean)
        else:
            healthy.append(clean)

    # 冷却中的镜像直接剔除，避免“明知不可用还继续打”。
    return healthy[: _history_max_mirrors_per_stock()]


def _parse_eastmoney_hist_json(stock_code: str, data_json: Dict[str, Any]) -> "pd.DataFrame":
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


def _probe_history_mirror(url: str) -> Tuple[bool, str]:
    probe_code = "000001"
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=40)).strftime("%Y%m%d")
    params = _eastmoney_history_request_params(probe_code, start_date, end_date)
    try:
        data_json = _request_session_get_json(
            url,
            params=params,
            timeout=(_history_connect_timeout_sec(), _history_read_timeout_sec()),
        )
        df = _parse_eastmoney_hist_json(probe_code, data_json)
        if df.empty:
            _mark_history_mirror_failed(url)
            return False, "empty"
        latest_series = df["日期"].dropna()
        latest_date = str(latest_series.iloc[-1]) if not latest_series.empty else "unknown"
        _mark_history_mirror_ok(url)
        return True, latest_date
    except Exception as e:
        _mark_history_mirror_failed(url)
        return False, str(e)


def _fetch_eastmoney_hist_frame(
    stock_code: str,
    days: int,
    start_date: str,
    end_date: str,
    mirror_urls: Optional[List[str]] = None,
    log: Optional[Callable[[str], None]] = None,
) -> "pd.DataFrame":
    """直接抓东方财富历史日线，并在多个镜像间轮换。"""
    params = _eastmoney_history_request_params(stock_code, start_date, end_date)
    mirrors = list(mirror_urls or _EASTMONEY_HISTORY_MIRRORS)
    last_exception: Optional[BaseException] = None
    deadline = time.time() + _history_total_timeout_sec()

    for base_url in mirrors:
        host = _history_mirror_host(base_url)
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        try:
            connect_timeout = min(_history_connect_timeout_sec(), max(0.5, remaining))
            read_timeout = min(_history_read_timeout_sec(), max(1.0, remaining))
            data_json = _request_session_get_json(
                base_url,
                params=params,
                timeout=(connect_timeout, read_timeout),
            )
            df = _parse_eastmoney_hist_json(stock_code, data_json)
            if df.empty:
                err = RuntimeError(f"{host} 返回空历史数据")
                last_exception = err
                _mark_history_mirror_failed(base_url)
                if log:
                    log(f"历史 {stock_code} 镜像 {host} 返回空数据，切换下一个镜像。")
                continue
            _mark_history_mirror_ok(base_url)
            return df
        except Exception as e:
            last_exception = e
            _mark_history_mirror_failed(base_url)
            if log:
                log(f"历史 {stock_code} 镜像 {host} 失败: {e}，切换下一个镜像。")
    if last_exception is not None:
        raise last_exception
    return pd.DataFrame()


# 须在 import akshare 之前执行：统一为 requests 补头；可选 SSL / 忽略环境代理
def _apply_network_patches() -> None:
    need_ssl = _use_insecure_ssl()
    need_no_proxy = _use_bypass_proxy()

    if need_ssl:
        import ssl

        ssl._create_default_https_context = ssl._create_unverified_context

        try:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except ImportError:
            pass

    try:
        import requests

        _orig_init = requests.Session.__init__
        _orig_req = requests.Session.request

        def _patched_init(self, *args, **kwargs):
            _orig_init(self, *args, **kwargs)
            if need_no_proxy:
                self.trust_env = False

        def _patched_request(self, method, url, **kwargs):
            if need_ssl:
                kwargs.setdefault("verify", False)
            u = str(url)
            if "eastmoney.com" in u:
                merged = dict(_EASTMONEY_HEADERS)
                merged.setdefault("Connection", "close")
                extra = kwargs.get("headers")
                if isinstance(extra, dict):
                    merged.update(extra)
                elif extra is not None:
                    try:
                        merged.update(dict(extra))
                    except (TypeError, ValueError):
                        pass
                kwargs["headers"] = merged
            return _orig_req(self, method, url, **kwargs)

        requests.Session.__init__ = _patched_init  # type: ignore[method-assign]
        requests.Session.request = _patched_request  # type: ignore[method-assign]
    except ImportError:
        pass


_apply_network_patches()

import akshare as ak
import pandas as pd
try:
    from pandas.errors import SettingWithCopyWarning as _AkshareWarningCategory
except ImportError:
    try:
        from pandas.errors import ChainedAssignmentError as _AkshareWarningCategory
    except ImportError:
        _AkshareWarningCategory = Warning

warnings.filterwarnings(
    "ignore",
    category=_AkshareWarningCategory,
    module=r"akshare\.stock\.stock_board_concept_em",
)


def _call_akshare_quietly(fn: Callable[..., T], *args, **kwargs) -> T:
    # AkShare's concept-board helpers emit noisy SettingWithCopyWarning logs
    # even when the returned data is usable. Silence only that warning locally.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", _AkshareWarningCategory)
        return _retry_ak_call(fn, *args, **kwargs)


def _first_existing_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    normalized = {str(col).strip(): col for col in columns}
    for name in candidates:
        key = str(name).strip()
        if key in normalized:
            return normalized[key]
    return None


def _find_fund_flow_column(columns: List[str], includes: List[str], excludes: Optional[List[str]] = None) -> Optional[str]:
    exclude_tokens = [str(x).strip() for x in (excludes or []) if str(x).strip()]
    for col in columns:
        text = str(col).strip()
        if not text:
            continue
        if all(token in text for token in includes):
            if any(token in text for token in exclude_tokens):
                continue
            return col
    return None

def clear_universe_data() -> None:
    """清空已保存的股票池和扫描快照。"""
    clear_universe_store()
    clear_scan_snapshots()


def clear_history_data() -> None:
    """清空已保存的历史日线。"""
    clear_history_store()


def _save_universe_store(
    df: pd.DataFrame, log: Optional[Callable[[str], None]] = None
) -> None:
    if df.empty or "code" not in df.columns:
        return
    save_universe_store(df)
    if log:
        log(f"股票池已保存 {len(df)} 只 → data/stock_store.sqlite3")


def _load_universe_store(
    log: Optional[Callable[[str], None]] = None,
) -> Optional[pd.DataFrame]:
    df = load_universe_store()
    if df is None or df.empty:
        return None
    if "name" not in df.columns:
        df["name"] = ""
    if "exchange" not in df.columns:
        df["exchange"] = df["code"].map(_infer_exchange)
    if "board" not in df.columns:
        df["board"] = df["code"].map(
            lambda x: "???"
            if str(x).strip().zfill(6).startswith("688")
            else _infer_sz_board(x)
        )
    if "concepts" not in df.columns:
        df["concepts"] = ""
    df["code"] = (
        df["code"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .str.zfill(6)
    )
    df["concepts"] = df["concepts"].astype(str).map(_normalize_concepts_text)
    if log:
        log(f"已从 data/stock_store.sqlite3 读取股票池 {len(df)} 只")
    return df[["code", "name", "exchange", "board", "concepts"]]


def _load_history_store(
    stock_code: str,
    min_rows: int,
    end_date: str,
    log: Optional[Callable[[str], None]] = None,
) -> Optional[pd.DataFrame]:
    df = load_history_store(stock_code)
    if df is None or df.empty or "date" not in df.columns or "close" not in df.columns:
        return None
    df["date"] = df["date"].astype(str).str.strip()
    df = df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    if len(df) < min_rows:
        return None
    if log:
        log(f"已从 data/stock_store.sqlite3 读取历史 {stock_code} {len(df)} 行")
    return df


def _save_history_store(stock_code: str, df: pd.DataFrame, keep_rows: int = 40) -> None:
    if df is None or df.empty:
        return
    if "date" not in df.columns:
        return
    out = df.copy()
    out["date"] = out["date"].astype(str).str.strip()
    out = out.sort_values("date").tail(max(keep_rows, 10)).reset_index(drop=True)
    save_history_store(stock_code, out)


def _load_fund_flow_store(
    stock_code: str,
    min_rows: int,
    log: Optional[Callable[[str], None]] = None,
) -> Optional[pd.DataFrame]:
    df = load_fund_flow_store(stock_code)
    if df is None or df.empty or "date" not in df.columns:
        return None
    df["date"] = df["date"].astype(str).str.strip()
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if len(df) < min_rows:
        return None
    if log:
        log(f"已从 data/stock_store.sqlite3 读取资金流 {stock_code} {len(df)} 行")
    return df


def _save_fund_flow_store(stock_code: str, df: pd.DataFrame, keep_rows: int = 40) -> None:
    if df is None or df.empty or "date" not in df.columns:
        return
    out = df.copy()
    out["date"] = out["date"].astype(str).str.strip()
    out = out.sort_values("date").tail(max(keep_rows, 10)).reset_index(drop=True)
    save_fund_flow_store(stock_code, out)


def _eastmoney_request_mirror_urls(url: str) -> List[str]:
    """东方财富 push 多节点；82 等单线路易在分页中途被断开，优先尝试无编号主域。"""
    from urllib.parse import urlparse, urlunparse

    raw = url.strip()
    p = urlparse(raw)
    netloc = (p.netloc or "").lower()
    if "eastmoney.com" not in netloc:
        return [raw]
    path = p.path or "/"
    original = p.netloc
    hosts = [
        "push2.eastmoney.com",
        original,
        "82.push2.eastmoney.com",
        "33.push2.eastmoney.com",
        "7.push2.eastmoney.com",
        "81.push2.eastmoney.com",
        "72.push2.eastmoney.com",
        "28.push2.eastmoney.com",
    ]
    seen: set[str] = set()
    out: List[str] = []
    for host in hosts:
        h = (host or "").strip()
        if not h:
            continue
        key = h.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(urlunparse(("https", h, path, "", "", "")))
    return out if out else [raw]


def _gupiao_request_with_retry(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    max_retries: int = 8,
    base_delay: float = 1.2,
    random_delay_range: Tuple[float, float] = (0.6, 2.2),
):
    """
    替换 akshare 内置 request_with_retry：显式浏览器头、多镜像、更长超时。
    原实现通过 `from ... import request_with_retry` 绑定，必须同时 patch utils.func。
    """
    import random

    import requests
    from requests.adapters import HTTPAdapter

    params = params or {}
    # fetch_paginated_data 传入 timeout=15，分页多时易断；东方财富接口抬高下限
    if "eastmoney.com" in url:
        timeout = max(int(timeout or 0), 30)
    last_exception: Optional[BaseException] = None

    mirrors = _eastmoney_request_mirror_urls(url)
    for mi, base_url in enumerate(mirrors):
        for attempt in range(max_retries):
            lg = _list_download_log
            if (
                lg
                and attempt == 0
                and mi == 0
                and "/api/qt/clist/get" in url
                and isinstance(params, dict)
            ):
                pn = params.get("pn", "?")
                lg(
                    f"列表分页：正在请求第 {pn} 页（共 {len(mirrors)} 个镜像可轮换，"
                    f"单页可能较慢或多次重试）…"
                )
            try:
                with requests.Session() as session:
                    if _use_bypass_proxy():
                        session.trust_env = False
                    adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1)
                    session.mount("http://", adapter)
                    session.mount("https://", adapter)
                    hdrs = dict(_EASTMONEY_HEADERS)
                    req_kw: Dict[str, Any] = {
                        "url": base_url,
                        "params": params,
                        "timeout": timeout,
                        "headers": hdrs,
                    }
                    if _use_insecure_ssl():
                        req_kw["verify"] = False
                    response = session.get(**req_kw)
                    response.raise_for_status()
                    return response
            except (requests.RequestException, ValueError) as e:
                last_exception = e
                if attempt < max_retries - 1:
                    delay = base_delay * (2**attempt) + random.uniform(
                        *random_delay_range
                    )
                    time.sleep(delay)
                else:
                    time.sleep(random.uniform(0.25, 0.85))
    if last_exception is not None:
        raise last_exception
    raise RuntimeError("request_with_retry: no attempt made")


def _patch_akshare_request_layer() -> None:
    import akshare.utils.func as ak_func
    import akshare.utils.request as ak_req

    ak_req.request_with_retry = _gupiao_request_with_retry
    ak_func.request_with_retry = _gupiao_request_with_retry


_patch_akshare_request_layer()


def _use_em_full_spot_for_list() -> bool:
    """设为 1 / em / eastmoney 时仍走东方财富分页全表（易卡死，不推荐）。"""
    return os.environ.get("GUPPIAO_LIST_SOURCE", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "em",
        "eastmoney",
        "efull",
    )


def _em_scalar(x: Any) -> float:
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


def _em_price_yuan(x: Any) -> float:
    """东财 stock/get 行情价字段多为整数，常见为「元×1000」。"""
    v = _em_scalar(x)
    if v == 0.0:
        return 0.0
    if abs(v) >= 500:
        return v / 1000.0
    return v


def _norm_code_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .str.zfill(6)
    )


def _norm_code(code: Any) -> str:
    return (
        str(code)
        .replace(".0", "")
        .strip()
        .zfill(6)
        if str(code).strip() and str(code).strip().lower() != "nan"
        else ""
    )


def _infer_sz_board(code: str) -> str:
    c = str(code).strip().zfill(6)
    if c.startswith(("300", "301")):
        return "创业板"
    if c.startswith(("000", "001", "002", "003")):
        return "深交所主板"
    return "深交所A股"


def _infer_exchange(code: str) -> str:
    c = str(code).strip().zfill(6)
    return "上交所" if c.startswith(("5", "6", "9")) else "深交所"


def _infer_market(code: str) -> str:
    c = str(code).strip().zfill(6)
    if c.startswith(("4", "8")):
        return "bj"
    return "sh" if c.startswith(("5", "6", "9")) else "sz"


def _normalize_concepts_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return ""
    parts: List[str] = []
    for raw in re.split(r"[、,，;；|/]+", text):
        item = raw.strip()
        if not item or item.lower() == "nan":
            continue
        if item not in parts:
            parts.append(item)
    return "、".join(parts)


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, str) and value.strip() == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _today_ymd() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _should_refresh_today_row(df: Optional[pd.DataFrame], date_col: str = "date") -> bool:
    if df is None or df.empty or date_col not in df.columns:
        return False
    latest_date = str(df[date_col].iloc[-1]).strip()
    if latest_date != _today_ymd():
        return False
    now = datetime.now()
    # 15:30 之后默认认为日线与日资金流已基本稳定，可直接复用缓存。
    return now.hour < 15 or (now.hour == 15 and now.minute < 30)


def _build_a_share_universe(log: Optional[Callable[[str], None]] = None) -> pd.DataFrame:
    """深交所 + 上交所（含科创板）官方列表，不含北交所；少量 HTTP，无东方财富 clist 分页。"""
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
            d["code"] = _norm_code_series(d["code"])
            d["exchange"] = exchange
            if label == "深交所 A 股":
                d["board"] = d["code"].map(_infer_sz_board)
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



class StockDataFetcher:
    def __init__(self):
        self._log: Optional[Callable[[str], None]] = None
        self._strong_pool_cache: Dict[str, pd.DataFrame] = {}
        self._concepts_cache: Optional[Dict[str, str]] = None
        self._universe_concepts_cache: Optional[Dict[str, str]] = None
        self._history_mirror_cache: List[str] = []
        self._history_mirror_checked_at: float = 0.0
        try:
            configured_limit = int(os.environ.get("GUPPIAO_CONCEPT_BOARD_LIMIT", "20").strip() or "20")
        except ValueError:
            configured_limit = 20
        self.concept_board_limit: int = max(5, min(configured_limit, 80))
        try:
            configured_timeout = float(os.environ.get("GUPPIAO_CONCEPT_FILL_TIMEOUT_SEC", "25").strip() or "25")
        except ValueError:
            configured_timeout = 25.0
        self.concept_fill_timeout_sec: float = max(5.0, configured_timeout)
        self._concepts_lock = threading.Lock()
        self._last_history_probe_failures: Dict[str, str] = {}

    def set_log_callback(self, cb: Optional[Callable[[str], None]]) -> None:
        self._log = cb

    def get_available_history_mirrors(self, force_refresh: bool = False) -> List[str]:
        now = time.time()
        if not force_refresh and self._history_mirror_cache and now - self._history_mirror_checked_at < 180:
            return list(self._history_mirror_cache)

        available: List[str] = []
        failures: Dict[str, str] = {}
        if self._log:
            self._log("开始检测东方财富历史接口镜像可用性...")
        for url in _EASTMONEY_HISTORY_MIRRORS:
            ok, detail = _probe_history_mirror(url)
            host = re.sub(r"^https?://", "", url).split("/", 1)[0]
            if ok:
                available.append(url)
                if self._log:
                    self._log(f"历史镜像可用 {host}，最新日期 {detail}")
            else:
                failures[host] = str(detail)
                if self._log:
                    self._log(f"历史镜像不可用 {host}：{detail}")
        self._history_mirror_cache = available
        self._history_mirror_checked_at = now
        self._last_history_probe_failures = failures
        if not available and self._log and failures:
            dns_failed = sum(1 for detail in failures.values() if _is_name_resolution_error(RuntimeError(detail)))
            if dns_failed == len(failures):
                self._log("历史镜像全部失败，且都属于 DNS 解析失败；当前更像是本机网络/解析环境异常，不是单个镜像故障。")
        return list(available)

    def get_last_history_probe_failures(self) -> Dict[str, str]:
        return dict(self._last_history_probe_failures)


    def clear_saved_universe_data(self) -> None:
        clear_universe_data()
        self._concepts_cache = None
        self._universe_concepts_cache = None

    def clear_history_data(self) -> None:
        clear_history_data()

    def _load_concepts_map(
        self,
        target_codes: Optional[List[str]] = None,
        max_boards: Optional[int] = None,
    ) -> Dict[str, str]:
        target_set = {
            _norm_code(code) for code in (target_codes or []) if _norm_code(code)
        }
        if not target_set:
            return {}
        with self._concepts_lock:
            if self._concepts_cache is None:
                self._concepts_cache = {}
            pending = {code for code in target_set if not self._concepts_cache.get(code)}
            if not pending:
                return {code: self._concepts_cache.get(code, "") for code in target_set}

        board_cap = max(1, int(max_boards or self.concept_board_limit))
        concept_map: Dict[str, List[str]] = {}
        started_at = time.time()
        if self._log:
            self._log(
                f"开始补全股票概念：目标 {len(pending)} 只，最多扫描 {board_cap} 个概念板块。"
            )

        try:
            boards = _call_akshare_quietly(ak.stock_board_concept_name_em)
        except Exception as e:
            if self._log:
                self._log(f"概念板块名称获取失败: {e}")
            with self._concepts_lock:
                return {code: self._concepts_cache.get(code, "") for code in target_set}

        if boards is None or boards.empty or "板块名称" not in boards.columns:
            with self._concepts_lock:
                return {code: self._concepts_cache.get(code, "") for code in target_set}

        board_names = [
            str(name).strip()
            for name in boards["板块名称"].tolist()
            if str(name).strip()
        ]
        if not board_names:
            with self._concepts_lock:
                return {code: self._concepts_cache.get(code, "") for code in target_set}

        board_names = board_names[:board_cap]
        total = len(board_names)
        found_codes: set[str] = set()

        for idx, board_name in enumerate(board_names, start=1):
            if pending and pending.issubset(found_codes):
                break
            if time.time() - started_at >= self.concept_fill_timeout_sec:
                if self._log:
                    self._log(
                        f"概念补全达到 {self.concept_fill_timeout_sec:.0f}s 超时上限，提前结束本轮。"
                    )
                break
            try:
                cons = _call_akshare_quietly(ak.stock_board_concept_cons_em, symbol=board_name)
            except Exception as e:
                if self._log and (idx % 10 == 0 or idx == total):
                    self._log(f"概念板块 {idx}/{total} {board_name} 获取失败: {e}")
                continue

            if cons is None or cons.empty:
                continue

            code_col = "代码" if "代码" in cons.columns else "code" if "code" in cons.columns else None
            if code_col is None:
                continue

            codes = cons[code_col].astype(str).map(_norm_code).tolist()
            for code in codes:
                if not code or code not in pending:
                    continue
                bucket = concept_map.setdefault(code, [])
                if board_name not in bucket:
                    bucket.append(board_name)
                found_codes.add(code)

            if self._log and (idx % 10 == 0 or idx == total):
                self._log(
                    f"概念板块进度 {idx}/{total}: {board_name}，已命中 {len(concept_map)} / {len(pending)} 只"
                )

        with self._concepts_lock:
            for code, names in concept_map.items():
                self._concepts_cache[code] = _normalize_concepts_text("、".join(names))
            return {code: self._concepts_cache.get(code, "") for code in target_set}

    def preload_stock_concepts(
        self,
        stock_codes: List[str],
        max_boards: Optional[int] = None,
    ) -> Dict[str, str]:
        target_codes = [_norm_code(code) for code in stock_codes if _norm_code(code)]
        if not target_codes:
            return {}
        return self._load_concepts_map(target_codes, max_boards=max_boards)

    def _set_universe_concepts_cache(self, df: pd.DataFrame) -> None:
        cache: Dict[str, str] = {}
        if df is not None and not df.empty and "code" in df.columns and "concepts" in df.columns:
            for code, concepts in zip(df["code"].astype(str), df["concepts"].astype(str)):
                norm_code = _norm_code(code)
                if norm_code:
                    cache[norm_code] = _normalize_concepts_text(concepts)
        self._universe_concepts_cache = cache

    def _normalize_trade_date(self, trade_date: str) -> str:
        return re.sub(r"\D", "", str(trade_date or ""))[:8]

    def _load_strong_pool(self, trade_date: str) -> pd.DataFrame:
        date_key = self._normalize_trade_date(trade_date)
        if not date_key:
            return pd.DataFrame()
        cached = self._strong_pool_cache.get(date_key)
        if cached is not None:
            return cached
        try:
            df = _retry_ak_call(ak.stock_zt_pool_strong_em, date=date_key)
        except Exception as e:
            if self._log:
                self._log(f"强势股池 {date_key} 获取失败: {e}")
            df = pd.DataFrame()
        self._strong_pool_cache[date_key] = df
        return df

    def get_limit_up_reason(self, stock_code: str, trade_date: str) -> str:
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return ""
        pool = self._load_strong_pool(trade_date)
        if pool is None or pool.empty:
            return ""
        if "代码" not in pool.columns or "入选理由" not in pool.columns:
            return ""
        match = pool[pool["代码"].astype(str).str.strip().str.zfill(6) == code]
        if match.empty:
            return ""
        reason = str(match.iloc[0].get("入选理由", "") or "").strip()
        if not reason or reason.lower() == "nan":
            return ""
        return reason

    def get_stock_concepts(self, stock_code: str) -> str:
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return ""
        if self._universe_concepts_cache is None:
            universe_df = _load_universe_store(None)
            if universe_df is not None and not universe_df.empty and "concepts" in universe_df.columns:
                self._set_universe_concepts_cache(universe_df)
            else:
                self._set_universe_concepts_cache(pd.DataFrame())
        cached = self._universe_concepts_cache.get(code, "") if self._universe_concepts_cache else ""
        if cached:
            return _normalize_concepts_text(cached)
        mapped = self._load_concepts_map([code], max_boards=self.concept_board_limit)
        return _normalize_concepts_text(mapped.get(code, ""))

    def get_fund_flow_data(
        self,
        stock_code: str,
        days: int = 30,
        force_refresh: bool = False,
    ) -> Optional[pd.DataFrame]:
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return None
        min_rows = max(1, int(days))
        if not force_refresh:
            cached = _load_fund_flow_store(code, min_rows=min_rows, log=self._log)
            if cached is not None and not cached.empty:
                has_big_order_data = False
                if "big_order_amount" in cached.columns:
                    big_order_series = pd.to_numeric(cached["big_order_amount"], errors="coerce")
                    has_big_order_data = bool(big_order_series.notna().any())
                if has_big_order_data and not _should_refresh_today_row(cached):
                    return cached.tail(days).reset_index(drop=True)
                if not has_big_order_data:
                    if self._log:
                        self._log(f"资金流 {code} 缓存缺少大单净额，自动刷新最新数据。")
                elif self._log:
                    self._log(f"资金流 {code} 命中当天缓存，但尚未收盘，改为刷新最新数据。")
        market = _infer_market(code)
        try:
            flow_df = _retry_ak_call(ak.stock_individual_fund_flow, stock=code, market=market)
        except Exception as e:
            if self._log:
                self._log(f"个股资金流 {code} 获取失败: {e}")
            return None
        if flow_df is None or flow_df.empty:
            return None
        source_columns = [str(col) for col in flow_df.columns.tolist()]
        rename_map: Dict[str, str] = {}

        date_col = _first_existing_column(source_columns, ["日期", "交易日", "date"])
        close_col = _first_existing_column(source_columns, ["收盘价", "收盘", "close"])
        change_pct_col = _first_existing_column(source_columns, ["涨跌幅", "change_pct"])
        main_amount_col = _first_existing_column(source_columns, ["主力净流入-净额", "主力净额"])
        main_ratio_col = _first_existing_column(source_columns, ["主力净流入-净占比", "主力净占比"])
        big_amount_col = _first_existing_column(source_columns, ["大单净流入-净额", "大单净额"])
        big_ratio_col = _first_existing_column(source_columns, ["大单净流入-净占比", "大单净占比"])
        super_amount_col = _first_existing_column(source_columns, ["超大单净流入-净额", "超大单净额"])
        super_ratio_col = _first_existing_column(source_columns, ["超大单净流入-净占比", "超大单净占比"])

        if main_amount_col is None:
            main_amount_col = _find_fund_flow_column(source_columns, ["主力", "净", "额"], excludes=["占比"])
        if big_amount_col is None:
            big_amount_col = _find_fund_flow_column(source_columns, ["大单", "净", "额"], excludes=["占比", "超大单"])
        if super_amount_col is None:
            super_amount_col = _find_fund_flow_column(source_columns, ["超大单", "净", "额"], excludes=["占比"])
        if main_ratio_col is None:
            main_ratio_col = _find_fund_flow_column(source_columns, ["主力", "净", "占比"])
        if big_ratio_col is None:
            big_ratio_col = _find_fund_flow_column(source_columns, ["大单", "净", "占比"], excludes=["超大单"])
        if super_ratio_col is None:
            super_ratio_col = _find_fund_flow_column(source_columns, ["超大单", "净", "占比"])

        for src, dst in [
            (date_col, "date"),
            (close_col, "close"),
            (change_pct_col, "change_pct"),
            (main_amount_col, "main_force_amount"),
            (main_ratio_col, "main_force_ratio"),
            (big_amount_col, "big_order_amount"),
            (big_ratio_col, "big_order_ratio"),
            (super_amount_col, "super_big_order_amount"),
            (super_ratio_col, "super_big_order_ratio"),
        ]:
            if src:
                rename_map[src] = dst

        df = flow_df.rename(columns=rename_map).copy()
        if "date" not in df.columns:
            return None
        if "big_order_amount" not in df.columns and self._log:
            self._log(f"个股资金流 {code} 未匹配到大单净额字段，返回列: {', '.join(source_columns)}")
        df["date"] = df["date"].astype(str).str.strip()
        for col in [
            "close",
            "change_pct",
            "main_force_amount",
            "main_force_ratio",
            "big_order_amount",
            "big_order_ratio",
            "super_big_order_amount",
            "super_big_order_ratio",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        _save_fund_flow_store(code, df, keep_rows=max(60, days + 10))
        return df.tail(days).reset_index(drop=True)

    def get_all_stocks(self, force_refresh: bool = False) -> pd.DataFrame:
        if _use_em_full_spot_for_list():
            return self._get_all_stocks_em_spot()
        if os.environ.get("GUPPIAO_REFRESH_UNIVERSE", "").strip().lower() in (
            "1",
            "true",
            "yes",
        ):
            force_refresh = True
        if not force_refresh:
            universe_df = _load_universe_store(self._log)
            if universe_df is not None and not universe_df.empty:
                self._set_universe_concepts_cache(universe_df)
                return universe_df
        if self._log:
            self._log(
                "从交易所构建股票池（深交所+上交所含科创板，不含北交所）…"
            )
        df = _build_a_share_universe(self._log)
        if not df.empty:
            _save_universe_store(df, self._log)
            self._set_universe_concepts_cache(df)
        return df

    def _get_all_stocks_em_spot(self) -> pd.DataFrame:
        global _list_download_log
        prev_log = _list_download_log
        _list_download_log = self._log
        try:
            if self._log:
                self._log("已开启 GUPPIAO_LIST_SOURCE=em：东方财富分页全表（耗时长，易限流）…")
            stock_list = _retry_ak_call(ak.stock_zh_a_spot_em)
            stock_list = stock_list.rename(columns={
                "代码": "code",
                "名称": "name",
                "最新价": "price",
                "涨跌幅": "change_pct",
                "涨跌额": "change_amount",
                "成交量": "volume",
                "成交额": "amount",
                "振幅": "amplitude",
                "最高": "high",
                "最低": "low",
                "今开": "open",
                "昨收": "pre_close",
                "量比": "volume_ratio",
                "换手率": "turnover_rate",
                "市盈率-动态": "pe_ratio",
                "市净率": "pb_ratio",
                "总市值": "total_mv",
                "流通市值": "circ_mv",
            })
            if "code" in stock_list.columns:
                stock_list["code"] = _norm_code_series(stock_list["code"])
            if "code" in stock_list.columns:
                stock_list["exchange"] = stock_list["code"].map(
                    lambda x: "上交所"
                    if str(x).startswith(("5", "6", "9"))
                    else "深交所"
                )
                stock_list["board"] = stock_list["code"].map(
                    lambda x: "科创板"
                    if str(x).startswith("688")
                    else _infer_sz_board(x)
                )
            save_universe_store(stock_list)
            self._set_universe_concepts_cache(stock_list)
            if self._log:
                self._log(f"东方财富全表下载完成，共 {len(stock_list)} 条。")
            return stock_list
        except Exception as e:
            if self._log:
                self._log(f"东方财富全表失败: {e}")
            print(f"获取股票列表失败: {e}")
            return pd.DataFrame()
        finally:
            _list_download_log = prev_log

    def get_history_data(
        self,
        stock_code: str,
        days: int = 10,
        force_refresh: bool = False,
        preferred_mirror: Optional[str] = None,
        mirror_pool: Optional[List[str]] = None,
    ) -> Optional[pd.DataFrame]:
        history_df: Optional[pd.DataFrame] = None
        try:
            stock_code = str(stock_code).strip().zfill(6)
            end_date = datetime.now().strftime('%Y%m%d')
            min_rows = max(1, days)

            if not force_refresh:
                history_df = _load_history_store(stock_code, min_rows, end_date, self._log)
                if history_df is not None and not history_df.empty:
                    if not _should_refresh_today_row(history_df):
                        return history_df.tail(days).reset_index(drop=True)
                    if self._log:
                        self._log(f"历史 {stock_code} 命中当天缓存，但尚未收盘，改为刷新最新日线。")

            start_date = (datetime.now() - timedelta(days=days + 15)).strftime('%Y%m%d')
            selected_mirrors = [x for x in (mirror_pool or self.get_available_history_mirrors()) if x]
            selected_mirrors = _prioritize_history_mirrors(
                selected_mirrors,
                preferred_mirror=preferred_mirror,
            )
            if not selected_mirrors:
                if history_df is not None and not history_df.empty:
                    if self._log:
                        self._log(f"历史 {stock_code} 当前无可用镜像，回退本地缓存。")
                    return history_df.tail(days).reset_index(drop=True)
                return None
            
            df = _history_retry_ak_call(
                _fetch_eastmoney_hist_frame,
                stock_code,
                days,
                start_date,
                end_date,
                selected_mirrors,
                self._log,
            )
            
            if df.empty:
                if history_df is not None and not history_df.empty:
                    if self._log:
                        self._log(f"历史 {stock_code} 刷新返回空结果，回退本地缓存。")
                    return history_df.tail(days).reset_index(drop=True)
                return None
            
            df = df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'change_pct',
                '涨跌额': 'change_amount',
                '换手率': 'turnover_rate'
            })
            for col in [
                "open",
                "close",
                "high",
                "low",
                "volume",
                "amount",
                "amplitude",
                "change_pct",
                "change_amount",
                "turnover_rate",
            ]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            df = df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
            _save_history_store(stock_code, df, keep_rows=max(40, days + 10))
            return df.tail(days).reset_index(drop=True)
        except Exception as e:
            if history_df is not None and not history_df.empty:
                if self._log:
                    self._log(f"历史 {stock_code} 刷新失败，回退本地缓存: {e}")
                return history_df.tail(days).reset_index(drop=True)
            if self._log:
                self._log(f"历史 {stock_code} 获取失败: {e}")
            print(f"获取股票 {stock_code} 历史数据失败: {e}")
            return None

    def get_intraday_data(self, stock_code: str) -> Optional[pd.DataFrame]:
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return None

        raw = None
        last_error: Optional[Exception] = None

        try:
            raw = _retry_ak_call(ak.stock_zh_a_hist_min_em, symbol=code, period="1", adjust="")
        except Exception as e:
            last_error = e
            if self._log:
                self._log(f"分时行情(主接口) {code} 获取失败: {e}")

        if raw is None or getattr(raw, "empty", True):
            try:
                raw = _retry_ak_call(ak.stock_zh_a_minute, symbol=code, period="1")
            except Exception as e:
                last_error = e
                if self._log:
                    self._log(f"分时行情(备用接口) {code} 获取失败: {e}")

        if raw is None or getattr(raw, "empty", True):
            if self._log and last_error is not None:
                self._log(f"分时行情 {code} 无可用数据: {last_error}")
            return None

        source_columns = [str(col) for col in raw.columns.tolist()]
        rename_map: Dict[str, str] = {}
        time_col = _first_existing_column(source_columns, ["时间", "日期时间", "datetime", "time"])
        open_col = _first_existing_column(source_columns, ["开盘", "open"])
        close_col = _first_existing_column(source_columns, ["收盘", "close", "最新价"])
        high_col = _first_existing_column(source_columns, ["最高", "high"])
        low_col = _first_existing_column(source_columns, ["最低", "low"])
        volume_col = _first_existing_column(source_columns, ["成交量", "volume"])
        amount_col = _first_existing_column(source_columns, ["成交额", "amount"])

        for src, dst in [
            (time_col, "time"),
            (open_col, "open"),
            (close_col, "close"),
            (high_col, "high"),
            (low_col, "low"),
            (volume_col, "volume"),
            (amount_col, "amount"),
        ]:
            if src:
                rename_map[src] = dst

        df = raw.rename(columns=rename_map).copy()
        if "time" not in df.columns:
            if self._log:
                self._log(f"分时行情 {code} 缺少时间列，返回列: {', '.join(source_columns)}")
            return None

        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
        if df.empty:
            return None

        # Some providers return multiple trading days in one payload.
        # Keep only the latest trading date for intraday rendering.
        latest_trade_date = df["time"].dt.date.max()
        if latest_trade_date is not None:
            df = df[df["time"].dt.date == latest_trade_date].reset_index(drop=True)
        if df.empty:
            return None

        for col in ["open", "close", "high", "low", "volume", "amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = None

        if len(df) > 300:
            df = df.tail(300).reset_index(drop=True)
        return df[["time", "open", "close", "high", "low", "volume", "amount"]]
