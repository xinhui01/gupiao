import os
import time
import re
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable, TypeVar, Tuple
from datetime import datetime, timedelta

from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

from stock_store import (
    clear_history as clear_history_store,
    clear_scan_snapshots,
    clear_universe as clear_universe_store,
    load_history as load_history_store,
    load_universe as load_universe_store,
    save_history as save_history_store,
    save_universe as save_universe_store,
)

T = TypeVar("T")

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


def _retry_ak_call(fn: Callable[..., T], *args, retries: int = 5, base_delay: float = 1.2, **kwargs) -> T:
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt < retries - 1 and _is_transient_network_error(e):
                time.sleep(base_delay * (attempt + 1))
                continue
            raise


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
    df["code"] = (
        df["code"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .str.zfill(6)
    )
    if log:
        log(f"已从 data/stock_store.sqlite3 读取股票池 {len(df)} 只")
    return df[["code", "name", "exchange", "board"]]


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

    def set_log_callback(self, cb: Optional[Callable[[str], None]]) -> None:
        self._log = cb


    def clear_saved_universe_data(self) -> None:
        clear_universe_data()

    def clear_history_data(self) -> None:
        clear_history_data()

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
                return universe_df
        if self._log:
            self._log(
                "从交易所构建股票池（深交所+上交所含科创板，不含北交所）…"
            )
        df = _build_a_share_universe(self._log)
        if not df.empty:
            _save_universe_store(df, self._log)
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
    ) -> Optional[pd.DataFrame]:
        try:
            stock_code = str(stock_code).strip().zfill(6)
            end_date = datetime.now().strftime('%Y%m%d')
            min_rows = max(1, days)

            if not force_refresh:
                history_df = _load_history_store(stock_code, min_rows, end_date, self._log)
                if history_df is not None and not history_df.empty:
                    return history_df.tail(days).reset_index(drop=True)

            start_date = (datetime.now() - timedelta(days=days + 15)).strftime('%Y%m%d')
            
            df = _retry_ak_call(
                ak.stock_zh_a_hist,
                symbol=stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="",
            )
            
            if df.empty:
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
            print(f"获取股票 {stock_code} 历史数据失败: {e}")
            return None

            df = _retry_ak_call(ak.stock_fund_flow_individual, symbol="即时")
            return df
        except Exception as e:
            print(f"获取个股资金流向排名失败: {e}")
            return None
