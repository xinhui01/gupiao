import os
import time
from functools import lru_cache
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable, TypeVar, Tuple
from datetime import datetime, timedelta

from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

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

_UNIVERSE_CACHE_DIR = _project_root() / "cache"
_UNIVERSE_CACHE_CSV = _UNIVERSE_CACHE_DIR / "a_share_universe.csv"
_UNIVERSE_META_JSON = _UNIVERSE_CACHE_DIR / "universe_meta.json"


def universe_cache_ttl_hours() -> float:
    try:
        return float(
            os.environ.get("GUPPIAO_UNIVERSE_CACHE_HOURS", "168").strip() or "168"
        )
    except ValueError:
        return 168.0


def clear_universe_disk_cache() -> None:
    """删除本地股票池缓存；下次 get_all_stocks 会重新从交易所拉列表。"""
    for p in (_UNIVERSE_CACHE_CSV, _UNIVERSE_META_JSON):
        try:
            if p.is_file():
                p.unlink()
        except OSError:
            pass


def _save_universe_cache(
    df: pd.DataFrame, log: Optional[Callable[[str], None]] = None
) -> None:
    if df.empty or "code" not in df.columns:
        return
    _UNIVERSE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out = (
        df[["code", "name"]].copy()
        if "name" in df.columns
        else df[["code"]].assign(name="")
    )
    out["code"] = out["code"].astype(str).str.strip().str.zfill(6)
    out.to_csv(_UNIVERSE_CACHE_CSV, index=False, encoding="utf-8-sig")
    import json

    meta = {
        "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": int(len(out)),
        "ttl_hours": universe_cache_ttl_hours(),
    }
    _UNIVERSE_META_JSON.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if log:
        log(f"股票池已缓存 {len(out)} 只 → cache/a_share_universe.csv")


def _load_universe_cache(
    log: Optional[Callable[[str], None]] = None,
) -> Optional[pd.DataFrame]:
    if not _UNIVERSE_CACHE_CSV.is_file():
        return None
    age_h = (time.time() - _UNIVERSE_CACHE_CSV.stat().st_mtime) / 3600
    ttl = universe_cache_ttl_hours()
    if age_h > ttl:
        if log:
            log(
                f"股票池缓存已过期（{age_h:.1f}h > {ttl:.0f}h），将重新从交易所拉取…"
            )
        return None
    try:
        df = pd.read_csv(_UNIVERSE_CACHE_CSV, dtype={"code": str})
    except Exception as e:
        if log:
            log(f"读取股票池缓存失败: {e}")
        return None
    if "code" not in df.columns:
        return None
    if "name" not in df.columns:
        df["name"] = ""
    df["code"] = (
        df["code"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .str.zfill(6)
    )
    if log:
        log(
            f"已从本地加载股票池 {len(df)} 只（缓存约 {age_h:.1f}h 前，TTL {ttl:.0f}h）"
        )
    return df[["code", "name"]]


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


def _build_a_share_universe(log: Optional[Callable[[str], None]] = None) -> pd.DataFrame:
    """深交所 + 上交所（含科创板）官方列表，不含北交所；少量 HTTP，无东方财富 clist 分页。"""
    parts: List[pd.DataFrame] = []
    tasks = [
        (
            "深交所 A 股",
            lambda: ak.stock_info_sz_name_code(symbol="A股列表"),
            {"A股代码": "code", "A股简称": "name"},
        ),
        (
            "上交所主板",
            lambda: ak.stock_info_sh_name_code(symbol="主板A股"),
            {"证券代码": "code", "证券简称": "name"},
        ),
        (
            "科创板",
            lambda: ak.stock_info_sh_name_code(symbol="科创板"),
            {"证券代码": "code", "证券简称": "name"},
        ),
    ]
    for label, fetch, cmap in tasks:
        try:
            raw = _retry_ak_call(fetch)
            if raw is None or getattr(raw, "empty", True):
                if log:
                    log(f"{label}: 无数据，跳过。")
                continue
            d = raw.rename(columns=cmap)[["code", "name"]].copy()
            d["code"] = _norm_code_series(d["code"])
            parts.append(d)
            if log:
                log(f"{label}: {len(d)} 只")
        except Exception as e:
            if log:
                log(f"{label} 失败: {e}（已跳过该段）")
    if not parts:
        return pd.DataFrame(columns=["code", "name"])
    out = pd.concat(parts, ignore_index=True)
    out = out.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    if log:
        log(f"合并去重后股票池共 {len(out)} 只。")
    return out


@lru_cache(maxsize=2048)
def _cached_bid_ask_items(symbol: str) -> Tuple[Tuple[str, float], ...]:
    sym = str(symbol).strip().zfill(6)
    df = _retry_ak_call(ak.stock_bid_ask_em, symbol=sym)
    m: Dict[str, float] = {}
    for _, row in df.iterrows():
        m[str(row["item"])] = _em_scalar(row["value"])
    return tuple(sorted(m.items()))


def _bid_ask_map(symbol: str) -> Dict[str, float]:
    return dict(_cached_bid_ask_items(str(symbol).strip().zfill(6)))


def clear_bid_ask_cache() -> None:
    _cached_bid_ask_items.cache_clear()


def _realtime_from_bid_ask(stock_code: str) -> Optional[Dict[str, Any]]:
    try:
        m = _bid_ask_map(stock_code)
    except Exception:
        return None
    if not m:
        return None
    sym = str(stock_code).strip().zfill(6)
    price = _em_price_yuan(m.get("最新"))
    chg_pct = _em_scalar(m.get("涨幅"))
    if abs(chg_pct) > 25:
        chg_pct = chg_pct / 100.0
    tr = _em_scalar(m.get("换手"))
    if tr > 50:
        tr = tr / 100.0
    vr = _em_scalar(m.get("量比"))
    if vr > 500:
        vr = vr / 100.0
    return {
        "code": sym,
        "name": "",
        "price": price,
        "change_pct": chg_pct,
        "volume": _em_scalar(m.get("总手")),
        "amount": _em_scalar(m.get("金额")),
        "high": _em_price_yuan(m.get("最高")),
        "low": _em_price_yuan(m.get("最低")),
        "open": _em_price_yuan(m.get("今开")),
        "pre_close": _em_price_yuan(m.get("昨收")),
        "volume_ratio": vr,
        "turnover_rate": tr,
    }


def _inner_outer_from_bid_ask(stock_code: str) -> Optional[Dict[str, Any]]:
    try:
        m = _bid_ask_map(stock_code)
    except Exception:
        return None
    if not m:
        return None
    inner_vol = _em_scalar(m.get("内盘"))
    outer_vol = _em_scalar(m.get("外盘"))
    total_vol = inner_vol + outer_vol
    inner_outer_ratio = outer_vol / inner_vol if inner_vol > 0 else 0.0
    return {
        "inner_volume": inner_vol,
        "outer_volume": outer_vol,
        "inner_outer_ratio": inner_outer_ratio,
        "outer_pct": outer_vol / total_vol * 100 if total_vol > 0 else 0.0,
        "inner_pct": inner_vol / total_vol * 100 if total_vol > 0 else 0.0,
    }


class StockDataFetcher:
    def __init__(self):
        self.stock_list_cache = None
        self.cache_time = None
        self.cache_duration = 3600
        self._log: Optional[Callable[[str], None]] = None

    def set_log_callback(self, cb: Optional[Callable[[str], None]]) -> None:
        self._log = cb

    def clear_quote_cache(self) -> None:
        clear_bid_ask_cache()

    def clear_saved_universe(self) -> None:
        clear_universe_disk_cache()

    def get_all_stocks(self, skip_cache: bool = False) -> pd.DataFrame:
        if _use_em_full_spot_for_list():
            return self._get_all_stocks_em_spot()
        if os.environ.get("GUPPIAO_REFRESH_UNIVERSE", "").strip().lower() in (
            "1",
            "true",
            "yes",
        ):
            skip_cache = True
        if not skip_cache:
            cached = _load_universe_cache(self._log)
            if cached is not None and not cached.empty:
                return cached
        if self._log:
            self._log(
                "从交易所构建股票池（深交所+上交所含科创板，不含北交所）…"
            )
        df = _build_a_share_universe(self._log)
        if not df.empty:
            _save_universe_cache(df, self._log)
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

    def get_realtime_data(self, stock_code: str) -> Optional[Dict[str, Any]]:
        try:
            return _realtime_from_bid_ask(stock_code)
        except Exception as e:
            print(f"获取股票 {stock_code} 实时数据失败: {e}")
            return None

    def get_individual_flow(self, stock_code: str) -> Optional[Dict[str, Any]]:
        try:
            df = _retry_ak_call(ak.stock_individual_flow_em, stock=stock_code)
            if df.empty:
                return None
            
            latest = df.iloc[-1]
            return {
                'date': latest['日期'],
                'main_net_inflow': float(latest['主力净流入-净额']) if latest['主力净流入-净额'] != '-' else 0,
                'main_net_inflow_pct': float(latest['主力净流入-净占比']) if latest['主力净流入-净占比'] != '-' else 0,
                'retail_net_inflow': float(latest['小单净流入-净额']) if latest['小单净流入-净额'] != '-' else 0,
                'retail_net_inflow_pct': float(latest['小单净流入-净占比']) if latest['小单净流入-净占比'] != '-' else 0,
            }
        except Exception as e:
            print(f"获取股票 {stock_code} 资金流向失败: {e}")
            return None

    def get_history_data(self, stock_code: str, days: int = 10) -> Optional[pd.DataFrame]:
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days + 10)).strftime('%Y%m%d')
            
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
            
            return df.tail(days)
        except Exception as e:
            print(f"获取股票 {stock_code} 历史数据失败: {e}")
            return None

    def get_inner_outer_disk(self, stock_code: str) -> Optional[Dict[str, Any]]:
        try:
            return _inner_outer_from_bid_ask(stock_code)
        except Exception as e:
            print(f"获取股票 {stock_code} 内外盘数据失败: {e}")
            return None

    def get_stock_fund_flow_rank(self) -> Optional[pd.DataFrame]:
        try:
            df = _retry_ak_call(ak.stock_fund_flow_individual, symbol="即时")
            return df
        except Exception as e:
            print(f"获取个股资金流向排名失败: {e}")
            return None
