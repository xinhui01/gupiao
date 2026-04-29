from __future__ import annotations

import collections
import os
import time
import re
import warnings
import threading
from concurrent.futures import as_completed
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable, TypeVar, Tuple
from datetime import datetime, timedelta

from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

def _project_root() -> Path:
    return Path(__file__).resolve().parent

def _use_insecure_ssl() -> bool:
    if os.environ.get("ASHARE_SCAN_INSECURE_SSL", "").strip().lower() in ("1", "true", "yes"):
        return True
    root = _project_root()
    return (root / "USE_INSECURE_SSL").is_file() or (root / ".ashare_scan_insecure_ssl").is_file()

def _use_bypass_proxy() -> bool:
    """不走 HTTP(S)_PROXY 等环境代理（避免公司代理对东方财富断开）。"""
    if os.environ.get("ASHARE_SCAN_BYPASS_PROXY", "").strip().lower() in ("1", "true", "yes"):
        return True
    root = _project_root()
    return (root / "USE_BYPASS_PROXY").is_file() or (root / ".ashare_scan_bypass_proxy").is_file()

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
                merged = _random_eastmoney_headers()
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
            elif "sina.com.cn" in u or "sinajs.cn" in u:
                kwargs.setdefault("headers", {})
                kwargs["headers"]["User-Agent"] = _random.choice(_USER_AGENT_POOL)
                kwargs["headers"]["Referer"] = "https://finance.sina.com.cn/"
            return _orig_req(self, method, url, **kwargs)

        requests.Session.__init__ = _patched_init  # type: ignore[method-assign]
        requests.Session.request = _patched_request  # type: ignore[method-assign]
    except ImportError:
        pass

_apply_network_patches()

import pandas as pd
import akshare as ak

from stock_logger import get_logger

logger = get_logger(__name__)

from stock_store import (
    clear_history as clear_history_store,
    clear_scan_snapshots,
    clear_universe as clear_universe_store,
    history_coverage_summary as load_history_coverage_summary,
    load_fund_flow as load_fund_flow_store,
    load_history as load_history_store,
    load_history_meta as load_history_meta_store,
    load_universe as load_universe_store,
    save_fund_flow as save_fund_flow_store,
    save_history as save_history_store,
    save_history_meta as save_history_meta_store,
    save_universe as save_universe_store,
)
from data_source_models import DATA_SOURCE_OPTIONS, DataProviderPlan, HistoryRequestPlan

T = TypeVar("T")

# DaemonThreadPoolExecutor 已迁移到 src/utils/daemon_executor.py；
# 此处重新导出，保持 `from stock_data import DaemonThreadPoolExecutor` 零修改。
from src.utils.daemon_executor import DaemonThreadPoolExecutor
from src.config import env_int, env_float


def _history_request_concurrency() -> int:
    return env_int("ASHARE_SCAN_HISTORY_CONCURRENCY", default=2, lo=1, hi=10)


def _history_min_request_interval_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_MIN_INTERVAL_SEC", default=2.5, lo=0.5, hi=15.0)


def _history_connect_timeout_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_CONNECT_TIMEOUT_SEC", default=2.5, lo=0.5, hi=10.0)


def _history_read_timeout_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_READ_TIMEOUT_SEC", default=4.0, lo=1.0, hi=15.0)


def _history_total_timeout_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_TOTAL_TIMEOUT_SEC", default=12.0, lo=3.0, hi=60.0)


def _history_host_cooldown_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_HOST_COOLDOWN_SEC", default=180.0, lo=10.0, hi=1800.0)


def _history_max_mirrors_per_stock() -> int:
    return env_int("ASHARE_SCAN_HISTORY_MAX_MIRRORS_PER_STOCK", default=3, lo=1, hi=8)


def _history_probe_success_target() -> int:
    return env_int("ASHARE_SCAN_HISTORY_PROBE_SUCCESS_TARGET", default=2, lo=1, hi=4)


def _history_block_window_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_BLOCK_WINDOW_SEC", default=180.0, lo=30.0, hi=3600.0)


def _history_block_threshold() -> int:
    return env_int("ASHARE_SCAN_HISTORY_BLOCK_THRESHOLD", default=3, lo=1, hi=10)


def _history_block_cooldown_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_BLOCK_COOLDOWN_SEC", default=900.0, lo=60.0, hi=7200.0)


from src.sources.eastmoney import throttling as _em_throttling
_HISTORY_REQUEST_SEMAPHORE = _em_throttling.REQUEST_SEMAPHORE
_HISTORY_REQUEST_RATE_LOCK = _em_throttling.REQUEST_RATE_LOCK

# ---- 自适应请求间隔 ----
# 实现已迁移到 src/sources/eastmoney/throttling.py
_adaptive_on_success = _em_throttling.adaptive_on_success
_adaptive_on_rate_limit = _em_throttling.adaptive_on_rate_limit
_adaptive_current_interval = _em_throttling.adaptive_current_interval

_HISTORY_DIAGNOSTICS_LOCK = _em_throttling._DIAGNOSTICS_LOCK
_HISTORY_DIAGNOSTICS = _em_throttling.DIAGNOSTICS
from src.sources.eastmoney import history as _em_history
_EASTMONEY_HISTORY_MIRRORS = _em_history.HISTORY_MIRRORS
# ---- 全局主机健康管理器 ----
# 实现已迁移到 src/network/host_health.py；下面用别名保持调用方零修改。
from src.network import host_health as _host_health
_GLOBAL_HOST_HEALTH = _host_health._HOST_HEALTH
_GLOBAL_HOST_HEALTH_LOCK = _host_health._HOST_HEALTH_LOCK
_GLOBAL_HOST_FAIL_COUNT = _host_health._HOST_FAIL_COUNT
_global_host_cooldown_sec = _host_health.cooldown_sec
_global_mark_host_failed = _host_health.mark_failed
_global_mark_host_ok = _host_health.mark_ok
_global_host_on_cooldown = _host_health.on_cooldown
_global_host_cooldown_remaining = _host_health.cooldown_remaining


# ---- 东方财富全局熔断器 ----
# 实现已迁移到 src/utils/em_circuit_breaker.py；此处保留原名的薄薄转发，
# 以便 stock_data / stock_filter 中几十处 `_eastmoney_circuit_breaker_*` 调用
# 零修改。真正的状态存在 EMCircuitBreaker 单例里。
from src.utils import em_circuit_breaker as _em_circuit_breaker


def _eastmoney_circuit_breaker_open() -> bool:
    return _em_circuit_breaker.is_open()


def _eastmoney_circuit_breaker_record_failure() -> None:
    _em_circuit_breaker.record_failure()


def _eastmoney_circuit_breaker_record_success() -> None:
    _em_circuit_breaker.record_success()


_global_filter_healthy_urls = _host_health.filter_healthy_urls


_HISTORY_BLOCK_LOCK = _em_throttling._BLOCK_LOCK

# 东方财富 / 通用 HTTP headers 实现已迁移到 src/network/headers.py。
import random as _random
from src.sources import _common as _sources_common
from src.utils import codes as _utils_codes
from src.utils import parsing as _utils_parsing
from src.sources.eastmoney import numeric as _em_numeric

from src.network.headers import (
    USER_AGENT_POOL as _USER_AGENT_POOL,
    REFERER_POOL as _REFERER_POOL,
    random_eastmoney_headers as _random_eastmoney_headers,
    random_eastmoney_cookie as _random_eastmoney_cookie,
)
_EASTMONEY_HEADERS = _random_eastmoney_headers()


# ---- 可选免费代理池 ----
# 实现已迁移到 src/network/proxy_pool.py；下面用别名保持调用方零修改。
from src.network.proxy_pool import (
    use_proxy_pool as _use_proxy_pool,
    get_proxy as _get_proxy,
    blacklist_proxy as _blacklist_proxy,
    refresh_proxy_pool as _refresh_proxy_pool,
)


# 拉取全市场列表分页时写入 GUI 日志（由 get_all_stocks 临时注册）
_list_download_log: Optional[Callable[[str], None]] = None


EastmoneyRateLimitError = _em_throttling.EastmoneyRateLimitError
HistoryAccessSuspendedError = _em_throttling.HistoryAccessSuspendedError
_increment_history_diagnostic = _em_throttling.increment_diagnostic
_history_diagnostics_snapshot = _em_throttling.diagnostics_snapshot


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


def _retry_ak_call(fn: Callable[..., T], *args, max_attempts: int = 2, base_delay: float = 1.0, **kwargs) -> T:
    for attempt in range(max_attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt < max_attempts - 1 and _is_transient_network_error(e):
                time.sleep(base_delay * (attempt + 1))
                continue
            raise


def _history_retry_ak_call(fn: Callable[..., T], *args, **kwargs) -> T:
    # 历史 K 线接口对并发和出口网络都更敏感，先串行闸门。
    # 具体的镜像轮换和短重试交给函数内部处理，避免外层再叠超长等待。
    with _HISTORY_REQUEST_SEMAPHORE:
        return fn(*args, **kwargs)


_history_access_blocked_until = _em_throttling.history_access_blocked_until
_record_history_block = _em_throttling.record_history_block
_wait_for_history_request_slot = _em_throttling.wait_for_history_request_slot


from src.sources._jsonp import random_callback as _random_jsonp_callback, strip_wrapper as _strip_jsonp_wrapper
from src.sources.eastmoney import history_parser as _em_history_parser


_eastmoney_history_request_params = _em_history_parser.request_params


# 核心 GET 包装：实现已迁移到 src/sources/eastmoney/session.py
from src.sources.eastmoney import session as _em_session
_request_session_get_json = _em_session.get_json


# 东财 intraday + auction snapshot 实现已迁移到 src/sources/eastmoney/intraday.py
from src.sources.eastmoney import intraday as _em_intraday
_fetch_eastmoney_auction_snapshot = _em_intraday.fetch_auction_snapshot
_fetch_eastmoney_intraday_1min = _em_intraday.fetch_intraday_1min
_empty_intraday_meta_payload = _em_intraday.empty_meta_payload
_normalize_intraday_source_frame = _em_intraday.normalize_source_frame
_resolve_intraday_trade_dates = _em_intraday.resolve_trade_dates
_select_intraday_trade_date = _em_intraday.select_trade_date
_slice_intraday_frame_by_trade_date = _em_intraday.slice_frame_by_trade_date


# 限流检测实现已迁移到 src/sources/eastmoney/rate_limit.py
from src.sources.eastmoney import rate_limit as _em_rate_limit
_looks_like_eastmoney_rate_limit = _em_rate_limit.looks_like_rate_limit
_eastmoney_json_indicates_rate_limit = _em_rate_limit.json_indicates_rate_limit


# Mirror 包装：实现已迁移到 src/sources/eastmoney/rate_limit.py + src/network/host_health.py
_history_mirror_host = _em_rate_limit.mirror_host_of
_mark_history_mirror_failed = _host_health.mark_failed
_mark_history_mirror_ok = _host_health.mark_ok
_history_mirror_on_cooldown = _host_health.on_cooldown


from src.sources.eastmoney import mirrors as _em_mirrors


def _prioritize_history_mirrors(
    mirror_urls,
    preferred_mirror=None,
):
    return _em_mirrors.prioritize_history_mirrors(
        mirror_urls, preferred_mirror, max_count=_history_max_mirrors_per_stock()
    )


_parse_eastmoney_hist_json = _em_history_parser.parse_hist_json


_normalize_history_frame = _sources_common.normalize_history_frame


# ---- 腾讯证券镜像池 ----
# 实现已迁移到 src/sources/tencent.py；下面用别名保持调用方零修改。
from src.sources import tencent as _tencent
_TENCENT_HISTORY_MIRRORS = _tencent.HISTORY_MIRRORS
_get_healthy_tencent_mirrors = _tencent._get_healthy_mirrors
_fetch_tencent_hist_direct = _tencent.fetch_hist_direct
_fetch_tencent_hist_frame = _tencent.fetch_hist_frame


# ---- 新浪财经反封保护 ----
# 实现已迁移到 src/sources/sina.py；下面是公共函数别名。
from src.sources import sina as _src_sina
_fetch_sina_hist_frame = _src_sina.fetch_hist_frame


# ---- 网易财经历史日线 ----
# 实现已迁移到 src/sources/netease.py；下面是公共函数别名。
from src.sources import netease as _src_netease
_fetch_netease_hist_frame = _src_netease.fetch_hist_frame


# ---- 百度股市通历史日线 ----
# 实现已迁移到 src/sources/baidu.py；下面是公共函数别名。
from src.sources import baidu as _src_baidu
_fetch_baidu_hist_frame = _src_baidu.fetch_hist_frame


# ---- 搜狐财经历史日线 ----
# 实现已迁移到 src/sources/sohu.py；下面是公共函数别名。
from src.sources import sohu as _src_sohu
_fetch_sohu_hist_frame = _src_sohu.fetch_hist_frame


# ---- 同花顺 (THS / 10jqka) 历史日线 ----
# 实现已迁移到 src/sources/ths.py；下面是公共函数别名。
from src.sources import ths as _src_ths
_fetch_ths_hist_frame = _src_ths.fetch_hist_frame


# ---- 华尔街见闻 (WallstreetCN) 历史日线 ----
# 实现已迁移到 src/sources/wscn.py；下面是公共函数别名。
from src.sources import wscn as _src_wscn
_fetch_wscn_hist_frame = _src_wscn.fetch_hist_frame


# 历史抓取主函数 + probe：实现已迁移到 src/sources/eastmoney/history.py
_probe_history_mirror = _em_history.probe_mirror
_fetch_eastmoney_hist_frame = _em_history.fetch_hist_frame


# AkShare 警告抑制实现已迁移到 src/sources/eastmoney/akshare_warnings.py
# import 时即注册全局 filterwarnings，无需在此重复。
from src.sources.eastmoney import akshare_warnings as _ak_warnings
_AkshareWarningCategory = _ak_warnings.AkshareWarningCategory
_call_akshare_quietly = _ak_warnings.call_quietly


def clear_universe_data() -> None:
    """清空已保存的股票池和扫描快照。"""
    clear_universe_store()
    clear_scan_snapshots()


def clear_history_data() -> None:
    """清空已保存的历史日线。"""
    clear_history_store()


# Store 包装层：实现已迁移到 src/services/store_facade.py
from src.services import store_facade as _store_facade
_save_universe_store = _store_facade.save_universe
_load_universe_store = _store_facade.load_universe
_load_history_store = _store_facade.load_history
_save_history_store = _store_facade.save_history
_load_fund_flow_store = _store_facade.load_fund_flow
_save_fund_flow_store = _store_facade.save_fund_flow


_eastmoney_request_mirror_urls = _em_mirrors.request_mirror_urls


# AkShare request_with_retry patch：实现已迁移到 src/sources/eastmoney/akshare_patch.py
from src.sources.eastmoney import akshare_patch as _ak_patch
_ashare_request_with_retry = _ak_patch.request_with_retry
_patch_akshare_request_layer = _ak_patch.apply
_patch_akshare_request_layer()


from src.sources import universe as _src_universe
_use_em_full_spot_for_list = _src_universe.use_em_full_spot_for_list


_em_scalar = _em_numeric.em_scalar


_em_price_yuan = _em_numeric.em_price_yuan


_norm_code_series = _utils_codes.norm_code_series


_norm_code = _utils_codes.norm_code


_infer_sz_board = _utils_codes.infer_sz_board


_infer_exchange = _utils_codes.infer_exchange


_infer_market = _sources_common.infer_market
_market_prefixed_code = _sources_common.market_prefixed_code


_normalize_concepts_text = _utils_parsing.normalize_concepts_text


_safe_float = _utils_parsing.safe_float


# 日期 / 缓存新鲜度：实现已迁移到 src/utils/cache_freshness.py
from src.utils import cache_freshness as _cache_fresh
_today_ymd = _cache_fresh.today_ymd
_should_refresh_today_row = _cache_fresh.should_refresh_today_row
_estimate_last_trade_date = _cache_fresh.estimate_last_trade_date
_is_history_cache_fresh = _cache_fresh.is_history_cache_fresh


_build_a_share_universe = _src_universe.build_a_share_universe


from src.utils.lru_cache import LRUCache as _LRUCache


class StockDataFetcher:
    def __init__(self):
        self._log: Optional[Callable[[str], None]] = None
        self._strong_pool_cache: Dict[str, pd.DataFrame] = _LRUCache(maxsize=30)
        self._limit_up_pool_cache: Dict[str, pd.DataFrame] = _LRUCache(maxsize=30)
        self._prev_limit_up_pool_cache: Dict[str, pd.DataFrame] = _LRUCache(maxsize=30)
        self._concepts_cache: Optional[Dict[str, str]] = None
        self._universe_concepts_cache: Optional[Dict[str, str]] = None
        self._history_mirror_cache: List[str] = []
        self._history_mirror_checked_at: float = 0.0
        try:
            configured_limit = int(os.environ.get("ASHARE_SCAN_CONCEPT_BOARD_LIMIT", "20").strip() or "20")
        except ValueError:
            configured_limit = 20
        self.concept_board_limit: int = max(5, min(configured_limit, 80))
        try:
            configured_timeout = float(os.environ.get("ASHARE_SCAN_CONCEPT_FILL_TIMEOUT_SEC", "25").strip() or "25")
        except ValueError:
            configured_timeout = 25.0
        self.concept_fill_timeout_sec: float = max(5.0, configured_timeout)
        self._concepts_lock = threading.Lock()
        self._last_history_probe_failures: Dict[str, str] = {}
        self._last_history_block_log_at: float = 0.0
        self._default_history_source: str = "auto"
        self._default_intraday_source: str = "auto"
        self._default_fund_flow_source: str = "auto"
        self._default_limit_up_reason_source: str = "auto"
        # 启动时预加载代理池（后台线程，不阻塞初始化）
        if _use_proxy_pool():
            threading.Thread(target=_refresh_proxy_pool, daemon=True).start()

    def set_log_callback(self, cb: Optional[Callable[[str], None]]) -> None:
        self._log = cb

    def history_request_concurrency_limit(self) -> int:
        return _history_request_concurrency()

    def get_history_cache_summary(self) -> Dict[str, Any]:
        payload = load_history_coverage_summary()
        payload["history_source"] = self._default_history_source
        return payload

    def _normalize_source(self, domain: str, source: str) -> str:
        options = DATA_SOURCE_OPTIONS.get(domain, ("auto",))
        value = str(source or "auto").strip().lower()
        return value if value in options else "auto"

    def normalize_history_source(self, source: str) -> str:
        return self._normalize_source("history", source)

    def normalize_intraday_source(self, source: str) -> str:
        value = str(source or "auto").strip().lower()
        if value == "legacy":
            value = "sina"
        return self._normalize_source("intraday", value)

    def normalize_fund_flow_source(self, source: str) -> str:
        return self._normalize_source("fund_flow", source)

    def normalize_limit_up_reason_source(self, source: str) -> str:
        return self._normalize_source("limit_up_reason", source)

    def set_default_history_source(self, source: str) -> None:
        self._default_history_source = self.normalize_history_source(source)

    def set_default_intraday_source(self, source: str) -> None:
        self._default_intraday_source = self.normalize_intraday_source(source)

    def set_default_fund_flow_source(self, source: str) -> None:
        self._default_fund_flow_source = self.normalize_fund_flow_source(source)

    def set_default_limit_up_reason_source(self, source: str) -> None:
        self._default_limit_up_reason_source = self.normalize_limit_up_reason_source(source)

    def _build_multi_source_plans(self, source: str) -> List[HistoryRequestPlan]:
        """构建多源并行请求计划列表，用于批量更新时分流。
        将可用的 eastmoney 镜像各自作为一个独立 plan，
        再加上 tencent 和 sina 作为补充源，实现负载均衡。
        """
        normalized = self.normalize_history_source(source)

        plans: List[HistoryRequestPlan] = []

        # 东方财富：每个健康镜像作为独立通道（熔断时 auto 模式跳过）
        em_skipped = False
        if normalized in ("auto", "eastmoney"):
            if normalized == "auto" and _eastmoney_circuit_breaker_open():
                em_skipped = True
                logger.debug("auto 模式：东财熔断中，跳过东财镜像")
            else:
                mirrors = self.get_available_history_mirrors()
                for mirror in mirrors:
                    plans.append(HistoryRequestPlan(
                        mode="network",
                        provider_sequence=("eastmoney",),
                        mirror_urls=(mirror,),
                        reason=f"multi-source-eastmoney-{_history_mirror_host(mirror)}",
                    ))

        # 腾讯/新浪/网易/百度/搜狐：作为补充分流通道（跳过正在冷却的源）
        if normalized in ("auto", "tencent"):
            tencent_healthy = _get_healthy_tencent_mirrors()
            if tencent_healthy:
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("tencent",),
                    mirror_urls=(),
                    reason="multi-source-tencent",
                ))
        if normalized in ("auto", "sina"):
            if not _global_host_on_cooldown("finance.sina.com.cn"):
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("sina",),
                    mirror_urls=(),
                    reason="multi-source-sina",
                ))
        if normalized in ("auto", "netease"):
            if not _global_host_on_cooldown("quotes.money.163.com"):
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("netease",),
                    mirror_urls=(),
                    reason="multi-source-netease",
                ))
        if normalized in ("auto", "baidu"):
            if not _global_host_on_cooldown("gushitong.baidu.com"):
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("baidu",),
                    mirror_urls=(),
                    reason="multi-source-baidu",
                ))
        if normalized in ("auto", "sohu"):
            if not _global_host_on_cooldown("q.stock.sohu.com"):
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("sohu",),
                    mirror_urls=(),
                    reason="multi-source-sohu",
                ))
        if normalized in ("auto", "ths"):
            if not _global_host_on_cooldown("d.10jqka.com.cn"):
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("ths",),
                    mirror_urls=(),
                    reason="multi-source-ths",
                ))
        if normalized in ("auto", "wscn"):
            if not _global_host_on_cooldown("api-ddc-wscn.awtmt.com"):
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("wscn",),
                    mirror_urls=(),
                    reason="multi-source-wscn",
                ))

        # 兜底：至少保证一个 auto plan
        if not plans:
            plans.append(self.build_history_request_plan(source=source, force_refresh=False))

        return plans

    def update_history_cache(
        self,
        max_stocks: int = 0,
        days: int = 60,
        source: Optional[str] = None,
        workers: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int, str, str, int, int, int], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        refresh_universe: bool = False,
        allowed_boards: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        universe = self.get_all_stocks(force_refresh=refresh_universe)
        if universe is None or universe.empty:
            return {"total": 0, "updated": 0, "failed": 0, "skipped": 0}
        if allowed_boards and "board" in universe.columns:
            allowed = {str(x).strip() for x in allowed_boards if str(x).strip()}
            if allowed:
                universe = universe[universe["board"].astype(str).isin(allowed)].reset_index(drop=True)
        if max_stocks and max_stocks > 0:
            universe = universe.head(max_stocks).reset_index(drop=True)
        rows = universe.to_dict("records")
        total = len(rows)
        if total <= 0:
            return {"total": 0, "updated": 0, "failed": 0, "skipped": 0}

        # ---- 多源并行分流策略 ----
        source_str = source or self._default_history_source
        multi_plans = self._build_multi_source_plans(source_str)
        plan_count = len(multi_plans)

        if self._log:
            plan_names = [p.reason for p in multi_plans]
            self._log(f"多源分流策略：{plan_count} 个通道 → {', '.join(plan_names)}")

        # 打乱股票顺序，避免同板块集中请求
        _random.shuffle(rows)

        worker_count = max(
            1,
            min(int(workers or self.history_request_concurrency_limit()), self.history_request_concurrency_limit()),
        )
        if plan_count > 1:
            worker_count = max(worker_count, min(plan_count + 1, 10))

        updated = 0
        failed = 0
        skipped = 0

        # 构建一个包含全部备用源的 fallback plan，用于单源失败后重试
        _all_fallback_providers = [
            p for plan in multi_plans for p in plan.provider_sequence
        ]
        # 去重保序
        _seen_providers: set[str] = set()
        _unique_fallback: list[str] = []
        for p in _all_fallback_providers:
            if p not in _seen_providers:
                _seen_providers.add(p)
                _unique_fallback.append(p)
        _fallback_plan = HistoryRequestPlan(
            mode="network",
            provider_sequence=tuple(_unique_fallback),
            mirror_urls=(),
            reason="multi-source-fallback",
        ) if len(_unique_fallback) > 1 else None

        def _work(item: Dict[str, Any], assigned_plan: HistoryRequestPlan) -> tuple[str, str, bool, bool]:
            """返回 (code, name, success, skipped)"""
            code = str(item.get("code", "")).strip().zfill(6)
            name = str(item.get("name", "") or "")
            if should_stop and should_stop():
                return code, name, False, True
            if _is_history_cache_fresh(code, max(1, days), self._log):
                return code, name, True, True

            # 检查分配的源是否已冷却，是则直接用 fallback
            _host_map = {
                "sina": "finance.sina.com.cn", "netease": "quotes.money.163.com",
                "baidu": "gushitong.baidu.com", "sohu": "q.stock.sohu.com",
                "ths": "d.10jqka.com.cn", "wscn": "api-ddc-wscn.awtmt.com",
            }
            assigned_all_cooled = all(
                _global_host_on_cooldown(_host_map[p])
                for p in assigned_plan.provider_sequence
                if p in _host_map
            ) if assigned_plan.provider_sequence else False

            use_plan = _fallback_plan if (assigned_all_cooled and _fallback_plan) else assigned_plan
            df = self.get_history_data(
                code, days=days, force_refresh=True, request_plan=use_plan,
            )
            if df is not None and not df.empty:
                return code, name, True, False
            # 如果用的是 assigned plan 失败了，再用 fallback 重试
            if use_plan is assigned_plan and _fallback_plan is not None:
                df = self.get_history_data(
                    code, days=days, force_refresh=True, request_plan=_fallback_plan,
                )
            return code, name, bool(df is not None and not df.empty), False

        with DaemonThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="hist-cache") as executor:
            futures = [
                executor.submit(_work, item, multi_plans[idx % plan_count])
                for idx, item in enumerate(rows)
            ]
            completed = 0
            for fut in as_completed(futures):
                completed += 1
                code, name, ok, was_skipped = fut.result()
                if should_stop and should_stop():
                    skipped = max(0, total - completed)
                    break
                if was_skipped:
                    skipped += 1
                elif ok:
                    updated += 1
                else:
                    failed += 1
                if progress_callback:
                    progress_callback(completed, total, code, name, updated, failed, skipped)

        return {
            "total": total,
            "updated": updated,
            "failed": failed,
            "skipped": skipped,
            "plan": f"multi-source/{plan_count}channels",
        }

    def build_intraday_request_plan(self, source: str = "auto") -> DataProviderPlan:
        normalized = self.normalize_intraday_source(source)
        if normalized == "eastmoney":
            return DataProviderPlan(mode="network", provider_sequence=("eastmoney",), reason="intraday-provider=eastmoney")
        if normalized == "sina":
            return DataProviderPlan(mode="network", provider_sequence=("sina",), reason="intraday-provider=sina")
        # auto 模式：东财熔断时优先用 sina
        if _eastmoney_circuit_breaker_open():
            return DataProviderPlan(mode="network", provider_sequence=("sina", "eastmoney"), reason="intraday-provider=auto(em-circuit-open)")
        return DataProviderPlan(mode="network", provider_sequence=("eastmoney", "sina"), reason="intraday-provider=auto")

    def build_fund_flow_request_plan(self, source: str = "auto") -> DataProviderPlan:
        normalized = self.normalize_fund_flow_source(source)
        if normalized == "eastmoney":
            return DataProviderPlan(mode="network", provider_sequence=("eastmoney",), reason="fund-flow-provider=eastmoney")
        if normalized == "ths":
            return DataProviderPlan(mode="network", provider_sequence=("ths",), reason="fund-flow-provider=ths")
        # auto 模式：东财熔断时优先用 ths
        if _eastmoney_circuit_breaker_open():
            return DataProviderPlan(mode="network", provider_sequence=("ths", "eastmoney"), reason="fund-flow-provider=auto(em-circuit-open)")
        return DataProviderPlan(mode="network", provider_sequence=("eastmoney", "ths"), reason="fund-flow-provider=auto")

    def build_limit_up_reason_plan(self, source: str = "auto") -> DataProviderPlan:
        normalized = self.normalize_limit_up_reason_source(source)
        if normalized == "eastmoney":
            return DataProviderPlan(mode="network", provider_sequence=("eastmoney",), reason="limit-up-provider=eastmoney")
        return DataProviderPlan(mode="network", provider_sequence=("eastmoney",), reason="limit-up-provider=auto")

    def build_history_request_plan(self, source: str = "auto", force_refresh: bool = False) -> HistoryRequestPlan:
        normalized = self.normalize_history_source(source)
        if normalized == "tencent":
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("tencent",),
                mirror_urls=(),
                reason="history-provider=tencent",
            )
        if normalized == "sina":
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("sina",),
                mirror_urls=(),
                reason="history-provider=sina",
            )
        if normalized == "netease":
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("netease",),
                mirror_urls=(),
                reason="history-provider=netease",
            )
        if normalized == "baidu":
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("baidu",),
                mirror_urls=(),
                reason="history-provider=baidu",
            )
        if normalized == "sohu":
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("sohu",),
                mirror_urls=(),
                reason="history-provider=sohu",
            )
        if normalized == "ths":
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("ths",),
                mirror_urls=(),
                reason="history-provider=ths",
            )
        if normalized == "wscn":
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("wscn",),
                mirror_urls=(),
                reason="history-provider=wscn",
            )

        mirrors = tuple(self.get_available_history_mirrors(force_refresh=force_refresh))
        if normalized == "eastmoney":
            if mirrors:
                return HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("eastmoney",),
                    mirror_urls=mirrors,
                    reason="history-provider=eastmoney",
                )
            failures = self.get_last_history_probe_failures()
            reason = ""
            if failures:
                reason = "；".join(f"{host}: {detail}" for host, detail in list(failures.items())[:3])
            if not reason:
                reason = "history-mirrors-unavailable"
            return HistoryRequestPlan(
                mode="cache_only",
                provider_sequence=("eastmoney",),
                mirror_urls=(),
                reason=reason,
            )

        _non_em_providers = ("tencent", "sina", "netease", "baidu", "sohu", "ths", "wscn")
        if _eastmoney_circuit_breaker_open():
            # 东财熔断中：auto 模式直接用非东财源，避免无意义的重试
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=_non_em_providers,
                mirror_urls=(),
                reason="history-provider=auto(eastmoney-circuit-open)",
            )
        if mirrors:
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("eastmoney",) + _non_em_providers,
                mirror_urls=mirrors,
                reason="history-provider=auto",
            )
        failures = self.get_last_history_probe_failures()
        reason = ""
        if failures:
            reason = "；".join(f"{host}: {detail}" for host, detail in list(failures.items())[:3])
        if not reason:
            reason = "history-mirrors-unavailable"
        return HistoryRequestPlan(
            mode="network",
            provider_sequence=_non_em_providers,
            mirror_urls=(),
            reason=reason,
        )

    def get_runtime_diagnostics(self) -> Dict[str, Any]:
        blocked_until = _history_access_blocked_until()
        now = time.time()
        diagnostics: Dict[str, Any] = _history_diagnostics_snapshot()
        diagnostics.update(
            {
                "history_concurrency_limit": _history_request_concurrency(),
                "history_min_interval_sec": _history_min_request_interval_sec(),
                "history_host_cooldown_sec": _history_host_cooldown_sec(),
                "history_block_threshold": _history_block_threshold(),
                "history_block_window_sec": _history_block_window_sec(),
                "history_block_cooldown_sec": _history_block_cooldown_sec(),
                "history_request_blocked": blocked_until > now,
                "history_request_blocked_for_sec": max(0, int(blocked_until - now)) if blocked_until > now else 0,
                "cached_mirror_count": len(self._history_mirror_cache),
                "cached_mirrors": [
                    _history_mirror_host(url) for url in self._history_mirror_cache
                ],
            }
        )
        return diagnostics

    def _log_history_access_suspended(self) -> bool:
        blocked_until = _history_access_blocked_until()
        now = time.time()
        if blocked_until <= now:
            return False
        if self._log and (now - self._last_history_block_log_at >= 10):
            remain = max(1, int(blocked_until - now))
            self._log(
                f"东方财富历史接口已进入冷却保护，接下来约 {remain}s 内不再发新请求，优先回退本地缓存。"
            )
            self._last_history_block_log_at = now
        return True

    def get_available_history_mirrors(self, force_refresh: bool = False) -> List[str]:
        now = time.time()
        if not force_refresh and now - self._history_mirror_checked_at < 180:
            _increment_history_diagnostic("probe_cache_hits")
            return list(self._history_mirror_cache)
        if self._log_history_access_suspended():
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
                if len(available) >= _history_probe_success_target():
                    break
            else:
                failures[host] = str(detail)
                if self._log:
                    self._log(f"历史镜像不可用 {host}：{detail}")
                if "冷却保护" in str(detail):
                    break
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

    def _load_strong_pool(self, trade_date: str, source: Optional[str] = None) -> pd.DataFrame:
        date_key = self._normalize_trade_date(trade_date)
        if not date_key:
            return pd.DataFrame()
        provider = self.normalize_limit_up_reason_source(source or self._default_limit_up_reason_source)
        cache_key = f"{provider}:{date_key}"
        cached = self._strong_pool_cache.get(cache_key)
        if cached is not None:
            return cached
        plan = self.build_limit_up_reason_plan(provider)
        df = pd.DataFrame()
        last_error: Optional[Exception] = None
        for provider_name in plan.provider_sequence:
            if provider_name == "eastmoney":
                if _eastmoney_circuit_breaker_open():
                    logger.debug("强势股池 %s：东财熔断中，跳过", date_key)
                    continue
                try:
                    df = _retry_ak_call(ak.stock_zt_pool_strong_em, date=date_key)
                    break
                except Exception as e:
                    last_error = e
                    if self._log:
                        self._log(f"强势股池 {date_key} 获取失败: {e}")
        if df is None or getattr(df, "empty", True):
            df = pd.DataFrame()
            if last_error is not None and self._log:
                self._log(f"涨停原因数据源全部失败 {date_key}: {last_error}")
        self._strong_pool_cache[cache_key] = df
        return df

    def get_limit_up_reason(self, stock_code: str, trade_date: str, source: Optional[str] = None) -> str:
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return ""
        pool = self._load_strong_pool(trade_date, source=source)
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

    def get_limit_up_pool(self, trade_date: str) -> pd.DataFrame:
        """获取指定日期的涨停板池。

        三级缓存：内存 → SQLite → 网络请求。
        历史日期的数据一旦入库，后续永远从本地读取。
        """
        date_key = self._normalize_trade_date(trade_date)
        if not date_key:
            return pd.DataFrame()

        # 1. 内存缓存
        mem_cached = self._limit_up_pool_cache.get(date_key)
        if mem_cached is not None:
            return mem_cached

        # 2. SQLite 持久缓存
        from stock_store import load_limit_up_pool, save_limit_up_pool
        db_cached = load_limit_up_pool(date_key)
        if db_cached is not None and not db_cached.empty:
            self._limit_up_pool_cache[date_key] = db_cached
            if self._log:
                self._log(f"涨停池 {date_key} 从本地缓存加载 {len(db_cached)} 只")
            return db_cached

        # 3. 网络请求（涨停池目前仅东财有接口）
        if _eastmoney_circuit_breaker_open():
            if self._log:
                self._log(f"涨停池 {date_key}：东财熔断中，暂无替代数据源。可尝试换 IP 或等待冷却结束。")
            return pd.DataFrame()
        try:
            df = _retry_ak_call(ak.stock_zt_pool_em, date=date_key)
            if df is not None and not df.empty:
                self._limit_up_pool_cache[date_key] = df
                save_limit_up_pool(date_key, df)
                if self._log:
                    self._log(f"涨停池 {date_key} 网络获取 {len(df)} 只，已保存到本地")
                return df
        except Exception as e:
            if self._log:
                self._log(f"涨停池 {date_key} 获取失败: {e}")
        empty = pd.DataFrame()
        self._limit_up_pool_cache[date_key] = empty
        return empty

    def get_previous_limit_up_pool(self, trade_date: str) -> pd.DataFrame:
        """获取指定日期的昨日涨停板池。三级缓存：内存 → SQLite → 网络。"""
        date_key = self._normalize_trade_date(trade_date)
        if not date_key:
            return pd.DataFrame()

        mem_cached = self._prev_limit_up_pool_cache.get(date_key)
        if mem_cached is not None:
            return mem_cached

        from stock_store import load_limit_up_pool, save_limit_up_pool
        db_cached = load_limit_up_pool(date_key, pool_type="previous")
        if db_cached is not None and not db_cached.empty:
            self._prev_limit_up_pool_cache[date_key] = db_cached
            return db_cached

        if _eastmoney_circuit_breaker_open():
            if self._log:
                self._log(f"昨日涨停池 {date_key}：东财熔断中，暂无替代数据源。")
            return pd.DataFrame()
        try:
            df = _retry_ak_call(ak.stock_zt_pool_previous_em, date=date_key)
            if df is not None and not df.empty:
                self._prev_limit_up_pool_cache[date_key] = df
                save_limit_up_pool(date_key, df, pool_type="previous")
                return df
        except Exception as e:
            if self._log:
                self._log(f"昨日涨停池 {date_key} 获取失败: {e}")
        empty = pd.DataFrame()
        self._prev_limit_up_pool_cache[date_key] = empty
        return empty

    def _recent_trade_dates(self, end_date: str, count: int) -> List[str]:
        date_key = self._normalize_trade_date(end_date)
        if not date_key:
            return []
        try:
            cursor = datetime.strptime(date_key, "%Y%m%d").date()
        except ValueError:
            return []

        target = max(1, int(count))
        dates: List[str] = []
        checked = 0
        max_checked = max(target * 7, 20)
        while len(dates) < target and checked < max_checked:
            if cursor.weekday() < 5:
                dates.append(cursor.strftime("%Y%m%d"))
            cursor -= timedelta(days=1)
            checked += 1
        dates.reverse()
        return dates

    def compare_limit_up_pools(
        self,
        today_date: str,
        yesterday_date: str,
    ) -> Dict[str, Any]:
        """对比今日与昨日首次涨停股票的差异。

        返回:
            today_first: 今日首次涨停列表（连板数=1）
            yesterday_first: 昨日首次涨停列表（昨日连板数=1）
            new_codes: 今日新增的首板股票代码（昨日未涨停）
            continued_codes: 昨日首板今日继续涨停的代码
            lost_codes: 昨日首板今日未涨停的代码
            industry_today: 今日首板行业分布
            industry_yesterday: 昨日首板行业分布
            industry_new: 今日新增首板行业分布
            summary: 文字总结
        """
        if self._log:
            self._log(f"正在获取涨停池对比数据: 今日={today_date}, 昨日={yesterday_date}")

        today_pool = self.get_limit_up_pool(today_date)
        prev_pool = self.get_previous_limit_up_pool(today_date)
        yesterday_pool = self.get_limit_up_pool(yesterday_date)

        result: Dict[str, Any] = {
            "today_date": today_date,
            "yesterday_date": yesterday_date,
            "today_pool_count": len(today_pool),
            "yesterday_pool_count": len(yesterday_pool),
            "today_first": [],
            "yesterday_first": [],
            "new_codes": [],
            "continued_codes": [],
            "lost_codes": [],
            "industry_today": {},
            "industry_yesterday": {},
            "industry_new": {},
            "summary": "",
        }

        # ---- 今日首板：连板数=1 的股票 ----
        today_first_df = pd.DataFrame()
        if not today_pool.empty and "连板数" in today_pool.columns:
            today_first_df = today_pool[today_pool["连板数"] == 1].copy()
            result["today_first"] = self._pool_to_records(today_first_df, "today")

        # ---- 昨日首板：从昨日涨停池中取连板数=1 的 ----
        yesterday_first_df = pd.DataFrame()
        if not yesterday_pool.empty and "连板数" in yesterday_pool.columns:
            yesterday_first_df = yesterday_pool[yesterday_pool["连板数"] == 1].copy()
            result["yesterday_first"] = self._pool_to_records(yesterday_first_df, "yesterday")

        # ---- 对比：新增 / 延续 / 流失 ----
        today_codes = set()
        if not today_first_df.empty and "代码" in today_first_df.columns:
            today_codes = set(today_first_df["代码"].astype(str).str.strip().str.zfill(6))

        yesterday_codes = set()
        if not yesterday_first_df.empty and "代码" in yesterday_first_df.columns:
            yesterday_codes = set(yesterday_first_df["代码"].astype(str).str.strip().str.zfill(6))

        # 昨日首板今日继续涨停（不限于首板，包括晋级二板）
        today_all_codes = set()
        if not today_pool.empty and "代码" in today_pool.columns:
            today_all_codes = set(today_pool["代码"].astype(str).str.strip().str.zfill(6))

        result["new_codes"] = sorted(today_codes - yesterday_codes)
        result["continued_codes"] = sorted(yesterday_codes & today_all_codes)
        result["lost_codes"] = sorted(yesterday_codes - today_all_codes)

        # ---- 行业分布 ----
        result["industry_today"] = self._count_industry(today_first_df)
        result["industry_yesterday"] = self._count_industry(yesterday_first_df)
        # 新增首板的行业分布
        if result["new_codes"] and not today_first_df.empty and "代码" in today_first_df.columns:
            new_set = set(result["new_codes"])
            new_df = today_first_df[today_first_df["代码"].astype(str).str.strip().str.zfill(6).isin(new_set)]
            result["industry_new"] = self._count_industry(new_df)

        # ---- 昨日首板今日表现（从 previous pool 取） ----
        yesterday_first_today_perf = []
        if not prev_pool.empty and "代码" in prev_pool.columns and yesterday_codes:
            prev_pool_codes = prev_pool.copy()
            prev_pool_codes["_code"] = prev_pool_codes["代码"].astype(str).str.strip().str.zfill(6)
            match = prev_pool_codes[prev_pool_codes["_code"].isin(yesterday_codes)]
            if not match.empty:
                for row in match.to_dict("records"):
                    code = str(row.get("代码", "") or "").strip().zfill(6)
                    yesterday_first_today_perf.append({
                        "code": code,
                        "name": str(row.get("名称", "") or ""),
                        "change_pct": float(row["涨跌幅"]) if pd.notna(row.get("涨跌幅")) else None,
                        "close": float(row["最新价"]) if pd.notna(row.get("最新价")) else None,
                        "still_limit_up": code in today_all_codes,
                    })
        result["yesterday_first_today_performance"] = yesterday_first_today_perf

        # ---- 文字总结 ----
        lines = []
        lines.append(f"今日({today_date}) 涨停 {result['today_pool_count']} 只，首板 {len(result['today_first'])} 只")
        lines.append(f"昨日({yesterday_date}) 涨停 {result['yesterday_pool_count']} 只，首板 {len(result['yesterday_first'])} 只")
        lines.append(f"今日新增首板: {len(result['new_codes'])} 只")
        lines.append(f"昨日首板今日继续涨停(含晋级): {len(result['continued_codes'])} 只")
        lines.append(f"昨日首板今日未涨停: {len(result['lost_codes'])} 只")
        if result["industry_today"]:
            top3 = sorted(result["industry_today"].items(), key=lambda x: -x[1])[:3]
            lines.append(f"今日首板 TOP3 行业: {'、'.join(f'{k}({v})' for k, v in top3)}")
        if result["industry_yesterday"]:
            top3 = sorted(result["industry_yesterday"].items(), key=lambda x: -x[1])[:3]
            lines.append(f"昨日首板 TOP3 行业: {'、'.join(f'{k}({v})' for k, v in top3)}")
        if yesterday_codes:
            rate = len(result['continued_codes']) / len(yesterday_codes) * 100
            lines.append(f"昨日首板晋级率: {rate:.1f}%")
        result["summary"] = "\n".join(lines)

        if self._log:
            self._log(result["summary"])
        return result

    def compare_limit_up_pools_window(
        self,
        today_date: str,
        compare_days: int = 2,
    ) -> Dict[str, Any]:
        window_days = max(2, int(compare_days or 2))
        trade_dates = self._recent_trade_dates(today_date, window_days)
        if len(trade_dates) < 2:
            fallback_today = self._normalize_trade_date(today_date)
            fallback_prev = self._recent_trade_dates(today_date, 2)
            if len(fallback_prev) >= 2:
                trade_dates = fallback_prev
            elif fallback_today:
                trade_dates = [fallback_today, fallback_today]
            else:
                trade_dates = []
        if len(trade_dates) < 2:
            return {
                "today_date": str(today_date or ""),
                "yesterday_date": "",
                "compare_days": 0,
                "trade_dates": [],
                "daily_stats": [],
                "summary": "未能解析有效交易日范围",
            }

        # get_limit_up_pool 已有三级缓存（内存→SQLite→网络），直接调用即可
        result = self.compare_limit_up_pools(trade_dates[-1], trade_dates[-2])

        daily_stats: List[Dict[str, Any]] = []
        for trade_date in trade_dates:
            pool_df = self.get_limit_up_pool(trade_date)  # 命中缓存，不会重复请求
            first_df = pd.DataFrame()
            if not pool_df.empty and "连板数" in pool_df.columns:
                first_df = pool_df[pool_df["连板数"] == 1].copy()
            industry_top = sorted(self._count_industry(first_df).items(), key=lambda x: -x[1])[:3]
            daily_stats.append({
                "trade_date": trade_date,
                "pool_count": int(len(pool_df)),
                "first_count": int(len(first_df)),
                "top_industries": industry_top,
            })

        first_counts = [item["first_count"] for item in daily_stats]
        avg_first = sum(first_counts) / len(first_counts) if first_counts else 0.0
        max_day = max(daily_stats, key=lambda item: item["first_count"]) if daily_stats else None
        min_day = min(daily_stats, key=lambda item: item["first_count"]) if daily_stats else None
        latest_delta = 0
        if len(daily_stats) >= 2:
            latest_delta = int(daily_stats[-1]["first_count"] - daily_stats[-2]["first_count"])

        summary_lines = [result.get("summary", "")]
        summary_lines.append("")
        summary_lines.append(f"最近 {len(trade_dates)} 个交易日首板概览:")
        for item in daily_stats:
            industries_text = "、".join(f"{name}({count})" for name, count in item["top_industries"]) or "-"
            summary_lines.append(
                f"{item['trade_date']}: 涨停 {item['pool_count']} 只，首板 {item['first_count']} 只，TOP行业 {industries_text}"
            )
        summary_lines.append(f"近{len(trade_dates)}日首板均值: {avg_first:.1f} 只")
        if max_day is not None and min_day is not None:
            summary_lines.append(
                f"首板高点/低点: {max_day['trade_date']} ({max_day['first_count']}只) / "
                f"{min_day['trade_date']} ({min_day['first_count']}只)"
            )
        if len(daily_stats) >= 2:
            sign = "+" if latest_delta > 0 else ""
            summary_lines.append(f"今日较前一交易日首板变化: {sign}{latest_delta} 只")

        result["compare_days"] = len(trade_dates)
        result["trade_dates"] = trade_dates
        result["daily_stats"] = daily_stats
        result["summary"] = "\n".join(line for line in summary_lines if line is not None)
        return result

    def _pool_to_records(self, df: pd.DataFrame, tag: str) -> List[Dict[str, Any]]:
        """将涨停池 DataFrame 转为标准记录列表。

        把 iterrows 换成 `to_dict("records")`：pandas 一次性向量化拷贝成纯 dict，
        循环里只做字段取值/类型转换，CPU 开销比 iterrows 明显低。
        """
        if df.empty:
            return []

        def _opt_float(v: Any) -> Optional[float]:
            return float(v) if pd.notna(v) else None

        def _opt_int(v: Any) -> int:
            return int(v) if pd.notna(v) else 0

        raw_rows = df.to_dict("records")
        records: List[Dict[str, Any]] = []
        for row in raw_rows:
            rec: Dict[str, Any] = {
                "code": str(row.get("代码", "") or "").strip().zfill(6),
                "name": str(row.get("名称", "") or ""),
                "change_pct": _opt_float(row.get("涨跌幅")),
                "close": _opt_float(row.get("最新价")),
                "industry": str(row.get("所属行业", "") or ""),
                "amount": _opt_float(row.get("成交额")),
                "market_cap": _opt_float(row.get("流通市值")),
                "turnover": _opt_float(row.get("换手率")),
            }
            if tag == "today":
                rec["first_board_time"] = str(row.get("首次封板时间", "") or "")
                rec["last_board_time"] = str(row.get("最后封板时间", "") or "")
                rec["break_count"] = _opt_int(row.get("炸板次数"))
                rec["board_amount"] = _opt_float(row.get("封板资金"))
            records.append(rec)
        return records

    @staticmethod
    def _count_industry(df: pd.DataFrame) -> Dict[str, int]:
        if df.empty or "所属行业" not in df.columns:
            return {}
        counts = df["所属行业"].astype(str).value_counts().to_dict()
        return {k: int(v) for k, v in counts.items() if k and k.lower() != "nan"}

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
        source: Optional[str] = None,
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
        plan = self.build_fund_flow_request_plan(source or self._default_fund_flow_source)
        flow_df = None
        last_error: Optional[Exception] = None
        for provider in plan.provider_sequence:
            if provider == "eastmoney":
                try:
                    flow_df = _retry_ak_call(ak.stock_individual_fund_flow, stock=code, market=market)
                    break
                except Exception as e:
                    last_error = e
                    if self._log:
                        self._log(f"个股资金流 {code} 获取失败: {e}")
            elif provider == "ths":
                try:
                    if self._log:
                        self._log(f"个股资金流 {code} 正在使用同花顺源补位。")
                    flow_df = _fetch_ths_fund_flow_frame(code)
                    if flow_df is not None and not flow_df.empty:
                        flow_df = flow_df.copy()
                        today_text = datetime.now().strftime("%Y-%m-%d")
                        if "日期" not in flow_df.columns and "date" not in flow_df.columns and "交易日" not in flow_df.columns:
                            flow_df["日期"] = today_text
                        break
                except Exception as e:
                    last_error = e
                    if self._log:
                        self._log(f"个股资金流 {code} 使用同花顺源失败: {e}")
        if flow_df is None:
            return None
        if flow_df is None or flow_df.empty:
            if last_error is not None and self._log:
                self._log(f"个股资金流 {code} 所有数据源失败: {last_error}")
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
                df[col] = df[col].map(_parse_cn_numeric)
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        _save_fund_flow_store(code, df, keep_rows=max(60, days + 10))
        return df.tail(days).reset_index(drop=True)

    def get_all_stocks(self, force_refresh: bool = False) -> pd.DataFrame:
        if _use_em_full_spot_for_list():
            return self._get_all_stocks_em_spot()
        if os.environ.get("ASHARE_SCAN_REFRESH_UNIVERSE", "").strip().lower() in (
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
                self._log("已开启 ASHARE_SCAN_LIST_SOURCE=em：东方财富分页全表（耗时长，易限流）…")
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
        request_plan: Optional[HistoryRequestPlan] = None,
    ) -> Optional[pd.DataFrame]:
        history_df: Optional[pd.DataFrame] = None
        try:
            stock_code = str(stock_code).strip().zfill(6)
            end_date = datetime.now().strftime('%Y%m%d')
            min_rows = max(1, days)
            if request_plan is None:
                request_plan = self.build_history_request_plan(source=self._default_history_source, force_refresh=False)

            if not force_refresh:
                # ---- 企业级缓存策略：先查 meta 判断新鲜度，再查数据 ----
                if _is_history_cache_fresh(stock_code, min_rows, self._log):
                    history_df = _load_history_store(stock_code, min_rows, end_date, self._log)
                    if history_df is not None and not history_df.empty:
                        _increment_history_diagnostic("cache_hits")
                        return history_df.tail(days).reset_index(drop=True)

                history_df = _load_history_store(stock_code, min_rows, end_date, self._log)
                if history_df is not None and not history_df.empty:
                    _increment_history_diagnostic("cache_hits")
                    if not _should_refresh_today_row(history_df):
                        return history_df.tail(days).reset_index(drop=True)
                    if self._log:
                        self._log(f"历史 {stock_code} 命中当天缓存，但尚未收盘，改为刷新最新日线。")

            eastmoney_only = bool(request_plan.provider_sequence) and all(
                provider == "eastmoney" for provider in request_plan.provider_sequence
            )
            if eastmoney_only and self._log_history_access_suspended():
                if history_df is not None and not history_df.empty:
                    _increment_history_diagnostic("fallback_cache_returns")
                    return history_df.tail(days).reset_index(drop=True)
                return None

            if request_plan is not None and request_plan.cache_only:
                if self._log:
                    self._log(f"历史 {stock_code} 使用扫描上下文 cache-only 策略，本次不访问东方财富。")
                if history_df is not None and not history_df.empty:
                    _increment_history_diagnostic("fallback_cache_returns")
                    return history_df.tail(days).reset_index(drop=True)
                return None

            start_date = (datetime.now() - timedelta(days=days + 15)).strftime('%Y%m%d')
            provider_sequence = list(request_plan.provider_sequence) if request_plan is not None else ["eastmoney"]
            if not provider_sequence:
                provider_sequence = ["eastmoney"]

            _PROVIDER_HOST = {
                "sina": "finance.sina.com.cn",
                "netease": "quotes.money.163.com",
                "baidu": "gushitong.baidu.com",
                "sohu": "q.stock.sohu.com",
                "ths": "d.10jqka.com.cn",
                "wscn": "api-ddc-wscn.awtmt.com",
            }

            last_error: Optional[BaseException] = None
            for provider in provider_sequence:
                # 跳过正在冷却中的源，避免无意义的调用和日志刷屏
                host = _PROVIDER_HOST.get(provider)
                if host and _global_host_on_cooldown(host):
                    last_error = RuntimeError(f"{provider} on cooldown, skipped")
                    continue

                if provider == "eastmoney":
                    if request_plan is not None:
                        raw_mirror_pool = list(request_plan.mirror_urls)
                    else:
                        raw_mirror_pool = mirror_pool if mirror_pool is not None else self.get_available_history_mirrors()
                    selected_mirrors = [x for x in raw_mirror_pool if x]
                    selected_mirrors = _prioritize_history_mirrors(
                        selected_mirrors,
                        preferred_mirror=preferred_mirror,
                    )
                    if not selected_mirrors:
                        last_error = RuntimeError("eastmoney-no-mirror")
                        continue
                    try:
                        df = _history_retry_ak_call(
                            _fetch_eastmoney_hist_frame,
                            stock_code,
                            days,
                            start_date,
                            end_date,
                            selected_mirrors,
                            self._log,
                        )
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用东财源失败，准备切换备用源: {e}")
                        continue
                    df = _normalize_history_frame(df)
                elif provider == "tencent":
                    try:
                        if self._log:
                            self._log(f"历史 {stock_code} 正在使用腾讯源补位。")
                        df = _history_retry_ak_call(
                            _fetch_tencent_hist_frame,
                            stock_code,
                            start_date,
                            end_date,
                        )
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用腾讯源失败，准备切换下一个备用源: {e}")
                        continue
                elif provider == "sina":
                    try:
                        if self._log:
                            self._log(f"历史 {stock_code} 正在使用新浪源补位。")
                        df = _history_retry_ak_call(
                            _fetch_sina_hist_frame,
                            stock_code,
                            start_date,
                            end_date,
                        )
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用新浪源失败: {e}")
                        continue
                elif provider == "netease":
                    try:
                        if self._log:
                            self._log(f"历史 {stock_code} 正在使用网易源补位。")
                        df = _fetch_netease_hist_frame(stock_code, start_date, end_date)
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用网易源失败: {e}")
                        continue
                elif provider == "baidu":
                    try:
                        if self._log:
                            self._log(f"历史 {stock_code} 正在使用百度源补位。")
                        df = _fetch_baidu_hist_frame(stock_code, start_date, end_date)
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用百度源失败: {e}")
                        continue
                elif provider == "sohu":
                    try:
                        if self._log:
                            self._log(f"历史 {stock_code} 正在使用搜狐源补位。")
                        df = _fetch_sohu_hist_frame(stock_code, start_date, end_date)
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用搜狐源失败: {e}")
                        continue
                elif provider == "ths":
                    try:
                        if self._log:
                            self._log(f"历史 {stock_code} 正在使用同花顺源补位。")
                        df = _fetch_ths_hist_frame(stock_code, start_date, end_date)
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用同花顺源失败: {e}")
                        continue
                elif provider == "wscn":
                    try:
                        if self._log:
                            self._log(f"历史 {stock_code} 正在使用华尔街见闻源补位。")
                        df = _fetch_wscn_hist_frame(stock_code, start_date, end_date)
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用华尔街见闻源失败: {e}")
                        continue
                else:
                    last_error = RuntimeError(f"unsupported-history-provider: {provider}")
                    continue

                if df is None or df.empty:
                    last_error = RuntimeError(f"{provider}-empty-history")
                    continue

                _save_history_store(stock_code, df)
                # 保存缓存元数据，用于后续新鲜度判断
                latest_td = ""
                if "date" in df.columns and not df.empty:
                    raw_date = str(df["date"].iloc[-1]).strip()
                    # 统一日期格式为 YYYY-MM-DD，避免后续比较出错
                    try:
                        normalized = raw_date.replace("/", "-").replace(".", "-")
                        if len(normalized) == 8 and normalized.isdigit():
                            latest_td = f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:]}"
                        else:
                            latest_td = normalized
                    except Exception:
                        latest_td = raw_date
                save_history_meta_store(stock_code, latest_td, len(df), source=provider)
                return df.tail(days).reset_index(drop=True)

            if history_df is not None and not history_df.empty:
                if self._log:
                    self._log(f"历史 {stock_code} 全部数据源失败，回退本地缓存: {last_error}")
                _increment_history_diagnostic("fallback_cache_returns")
                return history_df.tail(days).reset_index(drop=True)
            if last_error is not None:
                raise last_error
            return None
        except Exception as e:
            if not isinstance(e, (EastmoneyRateLimitError, HistoryAccessSuspendedError)):
                _increment_history_diagnostic("network_failures")
            if history_df is not None and not history_df.empty:
                if self._log:
                    self._log(f"历史 {stock_code} 刷新失败，回退本地缓存: {e}")
                _increment_history_diagnostic("fallback_cache_returns")
                return history_df.tail(days).reset_index(drop=True)
            if self._log:
                self._log(f"历史 {stock_code} 获取失败: {e}")
            print(f"获取股票 {stock_code} 历史数据失败: {e}")
            return None

    def get_intraday_data(
        self,
        stock_code: str,
        source: Optional[str] = None,
        day_offset: int = 0,
        target_trade_date: str = "",
        include_meta: bool = False,
    ) -> Any:
        import json as _json
        from stock_store import load_intraday_cache, save_intraday_cache

        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return None if not include_meta else _empty_intraday_meta_payload()

        today_str = _today_ymd()

        # ---- 本地缓存命中：过去交易日的分时数据不会再变 ----
        requested_date = str(target_trade_date or "").strip()
        if requested_date and requested_date < today_str:
            cached = load_intraday_cache(code, requested_date)
            if cached and cached.get("data_json"):
                try:
                    rows = _json.loads(cached["data_json"])
                    intraday_df = pd.DataFrame(rows)
                    if not intraday_df.empty and "time" in intraday_df.columns:
                        intraday_df["time"] = pd.to_datetime(intraday_df["time"], errors="coerce")
                        for col in ["open", "close", "high", "low", "volume", "amount", "avg_price"]:
                            if col in intraday_df.columns:
                                intraday_df[col] = pd.to_numeric(intraday_df[col], errors="coerce")
                        auction_snapshot = None
                        auction_raw = cached.get("auction_json", "")
                        if auction_raw:
                            try:
                                auction_snapshot = _json.loads(auction_raw)
                                if auction_snapshot and "time" in auction_snapshot:
                                    auction_snapshot["time"] = pd.to_datetime(auction_snapshot["time"], errors="coerce")
                            except Exception:
                                auction_snapshot = None
                        if self._log:
                            self._log(f"分时 {code} {requested_date} 从本地缓存读取 ({len(intraday_df)} 行)")
                        if include_meta:
                            return {
                                "intraday": intraday_df,
                                "selected_trade_date": requested_date,
                                "available_trade_dates": [requested_date],
                                "applied_day_offset": 0,
                                "auction": auction_snapshot,
                            }
                        return intraday_df
                except Exception:
                    pass  # 缓存损坏，回退网络

        # ---- 网络获取 ----
        raw = None
        auction_snapshot = None
        last_error: Optional[Exception] = None
        plan = self.build_intraday_request_plan(source or self._default_intraday_source)
        for provider in plan.provider_sequence:
            if provider == "eastmoney":
                try:
                    raw = _retry_ak_call(
                        _fetch_eastmoney_intraday_1min,
                        code,
                        ndays=5,
                        logger=self._log,
                    )
                    try:
                        auction_snapshot = _retry_ak_call(
                            _fetch_eastmoney_auction_snapshot,
                            code,
                            logger=self._log,
                        )
                    except Exception as pre_exc:
                        if self._log:
                            self._log(f"分时行情(东财竞价) {code} 获取失败，继续使用常规分时: {pre_exc}")
                except Exception as e:
                    last_error = e
                    if self._log:
                        self._log(f"分时行情(东财) {code} 获取失败: {e}")
            elif provider == "sina":
                try:
                    # 新浪接口要求 symbol 带 sh/sz 前缀（如 sh600519）
                    sina_symbol = _market_prefixed_code(code)
                    raw = _retry_ak_call(ak.stock_zh_a_minute, symbol=sina_symbol, period="1")
                except Exception as e:
                    last_error = e
                    if self._log:
                        self._log(f"分时行情(新浪) {code} 获取失败: {e}")
            if raw is not None and not getattr(raw, "empty", True):
                break

        if raw is None or getattr(raw, "empty", True):
            if self._log and last_error is not None:
                self._log(f"分时行情 {code} 无可用数据: {last_error}")
            return None if not include_meta else _empty_intraday_meta_payload()

        df = _normalize_intraday_source_frame(raw, code, logger=self._log)
        if df.empty:
            return None if not include_meta else _empty_intraday_meta_payload()

        trade_dates = _resolve_intraday_trade_dates(df)
        if not trade_dates:
            return None if not include_meta else _empty_intraday_meta_payload()

        selected_trade_date, applied_offset = _select_intraday_trade_date(
            trade_dates,
            day_offset=day_offset,
            target_trade_date=target_trade_date,
        )
        df = _slice_intraday_frame_by_trade_date(df, selected_trade_date)
        if df.empty:
            return None if not include_meta else _empty_intraday_meta_payload(
                selected_trade_date=selected_trade_date,
                available_trade_dates=trade_dates,
                applied_day_offset=applied_offset,
            )

        if auction_snapshot is not None and str(auction_snapshot.get("trade_date") or "") != selected_trade_date:
            auction_snapshot = None

        intraday_df = df[["time", "open", "close", "high", "low", "volume", "amount", "avg_price"]].copy()

        # ---- 缓存过去交易日的分时数据到本地 ----
        if selected_trade_date and selected_trade_date < today_str and not intraday_df.empty:
            try:
                save_df = intraday_df.copy()
                save_df["time"] = save_df["time"].astype(str)
                data_json = save_df.to_json(orient="records", force_ascii=False)
                auction_json = ""
                if auction_snapshot and isinstance(auction_snapshot, dict):
                    save_auction = dict(auction_snapshot)
                    if "time" in save_auction:
                        save_auction["time"] = str(save_auction["time"])
                    auction_json = _json.dumps(save_auction, ensure_ascii=False, default=str)
                save_intraday_cache(code, selected_trade_date, data_json, auction_json, len(intraday_df))
            except Exception:
                pass  # 缓存写入失败不影响正常流程

        if include_meta:
            payload = _empty_intraday_meta_payload(
                selected_trade_date=selected_trade_date,
                available_trade_dates=trade_dates,
                applied_day_offset=applied_offset,
                auction_snapshot=auction_snapshot,
            )
            payload["intraday"] = intraday_df
            return payload
        return intraday_df
