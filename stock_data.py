from __future__ import annotations

import os
import time
import re
import warnings
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable, TypeVar, Tuple
from datetime import datetime, timedelta

from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

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
from concurrent.futures.thread import _worker, _threads_queues

T = TypeVar("T")


class DaemonThreadPoolExecutor(ThreadPoolExecutor):
    def _adjust_thread_count(self):
        if self._idle_semaphore.acquire(timeout=0):
            return

        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            thread_name = "%s_%d" % (self._thread_name_prefix or self, num_threads)
            t = threading.Thread(
                name=thread_name,
                target=_worker,
                args=(
                    weakref.ref(self, weakref_cb),
                    self._work_queue,
                    self._initializer,
                    self._initargs,
                ),
                daemon=True,
            )
            t.start()
            self._threads.add(t)
            _threads_queues[t] = self._work_queue
def _history_request_concurrency() -> int:
    raw = os.environ.get("GUPPIAO_HISTORY_CONCURRENCY", "").strip()
    try:
        value = int(raw) if raw else 2
    except ValueError:
        value = 2
    return max(1, min(value, 6))


def _history_min_request_interval_sec() -> float:
    raw = os.environ.get("GUPPIAO_HISTORY_MIN_INTERVAL_SEC", "").strip()
    try:
        value = float(raw) if raw else 1.2
    except ValueError:
        value = 1.2
    return max(0.0, min(value, 10.0))


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


def _history_probe_success_target() -> int:
    raw = os.environ.get("GUPPIAO_HISTORY_PROBE_SUCCESS_TARGET", "").strip()
    try:
        value = int(raw) if raw else 2
    except ValueError:
        value = 2
    return max(1, min(value, 4))


def _history_block_window_sec() -> float:
    raw = os.environ.get("GUPPIAO_HISTORY_BLOCK_WINDOW_SEC", "").strip()
    try:
        value = float(raw) if raw else 180.0
    except ValueError:
        value = 180.0
    return max(30.0, min(value, 3600.0))


def _history_block_threshold() -> int:
    raw = os.environ.get("GUPPIAO_HISTORY_BLOCK_THRESHOLD", "").strip()
    try:
        value = int(raw) if raw else 3
    except ValueError:
        value = 3
    return max(1, min(value, 10))


def _history_block_cooldown_sec() -> float:
    raw = os.environ.get("GUPPIAO_HISTORY_BLOCK_COOLDOWN_SEC", "").strip()
    try:
        value = float(raw) if raw else 900.0
    except ValueError:
        value = 900.0
    return max(60.0, min(value, 7200.0))


_HISTORY_REQUEST_SEMAPHORE = threading.BoundedSemaphore(_history_request_concurrency())
_HISTORY_REQUEST_RATE_LOCK = threading.Lock()
_HISTORY_NEXT_REQUEST_AT = 0.0

# ---- 自适应请求间隔 ----
# 连续成功时逐步缩短间隔（加速），遇到限流时立即放大间隔（减速）。
_ADAPTIVE_INTERVAL_LOCK = threading.Lock()
_ADAPTIVE_INTERVAL_SEC = _history_min_request_interval_sec()  # 当前自适应间隔
_ADAPTIVE_SUCCESS_STREAK = 0  # 连续成功计数
_ADAPTIVE_MIN_INTERVAL = 0.3   # 自适应下限（秒）
_ADAPTIVE_MAX_INTERVAL = 8.0   # 自适应上限（秒）


def _adaptive_on_success() -> None:
    """网络请求成功后调用，逐步缩短间隔。"""
    global _ADAPTIVE_INTERVAL_SEC, _ADAPTIVE_SUCCESS_STREAK
    with _ADAPTIVE_INTERVAL_LOCK:
        _ADAPTIVE_SUCCESS_STREAK += 1
        # 每连续成功 10 次，间隔缩短 10%
        if _ADAPTIVE_SUCCESS_STREAK % 10 == 0:
            _ADAPTIVE_INTERVAL_SEC = max(
                _ADAPTIVE_MIN_INTERVAL,
                _ADAPTIVE_INTERVAL_SEC * 0.9,
            )


def _adaptive_on_rate_limit() -> None:
    """遇到限流时调用，立即放大间隔。"""
    global _ADAPTIVE_INTERVAL_SEC, _ADAPTIVE_SUCCESS_STREAK
    with _ADAPTIVE_INTERVAL_LOCK:
        _ADAPTIVE_SUCCESS_STREAK = 0
        # 间隔翻倍（但不超过上限）
        _ADAPTIVE_INTERVAL_SEC = min(
            _ADAPTIVE_MAX_INTERVAL,
            _ADAPTIVE_INTERVAL_SEC * 2.0,
        )


def _adaptive_current_interval() -> float:
    """获取当前自适应间隔。"""
    with _ADAPTIVE_INTERVAL_LOCK:
        return _ADAPTIVE_INTERVAL_SEC
_HISTORY_DIAGNOSTICS_LOCK = threading.Lock()
_HISTORY_DIAGNOSTICS: Dict[str, int] = {
    "cache_hits": 0,
    "network_requests": 0,
    "network_success": 0,
    "network_failures": 0,
    "fallback_cache_returns": 0,
    "rate_limit_events": 0,
    "cooldown_skips": 0,
    "probe_requests": 0,
    "probe_success": 0,
    "probe_failures": 0,
    "probe_cache_hits": 0,
}
_EASTMONEY_HISTORY_MIRRORS = [
    "https://push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://1.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://7.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://28.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://33.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://36.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://40.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://45.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://51.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://58.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://59.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://60.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://62.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://64.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://72.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://81.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://82.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://85.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://90.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://95.push2his.eastmoney.com/api/qt/stock/kline/get",
]
# ---- 全局主机健康管理器 ----
# 所有数据源（eastmoney / tencent / sina）共用同一个健康状态池，
# 任何主机失败一次即进入冷却，冷却期间所有调用方自动跳过该主机。
_GLOBAL_HOST_HEALTH: Dict[str, float] = {}  # host → cooldown_until timestamp
_GLOBAL_HOST_HEALTH_LOCK = threading.Lock()
_GLOBAL_HOST_FAIL_COUNT: Dict[str, int] = {}  # host → consecutive fail count


def _global_host_cooldown_sec() -> float:
    """全局主机冷却时间，默认 1 小时。可通过环境变量 GUPPIAO_HOST_COOLDOWN_SEC 配置。"""
    raw = os.environ.get("GUPPIAO_HOST_COOLDOWN_SEC", "").strip()
    if not raw:
        raw = os.environ.get("GUPPIAO_HISTORY_HOST_COOLDOWN_SEC", "").strip()
    try:
        value = float(raw) if raw else 3600.0
    except ValueError:
        value = 3600.0
    return max(60.0, min(value, 7200.0))


def _global_mark_host_failed(url_or_host: str) -> None:
    """标记主机失败，进入冷却。连续失败次数越多，冷却时间越长。"""
    host = re.sub(r"^https?://", "", str(url_or_host or "").strip()).split("/", 1)[0].lower()
    if not host:
        return
    base_cooldown = _global_host_cooldown_sec()
    with _GLOBAL_HOST_HEALTH_LOCK:
        count = _GLOBAL_HOST_FAIL_COUNT.get(host, 0) + 1
        _GLOBAL_HOST_FAIL_COUNT[host] = count
        # 首次失败: base_cooldown, 第二次: 2x, 第三次+: 3x (上限 3 小时)
        multiplier = min(count, 3)
        cooldown = min(base_cooldown * multiplier, 10800.0)
        _GLOBAL_HOST_HEALTH[host] = time.time() + cooldown


def _global_mark_host_ok(url_or_host: str) -> None:
    """标记主机成功，清除冷却状态和失败计数。"""
    host = re.sub(r"^https?://", "", str(url_or_host or "").strip()).split("/", 1)[0].lower()
    if not host:
        return
    with _GLOBAL_HOST_HEALTH_LOCK:
        _GLOBAL_HOST_HEALTH.pop(host, None)
        _GLOBAL_HOST_FAIL_COUNT.pop(host, None)


def _global_host_on_cooldown(url_or_host: str, now: Optional[float] = None) -> bool:
    """检查主机是否正在冷却中。"""
    host = re.sub(r"^https?://", "", str(url_or_host or "").strip()).split("/", 1)[0].lower()
    if not host:
        return False
    if now is None:
        now = time.time()
    with _GLOBAL_HOST_HEALTH_LOCK:
        cooldown_until = _GLOBAL_HOST_HEALTH.get(host, 0.0)
        if cooldown_until <= now:
            _GLOBAL_HOST_HEALTH.pop(host, None)
            _GLOBAL_HOST_FAIL_COUNT.pop(host, None)
            return False
        return True


def _global_host_cooldown_remaining(url_or_host: str) -> float:
    """返回主机剩余冷却秒数，0 表示不在冷却中。"""
    host = re.sub(r"^https?://", "", str(url_or_host or "").strip()).split("/", 1)[0].lower()
    if not host:
        return 0.0
    now = time.time()
    with _GLOBAL_HOST_HEALTH_LOCK:
        cooldown_until = _GLOBAL_HOST_HEALTH.get(host, 0.0)
        remain = cooldown_until - now
        return max(0.0, remain)


def _global_filter_healthy_urls(urls: List[str]) -> List[str]:
    """从 URL 列表中过滤掉正在冷却的主机。"""
    now = time.time()
    return [u for u in urls if not _global_host_on_cooldown(u, now)]


# 兼容旧接口：将旧的分散变量指向全局管理器
_HISTORY_MIRROR_HEALTH = _GLOBAL_HOST_HEALTH
_HISTORY_MIRROR_HEALTH_LOCK = _GLOBAL_HOST_HEALTH_LOCK
_HISTORY_BLOCK_LOCK = threading.Lock()
_HISTORY_BLOCKED_UNTIL = 0.0
_HISTORY_BLOCK_EVENTS: List[float] = []

# 东方财富接口常校验 Referer / UA；缺省时易被直接断开连接
# User-Agent 池：每次请求随机选取，降低指纹识别风险
import random as _random

_USER_AGENT_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
]

_REFERER_POOL = [
    "https://quote.eastmoney.com/",
    "https://data.eastmoney.com/",
    "https://guba.eastmoney.com/",
    "https://so.eastmoney.com/",
    "https://www.eastmoney.com/",
    "https://finance.eastmoney.com/",
]


def _random_eastmoney_headers() -> Dict[str, str]:
    """每次请求生成随机化的请求头，降低指纹识别风险。"""
    return {
        "User-Agent": _random.choice(_USER_AGENT_POOL),
        "Referer": _random.choice(_REFERER_POOL),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": _random.choice(["zh-CN,zh;q=0.9", "zh-CN,zh;q=0.9,en;q=0.8", "zh-CN,zh;q=0.8,en;q=0.6"]),
        "Accept-Encoding": "gzip, deflate",
        "Connection": "close",
    }


# 兼容旧引用
_EASTMONEY_HEADERS = _random_eastmoney_headers()


# ---- 可选免费代理池 ----
# 启用方式：环境变量 GUPPIAO_USE_PROXY_POOL=1 或项目根目录创建 USE_PROXY_POOL 文件
# 代理池会从多个免费源获取代理列表，验证后轮换使用，降低被封 IP 的风险。
_PROXY_POOL_LOCK = threading.Lock()
_PROXY_POOL: List[str] = []
_PROXY_POOL_REFRESHED_AT: float = 0.0
_PROXY_POOL_REFRESH_INTERVAL = 300.0  # 5 分钟刷新一次
_PROXY_BLACKLIST: Dict[str, float] = {}  # proxy → blacklist_until


def _use_proxy_pool() -> bool:
    if os.environ.get("GUPPIAO_USE_PROXY_POOL", "").strip().lower() in ("1", "true", "yes"):
        return True
    root = _project_root()
    return (root / "USE_PROXY_POOL").is_file() or (root / ".gupiao_proxy_pool").is_file()


def _fetch_free_proxies(logger: Optional[Callable[[str], None]] = None) -> List[str]:
    """从多个免费代理源获取 HTTPS 代理列表。"""
    import requests
    proxies: List[str] = []
    sources = [
        # 免费代理 API（返回纯文本 ip:port）
        ("https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=CN&ssl=yes&anonymity=all", "proxyscrape"),
        ("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", "github-speedx"),
        ("https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", "github-clarketm"),
    ]
    for url, name in sources:
        try:
            resp = requests.get(url, timeout=8, headers={"User-Agent": _random.choice(_USER_AGENT_POOL)})
            if resp.status_code == 200:
                lines = resp.text.strip().splitlines()
                for line in lines:
                    addr = line.strip()
                    if addr and ":" in addr and not addr.startswith("#"):
                        proxies.append(f"http://{addr}")
                if logger and lines:
                    logger(f"代理池: 从 {name} 获取 {len(lines)} 条")
        except Exception as e:
            if logger:
                logger(f"代理池: {name} 获取失败: {e}")
    # 去重
    return list(dict.fromkeys(proxies))


def _validate_proxy(proxy: str, timeout: float = 5.0) -> bool:
    """快速验证代理是否可用（用百度做测试目标）。"""
    import requests
    try:
        resp = requests.get(
            "https://www.baidu.com",
            proxies={"http": proxy, "https": proxy},
            timeout=timeout,
            headers={"User-Agent": _random.choice(_USER_AGENT_POOL)},
        )
        return resp.status_code == 200
    except Exception:
        return False


def _refresh_proxy_pool(logger: Optional[Callable[[str], None]] = None) -> None:
    """刷新代理池：获取 → 随机抽样验证 → 缓存。"""
    global _PROXY_POOL, _PROXY_POOL_REFRESHED_AT
    raw = _fetch_free_proxies(logger)
    if not raw:
        return
    # 随机抽样验证（最多验证 20 个，避免太慢）
    sample = _random.sample(raw, min(20, len(raw)))
    valid: List[str] = []
    for proxy in sample:
        if _validate_proxy(proxy, timeout=4.0):
            valid.append(proxy)
            if len(valid) >= 8:  # 够用即停
                break
    # 将未验证的也加入（标记为低优先级），验证过的放前面
    validated_set = set(valid)
    remaining = [p for p in raw if p not in validated_set]
    with _PROXY_POOL_LOCK:
        _PROXY_POOL = valid + remaining[:50]  # 保留最多 50 + 8 个
        _PROXY_POOL_REFRESHED_AT = time.time()
    if logger:
        logger(f"代理池: 刷新完成，验证可用 {len(valid)} 个，总计 {len(_PROXY_POOL)} 个")


def _get_proxy() -> Optional[str]:
    """获取一个可用代理地址。如果代理池为空或未启用，返回 None。"""
    if not _use_proxy_pool():
        return None
    now = time.time()
    with _PROXY_POOL_LOCK:
        # 自动刷新
        if now - _PROXY_POOL_REFRESHED_AT > _PROXY_POOL_REFRESH_INTERVAL:
            # 在后台线程刷新，避免阻塞
            threading.Thread(target=_refresh_proxy_pool, daemon=True).start()
        pool = [p for p in _PROXY_POOL if _PROXY_BLACKLIST.get(p, 0) <= now]
        if not pool:
            return None
        return _random.choice(pool)


def _blacklist_proxy(proxy: str) -> None:
    """将失败的代理加入黑名单 60 秒。"""
    with _PROXY_POOL_LOCK:
        _PROXY_BLACKLIST[proxy] = time.time() + 60.0


# 拉取全市场列表分页时写入 GUI 日志（由 get_all_stocks 临时注册）
_list_download_log: Optional[Callable[[str], None]] = None


class EastmoneyRateLimitError(RuntimeError):
    pass


class HistoryAccessSuspendedError(RuntimeError):
    pass


def _increment_history_diagnostic(key: str, step: int = 1) -> None:
    with _HISTORY_DIAGNOSTICS_LOCK:
        _HISTORY_DIAGNOSTICS[key] = int(_HISTORY_DIAGNOSTICS.get(key, 0)) + int(step)


def _history_diagnostics_snapshot() -> Dict[str, int]:
    with _HISTORY_DIAGNOSTICS_LOCK:
        return {str(k): int(v) for k, v in _HISTORY_DIAGNOSTICS.items()}


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

    _wait_for_history_request_slot()
    _increment_history_diagnostic("network_requests")

    proxy = _get_proxy()
    with requests.Session() as session:
        if proxy:
            session.proxies = {"http": proxy, "https": proxy}
        elif _use_bypass_proxy():
            session.trust_env = False
            session.proxies = {"http": None, "https": None}
        # 每次请求使用随机化请求头，降低被识别为机器人的风险
        req_kw: Dict[str, Any] = {
            "url": url,
            "params": params,
            "timeout": timeout,
            "headers": _random_eastmoney_headers(),
        }
        if _use_insecure_ssl():
            req_kw["verify"] = False
        try:
            response = session.get(**req_kw)
        except Exception:
            if proxy:
                _blacklist_proxy(proxy)
            raise
        response_text = response.text or ""
        if _looks_like_eastmoney_rate_limit(response.status_code, response_text):
            _adaptive_on_rate_limit()
            _increment_history_diagnostic("rate_limit_events")
            _increment_history_diagnostic("network_failures")
            message = _record_history_block(
                f"东方财富返回 {response.status_code}，疑似触发限流或封禁"
            )
            raise EastmoneyRateLimitError(message)
        response.raise_for_status()
        try:
            data_json = response.json()
        except ValueError as exc:
            if _looks_like_eastmoney_rate_limit(response.status_code, response_text):
                _adaptive_on_rate_limit()
                _increment_history_diagnostic("rate_limit_events")
                _increment_history_diagnostic("network_failures")
                message = _record_history_block("东方财富返回非 JSON 内容，疑似触发限流或封禁")
                raise EastmoneyRateLimitError(message) from exc
            _increment_history_diagnostic("network_failures")
            raise
        if _eastmoney_json_indicates_rate_limit(data_json):
            _adaptive_on_rate_limit()
            _increment_history_diagnostic("rate_limit_events")
            _increment_history_diagnostic("network_failures")
            message = _record_history_block("东方财富返回限流提示，进入冷却保护")
            raise EastmoneyRateLimitError(message)
        _adaptive_on_success()
        _increment_history_diagnostic("network_success")
        return data_json


def _fetch_eastmoney_auction_snapshot(
    stock_code: str,
    logger: Optional[Callable[[str], None]] = None,
) -> Optional[Dict[str, Any]]:
    market_code = 1 if str(stock_code).startswith("6") else 0
    url = "https://push2.eastmoney.com/api/qt/stock/trends2/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ndays": "1",
        "iscr": "1",
        "iscca": "1",
        "secid": f"{market_code}.{stock_code}",
    }
    # 竞价信息只作为分时页辅助标记，不能污染主分钟序列。
    import requests
    try:
        req_kw = {
            "url": url,
            "params": params,
            "timeout": 8.0,
            "headers": _random_eastmoney_headers(),
        }
        if _use_insecure_ssl():
            req_kw["verify"] = False
        with requests.Session() as session:
            if _use_bypass_proxy():
                session.trust_env = False
            r = session.get(**req_kw)
            r.raise_for_status()
            data_json = r.json()
    except Exception as exc:
        if logger:
            logger(f"竞价数据网络请求失败 {stock_code}: {exc}")
        return None
    if not data_json or not data_json.get("data"):
        return None

    trends = (data_json.get("data") or {}).get("trends") or []
    if not trends:
        return None
    temp_df = pd.DataFrame([str(item).split(",") for item in trends])
    if temp_df.empty:
        return None

    available_cols = ["时间", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "均价"]
    temp_df.columns = available_cols[:len(temp_df.columns)]
    if "时间" not in temp_df.columns:
        return None

    temp_df["时间"] = pd.to_datetime(temp_df["时间"], errors="coerce")
    temp_df = temp_df.dropna(subset=["时间"]).sort_values("时间").reset_index(drop=True)
    if temp_df.empty:
        return None

    numeric_cols = [c for c in temp_df.columns if c != "时间"]
    for col in numeric_cols:
        temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")

    auction_rows = temp_df[temp_df["时间"].dt.strftime("%H:%M") == "09:25"].reset_index(drop=True)
    if auction_rows.empty:
        return None

    row = auction_rows.iloc[-1]
    price_candidates = [
        row.get("收盘"),
        row.get("开盘"),
        row.get("均价"),
        row.get("最高"),
        row.get("最低"),
    ]
    auction_price = next(
        (
            float(value)
            for value in price_candidates
            if pd.notna(value) and float(value) > 0
        ),
        None,
    )
    if auction_price is None:
        return None

    amount = row.get("成交额")
    volume = row.get("成交量")
    avg_price = row.get("均价")
    return {
        "trade_date": row["时间"].date().isoformat(),
        "time": row["时间"],
        "price": auction_price,
        "open": float(row["开盘"]) if pd.notna(row.get("开盘")) else None,
        "high": float(row["最高"]) if pd.notna(row.get("最高")) else None,
        "low": float(row["最低"]) if pd.notna(row.get("最低")) else None,
        "avg_price": float(avg_price) if pd.notna(avg_price) and float(avg_price) > 0 else None,
        "volume": float(volume) if pd.notna(volume) and float(volume) > 0 else None,
        "amount": float(amount) if pd.notna(amount) and float(amount) > 0 else None,
    }


def _empty_intraday_meta_payload(
    selected_trade_date: str = "",
    available_trade_dates: Optional[List[str]] = None,
    applied_day_offset: int = 0,
    auction_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "intraday": None,
        "selected_trade_date": str(selected_trade_date or "").strip(),
        "available_trade_dates": [str(item).strip() for item in (available_trade_dates or []) if str(item).strip()],
        "applied_day_offset": int(applied_day_offset),
        "auction": auction_snapshot if isinstance(auction_snapshot, dict) else None,
    }


def _normalize_intraday_source_frame(
    raw_frame: "pd.DataFrame",
    stock_code: str,
    logger: Optional[Callable[[str], None]] = None,
) -> "pd.DataFrame":
    if raw_frame is None or getattr(raw_frame, "empty", True):
        return pd.DataFrame()

    source_columns = [str(col) for col in raw_frame.columns.tolist()]
    rename_map: Dict[str, str] = {}
    time_col = _first_existing_column(source_columns, ["时间", "日期时间", "datetime", "time"])
    open_col = _first_existing_column(source_columns, ["开盘", "open"])
    close_col = _first_existing_column(source_columns, ["收盘", "close", "最新价"])
    high_col = _first_existing_column(source_columns, ["最高", "high"])
    low_col = _first_existing_column(source_columns, ["最低", "low"])
    volume_col = _first_existing_column(source_columns, ["成交量", "volume"])
    amount_col = _first_existing_column(source_columns, ["成交额", "amount"])
    avg_price_col = _first_existing_column(source_columns, ["均价", "avg_price"])

    for src, dst in [
        (time_col, "time"),
        (open_col, "open"),
        (close_col, "close"),
        (high_col, "high"),
        (low_col, "low"),
        (volume_col, "volume"),
        (amount_col, "amount"),
        (avg_price_col, "avg_price"),
    ]:
        if src:
            rename_map[src] = dst

    normalized = raw_frame.rename(columns=rename_map).copy()
    if "time" not in normalized.columns:
        if logger:
            logger(f"分时行情 {stock_code} 缺少时间列，返回列: {', '.join(source_columns)}")
        return pd.DataFrame()

    normalized["time"] = pd.to_datetime(normalized["time"], errors="coerce")
    normalized = normalized.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    if normalized.empty:
        return pd.DataFrame()

    for col in ["open", "close", "high", "low", "volume", "amount", "avg_price"]:
        if col in normalized.columns:
            normalized[col] = pd.to_numeric(normalized[col], errors="coerce")
        else:
            normalized[col] = None

    # ---- 过滤竞价时段数据，避免与竞价标记重叠 ----
    # 东方财富分时接口可能返回 09:25~09:29 的竞价撮合数据，
    # 这些数据会和独立获取的竞价快照在图表上产生时间重叠。
    # 正式连续竞价从 09:30 开始，午盘从 13:00 开始；
    # 仅保留 [09:30, 11:30] ∪ [13:00, 15:00] 的有效交易分钟。
    hhmm = normalized["time"].dt.strftime("%H:%M")
    in_morning = (hhmm >= "09:30") & (hhmm <= "11:30")
    in_afternoon = (hhmm >= "13:00") & (hhmm <= "15:00")
    before_filter_len = len(normalized)
    normalized = normalized[in_morning | in_afternoon].reset_index(drop=True)
    if normalized.empty:
        return pd.DataFrame()
    filtered_count = before_filter_len - len(normalized)
    if filtered_count > 0 and logger:
        logger(f"分时行情 {stock_code} 过滤 {filtered_count} 条非交易时段数据（竞价/午休）")

    return normalized[["time", "open", "close", "high", "low", "volume", "amount", "avg_price"]]


def _resolve_intraday_trade_dates(df: "pd.DataFrame") -> List[str]:
    if df is None or df.empty or "time" not in df.columns:
        return []
    return sorted({d.isoformat() for d in df["time"].dt.date.dropna().tolist()})


def _select_intraday_trade_date(
    trade_dates: List[str],
    day_offset: int = 0,
    target_trade_date: str = "",
) -> Tuple[str, int]:
    if not trade_dates:
        return "", 0

    normalized_target = str(target_trade_date or "").strip()
    if normalized_target:
        selected_trade_date = ""
        if normalized_target in trade_dates:
            selected_trade_date = normalized_target
        else:
            for candidate in reversed(trade_dates):
                if candidate <= normalized_target:
                    selected_trade_date = candidate
                    break
            if not selected_trade_date:
                selected_trade_date = trade_dates[0]
        selected_index = trade_dates.index(selected_trade_date)
        return selected_trade_date, selected_index - (len(trade_dates) - 1)

    try:
        request_offset = int(day_offset)
    except (TypeError, ValueError):
        request_offset = 0
    max_back = len(trade_dates) - 1
    applied_offset = max(-max_back, min(request_offset, 0))
    selected_index = len(trade_dates) - 1 + applied_offset
    return trade_dates[selected_index], applied_offset


def _slice_intraday_frame_by_trade_date(df: "pd.DataFrame", selected_trade_date: str) -> "pd.DataFrame":
    if df is None or df.empty or "time" not in df.columns or not selected_trade_date:
        return pd.DataFrame()
    target_date = pd.to_datetime(selected_trade_date, errors="coerce").date()
    return df[df["time"].dt.date == target_date].reset_index(drop=True)


def _looks_like_eastmoney_rate_limit(status_code: int, response_text: str) -> bool:
    if int(status_code or 0) in (403, 418, 429, 451, 503):
        return True
    sample = str(response_text or "")[:400].lower()
    if not sample:
        return False
    tokens = (
        "访问过于频繁",
        "访问频繁",
        "请求过于频繁",
        "频繁",
        "forbidden",
        "access denied",
        "too many requests",
        "rate limit",
        "风控",
        "验证码",
    )
    return any(token in sample for token in tokens)


def _eastmoney_json_indicates_rate_limit(data_json: Any) -> bool:
    if not isinstance(data_json, dict):
        return False
    texts: List[str] = []
    for key in ("message", "msg", "rc", "rt", "code", "result", "reason"):
        value = data_json.get(key)
        if value is not None:
            texts.append(str(value))
    data_part = data_json.get("data")
    if isinstance(data_part, dict):
        for key in ("message", "msg", "tip", "reason"):
            value = data_part.get(key)
            if value is not None:
                texts.append(str(value))
    return any(_looks_like_eastmoney_rate_limit(200, text) for text in texts)


def _history_access_blocked_until() -> float:
    now = time.time()
    with _HISTORY_BLOCK_LOCK:
        global _HISTORY_BLOCKED_UNTIL
        if _HISTORY_BLOCKED_UNTIL <= now:
            _HISTORY_BLOCKED_UNTIL = 0.0
            _HISTORY_BLOCK_EVENTS.clear()
            return 0.0
        return _HISTORY_BLOCKED_UNTIL


def _record_history_block(reason: str) -> str:
    now = time.time()
    window_start = now - _history_block_window_sec()
    with _HISTORY_BLOCK_LOCK:
        global _HISTORY_BLOCKED_UNTIL
        _HISTORY_BLOCK_EVENTS[:] = [ts for ts in _HISTORY_BLOCK_EVENTS if ts >= window_start]
        _HISTORY_BLOCK_EVENTS.append(now)
        if len(_HISTORY_BLOCK_EVENTS) >= _history_block_threshold():
            _HISTORY_BLOCKED_UNTIL = max(_HISTORY_BLOCKED_UNTIL, now + _history_block_cooldown_sec())
        blocked_until = _HISTORY_BLOCKED_UNTIL
    if blocked_until > now:
        remain = max(1, int(blocked_until - now))
        return f"{reason}；已暂停新的东方财富历史请求，约 {remain}s 后再试"
    return reason


def _wait_for_history_request_slot() -> None:
    # 使用自适应间隔：连续成功则加速，遇到限流则减速
    min_interval = _adaptive_current_interval()
    while True:
        blocked_until = _history_access_blocked_until()
        now = time.time()
        if blocked_until > now:
            remain = max(1, int(blocked_until - now))
            _increment_history_diagnostic("cooldown_skips")
            raise HistoryAccessSuspendedError(
                f"东方财富历史接口正在冷却保护中，约 {remain}s 后恢复"
            )
        with _HISTORY_REQUEST_RATE_LOCK:
            global _HISTORY_NEXT_REQUEST_AT
            wait_sec = _HISTORY_NEXT_REQUEST_AT - now
            if wait_sec <= 0:
                _HISTORY_NEXT_REQUEST_AT = now + min_interval
                return
        time.sleep(min(wait_sec, 0.5))


def _history_mirror_host(url: str) -> str:
    text = re.sub(r"^https?://", "", str(url or "").strip())
    return text.split("/", 1)[0]


def _mark_history_mirror_failed(url: str) -> None:
    _global_mark_host_failed(url)


def _mark_history_mirror_ok(url: str) -> None:
    _global_mark_host_ok(url)


def _history_mirror_on_cooldown(url: str, now: Optional[float] = None) -> bool:
    return _global_host_on_cooldown(url, now)


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


def _normalize_history_frame(df: "pd.DataFrame") -> "pd.DataFrame":
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    rename_map = {
        "日期": "date",
        "时间": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "振幅": "amplitude",
        "涨跌幅": "change_pct",
        "涨跌额": "change_amount",
        "换手率": "turnover_rate",
    }
    out = out.rename(columns=rename_map)
    if "date" not in out.columns:
        return pd.DataFrame()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype(str)
    for col in ("open", "close", "high", "low", "volume", "amount", "amplitude", "change_pct", "change_amount", "turnover_rate"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "close" not in out.columns:
        return pd.DataFrame()
    close_series = pd.to_numeric(out["close"], errors="coerce")
    prev_close = close_series.shift(1)
    if "change_amount" not in out.columns:
        out["change_amount"] = close_series - prev_close
    if "change_pct" not in out.columns:
        out["change_pct"] = ((close_series / prev_close) - 1.0) * 100.0
    if "volume" not in out.columns and "amount" in out.columns:
        out["volume"] = pd.to_numeric(out["amount"], errors="coerce")
    if "amount" not in out.columns:
        out["amount"] = pd.Series([None] * len(out), dtype="float64")
    if "amplitude" not in out.columns and {"high", "low", "close"} <= set(out.columns):
        base_close = prev_close.where(prev_close.notna() & (prev_close != 0), close_series)
        out["amplitude"] = ((pd.to_numeric(out["high"], errors="coerce") - pd.to_numeric(out["low"], errors="coerce")) / base_close) * 100.0
    if "turnover_rate" not in out.columns:
        out["turnover_rate"] = pd.Series([None] * len(out), dtype="float64")
    keep_cols = [
        "date",
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
    ]
    return out[keep_cols].dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)


# ---- 腾讯证券镜像池 ----
_TENCENT_HISTORY_MIRRORS = [
    "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get",
    "https://web.ifzqgtimg.cn/appstock/app/fqkline/get",
]
# 腾讯镜像健康状态：委托给全局主机管理器

def _mark_tencent_mirror_failed(url: str) -> None:
    _global_mark_host_failed(url)


def _mark_tencent_mirror_ok(url: str) -> None:
    _global_mark_host_ok(url)


def _get_healthy_tencent_mirrors() -> List[str]:
    healthy = _global_filter_healthy_urls(_TENCENT_HISTORY_MIRRORS)
    return healthy if healthy else list(_TENCENT_HISTORY_MIRRORS)


def _fetch_tencent_hist_direct(
    stock_code: str,
    start_date: str,
    end_date: str,
    log: Optional[Callable[[str], None]] = None,
) -> "pd.DataFrame":
    """直接抓腾讯证券历史日线，带镜像轮换和 UA 随机化。"""
    import requests
    from akshare.utils import demjson

    symbol = _market_prefixed_code(stock_code)
    range_start = max(int(start_date[:4]), 2000)
    range_end = int(end_date[:4]) + 1

    mirrors = _get_healthy_tencent_mirrors()
    big_df = pd.DataFrame()

    for year in range(range_start, range_end):
        params = {
            "_var": f"kline_day{year}",
            "param": f"{symbol},day,{year}-01-01,{year + 1}-12-31,640,",
            "r": f"0.{_random.randint(1000000000, 9999999999)}",
        }
        last_error = None
        for mirror_url in mirrors:
            try:
                time.sleep(_random.uniform(0.1, 0.4))  # 小随机延时
                resp = requests.get(
                    mirror_url,
                    params=params,
                    timeout=(5, 10),
                    headers={
                        "User-Agent": _random.choice(_USER_AGENT_POOL),
                        "Referer": "https://gu.qq.com/",
                    },
                )
                if resp.status_code != 200:
                    _mark_tencent_mirror_failed(mirror_url)
                    last_error = RuntimeError(f"tencent HTTP {resp.status_code}")
                    continue
                data_text = resp.text
                idx = data_text.find("={")
                if idx < 0:
                    _mark_tencent_mirror_failed(mirror_url)
                    last_error = RuntimeError("tencent: bad response format")
                    continue
                data_json = demjson.decode(data_text[idx + 1:])["data"][symbol]
                if "day" in data_json:
                    temp_df = pd.DataFrame(data_json["day"])
                else:
                    temp_df = pd.DataFrame()
                if not temp_df.empty:
                    big_df = pd.concat([big_df, temp_df], ignore_index=True)
                _mark_tencent_mirror_ok(mirror_url)
                break
            except Exception as e:
                last_error = e
                _mark_tencent_mirror_failed(mirror_url)
                if log:
                    host = re.sub(r"^https?://", "", mirror_url).split("/", 1)[0]
                    log(f"腾讯 {stock_code} 镜像 {host} 年份 {year} 失败: {e}")
        # 单个年份全部镜像失败，继续下一年

    if big_df.empty:
        return pd.DataFrame()

    big_df = big_df.iloc[:, :6]
    big_df.columns = ["date", "open", "close", "high", "low", "amount"]
    for col in ["open", "close", "high", "low", "amount"]:
        big_df[col] = pd.to_numeric(big_df[col], errors="coerce")
    big_df["date"] = pd.to_datetime(big_df["date"], errors="coerce").dt.date.astype(str)
    big_df = big_df.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
    return _normalize_history_frame(big_df)


def _fetch_tencent_hist_frame(stock_code: str, start_date: str, end_date: str) -> "pd.DataFrame":
    """腾讯历史日线：优先使用自建直连（带镜像轮换），失败回退 akshare。"""
    try:
        df = _fetch_tencent_hist_direct(stock_code, start_date, end_date)
        if df is not None and not df.empty:
            return df
    except Exception:
        pass
    # 回退到 akshare
    symbol = _market_prefixed_code(stock_code)
    df = _retry_ak_call(
        ak.stock_zh_a_hist_tx,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        adjust="",
    )
    return _normalize_history_frame(df)


# ---- 新浪财经反封保护 ----
_SINA_HISTORY_MIRRORS = [
    "https://finance.sina.com.cn",
    "https://hq.sinajs.cn",
]
_SINA_REQUEST_LOCK = threading.Lock()
_SINA_NEXT_REQUEST_AT = 0.0
_SINA_MIN_INTERVAL = 1.5  # 新浪对频率更敏感


def _sina_throttle() -> None:
    """新浪专用节流阀，确保请求间隔。"""
    global _SINA_NEXT_REQUEST_AT
    while True:
        with _SINA_REQUEST_LOCK:
            now = time.time()
            wait = _SINA_NEXT_REQUEST_AT - now
            if wait <= 0:
                _SINA_NEXT_REQUEST_AT = now + _SINA_MIN_INTERVAL
                return
        time.sleep(min(wait, 0.5))


def _fetch_sina_hist_frame(stock_code: str, start_date: str, end_date: str) -> "pd.DataFrame":
    """新浪历史日线：带 UA 随机化 + 独立节流 + 重试 + 全局主机健康管理。"""
    # 如果新浪主机正在冷却中，直接跳过
    if _global_host_on_cooldown("finance.sina.com.cn"):
        remain = _global_host_cooldown_remaining("finance.sina.com.cn")
        raise RuntimeError(f"sina host on cooldown ({int(remain)}s remaining)")

    symbol = _market_prefixed_code(stock_code)
    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            _sina_throttle()
            import requests
            old_get = requests.get

            def _patched_get(url, **kwargs):
                if "sina.com.cn" in str(url) or "sinajs.cn" in str(url):
                    kwargs.setdefault("headers", {})
                    kwargs["headers"]["User-Agent"] = _random.choice(_USER_AGENT_POOL)
                    kwargs["headers"]["Referer"] = "https://finance.sina.com.cn/"
                return old_get(url, **kwargs)

            requests.get = _patched_get
            try:
                df = ak.stock_zh_a_daily(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="",
                )
            finally:
                requests.get = old_get

            _global_mark_host_ok("finance.sina.com.cn")
            return _normalize_history_frame(df)
        except Exception as e:
            last_error = e
            time.sleep(1.5 * (attempt + 1) + _random.uniform(0.3, 1.0))

    # 全部重试失败 → 标记新浪主机进入冷却
    _global_mark_host_failed("finance.sina.com.cn")
    if last_error is not None:
        raise last_error
    return pd.DataFrame()


def _probe_history_mirror(url: str) -> Tuple[bool, str]:
    probe_code = "000001"
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=40)).strftime("%Y%m%d")
    params = _eastmoney_history_request_params(probe_code, start_date, end_date)
    _increment_history_diagnostic("probe_requests")
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
        _increment_history_diagnostic("probe_success")
        return True, latest_date
    except HistoryAccessSuspendedError as e:
        _mark_history_mirror_failed(url)
        _increment_history_diagnostic("probe_failures")
        return False, str(e)
    except EastmoneyRateLimitError as e:
        _mark_history_mirror_failed(url)
        _increment_history_diagnostic("probe_failures")
        return False, str(e)
    except Exception as e:
        _mark_history_mirror_failed(url)
        _increment_history_diagnostic("probe_failures")
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
        except HistoryAccessSuspendedError as e:
            last_exception = e
            if log:
                log(f"历史 {stock_code} 暂停访问东方财富：{e}")
            break
        except EastmoneyRateLimitError as e:
            last_exception = e
            _mark_history_mirror_failed(base_url)
            if log:
                log(f"历史 {stock_code} 触发东方财富限流保护：{e}")
            break
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


def _parse_cn_numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None
    text = str(value).strip().replace(",", "")
    if not text or text.lower() == "nan":
        return None
    multiplier = 1.0
    if text.endswith("%"):
        text = text[:-1]
    if text.endswith("亿"):
        multiplier = 1e8
        text = text[:-1]
    elif text.endswith("万"):
        multiplier = 1e4
        text = text[:-1]
    try:
        return float(text) * multiplier
    except Exception:
        return None


def _fetch_ths_fund_flow_frame(stock_code: str):
    candidates = (
        ("stock_fund_flow_individual", ("symbol",)),
        ("stock_fund_flow_individual", ("stock",)),
        ("stock_individual_fund_flow_ths", ("symbol",)),
        ("stock_individual_fund_flow_ths", ("stock",)),
    )
    last_error: Optional[Exception] = None
    for func_name, arg_names in candidates:
        fn = getattr(ak, func_name, None)
        if fn is None:
            continue
        kwargs = {}
        for arg_name in arg_names:
            kwargs[arg_name] = stock_code
        try:
            df = _retry_ak_call(fn, **kwargs)
            if df is not None and not getattr(df, "empty", True):
                return df
        except Exception as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    raise RuntimeError("ths-fund-flow-function-unavailable")

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


def _save_history_store(stock_code: str, df: pd.DataFrame, keep_rows: int = 0) -> None:
    """保存历史数据到本地 SQLite。
    keep_rows=0 表示保存全部行（企业级策略：不截断，保证任意天数查询都能命中缓存）。
    保存前会做 OHLC 数据校验，有异常记录到日志但不阻止保存。
    """
    if df is None or df.empty:
        return
    if "date" not in df.columns:
        return
    out = df.copy()
    out["date"] = out["date"].astype(str).str.strip()
    out = out.sort_values("date").reset_index(drop=True)
    if keep_rows > 0:
        out = out.tail(max(keep_rows, 10)).reset_index(drop=True)

    # 数据校验（只记日志不阻止保存）
    try:
        from stock_validator import validate_ohlc, validate_change_pct
        ohlc_issues = validate_ohlc(out, stock_code)
        pct_issues = validate_change_pct(out, stock_code=stock_code)
        if ohlc_issues:
            logger.warning("%s 保存前检测到 %d 条 OHLC 异常", stock_code, len(ohlc_issues))
        if pct_issues:
            logger.warning("%s 保存前检测到 %d 条涨跌幅异常", stock_code, len(pct_issues))
    except Exception:
        pass

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
        "7.push2.eastmoney.com",
        "28.push2.eastmoney.com",
        "33.push2.eastmoney.com",
        "36.push2.eastmoney.com",
        "40.push2.eastmoney.com",
        "45.push2.eastmoney.com",
        "51.push2.eastmoney.com",
        "58.push2.eastmoney.com",
        "59.push2.eastmoney.com",
        "60.push2.eastmoney.com",
        "62.push2.eastmoney.com",
        "64.push2.eastmoney.com",
        "72.push2.eastmoney.com",
        "81.push2.eastmoney.com",
        "82.push2.eastmoney.com",
        "85.push2.eastmoney.com",
        "90.push2.eastmoney.com",
        "95.push2.eastmoney.com",
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


def _market_prefixed_code(code: str) -> str:
    norm = str(code or "").strip().zfill(6)
    return f"{_infer_market(norm)}{norm}"


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


def _estimate_last_trade_date() -> str:
    """估算最近一个交易日（不考虑节假日，仅排除周末）。
    周一~周五 15:30 前返回上一个交易日，15:30 后返回当天。
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


def _is_history_cache_fresh(
    stock_code: str,
    min_rows: int,
    log: Optional[Callable[[str], None]] = None,
) -> bool:
    """判断本地历史缓存是否足够新鲜，可以跳过网络请求。

    策略：
    1. 读取 history_meta 表中的 refreshed_at 和 latest_trade_date
    2. 如果 latest_trade_date >= 估算的最近交易日，并且 row_count >= min_rows → 新鲜
    3. 如果 refreshed_at 在今天 15:30 之后 → 新鲜（当天收盘后已刷新过）
    """
    meta = load_history_meta_store(stock_code)
    if meta is None:
        return False
    latest_td = str(meta.get("latest_trade_date") or "").strip()
    row_count = int(meta.get("row_count") or 0)
    refreshed_at = str(meta.get("refreshed_at") or "").strip()
    if not latest_td or row_count < min_rows:
        return False

    estimated_last_td = _estimate_last_trade_date()

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

        # 东方财富：每个健康镜像作为独立通道
        if normalized in ("auto", "eastmoney"):
            mirrors = self.get_available_history_mirrors()
            for mirror in mirrors:
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("eastmoney",),
                    mirror_urls=(mirror,),
                    reason=f"multi-source-eastmoney-{_history_mirror_host(mirror)}",
                ))

        # 腾讯/新浪：作为补充分流通道（跳过正在冷却的源）
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
        # 将可用数据源构建为多个 plan，按轮转方式分配给不同股票，
        # 避免所有请求打到同一个源上导致封 IP。
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
        # 多源模式下适当提高并发：通道数 × 基础并发（但受环境变量上限约束）
        if plan_count > 1:
            worker_count = max(worker_count, min(plan_count + 1, 6))

        updated = 0
        failed = 0
        skipped = 0

        def _work(item: Dict[str, Any], assigned_plan: HistoryRequestPlan) -> tuple[str, str, bool, bool]:
            """返回 (code, name, success, skipped)"""
            code = str(item.get("code", "")).strip().zfill(6)
            name = str(item.get("name", "") or "")
            if should_stop and should_stop():
                return code, name, False, True
            # 智能刷新：如果本地缓存已经足够新鲜，跳过网络请求
            if _is_history_cache_fresh(code, max(1, days), self._log):
                return code, name, True, True
            df = self.get_history_data(
                code,
                days=days,
                force_refresh=True,
                request_plan=assigned_plan,
            )
            return code, name, bool(df is not None and not df.empty), False

        with DaemonThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="hist-cache") as executor:
            # 轮转分配 plan 给每只股票
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
        return DataProviderPlan(mode="network", provider_sequence=("eastmoney", "sina"), reason="intraday-provider=auto")

    def build_fund_flow_request_plan(self, source: str = "auto") -> DataProviderPlan:
        normalized = self.normalize_fund_flow_source(source)
        if normalized == "eastmoney":
            return DataProviderPlan(mode="network", provider_sequence=("eastmoney",), reason="fund-flow-provider=eastmoney")
        if normalized == "ths":
            return DataProviderPlan(mode="network", provider_sequence=("ths",), reason="fund-flow-provider=ths")
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

        if mirrors:
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("eastmoney", "tencent", "sina"),
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
            provider_sequence=("tencent", "sina"),
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
        """获取指定日期的涨停板池（东方财富）。"""
        date_key = self._normalize_trade_date(trade_date)
        if not date_key:
            return pd.DataFrame()
        try:
            df = _retry_ak_call(ak.stock_zt_pool_em, date=date_key)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            if self._log:
                self._log(f"涨停池 {date_key} 获取失败: {e}")
        return pd.DataFrame()

    def get_previous_limit_up_pool(self, trade_date: str) -> pd.DataFrame:
        """获取指定日期的昨日涨停板池（东方财富），即昨日涨停股今日表现。"""
        date_key = self._normalize_trade_date(trade_date)
        if not date_key:
            return pd.DataFrame()
        try:
            df = _retry_ak_call(ak.stock_zt_pool_previous_em, date=date_key)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            if self._log:
                self._log(f"昨日涨停池 {date_key} 获取失败: {e}")
        return pd.DataFrame()

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
                for _, row in match.iterrows():
                    code = str(row.get("代码", "")).strip().zfill(6)
                    yesterday_first_today_perf.append({
                        "code": code,
                        "name": str(row.get("名称", "")),
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

        # 先批量获取所有日期的涨停池到缓存，避免 compare_limit_up_pools 内部重复请求
        pool_cache: Dict[str, pd.DataFrame] = {}
        for td in trade_dates:
            if td not in pool_cache:
                pool_cache[td] = self.get_limit_up_pool(td)
        # 临时 patch get_limit_up_pool 让 compare_limit_up_pools 复用缓存
        _orig_get_pool = self.get_limit_up_pool
        self.get_limit_up_pool = lambda date_str: pool_cache.get(  # type: ignore[assignment]
            self._normalize_trade_date(date_str), _orig_get_pool(date_str)
        )
        try:
            result = self.compare_limit_up_pools(trade_dates[-1], trade_dates[-2])
        finally:
            self.get_limit_up_pool = _orig_get_pool  # type: ignore[assignment]

        daily_stats: List[Dict[str, Any]] = []
        for trade_date in trade_dates:
            pool_df = pool_cache.get(trade_date, pd.DataFrame())
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
        """将涨停池 DataFrame 转为标准记录列表。"""
        records = []
        if df.empty:
            return records
        for _, row in df.iterrows():
            rec: Dict[str, Any] = {
                "code": str(row.get("代码", "")).strip().zfill(6),
                "name": str(row.get("名称", "")),
                "change_pct": float(row["涨跌幅"]) if pd.notna(row.get("涨跌幅")) else None,
                "close": float(row["最新价"]) if pd.notna(row.get("最新价")) else None,
                "industry": str(row.get("所属行业", "")),
                "amount": float(row["成交额"]) if pd.notna(row.get("成交额")) else None,
                "market_cap": float(row["流通市值"]) if pd.notna(row.get("流通市值")) else None,
                "turnover": float(row["换手率"]) if pd.notna(row.get("换手率")) else None,
            }
            if tag == "today":
                rec["first_board_time"] = str(row.get("首次封板时间", ""))
                rec["last_board_time"] = str(row.get("最后封板时间", ""))
                rec["break_count"] = int(row["炸板次数"]) if pd.notna(row.get("炸板次数")) else 0
                rec["board_amount"] = float(row["封板资金"]) if pd.notna(row.get("封板资金")) else None
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

            last_error: Optional[BaseException] = None
            for provider in provider_sequence:
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
                    latest_td = str(df["date"].iloc[-1]).strip()
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
                    raw = _retry_ak_call(ak.stock_zh_a_hist_min_em, symbol=code, period="1", adjust="")
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
                    raw = _retry_ak_call(ak.stock_zh_a_minute, symbol=code, period="1")
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
