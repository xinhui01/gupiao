"""东方财富历史接口的节流 / 阻塞 / 自适应间隔 / 诊断子系统。

这是 ``_request_session_get_json`` 与历史抓取链路所依赖的核心 state，集中放在这里。

包含：
- 异常类 ``EastmoneyRateLimitError`` / ``HistoryAccessSuspendedError``
- 自适应请求间隔：连续成功加速、限流惩罚减速（``adaptive_on_success`` / ``adaptive_on_rate_limit`` / ``adaptive_current_interval``）
- 历史接口阻塞冷却：限流积累 N 次进入 cooldown（``history_access_blocked_until`` / ``record_history_block``）
- 请求 slot 等待：自适应间隔 + 抖动 + 阻塞检查（``wait_for_history_request_slot``）
- 诊断计数：``increment_diagnostic`` / ``diagnostics_snapshot``
- 共享信号量与 rate lock：``REQUEST_SEMAPHORE`` / ``REQUEST_RATE_LOCK``
"""
from __future__ import annotations

import random
import threading
import time
from typing import Dict, List

from src.config import env_float, env_int


# ---- 异常类 ----
class EastmoneyRateLimitError(RuntimeError):
    """东方财富返回了限流页面 / JSON tip。"""


class HistoryAccessSuspendedError(RuntimeError):
    """历史接口当前正在 cooldown 保护中，请求被主动拒绝。"""


# ---- 配置 getter（包内使用） ----
def _history_min_request_interval_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_MIN_INTERVAL_SEC", default=2.5, lo=0.5, hi=15.0)


def _history_block_window_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_BLOCK_WINDOW_SEC", default=180.0, lo=30.0, hi=3600.0)


def _history_block_threshold() -> int:
    return env_int("ASHARE_SCAN_HISTORY_BLOCK_THRESHOLD", default=3, lo=1, hi=10)


def _history_block_cooldown_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_BLOCK_COOLDOWN_SEC", default=900.0, lo=60.0, hi=7200.0)


def _history_request_concurrency() -> int:
    return env_int("ASHARE_SCAN_HISTORY_CONCURRENCY", default=2, lo=1, hi=10)


# ---- 共享信号量 / rate lock ----
REQUEST_SEMAPHORE = threading.BoundedSemaphore(_history_request_concurrency())
REQUEST_RATE_LOCK = threading.Lock()
_NEXT_REQUEST_AT = 0.0


# ---- 自适应间隔状态 ----
_ADAPTIVE_INTERVAL_LOCK = threading.Lock()
_ADAPTIVE_INTERVAL_SEC = _history_min_request_interval_sec()
_ADAPTIVE_SUCCESS_STREAK = 0
_ADAPTIVE_MIN_INTERVAL = 1.0
_ADAPTIVE_MAX_INTERVAL = 15.0
_ADAPTIVE_RATE_LIMIT_COUNT = 0


def adaptive_on_success() -> None:
    """网络请求成功后调用，逐步缩短间隔（保守策略）。"""
    global _ADAPTIVE_INTERVAL_SEC, _ADAPTIVE_SUCCESS_STREAK
    with _ADAPTIVE_INTERVAL_LOCK:
        _ADAPTIVE_SUCCESS_STREAK += 1
        # 每连续成功 20 次才缩短 5%，比之前更保守
        if _ADAPTIVE_SUCCESS_STREAK % 20 == 0:
            _ADAPTIVE_INTERVAL_SEC = max(
                _ADAPTIVE_MIN_INTERVAL,
                _ADAPTIVE_INTERVAL_SEC * 0.95,
            )


def adaptive_on_rate_limit() -> None:
    """遇到限流时调用，渐进式惩罚——每次限流惩罚更重。"""
    global _ADAPTIVE_INTERVAL_SEC, _ADAPTIVE_SUCCESS_STREAK, _ADAPTIVE_RATE_LIMIT_COUNT
    with _ADAPTIVE_INTERVAL_LOCK:
        _ADAPTIVE_SUCCESS_STREAK = 0
        _ADAPTIVE_RATE_LIMIT_COUNT += 1
        # 首次限流：间隔翻倍；后续每次额外加 50%
        multiplier = 2.0 + (_ADAPTIVE_RATE_LIMIT_COUNT - 1) * 0.5
        _ADAPTIVE_INTERVAL_SEC = min(
            _ADAPTIVE_MAX_INTERVAL,
            _ADAPTIVE_INTERVAL_SEC * multiplier,
        )


def adaptive_current_interval() -> float:
    """获取当前自适应间隔。"""
    with _ADAPTIVE_INTERVAL_LOCK:
        return _ADAPTIVE_INTERVAL_SEC


# ---- 诊断计数 ----
_DIAGNOSTICS_LOCK = threading.Lock()
DIAGNOSTICS: Dict[str, int] = {
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


def increment_diagnostic(key: str, step: int = 1) -> None:
    with _DIAGNOSTICS_LOCK:
        DIAGNOSTICS[key] = int(DIAGNOSTICS.get(key, 0)) + int(step)


def diagnostics_snapshot() -> Dict[str, int]:
    with _DIAGNOSTICS_LOCK:
        return {str(k): int(v) for k, v in DIAGNOSTICS.items()}


# ---- 历史接口阻塞冷却 ----
_BLOCK_LOCK = threading.Lock()
_BLOCKED_UNTIL = 0.0
_BLOCK_EVENTS: List[float] = []


def history_access_blocked_until() -> float:
    """返回历史接口冷却结束时间戳；若未冷却则返回 0 并清空事件窗口。"""
    global _BLOCKED_UNTIL
    now = time.time()
    with _BLOCK_LOCK:
        if _BLOCKED_UNTIL <= now:
            _BLOCKED_UNTIL = 0.0
            _BLOCK_EVENTS.clear()
            return 0.0
        return _BLOCKED_UNTIL


def record_history_block(reason: str) -> str:
    """记录一次限流事件；阈值内累积达上限即进入 cooldown。返回带 cooldown 提示的原因字符串。"""
    global _BLOCKED_UNTIL
    now = time.time()
    window_start = now - _history_block_window_sec()
    with _BLOCK_LOCK:
        _BLOCK_EVENTS[:] = [ts for ts in _BLOCK_EVENTS if ts >= window_start]
        _BLOCK_EVENTS.append(now)
        if len(_BLOCK_EVENTS) >= _history_block_threshold():
            _BLOCKED_UNTIL = max(_BLOCKED_UNTIL, now + _history_block_cooldown_sec())
        blocked_until = _BLOCKED_UNTIL
    if blocked_until > now:
        remain = max(1, int(blocked_until - now))
        return f"{reason}；已暂停新的东方财富历史请求，约 {remain}s 后再试"
    return reason


# ---- 请求 slot 等待 ----
def wait_for_history_request_slot() -> None:
    """获取一个请求 slot：检查冷却 → 加自适应间隔 + 抖动 → 等到 slot 可用。

    若历史接口正处于 cooldown 中，立即抛 ``HistoryAccessSuspendedError``。
    """
    global _NEXT_REQUEST_AT
    min_interval = adaptive_current_interval()
    jitter = min_interval * random.uniform(-0.3, 0.3)
    actual_interval = max(0.5, min_interval + jitter)
    while True:
        blocked_until = history_access_blocked_until()
        now = time.time()
        if blocked_until > now:
            remain = max(1, int(blocked_until - now))
            increment_diagnostic("cooldown_skips")
            raise HistoryAccessSuspendedError(
                f"东方财富历史接口正在冷却保护中，约 {remain}s 后恢复"
            )
        with REQUEST_RATE_LOCK:
            wait_sec = _NEXT_REQUEST_AT - now
            if wait_sec <= 0:
                _NEXT_REQUEST_AT = now + actual_interval
                return
        time.sleep(min(wait_sec, 0.5))
