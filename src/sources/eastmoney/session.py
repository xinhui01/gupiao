"""东方财富历史接口的核心 GET 请求包装。

封装：
- 自适应间隔等待 + 诊断计数（``throttling``）
- 代理池 + 黑名单（``src.network.proxy_pool``）
- 随机化 headers（``src.network.headers``）
- 限流检测 + 阻塞冷却记录 + ``EastmoneyRateLimitError`` 抛出
- JSONP 包装剥离 + 双层 JSON 容错解析

调用方只需传 url / params / timeout，拿到的是已经过限流检查的 dict。
"""
from __future__ import annotations

import json as _json
from typing import Any, Dict, Tuple

from src.network.headers import random_eastmoney_headers
from src.network.proxy_pool import blacklist_proxy, get_proxy
from src.sources._jsonp import strip_wrapper as _strip_jsonp_wrapper
from src.sources.eastmoney import throttling as _throttling
from src.sources.eastmoney.rate_limit import (
    json_indicates_rate_limit as _json_indicates_rate_limit,
    looks_like_rate_limit as _looks_like_rate_limit,
)
from src.sources.eastmoney.throttling import EastmoneyRateLimitError


def get_json(url: str, params: Dict[str, Any], timeout: Tuple[int, int]) -> Dict[str, Any]:
    """对东方财富接口发一次 GET，返回解析后的 JSON dict。

    限流时抛 ``EastmoneyRateLimitError``；其它网络/HTTP 错误透传 requests 异常。
    """
    import requests
    # 延迟 import 避免循环：env-toggle 仍住在 stock_data
    from stock_data import _use_bypass_proxy, _use_insecure_ssl

    _throttling.wait_for_history_request_slot()
    _throttling.increment_diagnostic("network_requests")

    proxy = get_proxy()
    with requests.Session() as session:
        if proxy:
            session.proxies = {"http": proxy, "https": proxy}
        elif _use_bypass_proxy():
            session.trust_env = False
            session.proxies = {"http": None, "https": None}
        req_kw: Dict[str, Any] = {
            "url": url,
            "params": params,
            "timeout": timeout,
            "headers": random_eastmoney_headers(),
        }
        if _use_insecure_ssl():
            req_kw["verify"] = False
        try:
            response = session.get(**req_kw)
        except Exception:
            if proxy:
                blacklist_proxy(proxy)
            raise
        response_text = response.text or ""
        if _looks_like_rate_limit(response.status_code, response_text):
            _throttling.adaptive_on_rate_limit()
            _throttling.increment_diagnostic("rate_limit_events")
            _throttling.increment_diagnostic("network_failures")
            message = _throttling.record_history_block(
                f"东方财富返回 {response.status_code}，疑似触发限流或封禁"
            )
            raise EastmoneyRateLimitError(message)
        response.raise_for_status()

        # 处理 JSONP 包装：东方财富接口返回 callback({json}) 格式
        raw_text = _strip_jsonp_wrapper(response_text)
        try:
            data_json = _json.loads(raw_text)
        except ValueError:
            try:
                data_json = response.json()
            except ValueError as exc:
                if _looks_like_rate_limit(response.status_code, response_text):
                    _throttling.adaptive_on_rate_limit()
                    _throttling.increment_diagnostic("rate_limit_events")
                    _throttling.increment_diagnostic("network_failures")
                    message = _throttling.record_history_block(
                        "东方财富返回非 JSON 内容，疑似触发限流或封禁"
                    )
                    raise EastmoneyRateLimitError(message) from exc
                _throttling.increment_diagnostic("network_failures")
                raise
        if _json_indicates_rate_limit(data_json):
            _throttling.adaptive_on_rate_limit()
            _throttling.increment_diagnostic("rate_limit_events")
            _throttling.increment_diagnostic("network_failures")
            message = _throttling.record_history_block("东方财富返回限流提示，进入冷却保护")
            raise EastmoneyRateLimitError(message)
        _throttling.adaptive_on_success()
        _throttling.increment_diagnostic("network_success")
        return data_json
