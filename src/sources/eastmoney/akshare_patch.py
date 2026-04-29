"""AkShare 内部 ``request_with_retry`` 的替换实现 + 注入器。

替换原因：
- AkShare 默认实现重试间隔短、超时短、headers 简陋，遇东财限流容易雪崩
- 用我们的 headers 池 + 镜像轮换 + 总 deadline + 熔断接入

注入方式：
- AkShare 不同模块通过 ``from ... import request_with_retry`` 绑定函数对象，
  必须同时 patch ``akshare.utils.request`` 和 ``akshare.utils.func`` 两处。
"""
from __future__ import annotations

import time
from typing import Any, Dict, Optional, Tuple

from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

from src.network.headers import random_eastmoney_headers
from src.sources.eastmoney.mirrors import request_mirror_urls
from src.utils import em_circuit_breaker as _em_circuit_breaker


_TOTAL_DEADLINE_SEC = 20.0


def request_with_retry(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 15,
    max_retries: int = 3,
    base_delay: float = 1.0,
    random_delay_range: Tuple[float, float] = (0.3, 1.0),
):
    """替换 akshare 内置 request_with_retry：显式浏览器头、多镜像、更长超时。

    增加 total deadline（所有镜像+重试合计不超过 20s），防止东财不可达时无限阻塞。
    """
    import random

    import requests
    from requests.adapters import HTTPAdapter

    # list-fetch 进度日志通道：调用方在 stock_data 模块层临时设置 _list_download_log
    import stock_data as _stock_data
    # env-toggle 仍住在 stock_data
    from stock_data import _use_bypass_proxy, _use_insecure_ssl

    deadline = time.time() + _TOTAL_DEADLINE_SEC

    params = params or {}
    if "eastmoney.com" in url:
        timeout = max(int(timeout or 0), 10)

    # 熔断：如果东财全局处于冷却期，直接快速失败
    if "eastmoney.com" in url and _em_circuit_breaker.is_open():
        raise RequestsConnectionError(
            "东方财富接口熔断中（连续失败过多），跳过本次请求"
        )

    last_exception: Optional[BaseException] = None

    mirrors = request_mirror_urls(url)
    for mi, base_url in enumerate(mirrors):
        for attempt in range(max_retries):
            remaining = deadline - time.time()
            if remaining <= 0:
                if last_exception is None:
                    last_exception = RequestsTimeout(
                        f"request_with_retry 总时限 {_TOTAL_DEADLINE_SEC:.0f}s 已到"
                    )
                raise last_exception

            lg = getattr(_stock_data, "_list_download_log", None)
            if (
                lg
                and attempt == 0
                and mi == 0
                and "/api/qt/clist/get" in url
                and isinstance(params, dict)
            ):
                pn = params.get("pn", "?")
                lg(
                    f"列表分页：正在请求第 {pn} 页（共 {len(mirrors)} 个镜像可轮换）…"
                )
            try:
                per_req_timeout = min(timeout, max(2, remaining))
                with requests.Session() as session:
                    if _use_bypass_proxy():
                        session.trust_env = False
                    adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1)
                    session.mount("http://", adapter)
                    session.mount("https://", adapter)
                    hdrs = random_eastmoney_headers()
                    req_kw: Dict[str, Any] = {
                        "url": base_url,
                        "params": params,
                        "timeout": per_req_timeout,
                        "headers": hdrs,
                    }
                    if _use_insecure_ssl():
                        req_kw["verify"] = False
                    response = session.get(**req_kw)
                    response.raise_for_status()
                    if "eastmoney.com" in url:
                        _em_circuit_breaker.record_success()
                    return response
            except (requests.RequestException, ValueError) as e:
                last_exception = e
                if "eastmoney.com" in url:
                    _em_circuit_breaker.record_failure()
                if attempt < max_retries - 1 and (deadline - time.time()) > 1:
                    delay = min(
                        base_delay * (1.5 ** attempt) + random.uniform(*random_delay_range),
                        max(0.5, deadline - time.time() - 1),
                    )
                    time.sleep(delay)
    if last_exception is not None:
        raise last_exception
    raise RuntimeError("request_with_retry: no attempt made")


def apply() -> None:
    """把替换实现注入到 AkShare 内部两处导入点。"""
    import akshare.utils.func as ak_func
    import akshare.utils.request as ak_req

    ak_req.request_with_retry = request_with_retry
    ak_func.request_with_retry = request_with_retry
