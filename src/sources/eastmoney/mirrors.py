"""东方财富镜像 URL 构造 + 健康度优先级排序。

- ``request_mirror_urls``：把单个 push2 URL 扩展成多节点候选列表（push2 / 82.push2 / 40.push2 等）。
- ``prioritize_history_mirrors``：从候选列表里剔除冷却中的主机，按健康度截断。
"""
from __future__ import annotations

import time
from typing import List, Optional
from urllib.parse import urlparse, urlunparse

from src.network.host_health import on_cooldown


def request_mirror_urls(url: str) -> List[str]:
    """东方财富 push 多节点；82 等单线路易在分页中途被断开，优先尝试无编号主域。"""
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
        "40.push2.eastmoney.com",
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


def prioritize_history_mirrors(
    mirror_urls: List[str],
    preferred_mirror: Optional[str] = None,
    max_count: int = 3,
) -> List[str]:
    """剔除冷却中的镜像，按健康度截断。"""
    now = time.time()
    seen: set[str] = set()

    candidates: List[str] = []
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
        if on_cooldown(clean, now):
            cooling.append(clean)
        else:
            healthy.append(clean)

    # 冷却中的镜像直接剔除，避免"明知不可用还继续打"。
    return healthy[:max_count]
