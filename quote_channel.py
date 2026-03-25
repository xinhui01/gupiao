"""
行情「分组 / 多源」扩展点（预留）。

当前筛选依赖东方财富字段：量比、内外盘、主力净流入、日 K 等。新浪 `hq.sinajs.cn` 多为价量字符串、
往往无与东财一致的「量比」；同花顺 / 雪球在 AKShare 里常见为板块数据或需 Cookie，不能简单按组替换。

若要做「分组 + 多接口」提速，需要：
1. 为每个数据源实现与 ``stock_data._realtime_from_bid_ask`` 相同结构的 dict（至少满足 ``filter_stock`` 前置条件）；
2. 对无法提供的字段（如量比）要么换筛选策略，要么该组仍回退东财；
3. 注意各站点服务条款与请求频率。

环境变量 ``GUPPIAO_QUOTE_SHARDS`` 仅保留为将来按代码哈希分片时使用（默认 1 = 不分片）。
"""

from __future__ import annotations

import hashlib
import os


def quote_shard_index(stock_code: str, num_shards: int) -> int:
    if num_shards <= 1:
        return 0
    key = str(stock_code).strip().zfill(6).encode()
    h = hashlib.md5(key, usedforsecurity=False).hexdigest()
    return int(h, 16) % num_shards


def quote_shard_count() -> int:
    try:
        n = int(os.environ.get("GUPPIAO_QUOTE_SHARDS", "1").strip() or "1")
    except ValueError:
        n = 1
    return max(1, min(n, 16))
