"""简单的 LRU 缓存（基于 ``collections.OrderedDict``）。

超出 ``maxsize`` 时自动淘汰最旧条目，访问时把该 key 移到末尾。
读写都是 O(1)，线程不安全（调用方自行加锁）。
"""
from __future__ import annotations

import collections


class LRUCache(collections.OrderedDict):
    def __init__(self, maxsize: int = 30, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._maxsize = maxsize

    def __setitem__(self, key, value):
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        while len(self) > self._maxsize:
            self.popitem(last=False)

    def __getitem__(self, key):
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value
