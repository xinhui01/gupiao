"""JSONP 工具：随机回调名 + 包装剥离。

通用函数，被东方财富和搜狐等返回 JSONP 格式的源共用。
"""
from __future__ import annotations

import random
import time


def random_callback() -> str:
    """生成随机 JSONP 回调名，模拟东方财富网页的真实调用模式。"""
    ts = int(time.time() * 1000)
    rand = random.randint(1000000, 9999999)
    prefix = random.choice(["jQuery", "jQuery1124", "jQuery35", "jQuery36"])
    return f"{prefix}{rand}_{ts}"


def strip_wrapper(text: str) -> str:
    """剥离 JSONP 回调包装，提取内部 JSON。

    例如 ``jQuery123456_1234567890({...})`` → ``{...}``。
    """
    s = text.strip()
    if not s:
        return s
    lp = s.find("(")
    rp = s.rfind(")")
    if lp >= 0 and rp > lp:
        inner = s[lp + 1 : rp].strip()
        if inner:
            return inner
    return s
