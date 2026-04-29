"""AkShare 警告抑制 + 静默调用包装。

AkShare 的概念板块 helper 偶尔在数据可用时也会抛 ``SettingWithCopyWarning``，
本模块在 import 时把这类噪声过滤掉，并暴露 ``call_quietly(fn, ...)`` 包装供调用方使用。
"""
from __future__ import annotations

import warnings
from typing import Any, Callable, TypeVar


# 探测可用的 warning 类（pandas 各版本符号不同）
try:
    from pandas.errors import SettingWithCopyWarning as AkshareWarningCategory  # type: ignore
except ImportError:
    try:
        from pandas.errors import ChainedAssignmentError as AkshareWarningCategory  # type: ignore
    except ImportError:
        AkshareWarningCategory = Warning  # type: ignore


# 模块级副作用：注册全局过滤，覆盖 akshare 概念板块模块的 noisy 警告
warnings.filterwarnings(
    "ignore",
    category=AkshareWarningCategory,
    module=r"akshare\.stock\.stock_board_concept_em",
)


T = TypeVar("T")


def call_quietly(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """临时屏蔽 ``AkshareWarningCategory`` 调用 fn，外加 stock_data 的 retry 包装。"""
    # 延迟 import：_retry_ak_call 需要 stock_data 顶层已加载
    from stock_data import _retry_ak_call

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", AkshareWarningCategory)
        return _retry_ak_call(fn, *args, **kwargs)
