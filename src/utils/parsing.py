"""通用值解析工具：safe_float / 中文数字 / 概念字符串归一化 / 列名匹配。

纯函数模块。
"""
from __future__ import annotations

import re
from typing import Any, List, Optional


def safe_float(value: Any) -> Optional[float]:
    """尽力把任意输入转成 float；失败返回 None（不抛异常）。"""
    try:
        if value is None:
            return None
        if isinstance(value, str) and value.strip() == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_cn_numeric(value: Any) -> Optional[float]:
    """解析带"亿/万/%"后缀的中文数字字符串为 float。"""
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


def normalize_concepts_text(value: Any) -> str:
    """概念/题材字符串归一化：拆分多种分隔符、去重、保持顺序、用 "、" 重新拼接。"""
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


def find_fund_flow_column(
    columns: List[str],
    includes: List[str],
    excludes: Optional[List[str]] = None,
) -> Optional[str]:
    """从列名列表里找第一个**同时包含**所有 ``includes`` 关键字、
    且**不包含**任何 ``excludes`` 关键字的列名。"""
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
