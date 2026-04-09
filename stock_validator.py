"""
数据校验模块。

提供 OHLC 异常值检测、交易日缺口检测等功能。
在保存历史数据前调用可提前发现脏数据。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from stock_logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# OHLC 基本校验
# ---------------------------------------------------------------------------

def validate_ohlc(df: pd.DataFrame, stock_code: str = "") -> List[Dict[str, Any]]:
    """校验 OHLC 数据的基本合理性。

    返回不合格行的列表，每项含 date / issue / detail。
    空 DataFrame 直接返回 []。
    """
    issues: List[Dict[str, Any]] = []
    if df is None or df.empty:
        return issues

    required = {"open", "close", "high", "low"}
    if not required.issubset(set(df.columns)):
        return issues

    for idx, row in df.iterrows():
        date_str = str(row.get("date", idx))
        o, c, h, l = row.get("open"), row.get("close"), row.get("high"), row.get("low")
        try:
            o, c, h, l = float(o), float(c), float(h), float(l)
        except (TypeError, ValueError):
            issues.append({"date": date_str, "issue": "非数值", "detail": f"O={o} C={c} H={h} L={l}"})
            continue

        # 非正价格
        if any(v <= 0 for v in (o, c, h, l)):
            issues.append({"date": date_str, "issue": "价格<=0", "detail": f"O={o} C={c} H={h} L={l}"})
            continue

        # High 不是最高
        if h < max(o, c) - 0.005:
            issues.append({"date": date_str, "issue": "high < max(open, close)", "detail": f"H={h} O={o} C={c}"})

        # Low 不是最低
        if l > min(o, c) + 0.005:
            issues.append({"date": date_str, "issue": "low > min(open, close)", "detail": f"L={l} O={o} C={c}"})

        # High < Low
        if h < l - 0.005:
            issues.append({"date": date_str, "issue": "high < low", "detail": f"H={h} L={l}"})

    if issues and stock_code:
        logger.warning("%s 存在 %d 条 OHLC 异常", stock_code, len(issues))
    return issues


# ---------------------------------------------------------------------------
# 涨跌幅异常检测
# ---------------------------------------------------------------------------

def validate_change_pct(
    df: pd.DataFrame,
    max_change_pct: float = 22.0,
    stock_code: str = "",
) -> List[Dict[str, Any]]:
    """检测涨跌幅超出合理范围（默认 ±22%，覆盖创业板/科创板 20% 加容差）。"""
    issues: List[Dict[str, Any]] = []
    if df is None or df.empty or "change_pct" not in df.columns:
        return issues

    for idx, row in df.iterrows():
        date_str = str(row.get("date", idx))
        try:
            pct = float(row["change_pct"])
        except (TypeError, ValueError):
            continue
        if abs(pct) > max_change_pct:
            issues.append({"date": date_str, "issue": "涨跌幅异常", "detail": f"change_pct={pct:.2f}%"})

    if issues and stock_code:
        logger.warning("%s 存在 %d 条涨跌幅异常 (>%.0f%%)", stock_code, len(issues), max_change_pct)
    return issues


# ---------------------------------------------------------------------------
# 交易日缺口检测
# ---------------------------------------------------------------------------

def detect_date_gaps(
    df: pd.DataFrame,
    max_gap_calendar_days: int = 7,
    stock_code: str = "",
) -> List[Dict[str, Any]]:
    """检测交易日之间的异常大间隔（排除正常周末/节假日）。

    默认认为连续超过 7 个自然日无交易数据即为缺口（涵盖大部分假期，
    如春节/国庆可能达到 9~10 天，可适当调大阈值）。
    """
    gaps: List[Dict[str, Any]] = []
    if df is None or df.empty or "date" not in df.columns:
        return gaps

    dates = pd.to_datetime(df["date"], errors="coerce").dropna().sort_values()
    if len(dates) < 2:
        return gaps

    for i in range(1, len(dates)):
        delta = (dates.iloc[i] - dates.iloc[i - 1]).days
        if delta > max_gap_calendar_days:
            gaps.append({
                "from_date": str(dates.iloc[i - 1].date()),
                "to_date": str(dates.iloc[i].date()),
                "gap_days": delta,
            })

    if gaps and stock_code:
        logger.info("%s 存在 %d 个交易日缺口", stock_code, len(gaps))
    return gaps


# ---------------------------------------------------------------------------
# 一站式校验
# ---------------------------------------------------------------------------

def validate_history(
    df: pd.DataFrame,
    stock_code: str = "",
    max_change_pct: float = 22.0,
    max_gap_calendar_days: int = 7,
) -> Dict[str, List[Dict[str, Any]]]:
    """综合校验历史数据，返回各类问题汇总。"""
    return {
        "ohlc": validate_ohlc(df, stock_code),
        "change_pct": validate_change_pct(df, max_change_pct, stock_code),
        "date_gaps": detect_date_gaps(df, max_gap_calendar_days, stock_code),
    }


def has_issues(report: Dict[str, List]) -> bool:
    """判断校验报告是否包含任何问题。"""
    return any(bool(v) for v in report.values())
