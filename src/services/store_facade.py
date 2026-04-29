"""SQLite store 的薄包装层。

直接调用 stock_store 的原子读写，再加：
- 空 frame guard
- 列归一化 + 排序去重
- 可选的 OHLC 数据校验（仅日志，不阻止保存）
- 可选的日志回调

调用方（stock_data 等）只需关心业务语义；底层 schema 决策仍在 stock_store。
"""
from __future__ import annotations

from typing import Callable, Optional

import pandas as pd

from src.utils.codes import infer_exchange, infer_sz_board, norm_code_series
from src.utils.parsing import normalize_concepts_text
from stock_logger import get_logger
from stock_store import (
    load_fund_flow as _raw_load_fund_flow,
    load_history as _raw_load_history,
    load_universe as _raw_load_universe,
    save_fund_flow as _raw_save_fund_flow,
    save_history as _raw_save_history,
    save_universe as _raw_save_universe,
)


_logger = get_logger(__name__)


def save_universe(df: pd.DataFrame, log: Optional[Callable[[str], None]] = None) -> None:
    if df.empty or "code" not in df.columns:
        return
    _raw_save_universe(df)
    if log:
        log(f"股票池已保存 {len(df)} 只 → data/stock_store.sqlite3")


def load_universe(log: Optional[Callable[[str], None]] = None) -> Optional[pd.DataFrame]:
    df = _raw_load_universe()
    if df is None or df.empty:
        return None
    if "name" not in df.columns:
        df["name"] = ""
    if "exchange" not in df.columns:
        df["exchange"] = df["code"].map(infer_exchange)
    if "board" not in df.columns:
        df["board"] = df["code"].map(
            lambda x: "???"
            if str(x).strip().zfill(6).startswith("688")
            else infer_sz_board(x)
        )
    if "concepts" not in df.columns:
        df["concepts"] = ""
    df["code"] = norm_code_series(df["code"])
    df["concepts"] = df["concepts"].astype(str).map(normalize_concepts_text)
    if log:
        log(f"已从 data/stock_store.sqlite3 读取股票池 {len(df)} 只")
    return df[["code", "name", "exchange", "board", "concepts"]]


def load_history(
    stock_code: str,
    min_rows: int,
    end_date: str,
    log: Optional[Callable[[str], None]] = None,
) -> Optional[pd.DataFrame]:
    df = _raw_load_history(stock_code)
    if df is None or df.empty or "date" not in df.columns or "close" not in df.columns:
        return None
    df["date"] = df["date"].astype(str).str.strip()
    df = df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    if len(df) < min_rows:
        return None
    if log:
        log(f"已从 data/stock_store.sqlite3 读取历史 {stock_code} {len(df)} 行")
    return df


def save_history(stock_code: str, df: pd.DataFrame, keep_rows: int = 0) -> None:
    """保存历史数据到本地 SQLite。

    ``keep_rows=0`` 表示保存全部行（企业级策略：不截断，保证任意天数查询都能命中缓存）。
    保存前会做 OHLC 数据校验，有异常记录到日志但不阻止保存。
    """
    if df is None or df.empty:
        return
    if "date" not in df.columns:
        return
    out = df.copy()
    out["date"] = out["date"].astype(str).str.strip()
    out = out.sort_values("date").reset_index(drop=True)
    if keep_rows > 0:
        out = out.tail(max(keep_rows, 10)).reset_index(drop=True)

    # 数据校验（只记日志不阻止保存）
    try:
        from stock_validator import validate_change_pct, validate_ohlc

        ohlc_issues = validate_ohlc(out, stock_code)
        pct_issues = validate_change_pct(out, stock_code=stock_code)
        if ohlc_issues:
            _logger.warning("%s 保存前检测到 %d 条 OHLC 异常", stock_code, len(ohlc_issues))
        if pct_issues:
            _logger.warning("%s 保存前检测到 %d 条涨跌幅异常", stock_code, len(pct_issues))
    except Exception:
        pass

    _raw_save_history(stock_code, out)


def load_fund_flow(
    stock_code: str,
    min_rows: int,
    log: Optional[Callable[[str], None]] = None,
) -> Optional[pd.DataFrame]:
    df = _raw_load_fund_flow(stock_code)
    if df is None or df.empty or "date" not in df.columns:
        return None
    df["date"] = df["date"].astype(str).str.strip()
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if len(df) < min_rows:
        return None
    if log:
        log(f"已从 data/stock_store.sqlite3 读取资金流 {stock_code} {len(df)} 行")
    return df


def save_fund_flow(stock_code: str, df: pd.DataFrame, keep_rows: int = 40) -> None:
    if df is None or df.empty or "date" not in df.columns:
        return
    out = df.copy()
    out["date"] = out["date"].astype(str).str.strip()
    out = out.sort_values("date").tail(max(keep_rows, 10)).reset_index(drop=True)
    _raw_save_fund_flow(stock_code, out)
