from __future__ import annotations

import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from typing import List, Dict, Any, Optional, Callable, Tuple
import threading
import weakref

import pandas as pd

from scan_models import FilterSettings, HistoryRequestPlan
from stock_data import StockDataFetcher
from stock_logger import get_logger

logger = get_logger(__name__)
from concurrent.futures.thread import _worker, _threads_queues


class DaemonThreadPoolExecutor(ThreadPoolExecutor):
    def _adjust_thread_count(self):
        if self._idle_semaphore.acquire(timeout=0):
            return

        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            thread_name = "%s_%d" % (self._thread_name_prefix or self, num_threads)
            t = threading.Thread(
                name=thread_name,
                target=_worker,
                args=(
                    weakref.ref(self, weakref_cb),
                    self._work_queue,
                    self._initializer,
                    self._initargs,
                ),
                daemon=True,
            )
            t.start()
            self._threads.add(t)
            _threads_queues[t] = self._work_queue


class StockFilter:
    def __init__(self):
        self.fetcher = StockDataFetcher()
        self._log: Optional[Callable[[str], None]] = None
        self.apply_settings(FilterSettings())

    def set_log_callback(self, cb: Optional[Callable[[str], None]]) -> None:
        self._log = cb
        self.fetcher.set_log_callback(cb)

    def set_history_source_preference(self, source: str) -> None:
        self.fetcher.set_default_history_source(source)

    def set_intraday_source_preference(self, source: str) -> None:
        self.fetcher.set_default_intraday_source(source)

    def set_fund_flow_source_preference(self, source: str) -> None:
        self.fetcher.set_default_fund_flow_source(source)

    def set_limit_up_reason_source_preference(self, source: str) -> None:
        self.fetcher.set_default_limit_up_reason_source(source)

    def _log_runtime_diagnostics(self, stage: str) -> None:
        if not self._log:
            return
        diag = self.fetcher.get_runtime_diagnostics()
        self._log(
            f"【诊断/{stage}】历史并发上限={diag.get('history_concurrency_limit')}，"
            f"最小请求间隔={diag.get('history_min_interval_sec')}s，"
            f"镜像缓存={diag.get('cached_mirror_count')}，"
            f"冷却中={'是' if diag.get('history_request_blocked') else '否'}"
        )
        self._log(
            f"【诊断/{stage}】缓存命中={diag.get('cache_hits')}，网络请求={diag.get('network_requests')}，"
            f"成功={diag.get('network_success')}，失败={diag.get('network_failures')}，"
            f"缓存回退={diag.get('fallback_cache_returns')}，限流事件={diag.get('rate_limit_events')}，"
            f"冷却跳过={diag.get('cooldown_skips')}"
        )

    def _resolve_stock_identity(self, universe: Optional[pd.DataFrame], stock_code: str) -> Dict[str, str]:
        code = str(stock_code or "").strip().zfill(6)
        if universe is None or universe.empty or not code:
            return {"name": "", "board": "", "exchange": ""}
        try:
            match = universe[universe["code"].astype(str).str.zfill(6) == code]
        except Exception:
            return {"name": "", "board": "", "exchange": ""}
        if match.empty:
            return {"name": "", "board": "", "exchange": ""}
        row = match.iloc[0]
        return {
            "name": str(row.get("name", "") or ""),
            "board": str(row.get("board", "") or ""),
            "exchange": str(row.get("exchange", "") or ""),
        }

    def _enrich_analysis_with_history_snapshot(
        self,
        analysis: Dict[str, Any],
        history: Optional[pd.DataFrame],
    ) -> None:
        if history is not None and not history.empty:
            latest_row = history.iloc[-1]
            analysis["latest_volume"] = latest_row.get("volume")
            analysis["latest_amount"] = latest_row.get("amount")
            analysis["quote_time"] = str(latest_row.get("date", "") or "")
            return
        analysis["latest_volume"] = None
        analysis["latest_amount"] = None
        analysis["quote_time"] = ""

    def _enrich_analysis_with_fund_flow(
        self,
        analysis: Dict[str, Any],
        fund_flow_df: Optional[pd.DataFrame],
    ) -> None:
        if fund_flow_df is not None and not fund_flow_df.empty:
            latest_flow = fund_flow_df.iloc[-1]
            analysis["flow_date"] = str(latest_flow.get("date", "") or "")
            analysis["main_force_amount"] = latest_flow.get("main_force_amount")
            analysis["big_order_amount"] = latest_flow.get("big_order_amount")
            analysis["super_big_order_amount"] = latest_flow.get("super_big_order_amount")
            analysis["main_force_ratio"] = latest_flow.get("main_force_ratio")
            analysis["big_order_ratio"] = latest_flow.get("big_order_ratio")
            analysis["super_big_order_ratio"] = latest_flow.get("super_big_order_ratio")
            analysis["fund_flow_history"] = fund_flow_df.to_dict("records")
            return
        analysis["flow_date"] = ""
        analysis["main_force_amount"] = None
        analysis["big_order_amount"] = None
        analysis["super_big_order_amount"] = None
        analysis["main_force_ratio"] = None
        analysis["big_order_ratio"] = None
        analysis["super_big_order_ratio"] = None
        analysis["fund_flow_history"] = []

    def _enrich_analysis_with_indicators(
        self,
        analysis: Dict[str, Any],
        history: Optional[pd.DataFrame],
    ) -> None:
        """在 analysis 字典中追加 MACD/KDJ/RSI/BOLL 最新值。"""
        if history is None or history.empty or "close" not in history.columns:
            analysis["macd_dif"] = None
            analysis["macd_dea"] = None
            analysis["macd_bar"] = None
            analysis["kdj_k"] = None
            analysis["kdj_d"] = None
            analysis["kdj_j"] = None
            analysis["rsi_6"] = None
            analysis["rsi_12"] = None
            analysis["boll_upper"] = None
            analysis["boll_mid"] = None
            analysis["boll_lower"] = None
            return
        try:
            from stock_indicators import calc_macd, calc_kdj, calc_rsi, calc_boll
            close = pd.to_numeric(history["close"], errors="coerce")
            m = calc_macd(close)
            analysis["macd_dif"] = round(float(m["dif"].iloc[-1]), 3) if not pd.isna(m["dif"].iloc[-1]) else None
            analysis["macd_dea"] = round(float(m["dea"].iloc[-1]), 3) if not pd.isna(m["dea"].iloc[-1]) else None
            analysis["macd_bar"] = round(float(m["macd"].iloc[-1]), 3) if not pd.isna(m["macd"].iloc[-1]) else None

            if all(c in history.columns for c in ("high", "low")):
                k = calc_kdj(history["high"], history["low"], close)
                analysis["kdj_k"] = round(float(k["k"].iloc[-1]), 2) if not pd.isna(k["k"].iloc[-1]) else None
                analysis["kdj_d"] = round(float(k["d"].iloc[-1]), 2) if not pd.isna(k["d"].iloc[-1]) else None
                analysis["kdj_j"] = round(float(k["j"].iloc[-1]), 2) if not pd.isna(k["j"].iloc[-1]) else None
            else:
                analysis["kdj_k"] = analysis["kdj_d"] = analysis["kdj_j"] = None

            r = calc_rsi(close, periods=(6, 12))
            analysis["rsi_6"] = round(float(r["rsi_6"].iloc[-1]), 2) if not pd.isna(r["rsi_6"].iloc[-1]) else None
            analysis["rsi_12"] = round(float(r["rsi_12"].iloc[-1]), 2) if not pd.isna(r["rsi_12"].iloc[-1]) else None

            b = calc_boll(close)
            analysis["boll_upper"] = round(float(b["upper"].iloc[-1]), 2) if not pd.isna(b["upper"].iloc[-1]) else None
            analysis["boll_mid"] = round(float(b["mid"].iloc[-1]), 2) if not pd.isna(b["mid"].iloc[-1]) else None
            analysis["boll_lower"] = round(float(b["lower"].iloc[-1]), 2) if not pd.isna(b["lower"].iloc[-1]) else None
        except Exception as exc:
            logger.debug("技术指标计算失败: %s", exc)

    def _build_stock_detail_payload(
        self,
        stock_code: str,
        stock_identity: Dict[str, str],
        history: Optional[pd.DataFrame],
        analysis: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "code": str(stock_code).strip().zfill(6),
            "name": str(stock_identity.get("name", "") or ""),
            "board": str(stock_identity.get("board", "") or ""),
            "exchange": str(stock_identity.get("exchange", "") or ""),
            "history": history,
            "analysis": analysis,
        }

    def get_settings(self) -> FilterSettings:
        return FilterSettings(
            trend_days=int(self.trend_days),
            ma_period=int(self.ma_period),
            limit_up_lookback_days=int(self.limit_up_lookback_days),
            volume_lookback_days=int(self.volume_lookback_days),
            volume_expand_enabled=bool(self.volume_expand_enabled),
            volume_expand_factor=float(self.volume_expand_factor),
            require_limit_up_within_days=bool(self.require_limit_up_within_days),
        )

    def apply_settings(self, settings: FilterSettings) -> None:
        self.trend_days = max(1, int(settings.trend_days))
        self.ma_period = max(1, int(settings.ma_period))
        self.limit_up_lookback_days = max(1, int(settings.limit_up_lookback_days))
        self.volume_lookback_days = max(1, int(settings.volume_lookback_days))
        self.volume_expand_enabled = bool(settings.volume_expand_enabled)
        self.volume_expand_factor = max(1.0, float(settings.volume_expand_factor))
        self.require_limit_up_within_days = bool(settings.require_limit_up_within_days)

    def _call_with_timeout(
        self,
        task: Callable[[], Any],
        timeout_sec: float,
        fallback: Any = None,
        task_name: str = "任务",
    ) -> Any:
        result: Dict[str, Any] = {}
        error: Dict[str, Exception] = {}
        done = threading.Event()

        def _runner() -> None:
            try:
                result["value"] = task()
            except Exception as exc:
                error["err"] = exc
            finally:
                done.set()

        threading.Thread(target=_runner, daemon=True).start()
        if not done.wait(timeout=max(0.5, float(timeout_sec))):
            if self._log:
                self._log(f"{task_name} 超时（>{timeout_sec:.0f}s），已跳过。")
            return fallback
        if "err" in error:
            if self._log:
                self._log(f"{task_name} 失败: {error['err']}")
            return fallback
        return result.get("value", fallback)

    def check_close_above_ma(
        self, history_data: pd.DataFrame, streak_days: int, ma_period: int
    ) -> bool:
        if history_data is None or history_data.empty:
            return False
        if "close" not in history_data.columns:
            return False

        df = (
            history_data.sort_values("date").reset_index(drop=True)
            if "date" in history_data.columns
            else history_data.reset_index(drop=True)
        )
        need = streak_days + ma_period - 1
        if len(df) < need:
            return False

        close = pd.to_numeric(df["close"], errors="coerce")
        ma = close.rolling(window=ma_period, min_periods=ma_period).mean()
        recent_close = close.tail(streak_days)
        recent_ma = ma.tail(streak_days)
        if recent_close.isna().any() or recent_ma.isna().any():
            return False
        return bool((recent_close.values > recent_ma.values).all())

    def _limit_up_threshold(self, board: str = "", stock_name: str = "") -> float:
        name = str(stock_name or "").upper().strip()
        if name.startswith("ST") or name.startswith("*ST"):
            return 5.0
        b = str(board or "").strip()
        if b in ("创业板", "科创板"):
            return 20.0
        return 10.0

    def _create_empty_analysis_result(
        self,
        volume_days: int,
        volume_enabled: bool,
        volume_factor: float,
    ) -> Dict[str, Any]:
        return {
            "passed": False,
            "latest_date": None,
            "latest_close": None,
            "latest_ma": None,
            "latest_ma10": None,
            "latest_change_pct": None,
            "five_day_return": None,
            "recent_closes": [],
            "recent_ma": [],
            "volume_lookback_days": volume_days,
            "volume_expand_enabled": volume_enabled,
            "volume_expand_factor": volume_factor,
            "volume_min": None,
            "volume_max": None,
            "volume_expand_ratio": None,
            "latest_volume_ratio": None,
            "volume_expand": False,
            "limit_up_threshold": None,
            "limit_up": False,
            "limit_up_within_days": False,
            "limit_up_hit_dates": [],
            "limit_up_streak": 0,
            "broken_limit_up": False,
            "broken_streak_count": 0,
            "volume_break_limit_up": False,
            "after_two_limit_up": False,
            "score": 0,
            "score_breakdown": "",
            "summary": "",
        }

    def _populate_price_metrics(
        self,
        result: Dict[str, Any],
        df: pd.DataFrame,
        close: pd.Series,
        ma: pd.Series,
        streak_days: int,
        ma_period: int,
    ) -> None:
        recent = df.tail(streak_days).copy()
        recent_close = pd.to_numeric(recent["close"], errors="coerce")
        recent_ma = ma.tail(streak_days)

        result["latest_date"] = str(df["date"].iloc[-1])
        result["latest_close"] = float(close.iloc[-1]) if not pd.isna(close.iloc[-1]) else None
        result["latest_ma"] = float(ma.iloc[-1]) if not pd.isna(ma.iloc[-1]) else None
        ma10 = close.rolling(window=10, min_periods=10).mean()
        result["latest_ma10"] = float(ma10.iloc[-1]) if not pd.isna(ma10.iloc[-1]) else None
        result["recent_closes"] = [float(v) if not pd.isna(v) else None for v in recent_close.tolist()]
        result["recent_ma"] = [float(v) if not pd.isna(v) else None for v in recent_ma.tolist()]

        if "change_pct" in df.columns:
            change_pct = pd.to_numeric(df["change_pct"], errors="coerce")
            if not change_pct.empty and not pd.isna(change_pct.iloc[-1]):
                result["latest_change_pct"] = float(change_pct.iloc[-1])

        if len(df) >= streak_days and not pd.isna(close.iloc[-1]) and not pd.isna(close.iloc[-streak_days]):
            prev_close = close.iloc[-streak_days]
            if not pd.isna(prev_close) and prev_close != 0:
                result["five_day_return"] = (float(close.iloc[-1]) / float(prev_close) - 1.0) * 100.0

        if len(df) >= streak_days + ma_period - 1 and not recent_close.isna().any() and not recent_ma.isna().any():
            result["passed"] = bool((recent_close.values > recent_ma.values).all())

    def _apply_volume_analysis(
        self,
        result: Dict[str, Any],
        df: pd.DataFrame,
        volume_days: int,
        volume_enabled: bool,
        volume_factor: float,
    ) -> Optional[pd.Series]:
        if "volume" not in df.columns or len(df) < volume_days:
            return None

        volume = pd.to_numeric(df["volume"], errors="coerce")
        recent_volume = volume.tail(volume_days).dropna()
        if not recent_volume.empty:
            vmin = float(recent_volume.min())
            vmax = float(recent_volume.max())
            ratio = None
            if vmin > 0:
                ratio = float(vmax / vmin)
            result["volume_min"] = vmin
            result["volume_max"] = vmax
            result["volume_expand_ratio"] = ratio
            result["volume_expand"] = bool(volume_enabled and ratio is not None and ratio >= volume_factor)

        compare_window = volume.iloc[-(volume_days + 1):-1].dropna() if len(volume) > 1 else pd.Series(dtype=float)
        if compare_window.empty:
            compare_window = recent_volume
        latest_volume = volume.iloc[-1] if not volume.empty else None
        if latest_volume is not None and not pd.isna(latest_volume) and not compare_window.empty:
            avg_volume = float(compare_window.mean())
            if avg_volume > 0:
                result["latest_volume_ratio"] = float(float(latest_volume) / avg_volume * 100.0)
        return volume

    def _calculate_limit_up_streak(self, mask: pd.Series) -> int:
        streak = 0
        for flag in reversed(mask.tolist()):
            if bool(flag):
                streak += 1
            else:
                break
        return streak

    def _calculate_broken_limit_up_streak(self, mask: pd.Series) -> int:
        if len(mask) < 2 or bool(mask.iloc[-1]) or not bool(mask.iloc[-2]):
            return 0
        broken_streak = 0
        idx = len(mask) - 2
        while idx >= 0 and bool(mask.iloc[idx]):
            broken_streak += 1
            idx -= 1
        return broken_streak

    def _apply_limit_up_analysis(
        self,
        result: Dict[str, Any],
        df: pd.DataFrame,
        board: str,
        stock_name: str,
        lookback_days: int,
        volume: Optional[pd.Series],
        volume_enabled: bool,
        volume_factor: float,
    ) -> None:
        threshold = self._limit_up_threshold(board=board, stock_name=stock_name)
        result["limit_up_threshold"] = threshold
        if "change_pct" not in df.columns:
            return

        change_pct = pd.to_numeric(df["change_pct"], errors="coerce")
        full_limit_up_mask = (change_pct >= (threshold - 0.2)).fillna(False)
        limit_up_mask = change_pct.tail(max(lookback_days, 1)) >= (threshold - 0.2)
        recent_dates = df.tail(max(lookback_days, 1))["date"].astype(str).tolist()
        hit_dates = [d for d, hit in zip(recent_dates, limit_up_mask.tolist()) if bool(hit)]
        result["limit_up_hit_dates"] = hit_dates
        result["limit_up_within_days"] = bool(hit_dates)
        result["limit_up"] = bool(
            not pd.isna(result["latest_change_pct"])
            and result["latest_change_pct"] >= (threshold - 0.2)
        )
        result["limit_up_streak"] = self._calculate_limit_up_streak(full_limit_up_mask)

        broken_streak = self._calculate_broken_limit_up_streak(full_limit_up_mask)
        result["broken_limit_up"] = broken_streak > 0
        result["broken_streak_count"] = broken_streak
        result["after_two_limit_up"] = bool(result["broken_limit_up"] and broken_streak >= 2)

        if not result["broken_limit_up"] or volume is None or not volume_enabled:
            return

        break_volume = volume.iloc[-1] if len(volume) >= 1 else None
        streak_volumes = volume.iloc[-1 - broken_streak:-1] if broken_streak > 0 else pd.Series(dtype=float)
        streak_volumes = streak_volumes.dropna()
        if (
            break_volume is None
            or pd.isna(break_volume)
            or float(break_volume) <= 0
            or streak_volumes.empty
        ):
            return
        base_volume = float(streak_volumes.min())
        if base_volume > 0:
            break_ratio = float(break_volume) / base_volume
            result["volume_break_limit_up"] = bool(break_ratio >= volume_factor)

    def _build_analysis_summary(
        self,
        result: Dict[str, Any],
        streak_days: int,
        ma_period: int,
        volume_days: int,
        volume_enabled: bool,
    ) -> str:
        if result["passed"]:
            summary = (
                f"最近{streak_days}日收盘全部高于MA{ma_period}，"
                f"最新收盘 {result['latest_close']:.2f} / MA{ma_period} {result['latest_ma']:.2f}"
            )
        else:
            summary = f"未满足最近{streak_days}日收盘全部高于MA{ma_period}"

        if volume_enabled and result["volume_expand"]:
            ratio_text = "-" if result["volume_expand_ratio"] is None else f"{result['volume_expand_ratio']:.2f}倍"
            summary = f"{summary}；近{volume_days}日放量 {ratio_text}"
        elif not volume_enabled:
            summary = f"{summary}；放量倍数检测已关闭"

        if result["limit_up_streak"] >= 2:
            summary = f"{summary}；连板 {result['limit_up_streak']} 板"
        if result["broken_limit_up"]:
            summary = f"{summary}；断板，前序连板 {result['broken_streak_count']} 板"
        if result["volume_break_limit_up"]:
            summary = f"{summary}；放量后断板"
        return summary

    def _calculate_trade_score(
        self,
        result: Dict[str, Any],
        streak_days: int,
        ma_period: int,
        volume_enabled: bool,
    ) -> tuple[int, str]:
        score = 50.0
        reasons: List[str] = []

        if result.get("passed"):
            score += 18
            reasons.append(f"站上MA{ma_period}+18")
        else:
            score -= 10
            reasons.append(f"跌破MA{ma_period}-10")

        five_day_return = result.get("five_day_return")
        if five_day_return is not None:
            if five_day_return >= 15:
                score += 12
                reasons.append("5日强势+12")
            elif five_day_return >= 5:
                score += 8
                reasons.append("5日偏强+8")
            elif five_day_return <= -8:
                score -= 8
                reasons.append("5日转弱-8")

        latest_change_pct = result.get("latest_change_pct")
        if latest_change_pct is not None:
            if latest_change_pct >= 9.5:
                score += 14
                reasons.append("当日涨停+14")
            elif latest_change_pct >= 5:
                score += 6
                reasons.append("当日走强+6")
            elif latest_change_pct <= -5:
                score -= 8
                reasons.append("当日大跌-8")

        limit_up_streak = int(result.get("limit_up_streak") or 0)
        if limit_up_streak >= 3:
            score += 10
            reasons.append("高连板+10")
        elif limit_up_streak == 2:
            score += 7
            reasons.append("二连板+7")
        elif result.get("limit_up_within_days"):
            score += 4
            reasons.append(f"{streak_days}日内有涨停+4")

        if volume_enabled and result.get("volume_expand"):
            score += 8
            reasons.append("放量有效+8")
        elif volume_enabled and result.get("volume_expand_ratio") is not None:
            ratio = float(result["volume_expand_ratio"])
            if ratio < max(1.2, self.volume_expand_factor * 0.8):
                score -= 4
                reasons.append("量能偏弱-4")

        latest_volume_ratio = result.get("latest_volume_ratio")
        if latest_volume_ratio is not None:
            if latest_volume_ratio >= 180:
                score += 6
                reasons.append("量比活跃+6")
            elif latest_volume_ratio < 80:
                score -= 3
                reasons.append("量比不足-3")

        if result.get("broken_limit_up"):
            score -= 10
            reasons.append("断板-10")
        if result.get("after_two_limit_up"):
            score -= 6
            reasons.append("二板后断板-6")
        if result.get("volume_break_limit_up"):
            score -= 5
            reasons.append("放量断板-5")

        final_score = max(0, min(100, int(round(score))))
        return final_score, " / ".join(reasons[:6])

    def analyze_history(
        self,
        history_data: pd.DataFrame,
        streak_days: Optional[int] = None,
        ma_period: Optional[int] = None,
        limit_up_lookback_days: Optional[int] = None,
        volume_lookback_days: Optional[int] = None,
        volume_expand_enabled: Optional[bool] = None,
        volume_expand_factor: Optional[float] = None,
        board: str = "",
        stock_name: str = "",
        stock_code: str = "",
    ) -> Dict[str, Any]:
        streak_days = int(streak_days or self.trend_days)
        ma_period = int(ma_period or self.ma_period)
        lookback_days = int(limit_up_lookback_days or self.limit_up_lookback_days)
        volume_days = int(volume_lookback_days or self.volume_lookback_days)
        volume_enabled = self.volume_expand_enabled if volume_expand_enabled is None else bool(volume_expand_enabled)
        volume_factor = float(volume_expand_factor or self.volume_expand_factor)
        result = self._create_empty_analysis_result(volume_days, volume_enabled, volume_factor)
        if history_data is None or history_data.empty:
            result["summary"] = "无历史数据"
            return result
        if "date" not in history_data.columns or "close" not in history_data.columns:
            result["summary"] = "历史数据缺少 date/close"
            return result

        df = history_data.sort_values("date").reset_index(drop=True)
        close = pd.to_numeric(df["close"], errors="coerce")
        ma = close.rolling(window=ma_period, min_periods=ma_period).mean()
        self._populate_price_metrics(result, df, close, ma, streak_days, ma_period)
        volume = self._apply_volume_analysis(result, df, volume_days, volume_enabled, volume_factor)
        self._apply_limit_up_analysis(
            result,
            df,
            board,
            stock_name,
            lookback_days,
            volume,
            volume_enabled,
            volume_factor,
        )
        result["summary"] = self._build_analysis_summary(
            result,
            streak_days,
            ma_period,
            volume_days,
            volume_enabled,
        )
        score, score_breakdown = self._calculate_trade_score(
            result,
            streak_days,
            ma_period,
            volume_enabled,
        )
        result["score"] = score
        result["score_breakdown"] = score_breakdown
        return result

    def _build_filter_result_shell(
        self,
        stock_code: str,
        stock_name: str,
        board: str,
        exchange: str,
    ) -> Dict[str, Any]:
        result = {
            "code": str(stock_code).strip().zfill(6),
            "name": stock_name or "",
            "passed": False,
            "reasons": [],
            "data": {},
        }
        if board:
            result["data"]["board"] = board
        if exchange:
            result["data"]["exchange"] = exchange
        return result

    def _resolve_filter_history_days(self) -> int:
        return max(
            14,
            self.trend_days + self.ma_period + 4,
            self.limit_up_lookback_days + self.ma_period + 4,
            self.volume_lookback_days + 4,
        )

    def _attach_filter_analysis(
        self,
        result: Dict[str, Any],
        history_data: pd.DataFrame,
        stock_code: str,
        stock_name: str,
        board: str,
    ) -> Dict[str, Any]:
        analysis = self.analyze_history(
            history_data,
            self.trend_days,
            self.ma_period,
            self.limit_up_lookback_days,
            self.volume_lookback_days,
            self.volume_expand_enabled,
            self.volume_expand_factor,
            board=board,
            stock_name=stock_name,
            stock_code=stock_code,
        )
        result["data"]["analysis"] = analysis
        result["data"]["history_tail"] = history_data.tail(max(self.trend_days, self.limit_up_lookback_days)).copy()
        return analysis

    def _apply_limit_up_requirement_failure(
        self,
        result: Dict[str, Any],
        analysis: Dict[str, Any],
    ) -> bool:
        if not self.require_limit_up_within_days or analysis.get("limit_up_within_days"):
            return False
        analysis["summary"] = (
            f"{analysis['summary']}；未命中过去{self.limit_up_lookback_days}个交易日涨停条件"
            if analysis.get("summary")
            else f"未命中过去{self.limit_up_lookback_days}个交易日涨停条件"
        )
        result["reasons"].append(analysis["summary"])
        return True

    def _finalize_filter_result(
        self,
        result: Dict[str, Any],
        analysis: Dict[str, Any],
    ) -> Dict[str, Any]:
        result["passed"] = bool(analysis.get("passed"))
        result["reasons"].append(analysis["summary"])
        return result

    def filter_stock(
        self,
        stock_code: str,
        stock_name: str = "",
        board: str = "",
        exchange: str = "",
        history_mirror: Optional[str] = None,
        mirror_pool: Optional[List[str]] = None,
        history_plan: Optional[HistoryRequestPlan] = None,
    ) -> Dict[str, Any]:
        result = self._build_filter_result_shell(stock_code, stock_name, board, exchange)
        history_data = self.fetcher.get_history_data(
            stock_code,
            days=self._resolve_filter_history_days(),
            preferred_mirror=history_mirror,
            mirror_pool=mirror_pool,
            request_plan=history_plan,
        )
        result["data"]["history"] = history_data
        if history_data is None or history_data.empty:
            result["reasons"].append("无法获取历史数据")
            return result

        analysis = self._attach_filter_analysis(result, history_data, stock_code, stock_name, board)
        if self._apply_limit_up_requirement_failure(result, analysis):
            return result
        return self._finalize_filter_result(result, analysis)

    def _result_sort_key(self, item: Dict[str, Any]):
        analysis = (item.get("data", {}) or {}).get("analysis") or {}
        five_day_return = analysis.get("five_day_return")
        volume_expand_ratio = analysis.get("volume_expand_ratio")
        latest_change_pct = analysis.get("latest_change_pct")
        return (
            five_day_return if five_day_return is not None else float("-inf"),
            volume_expand_ratio if volume_expand_ratio is not None else float("-inf"),
            1 if analysis.get("limit_up_within_days") else 0,
            latest_change_pct if latest_change_pct is not None else float("-inf"),
            str(item.get("code", "")),
        )

    def _filter_scan_universe(
        self,
        all_stocks: pd.DataFrame,
        allowed_boards: Optional[List[str]],
        allowed_exchanges: Optional[List[str]],
        log: Optional[Callable[[str], None]] = None,
    ) -> pd.DataFrame:
        filtered = all_stocks
        if allowed_boards and "board" in filtered.columns:
            allowed_board_set = {str(x).strip() for x in allowed_boards if str(x).strip()}
            if allowed_board_set:
                before = len(filtered)
                filtered = filtered[filtered["board"].astype(str).isin(allowed_board_set)]
                if log:
                    log(f"板块过滤：保留 {len(filtered)}/{before} 只，目标板块 {', '.join(sorted(allowed_board_set))}")

        if allowed_exchanges and "exchange" in filtered.columns:
            allowed_exchange_set = {str(x).strip() for x in allowed_exchanges if str(x).strip()}
            if allowed_exchange_set:
                before = len(filtered)
                filtered = filtered[filtered["exchange"].astype(str).isin(allowed_exchange_set)]
                if log:
                    log(f"交易所过滤：保留 {len(filtered)}/{before} 只，目标交易所 {', '.join(sorted(allowed_exchange_set))}")
        return filtered

    def _limit_scan_subset(self, all_stocks: pd.DataFrame, max_stocks: int) -> pd.DataFrame:
        if max_stocks and max_stocks > 0:
            return all_stocks.head(max_stocks).reset_index(drop=True)
        return all_stocks.reset_index(drop=True)

    def _resolve_scan_workers(self, max_workers: Optional[int]) -> Tuple[int, int]:
        if max_workers is None:
            try:
                max_workers = int(os.environ.get("GUPPIAO_SCAN_WORKERS", "3").strip() or "3")
            except ValueError:
                max_workers = 3
        requested_workers = max(1, min(int(max_workers), 16))
        history_workers = max(1, int(self.fetcher.history_request_concurrency_limit()))
        return requested_workers, min(requested_workers, history_workers)

    def _build_scan_history_plan(self, history_source: str, local_history_only: bool) -> HistoryRequestPlan:
        if local_history_only:
            return HistoryRequestPlan(
                mode="cache_only",
                provider_sequence=("local-cache",),
                mirror_urls=(),
                reason="scan-local-cache-only",
            )
        return self.fetcher.build_history_request_plan(
            source=history_source,
            force_refresh=False,
        )

    def _assign_scan_jobs(
        self,
        subset: pd.DataFrame,
        available_mirrors: List[str],
    ) -> Tuple[List[Tuple[Dict[str, Any], Optional[str]]], Dict[str, int]]:
        rows = subset.to_dict("records")
        random.Random(int(time.time())).shuffle(rows)
        assigned_jobs: List[Tuple[Dict[str, Any], Optional[str]]] = []
        mirror_counts: Dict[str, int] = {}
        for idx, row in enumerate(rows):
            mirror = available_mirrors[idx % len(available_mirrors)] if available_mirrors else None
            if mirror:
                mirror_counts[mirror] = mirror_counts.get(mirror, 0) + 1
            assigned_jobs.append((row, mirror))
        return assigned_jobs, mirror_counts

    def _log_scan_history_context(
        self,
        log: Optional[Callable[[str], None]],
        coverage: Dict[str, Any],
        history_plan: HistoryRequestPlan,
        local_history_only: bool,
    ) -> None:
        if not log:
            return
        log(
            f"历史缓存覆盖率：{coverage.get('covered_count', 0)}/{coverage.get('universe_count', 0)} "
            f"({coverage.get('coverage_ratio', 0.0) * 100:.1f}%)，最新交易日 {coverage.get('latest_trade_date') or '-'}"
        )
        log(f"历史数据源策略：{'/'.join(history_plan.provider_sequence)}")
        if history_plan.cache_only:
            if history_plan.reason and history_plan.reason != "scan-local-cache-only":
                log(f"历史接口镜像当前不可用，最近探测失败示例：{history_plan.reason}")
            if history_plan.reason == "scan-local-cache-only":
                log("本轮扫描使用本地历史缓存，不发起公网历史请求；未命中缓存的股票会被跳过。")
            else:
                log("历史接口暂不可用，本轮改为缓存优先扫描；未命中本地缓存的股票会被跳过。")
            return
        if not history_plan.mirror_urls and history_plan.provider_sequence and history_plan.provider_sequence[0] != "eastmoney":
            log("当前扫描已切换到非东方财富历史源。")

    def _log_scan_execution_context(
        self,
        log: Optional[Callable[[str], None]],
        total_universe: int,
        total: int,
        workers: int,
        requested_workers: int,
        local_history_only: bool,
        available_mirrors: List[str],
        mirror_counts: Dict[str, int],
    ) -> None:
        if not log:
            return
        log(f"【阶段 2/3】股票池 {total_universe} 只，本次扫描 {total} 只，最近{self.trend_days}日收盘 > MA{self.ma_period}，并发 {workers} 线程。")
        if requested_workers != workers:
            log(f"并发保护已生效：你请求 {requested_workers} 线程，但历史接口当前只允许 {workers} 个并发，以降低东方财富限流风险。")
        log("说明：扫描阶段只拉历史日线，不拉实时、资金流或内外盘。")
        if local_history_only:
            log("说明：扫描阶段默认只读本地缓存；首次或缓存不足时请先执行“更新历史缓存”。")
        else:
            log("说明：历史请求优先使用所选数据源；若为自动模式，会在东财失败后切换到腾讯/新浪。")
        if available_mirrors:
            mirror_summary = "，".join(
                f"{mirror.split('//', 1)[-1].split('/', 1)[0]}={mirror_counts.get(mirror, 0)}"
                for mirror in available_mirrors
            )
            log(f"历史镜像分区：{mirror_summary}")
        else:
            log("历史镜像分区：cache-only")

    def _submit_scan_tasks(
        self,
        executor: ThreadPoolExecutor,
        assigned_jobs: List[Tuple[Dict[str, Any], Optional[str]]],
        available_mirrors: List[str],
        history_plan: HistoryRequestPlan,
    ) -> Dict[Any, Tuple[str, str, str, str, Optional[str]]]:
        future_to_meta = {}
        for row, mirror in assigned_jobs:
            code = str(row["code"]).strip().zfill(6)
            name = str(row.get("name", "") or "")
            board = str(row.get("board", "") or "")
            exchange = str(row.get("exchange", "") or "")
            future = executor.submit(
                self.filter_stock,
                code,
                name,
                board,
                exchange,
                mirror,
                available_mirrors,
                history_plan,
            )
            future_to_meta[future] = (code, name, board, exchange, mirror)
        return future_to_meta

    def _pending_scan_sample_text(
        self,
        pending,
        future_to_meta: Dict[Any, Tuple[str, str, str, str, Optional[str]]],
    ) -> str:
        sample = [
            (
                f"{future_to_meta[f][0]}@{future_to_meta[f][4].split('//', 1)[-1].split('/', 1)[0]}"
                if future_to_meta[f][4]
                else f"{future_to_meta[f][0]}@cache-only"
            )
            for f in list(pending)[:3]
        ]
        return "、".join(sample) if sample else "-"

    def _scan_mirror_label(self, mirror: Optional[str]) -> str:
        if not mirror:
            return "cache-only"
        return mirror.split("//", 1)[-1].split("/", 1)[0]

    def _should_stop_scan(
        self,
        should_stop: Optional[Callable[[], bool]],
        log: Optional[Callable[[str], None]],
    ) -> bool:
        if not should_stop or not should_stop():
            return False
        if log:
            log("收到停止信号，正在取消未完成任务...")
        return True

    def _log_pending_scan_wait(
        self,
        log: Optional[Callable[[str], None]],
        pending,
        future_to_meta: Dict[Any, Tuple[str, str, str, str, Optional[str]]],
        completed: int,
        total: int,
        results: List[Dict[str, Any]],
        started_at: float,
        last_report: float,
    ) -> float:
        now = time.time()
        if not log or now - last_report < 10:
            return last_report
        elapsed = now - started_at
        sample_text = self._pending_scan_sample_text(pending, future_to_meta)
        log(
            f"进度 {completed}/{total}，命中 {len(results)} 只，已用时 {elapsed:.1f}s，"
            f"仍在等待历史数据返回，示例代码 {sample_text}"
        )
        return now

    def _build_scan_error_result(
        self,
        code: str,
        name: str,
        board: str,
        exchange: str,
        mirror: Optional[str],
        error: Exception,
        log: Optional[Callable[[str], None]],
    ) -> Dict[str, Any]:
        if log:
            log(f"  {code} {name} 检测异常[{self._scan_mirror_label(mirror)}]: {error}")
        return {
            "code": code,
            "name": name,
            "passed": False,
            "reasons": [str(error)],
            "data": {"board": board, "exchange": exchange},
        }

    def _resolve_scan_future_result(
        self,
        fut,
        future_to_meta: Dict[Any, Tuple[str, str, str, str, Optional[str]]],
        log: Optional[Callable[[str], None]],
    ) -> Tuple[str, str, str, str, Optional[str], Dict[str, Any]]:
        code, name, board, exchange, mirror = future_to_meta[fut]
        try:
            filter_result = fut.result()
        except Exception as exc:
            filter_result = self._build_scan_error_result(code, name, board, exchange, mirror, exc, log)
        return code, name, board, exchange, mirror, filter_result

    def _log_scan_pass_result(
        self,
        log: Optional[Callable[[str], None]],
        completed: int,
        total: int,
        code: str,
        name: str,
        filter_result: Dict[str, Any],
    ) -> None:
        if not log or not filter_result.get("passed"):
            return
        analysis = filter_result.get("data", {}).get("analysis") or {}
        log(
            f"  通过 {completed}/{total} {code} {name} "
            f"最新收盘 {analysis.get('latest_close', 0):.2f} / MA{self.ma_period} {analysis.get('latest_ma', 0):.2f}"
        )

    def _append_scan_hit(
        self,
        results: List[Dict[str, Any]],
        filter_result: Dict[str, Any],
        name: str,
        board: str,
        exchange: str,
    ) -> None:
        if not filter_result.get("passed"):
            return
        filter_result["name"] = name
        filter_result.setdefault("data", {})
        filter_result["data"].setdefault("board", board)
        filter_result["data"].setdefault("exchange", exchange)
        results.append(filter_result)

    def _notify_scan_progress(
        self,
        progress_callback,
        completed: int,
        total: int,
        code: str,
        name: str,
    ) -> None:
        if not progress_callback:
            return
        try:
            progress_callback(completed, total, code, name)
        except StopIteration:
            raise

    def _maybe_log_scan_progress(
        self,
        log: Optional[Callable[[str], None]],
        completed: int,
        total: int,
        results: List[Dict[str, Any]],
        started_at: float,
        last_report: float,
        report_every: int,
        code: str,
        name: str,
        mirror: Optional[str],
    ) -> float:
        now = time.time()
        if not log or (completed % report_every != 0 and now - last_report < 10):
            return last_report
        elapsed = now - started_at
        log(
            f"进度 {completed}/{total}，命中 {len(results)} 只，已用时 {elapsed:.1f}s，"
            f"当前 {code} {name} @ {self._scan_mirror_label(mirror)}"
        )
        return now

    def scan_all_stocks(
        self,
        max_stocks: int = 0,
        progress_callback=None,
        max_workers: Optional[int] = None,
        history_source: str = "auto",
        local_history_only: bool = True,
        should_stop: Optional[Callable[[], bool]] = None,
        refresh_universe: bool = False,
        allowed_boards: Optional[List[str]] = None,
        allowed_exchanges: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        log = self._log
        t0 = time.time()
        if log:
            log("【阶段 1/3】加载股票池...")
        self._log_runtime_diagnostics("扫描前")

        all_stocks = self.fetcher.get_all_stocks(force_refresh=refresh_universe)
        if all_stocks.empty:
            if log:
                log("股票池为空，扫描终止。")
            return []

        total_universe = len(all_stocks)
        all_stocks = self._filter_scan_universe(all_stocks, allowed_boards, allowed_exchanges, log=log)
        subset = self._limit_scan_subset(all_stocks, max_stocks)
        total = len(subset)
        requested_workers, workers = self._resolve_scan_workers(max_workers)

        coverage = self.fetcher.get_history_cache_summary()
        history_plan = self._build_scan_history_plan(history_source, local_history_only)
        available_mirrors = list(history_plan.mirror_urls)
        self._log_scan_history_context(log, coverage, history_plan, local_history_only)
        assigned_jobs, mirror_counts = self._assign_scan_jobs(subset, available_mirrors)
        self._log_scan_execution_context(
            log,
            total_universe,
            total,
            workers,
            requested_workers,
            local_history_only,
            available_mirrors,
            mirror_counts,
        )

        results: List[Dict[str, Any]] = []
        completed = 0
        last_report = time.time()
        report_every = 25

        executor = DaemonThreadPoolExecutor(max_workers=workers)
        try:
            future_to_meta = self._submit_scan_tasks(executor, assigned_jobs, available_mirrors, history_plan)
            if log:
                log("【阶段 3/3】开始逐只拉取历史日线并计算结果...")

            pending = set(future_to_meta)
            while pending:
                done, pending = wait(pending, timeout=2.0, return_when=FIRST_COMPLETED)
                if not done:
                    if self._should_stop_scan(should_stop, log):
                        break
                    last_report = self._log_pending_scan_wait(
                        log,
                        pending,
                        future_to_meta,
                        completed,
                        total,
                        results,
                        t0,
                        last_report,
                    )
                    continue

                for fut in done:
                    if self._should_stop_scan(should_stop, log):
                        pending.clear()
                        break
                    code, name, board, exchange, mirror, filter_result = self._resolve_scan_future_result(
                        fut,
                        future_to_meta,
                        log,
                    )
                    completed += 1
                    self._log_scan_pass_result(log, completed, total, code, name, filter_result)
                    self._append_scan_hit(results, filter_result, name, board, exchange)
                    self._notify_scan_progress(progress_callback, completed, total, code, name)
                    last_report = self._maybe_log_scan_progress(
                        log,
                        completed,
                        total,
                        results,
                        t0,
                        last_report,
                        report_every,
                        code,
                        name,
                        mirror,
                    )
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        results.sort(key=self._result_sort_key, reverse=True)

        if log:
            elapsed = time.time() - t0
            log(f"【完成】扫描结束，命中 {len(results)} 只，用时 {elapsed:.1f}s。")
        self._log_runtime_diagnostics("扫描后")

        return results

    def get_stock_detail_quick(self, stock_code: str) -> Dict[str, Any]:
        code = str(stock_code).strip().zfill(6)
        history_days = max(80, self.trend_days + self.limit_up_lookback_days + self.ma_period + 20)
        history = self._call_with_timeout(
            lambda: self.fetcher.get_history_data(code, days=history_days),
            timeout_sec=15.0,
            fallback=None,
            task_name=f"详情历史 {code}",
        )
        analysis = self.analyze_history(
            history,
            self.trend_days,
            self.ma_period,
            self.limit_up_lookback_days,
            self.volume_lookback_days,
            self.volume_expand_enabled,
            self.volume_expand_factor,
            stock_code=stock_code,
        )
        self._enrich_analysis_with_history_snapshot(analysis, history)
        return self._build_stock_detail_payload(
            code,
            {"name": "", "board": "", "exchange": ""},
            history,
            analysis,
        )

    def get_stock_detail(
        self,
        stock_code: str,
        preloaded_history: Optional[pd.DataFrame] = None,
    ) -> Dict[str, Any]:
        code = str(stock_code).strip().zfill(6)
        history_days = max(80, self.trend_days + self.limit_up_lookback_days + self.ma_period + 20)

        # ---- 并行获取：历史 / 股票池 / 资金流同时发起 ----
        from concurrent.futures import ThreadPoolExecutor, as_completed
        history = preloaded_history
        universe = None
        fund_flow_df = None

        tasks = {}
        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="detail") as pool:
            if history is None:
                tasks["history"] = pool.submit(
                    self._call_with_timeout,
                    lambda: self.fetcher.get_history_data(code, days=history_days),
                    15.0, None, f"详情历史 {code}",
                )
            tasks["universe"] = pool.submit(
                self._call_with_timeout,
                lambda: self.fetcher.get_all_stocks(),
                8.0, None, f"详情股票池 {code}",
            )
            tasks["fund_flow"] = pool.submit(
                self._call_with_timeout,
                lambda: self.fetcher.get_fund_flow_data(code, days=30, force_refresh=False),
                10.0, None, f"详情资金流 {code}",
            )
            for key, fut in tasks.items():
                try:
                    result = fut.result()
                    if key == "history":
                        history = result
                    elif key == "universe":
                        universe = result
                    elif key == "fund_flow":
                        fund_flow_df = result
                except Exception:
                    pass

        stock_identity = self._resolve_stock_identity(universe, code)
        analysis = self.analyze_history(
            history,
            self.trend_days,
            self.ma_period,
            self.limit_up_lookback_days,
            self.volume_lookback_days,
            self.volume_expand_enabled,
            self.volume_expand_factor,
            board=stock_identity["board"],
            stock_name=stock_identity["name"],
            stock_code=stock_code,
        )
        self._enrich_analysis_with_history_snapshot(analysis, history)
        self._enrich_analysis_with_fund_flow(analysis, fund_flow_df)
        self._enrich_analysis_with_indicators(analysis, history)
        return self._build_stock_detail_payload(code, stock_identity, history, analysis)

    def get_stock_detail_history(self, stock_code: str, days: int) -> Optional[pd.DataFrame]:
        code = str(stock_code).strip().zfill(6)
        history_days = max(60, int(days))
        return self._call_with_timeout(
            lambda: self.fetcher.get_history_data(code, days=history_days),
            timeout_sec=15.0,
            fallback=None,
            task_name=f"补充详情历史 {code}",
        )

    # ================= 涨停技术形态分类 =================

    def classify_limit_up_pattern(
        self,
        stock_code: str,
        board: str = "",
        stock_name: str = "",
    ) -> Dict[str, Any]:
        """对涨停股进行技术形态分类，返回形态标签和详细指标。

        形态类型:
        - 回踩MA5涨停: 前一日或前两日收盘接近/低于MA5，涨停日拉回
        - 超跌反弹涨停: 近10日跌幅>10%，或收盘在MA20以下
        - 趋势加速涨停: MA5>MA10>MA20 多头排列，涨停加速
        - 高位连板: 连板数>=2
        - 突破平台涨停: 近10日振幅小（横盘），涨停突破
        - 首板低位涨停: 股价在近60日低位（<30%分位）
        - 其他涨停: 不符合以上任何分类
        """
        code = str(stock_code).strip().zfill(6)
        result: Dict[str, Any] = {
            "code": code,
            "pattern": "其他涨停",
            "pattern_detail": "",
            "ma5": None,
            "ma10": None,
            "ma20": None,
            "close": None,
            "prev_close": None,
            "change_pct": None,
            "distance_ma5_pct": None,
            "trend_10d_pct": None,
            "position_60d_pct": None,
            "volatility_10d": None,
            "volume_burst_ratio": None,
            "amount_burst_ratio": None,
            "is_volume_burst": False,
            "consecutive_boards": 0,
        }

        history = self._call_with_timeout(
            lambda: self.fetcher.get_history_data(code, days=65),
            timeout_sec=10.0,
            fallback=None,
            task_name=f"涨停分类 {code}",
        )
        if history is None or history.empty or len(history) < 10:
            result["pattern"] = "数据不足"
            return result

        df = history.sort_values("date").reset_index(drop=True)
        close = pd.to_numeric(df["close"], errors="coerce")
        change_pct = pd.to_numeric(df.get("change_pct"), errors="coerce") if "change_pct" in df.columns else pd.Series(dtype=float)
        volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)
        amount = pd.to_numeric(df.get("amount"), errors="coerce") if "amount" in df.columns else pd.Series(dtype=float)

        ma5 = close.rolling(5, min_periods=5).mean()
        ma10 = close.rolling(10, min_periods=10).mean()
        ma20 = close.rolling(20, min_periods=20).mean()

        latest_close = float(close.iloc[-1]) if not pd.isna(close.iloc[-1]) else None
        prev_close = float(close.iloc[-2]) if len(close) >= 2 and not pd.isna(close.iloc[-2]) else None
        latest_ma5 = float(ma5.iloc[-1]) if not pd.isna(ma5.iloc[-1]) else None
        latest_ma10 = float(ma10.iloc[-1]) if not pd.isna(ma10.iloc[-1]) else None
        latest_ma20 = float(ma20.iloc[-1]) if not pd.isna(ma20.iloc[-1]) else None
        prev_ma5 = float(ma5.iloc[-2]) if len(ma5) >= 2 and not pd.isna(ma5.iloc[-2]) else None

        result["close"] = latest_close
        result["prev_close"] = prev_close
        result["ma5"] = latest_ma5
        result["ma10"] = latest_ma10
        result["ma20"] = latest_ma20
        if not change_pct.empty and not pd.isna(change_pct.iloc[-1]):
            result["change_pct"] = float(change_pct.iloc[-1])

        # 距MA5百分比
        if latest_close and latest_ma5 and latest_ma5 > 0:
            result["distance_ma5_pct"] = round((latest_close / latest_ma5 - 1) * 100, 2)

        # 10日涨跌幅
        if len(close) >= 11 and not pd.isna(close.iloc[-11]) and close.iloc[-11] > 0:
            result["trend_10d_pct"] = round((float(close.iloc[-1]) / float(close.iloc[-11]) - 1) * 100, 2)

        # 60日位置分位
        if len(close) >= 20:
            window = close.tail(min(60, len(close)))
            window_valid = window.dropna()
            if len(window_valid) >= 10 and latest_close is not None:
                rank = float((window_valid < latest_close).sum()) / len(window_valid) * 100
                result["position_60d_pct"] = round(rank, 1)

        # 近10日振幅（用于判断横盘）
        if len(close) >= 11:
            recent_10 = close.iloc[-11:-1].dropna()
            if len(recent_10) >= 5 and recent_10.mean() > 0:
                result["volatility_10d"] = round(float(recent_10.std() / recent_10.mean() * 100), 2)

        # 暴量倍数：当日量/额 相对前5日均值
        if len(volume) >= 6 and not pd.isna(volume.iloc[-1]):
            prev_volume = volume.iloc[-6:-1].dropna()
            if not prev_volume.empty and float(prev_volume.mean()) > 0:
                result["volume_burst_ratio"] = round(float(volume.iloc[-1]) / float(prev_volume.mean()), 2)
        if len(amount) >= 6 and not pd.isna(amount.iloc[-1]):
            prev_amount = amount.iloc[-6:-1].dropna()
            if not prev_amount.empty and float(prev_amount.mean()) > 0:
                result["amount_burst_ratio"] = round(float(amount.iloc[-1]) / float(prev_amount.mean()), 2)
        volume_burst = result["volume_burst_ratio"]
        amount_burst = result["amount_burst_ratio"]
        result["is_volume_burst"] = bool(
            (volume_burst is not None and volume_burst >= 2.5)
            or (amount_burst is not None and amount_burst >= 2.5)
        )

        # 连板数
        threshold = self._limit_up_threshold(board=board, stock_name=stock_name)
        if not change_pct.empty:
            mask = (change_pct >= (threshold - 0.2)).fillna(False)
            streak = self._calculate_limit_up_streak(mask)
            result["consecutive_boards"] = streak

        # ---- 分类逻辑（优先级从高到低）----
        streak = result["consecutive_boards"]
        dist_ma5 = result["distance_ma5_pct"]
        trend_10d = result["trend_10d_pct"]
        pos_60d = result["position_60d_pct"]
        vol_10d = result["volatility_10d"]
        volume_burst = result["volume_burst_ratio"]
        amount_burst = result["amount_burst_ratio"]

        if streak >= 2:
            result["pattern"] = "高位连板"
            result["pattern_detail"] = f"连板{streak}板"

        elif result["is_volume_burst"]:
            result["pattern"] = "暴量涨停"
            parts = []
            if volume_burst is not None:
                parts.append(f"量比前5日均量 {volume_burst:.2f}倍")
            if amount_burst is not None:
                parts.append(f"额比前5日均额 {amount_burst:.2f}倍")
            result["pattern_detail"] = "，".join(parts)

        elif (prev_close is not None and prev_ma5 is not None and prev_close <= prev_ma5 * 1.01
              and dist_ma5 is not None and dist_ma5 > 0):
            result["pattern"] = "回踩MA5涨停"
            detail_parts = []
            if prev_close and prev_ma5:
                detail_parts.append(f"前日收盘{prev_close:.2f}/MA5 {prev_ma5:.2f}")
            result["pattern_detail"] = "，".join(detail_parts)

        elif (trend_10d is not None and trend_10d < -10) or (
            latest_close is not None and latest_ma20 is not None and latest_close < latest_ma20
        ):
            result["pattern"] = "超跌反弹涨停"
            parts = []
            if trend_10d is not None:
                parts.append(f"10日跌{trend_10d:.1f}%")
            if latest_close and latest_ma20 and latest_close < latest_ma20:
                parts.append(f"低于MA20({latest_ma20:.2f})")
            result["pattern_detail"] = "，".join(parts)

        elif (latest_ma5 is not None and latest_ma10 is not None and latest_ma20 is not None
              and latest_ma5 > latest_ma10 > latest_ma20):
            result["pattern"] = "趋势加速涨停"
            result["pattern_detail"] = f"MA5({latest_ma5:.2f})>MA10({latest_ma10:.2f})>MA20({latest_ma20:.2f})"

        elif vol_10d is not None and vol_10d < 2.0:
            result["pattern"] = "突破平台涨停"
            result["pattern_detail"] = f"近10日波动率仅{vol_10d:.2f}%，横盘后突破"

        elif pos_60d is not None and pos_60d < 30:
            result["pattern"] = "首板低位涨停"
            result["pattern_detail"] = f"60日分位{pos_60d:.0f}%"

        else:
            result["pattern"] = "其他涨停"
            parts = []
            if dist_ma5 is not None:
                parts.append(f"距MA5 {dist_ma5:+.1f}%")
            if trend_10d is not None:
                parts.append(f"10日{trend_10d:+.1f}%")
            result["pattern_detail"] = "，".join(parts) if parts else ""

        return result

    def _prefetch_history_for_pool(
        self,
        codes: List[str],
        days: int = 65,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> None:
        """批量预取涨停池股票的历史数据到本地缓存。
        已有缓存的跳过，缺失的并行拉取，确保后续分类时不再逐只走网络。
        """
        from stock_data import _is_history_cache_fresh
        need_fetch: List[str] = []
        for code in codes:
            c = str(code).strip().zfill(6)
            if not _is_history_cache_fresh(c, min(10, days)):
                # 再检查 SQLite 有没有足够行数
                from stock_store import load_history as _load_h
                cached = _load_h(c, limit=days)
                if cached is None or cached.empty or len(cached) < 10:
                    need_fetch.append(c)

        if not need_fetch:
            if progress_callback:
                progress_callback(len(codes), len(codes), "全部已有缓存")
            return

        total = len(need_fetch)
        if self._log:
            self._log(f"涨停分类：需预取 {total}/{len(codes)} 只股票的历史数据")

        completed = 0
        def _fetch_one(code: str) -> None:
            nonlocal completed
            self.fetcher.get_history_data(code, days=days, force_refresh=False)
            completed += 1
            if progress_callback:
                progress_callback(completed, total, f"预取 {code}")

        workers = min(4, max(1, total // 5))
        executor = DaemonThreadPoolExecutor(max_workers=workers, thread_name_prefix="zt-prefetch")
        try:
            futures = [executor.submit(_fetch_one, c) for c in need_fetch]
            from concurrent.futures import as_completed
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception:
                    pass
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def classify_limit_up_pool(
        self,
        pool_records: List[Dict[str, Any]],
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> List[Dict[str, Any]]:
        """对涨停池中的每只股票进行技术形态分类。
        自动先批量预取缺失的历史数据，再逐只分类。
        """
        codes = [str(r.get("code", "")).strip().zfill(6) for r in pool_records]

        # 阶段1：批量预取历史数据（缓存已有的秒过）
        self._prefetch_history_for_pool(
            codes, days=65,
            progress_callback=lambda c, t, info:
                progress_callback(c, t, f"[预取] {info}") if progress_callback else None,
        )

        # 阶段2：逐只分类（全部从本地缓存读取，秒出）
        results: List[Dict[str, Any]] = []
        total = len(pool_records)
        for idx, rec in enumerate(pool_records):
            code = str(rec.get("code", "")).strip().zfill(6)
            name = str(rec.get("name", ""))
            industry = str(rec.get("industry", ""))
            classification = self.classify_limit_up_pattern(
                code,
                board=rec.get("board", ""),
                stock_name=name,
            )
            classification["name"] = name
            classification["industry"] = industry
            for key in ("amount", "market_cap", "turnover", "first_board_time",
                         "last_board_time", "break_count", "board_amount"):
                if key in rec:
                    classification[key] = rec[key]
            results.append(classification)
            if progress_callback:
                progress_callback(idx + 1, total, f"{code} {name}")
        return results

    def _resolve_intraday_prev_close(
        self,
        history_df: Optional[pd.DataFrame],
        selected_trade_date: str,
    ) -> Optional[float]:
        if history_df is None or history_df.empty or "close" not in history_df.columns:
            return None

        df = history_df.copy()
        if "date" in df.columns:
            df["date"] = df["date"].astype(str).str.strip()
        else:
            df["date"] = ""
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"]).sort_values("date").reset_index(drop=True)
        if df.empty:
            return None

        target_date = str(selected_trade_date or "").strip()
        if target_date:
            previous_rows = df[df["date"] < target_date]
            if not previous_rows.empty:
                return float(previous_rows.iloc[-1]["close"])

        if len(df) >= 2:
            return float(df.iloc[-2]["close"])
        return float(df.iloc[-1]["close"])

    def get_stock_intraday(
        self,
        stock_code: str,
        day_offset: int = 0,
        target_trade_date: str = "",
    ) -> Dict[str, Any]:
        code = str(stock_code).strip().zfill(6)

        # ---- 并行获取：分时数据 + 历史(昨收)同时发起 ----
        from concurrent.futures import ThreadPoolExecutor
        intraday_payload = {}
        history_df = None

        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="intraday") as pool:
            fut_intraday = pool.submit(
                self._call_with_timeout,
                lambda: self.fetcher.get_intraday_data(
                    code, day_offset=day_offset,
                    target_trade_date=target_trade_date, include_meta=True,
                ),
                12.0, {}, f"分时 {code}",
            )
            fut_history = pool.submit(
                self._call_with_timeout,
                lambda: self.fetcher.get_history_data(code, days=20),
                6.0, None, f"分时昨收 {code}",
            )
            try:
                intraday_payload = fut_intraday.result() or {}
            except Exception:
                intraday_payload = {}
            try:
                history_df = fut_history.result()
            except Exception:
                history_df = None

        intraday_df = None
        selected_trade_date = ""
        available_trade_dates: List[str] = []
        applied_day_offset = 0
        auction_snapshot = None
        if isinstance(intraday_payload, dict):
            intraday_df = intraday_payload.get("intraday")
            selected_trade_date = str(intraday_payload.get("selected_trade_date") or "")
            available_trade_dates = [str(d) for d in (intraday_payload.get("available_trade_dates") or [])]
            raw_auction = intraday_payload.get("auction")
            if isinstance(raw_auction, dict):
                auction_snapshot = raw_auction
            try:
                applied_day_offset = int(intraday_payload.get("applied_day_offset") or 0)
            except (TypeError, ValueError):
                applied_day_offset = 0

        prev_close = self._resolve_intraday_prev_close(history_df, selected_trade_date)
        return {
            "code": code,
            "intraday": intraday_df,
            "prev_close": prev_close,
            "selected_trade_date": selected_trade_date,
            "available_trade_dates": available_trade_dates,
            "applied_day_offset": applied_day_offset,
            "auction": auction_snapshot,
        }

    # ================= 涨停预测 =================

    def predict_limit_up_candidates(
        self,
        trade_date: str,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, Any]:
        """基于今日涨停池 + 全量扫描结果，筛选明日涨停候选股。

        两个维度：
        1. 连板延续候选：今日涨停且连板>=1，根据封板强度/量能/板块热度打分
        2. 首板候选：从今日强势股中筛选有涨停潜力的（量能蓄势、资金异动、技术共振、低位突破）

        返回:
            continuation_candidates: 连板延续候选列表
            first_board_candidates: 首板候选列表
            hot_industries: 热门行业统计
            summary: 文字摘要
        """
        if self._log:
            self._log(f"涨停预测：正在获取 {trade_date} 涨停池数据...")

        # 获取今日涨停池（全量，不限首板）
        today_pool_df = self.fetcher.get_limit_up_pool(trade_date)
        if today_pool_df is None or today_pool_df.empty:
            return {
                "trade_date": trade_date,
                "continuation_candidates": [],
                "first_board_candidates": [],
                "hot_industries": {},
                "summary": f"{trade_date} 未获取到涨停池数据",
            }

        # 解析涨停池为记录
        all_pool_records = self._parse_full_pool(today_pool_df)
        hot_industries = self._count_pool_industries(today_pool_df)

        # ---------- 维度1: 连板延续候选 ----------
        if self._log:
            self._log(f"涨停预测：分析 {len(all_pool_records)} 只涨停股的延续潜力...")
        if progress_callback:
            progress_callback(0, len(all_pool_records), "分析连板延续...")

        # 预取历史数据
        codes = [r["code"] for r in all_pool_records]
        self._prefetch_history_for_pool(codes, days=65, progress_callback=progress_callback)

        continuation_candidates = []
        for idx, rec in enumerate(all_pool_records):
            score_info = self._score_continuation(rec, hot_industries)
            if score_info["score"] >= 40:
                continuation_candidates.append(score_info)
            if progress_callback:
                progress_callback(idx + 1, len(all_pool_records),
                                  f"连板分析 {rec['code']} {rec.get('name', '')}")

        continuation_candidates.sort(key=lambda x: -x["score"])

        # ---------- 维度2: 首板候选（从非涨停的强势股中筛选） ----------
        if self._log:
            self._log("涨停预测：正在筛选首板候选...")
        if progress_callback:
            progress_callback(0, 1, "获取强势股列表...")

        first_board_candidates = self._scan_first_board_candidates(
            trade_date, today_pool_df, hot_industries, progress_callback
        )

        # 摘要
        summary_lines = [
            f"预测日期：基于 {trade_date} 数据预测次日涨停候选",
            f"今日涨停总数：{len(all_pool_records)} 只",
            f"连板延续候选：{len(continuation_candidates)} 只（得分>=40）",
            f"首板候选：{len(first_board_candidates)} 只（得分>=50）",
        ]
        if hot_industries:
            top3 = sorted(hot_industries.items(), key=lambda x: -x[1])[:3]
            summary_lines.append(f"热门行业：{'、'.join(f'{k}({v})' for k, v in top3)}")

        return {
            "trade_date": trade_date,
            "continuation_candidates": continuation_candidates,
            "first_board_candidates": first_board_candidates,
            "hot_industries": hot_industries,
            "summary": "\n".join(summary_lines),
        }

    def _parse_full_pool(self, pool_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """将涨停池 DataFrame 解析为完整记录列表（包含所有连板数）。"""
        records = []
        if pool_df.empty:
            return records
        for _, row in pool_df.iterrows():
            rec: Dict[str, Any] = {
                "code": str(row.get("代码", "")).strip().zfill(6),
                "name": str(row.get("名称", "")),
                "change_pct": float(row["涨跌幅"]) if pd.notna(row.get("涨跌幅")) else None,
                "close": float(row["最新价"]) if pd.notna(row.get("最新价")) else None,
                "industry": str(row.get("所属行业", "")),
                "amount": float(row["成交额"]) if pd.notna(row.get("成交额")) else None,
                "market_cap": float(row["流通市值"]) if pd.notna(row.get("流通市值")) else None,
                "turnover": float(row["换手率"]) if pd.notna(row.get("换手率")) else None,
                "consecutive_boards": int(row["连板数"]) if pd.notna(row.get("连板数")) else 1,
                "first_board_time": str(row.get("首次封板时间", "")),
                "last_board_time": str(row.get("最后封板时间", "")),
                "break_count": int(row["炸板次数"]) if pd.notna(row.get("炸板次数")) else 0,
                "board_amount": float(row["封板资金"]) if pd.notna(row.get("封板资金")) else None,
            }
            records.append(rec)
        return records

    @staticmethod
    def _count_pool_industries(pool_df: pd.DataFrame) -> Dict[str, int]:
        if pool_df.empty or "所属行业" not in pool_df.columns:
            return {}
        counts = pool_df["所属行业"].astype(str).value_counts().to_dict()
        return {k: int(v) for k, v in counts.items() if k and k.lower() != "nan"}

    def _score_continuation(
        self,
        rec: Dict[str, Any],
        hot_industries: Dict[str, int],
    ) -> Dict[str, Any]:
        """对涨停股进行连板延续评分。满分100。"""
        code = rec["code"]
        name = rec.get("name", "")
        score = 0.0
        reasons: List[str] = []

        boards = rec.get("consecutive_boards", 1)
        break_count = rec.get("break_count", 0)
        board_amount = rec.get("board_amount")
        first_time = rec.get("first_board_time", "")
        industry = rec.get("industry", "")
        turnover = rec.get("turnover")

        # 1. 连板数基础分（连板越高，延续概率越高到一定程度后降低）
        if boards >= 5:
            score += 30
            reasons.append(f"{boards}连板+30")
        elif boards >= 3:
            score += 25
            reasons.append(f"{boards}连板+25")
        elif boards == 2:
            score += 20
            reasons.append("2连板+20")
        else:
            score += 10
            reasons.append("首板+10")

        # 2. 封板强度（炸板次数少、封板时间早）
        if break_count == 0:
            score += 15
            reasons.append("未炸板+15")
        elif break_count == 1:
            score += 8
            reasons.append("炸板1次+8")
        else:
            score -= 5
            reasons.append(f"炸板{break_count}次-5")

        if first_time:
            try:
                parts = first_time.split(":")
                hour = int(parts[0])
                minute = int(parts[1]) if len(parts) > 1 else 0
                seal_minutes = hour * 60 + minute
                if seal_minutes <= 9 * 60 + 35:
                    score += 15
                    reasons.append("秒板/早封+15")
                elif seal_minutes <= 10 * 60:
                    score += 10
                    reasons.append("上午早封+10")
                elif seal_minutes <= 11 * 60 + 30:
                    score += 5
                    reasons.append("上午封板+5")
                elif seal_minutes >= 14 * 60 + 30:
                    score -= 5
                    reasons.append("尾盘封板-5")
            except (ValueError, IndexError):
                pass

        # 3. 板块热度加分
        if industry and hot_industries.get(industry, 0) >= 3:
            score += 10
            reasons.append(f"板块热({hot_industries[industry]}只)+10")
        elif industry and hot_industries.get(industry, 0) >= 2:
            score += 5
            reasons.append(f"板块有{hot_industries[industry]}只+5")

        # 4. 换手率（适中的换手更健康）
        if turnover is not None:
            if 3 <= turnover <= 15:
                score += 5
                reasons.append(f"换手{turnover:.1f}%适中+5")
            elif turnover > 30:
                score -= 5
                reasons.append(f"换手{turnover:.1f}%过高-5")

        # 5. 从历史数据补充技术面信号
        history = self._call_with_timeout(
            lambda: self.fetcher.get_history_data(code, days=65),
            timeout_sec=8.0,
            fallback=None,
            task_name=f"预测历史 {code}",
        )
        if history is not None and not history.empty and len(history) >= 10:
            df = history.sort_values("date").reset_index(drop=True)
            close = pd.to_numeric(df["close"], errors="coerce")
            volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)

            ma5 = close.rolling(5, min_periods=5).mean()
            ma10 = close.rolling(10, min_periods=10).mean()
            ma20 = close.rolling(20, min_periods=20).mean()

            latest_ma5 = float(ma5.iloc[-1]) if not pd.isna(ma5.iloc[-1]) else None
            latest_ma10 = float(ma10.iloc[-1]) if not pd.isna(ma10.iloc[-1]) else None
            latest_ma20 = float(ma20.iloc[-1]) if not pd.isna(ma20.iloc[-1]) else None

            # 均线多头排列
            if (latest_ma5 is not None and latest_ma10 is not None and latest_ma20 is not None
                    and latest_ma5 > latest_ma10 > latest_ma20):
                score += 10
                reasons.append("多头排列+10")

            # 量能：当日量/前5日均量
            if len(volume) >= 6 and not pd.isna(volume.iloc[-1]):
                prev_vol = volume.iloc[-6:-1].dropna()
                if not prev_vol.empty and float(prev_vol.mean()) > 0:
                    vol_ratio = float(volume.iloc[-1]) / float(prev_vol.mean())
                    if 1.0 <= vol_ratio <= 3.0:
                        score += 5
                        reasons.append(f"量比{vol_ratio:.1f}适中+5")
                    elif vol_ratio > 5.0:
                        score -= 5
                        reasons.append(f"量比{vol_ratio:.1f}过大-5")

        final_score = max(0, min(100, int(round(score))))
        return {
            "code": code,
            "name": name,
            "industry": industry,
            "consecutive_boards": boards,
            "close": rec.get("close"),
            "change_pct": rec.get("change_pct"),
            "turnover": turnover,
            "break_count": break_count,
            "first_board_time": first_time,
            "board_amount": board_amount,
            "score": final_score,
            "reasons": " / ".join(reasons[:8]),
            "predict_type": "连板延续",
        }

    def _scan_first_board_candidates(
        self,
        trade_date: str,
        today_pool_df: pd.DataFrame,
        hot_industries: Dict[str, int],
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> List[Dict[str, Any]]:
        """从涨停池之外的强势股中筛选首板候选。

        筛选逻辑:
        1. 获取当日涨幅 5%~9.5% 的股票（接近涨停但未涨停）
        2. 结合量能、均线、位置等信号打分
        """
        # 取今日涨停的代码集合，排除已涨停的
        zt_codes = set()
        if not today_pool_df.empty and "代码" in today_pool_df.columns:
            zt_codes = set(today_pool_df["代码"].astype(str).str.strip().str.zfill(6))

        # 从已有扫描结果或涨停池相关板块的强势股中获取候选
        # 优先使用涨停池的行业，获取同行业强势股
        if self._log:
            self._log("涨停预测：获取强势股列表（涨幅5%~9.5%）...")

        strong_stocks = self._fetch_strong_stocks(trade_date, zt_codes)
        if not strong_stocks:
            if self._log:
                self._log("涨停预测：未获取到符合条件的强势股")
            return []

        # 预取历史数据
        codes = [r["code"] for r in strong_stocks]
        self._prefetch_history_for_pool(codes, days=65, progress_callback=progress_callback)

        candidates = []
        total = len(strong_stocks)
        for idx, rec in enumerate(strong_stocks):
            score_info = self._score_first_board_candidate(rec, hot_industries)
            if score_info["score"] >= 50:
                candidates.append(score_info)
            if progress_callback:
                progress_callback(idx + 1, total, f"首板分析 {rec['code']} {rec.get('name', '')}")

        candidates.sort(key=lambda x: -x["score"])
        return candidates[:30]  # 最多返回30只

    def _fetch_strong_stocks(
        self, trade_date: str, exclude_codes: set
    ) -> List[Dict[str, Any]]:
        """获取当日涨幅在 5%~9.5% 之间的强势股。"""
        try:
            import akshare as ak
            from stock_data import _retry_ak_call
            df = _retry_ak_call(ak.stock_zh_a_spot_em)
            if df is None or df.empty:
                return []
        except Exception as e:
            if self._log:
                self._log(f"涨停预测：获取实时行情失败: {e}")
            return []

        records = []
        for _, row in df.iterrows():
            code = str(row.get("代码", "")).strip().zfill(6)
            if code in exclude_codes:
                continue
            name = str(row.get("名称", ""))
            # 排除 ST
            if "ST" in name.upper():
                continue
            change_pct = float(row["涨跌幅"]) if pd.notna(row.get("涨跌幅")) else None
            if change_pct is None or change_pct < 5.0 or change_pct >= 9.5:
                continue
            close = float(row["最新价"]) if pd.notna(row.get("最新价")) else None
            if close is None or close <= 0:
                continue
            volume = float(row["成交量"]) if pd.notna(row.get("成交量")) else None
            amount = float(row["成交额"]) if pd.notna(row.get("成交额")) else None
            turnover = float(row["换手率"]) if pd.notna(row.get("换手率")) else None
            # 排除成交额过小的（流动性差）
            if amount is not None and amount < 5000_0000:
                continue
            records.append({
                "code": code,
                "name": name,
                "change_pct": change_pct,
                "close": close,
                "volume": volume,
                "amount": amount,
                "turnover": turnover,
                "industry": "",  # 后续从 universe 补充
            })
        # 按涨幅排序，取前 80 只分析
        records.sort(key=lambda x: -(x.get("change_pct") or 0))
        return records[:80]

    def _score_first_board_candidate(
        self,
        rec: Dict[str, Any],
        hot_industries: Dict[str, int],
    ) -> Dict[str, Any]:
        """对非涨停强势股进行明日首板涨停潜力评分。满分100。"""
        code = rec["code"]
        name = rec.get("name", "")
        score = 0.0
        reasons: List[str] = []
        change_pct = rec.get("change_pct", 0)
        turnover = rec.get("turnover")

        # 1. 当日涨幅越接近涨停越好
        if change_pct is not None:
            if change_pct >= 8:
                score += 20
                reasons.append(f"涨{change_pct:.1f}%接近涨停+20")
            elif change_pct >= 7:
                score += 15
                reasons.append(f"涨{change_pct:.1f}%+15")
            elif change_pct >= 5:
                score += 10
                reasons.append(f"涨{change_pct:.1f}%+10")

        # 2. 技术面分析
        history = self._call_with_timeout(
            lambda: self.fetcher.get_history_data(code, days=65),
            timeout_sec=8.0,
            fallback=None,
            task_name=f"首板预测 {code}",
        )

        industry = ""
        ma_bullish = False
        position_60d = None
        vol_ratio = None
        trend_10d = None

        if history is not None and not history.empty and len(history) >= 10:
            df = history.sort_values("date").reset_index(drop=True)
            close = pd.to_numeric(df["close"], errors="coerce")
            volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)
            change = pd.to_numeric(df.get("change_pct"), errors="coerce") if "change_pct" in df.columns else pd.Series(dtype=float)

            ma5 = close.rolling(5, min_periods=5).mean()
            ma10 = close.rolling(10, min_periods=10).mean()
            ma20 = close.rolling(20, min_periods=20).mean()

            latest_ma5 = float(ma5.iloc[-1]) if not pd.isna(ma5.iloc[-1]) else None
            latest_ma10 = float(ma10.iloc[-1]) if not pd.isna(ma10.iloc[-1]) else None
            latest_ma20 = float(ma20.iloc[-1]) if not pd.isna(ma20.iloc[-1]) else None
            latest_close = float(close.iloc[-1]) if not pd.isna(close.iloc[-1]) else None

            # 均线多头排列
            if (latest_ma5 is not None and latest_ma10 is not None and latest_ma20 is not None
                    and latest_ma5 > latest_ma10 > latest_ma20):
                score += 10
                reasons.append("多头排列+10")
                ma_bullish = True

            # 站上MA5
            if latest_close is not None and latest_ma5 is not None and latest_close > latest_ma5:
                score += 5
                reasons.append("站上MA5+5")

            # 60日位置分位
            if len(close) >= 20 and latest_close is not None:
                window = close.tail(min(60, len(close))).dropna()
                if len(window) >= 10:
                    position_60d = float((window < latest_close).sum()) / len(window) * 100
                    if position_60d < 30:
                        score += 10
                        reasons.append(f"低位{position_60d:.0f}%+10")
                    elif position_60d > 80:
                        score -= 5
                        reasons.append(f"高位{position_60d:.0f}%-5")

            # 10日趋势
            if len(close) >= 11 and not pd.isna(close.iloc[-11]) and close.iloc[-11] > 0:
                trend_10d = (float(close.iloc[-1]) / float(close.iloc[-11]) - 1) * 100
                if 5 <= trend_10d <= 20:
                    score += 5
                    reasons.append(f"10日涨{trend_10d:.1f}%+5")

            # 量能蓄势：近3日缩量后今日放量
            if len(volume) >= 6 and not pd.isna(volume.iloc[-1]):
                prev_3_vol = volume.iloc[-4:-1].dropna()
                prev_5_vol = volume.iloc[-6:-1].dropna()
                if not prev_5_vol.empty and float(prev_5_vol.mean()) > 0:
                    vol_ratio = float(volume.iloc[-1]) / float(prev_5_vol.mean())
                    if not prev_3_vol.empty and float(prev_3_vol.mean()) > 0:
                        recent_shrink = float(prev_3_vol.mean()) / float(prev_5_vol.mean())
                        # 前3日缩量（<0.8倍均量）且今日放量（>1.5倍）
                        if recent_shrink < 0.8 and vol_ratio > 1.5:
                            score += 15
                            reasons.append(f"缩量蓄势后放量{vol_ratio:.1f}x+15")
                        elif vol_ratio > 2.0:
                            score += 10
                            reasons.append(f"放量{vol_ratio:.1f}x+10")
                        elif vol_ratio > 1.5:
                            score += 5
                            reasons.append(f"温和放量{vol_ratio:.1f}x+5")

            # MACD金叉信号
            try:
                from stock_indicators import calc_macd
                macd_data = calc_macd(close)
                dif = macd_data["dif"]
                dea = macd_data["dea"]
                if (len(dif) >= 2 and not pd.isna(dif.iloc[-1]) and not pd.isna(dif.iloc[-2])
                        and not pd.isna(dea.iloc[-1]) and not pd.isna(dea.iloc[-2])):
                    # 今日DIF上穿DEA 或 DIF>DEA且差值扩大
                    if dif.iloc[-2] <= dea.iloc[-2] and dif.iloc[-1] > dea.iloc[-1]:
                        score += 10
                        reasons.append("MACD金叉+10")
                    elif dif.iloc[-1] > dea.iloc[-1] > 0:
                        score += 5
                        reasons.append("MACD多头+5")
            except Exception:
                pass

        # 3. 板块热度
        if industry and hot_industries.get(industry, 0) >= 3:
            score += 10
            reasons.append(f"热门板块({hot_industries[industry]}只)+10")
        elif industry and hot_industries.get(industry, 0) >= 2:
            score += 5
            reasons.append(f"板块有{hot_industries[industry]}只+5")

        # 4. 换手率
        if turnover is not None:
            if 5 <= turnover <= 20:
                score += 5
                reasons.append(f"换手{turnover:.1f}%适中+5")
            elif turnover > 40:
                score -= 5
                reasons.append(f"换手{turnover:.1f}%过高-5")

        final_score = max(0, min(100, int(round(score))))
        return {
            "code": code,
            "name": name,
            "industry": industry,
            "close": rec.get("close"),
            "change_pct": change_pct,
            "turnover": turnover,
            "vol_ratio": round(vol_ratio, 2) if vol_ratio is not None else None,
            "position_60d": round(position_60d, 1) if position_60d is not None else None,
            "trend_10d": round(trend_10d, 1) if trend_10d is not None else None,
            "ma_bullish": ma_bullish,
            "score": final_score,
            "reasons": " / ".join(reasons[:8]),
            "predict_type": "首板候选",
        }
