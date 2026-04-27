from __future__ import annotations

import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED, TimeoutError as FutureTimeoutError
from typing import List, Dict, Any, Optional, Callable, Tuple
import threading

import pandas as pd

from scan_models import FilterSettings, HistoryRequestPlan
from src.models.analysis_models import HistoryAnalysisConfig
from src.services.history_analysis_service import HistoryAnalysisService
from src.utils.cancel_token import CancelToken, coerce_should_stop
from stock_data import StockDataFetcher, DaemonThreadPoolExecutor
from stock_logger import get_logger
from stock_store import save_last_limit_up_prediction

logger = get_logger(__name__)


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
            strong_ft_enabled=bool(self.strong_ft_enabled),
            strong_ft_max_pullback_pct=float(self.strong_ft_max_pullback_pct),
            strong_ft_max_volume_ratio=float(self.strong_ft_max_volume_ratio),
            strong_ft_min_hold_days=int(self.strong_ft_min_hold_days),
        )

    def apply_settings(self, settings: FilterSettings) -> None:
        self.trend_days = max(1, int(settings.trend_days))
        self.ma_period = max(1, int(settings.ma_period))
        self.limit_up_lookback_days = max(1, int(settings.limit_up_lookback_days))
        self.volume_lookback_days = max(1, int(settings.volume_lookback_days))
        self.volume_expand_enabled = bool(settings.volume_expand_enabled)
        self.volume_expand_factor = max(1.0, float(settings.volume_expand_factor))
        self.require_limit_up_within_days = bool(settings.require_limit_up_within_days)
        self.strong_ft_enabled = bool(settings.strong_ft_enabled)
        self.strong_ft_max_pullback_pct = max(0.0, float(settings.strong_ft_max_pullback_pct))
        self.strong_ft_max_volume_ratio = max(0.0, float(settings.strong_ft_max_volume_ratio))
        self.strong_ft_min_hold_days = max(0, int(settings.strong_ft_min_hold_days))

    _timeout_pool: Optional[ThreadPoolExecutor] = None
    _timeout_pool_lock = threading.Lock()

    @classmethod
    def _get_timeout_pool(cls) -> ThreadPoolExecutor:
        if cls._timeout_pool is None:
            with cls._timeout_pool_lock:
                if cls._timeout_pool is None:
                    cls._timeout_pool = DaemonThreadPoolExecutor(
                        max_workers=4, thread_name_prefix="timeout"
                    )
        return cls._timeout_pool

    def _call_with_timeout(
        self,
        task: Callable[[], Any],
        timeout_sec: float,
        fallback: Any = None,
        task_name: str = "任务",
        cancel_token: Optional[CancelToken] = None,
    ) -> Any:
        # 已取消：直接跳过，连调度都不做
        if cancel_token is not None and cancel_token.is_cancelled():
            return fallback
        pool = self._get_timeout_pool()
        future = pool.submit(task)
        deadline = time.time() + max(0.5, float(timeout_sec))
        try:
            # 用短轮询等待，这样取消信号到来时最多等一个 poll 间隔
            if cancel_token is None:
                return future.result(timeout=max(0.5, float(timeout_sec)))
            poll = 0.2
            while True:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise FutureTimeoutError()
                if cancel_token.is_cancelled():
                    future.cancel()
                    return fallback
                try:
                    return future.result(timeout=min(poll, remaining))
                except FutureTimeoutError:
                    continue
        except FutureTimeoutError:
            future.cancel()
            if self._log:
                self._log(f"{task_name} 超时（>{timeout_sec:.0f}s），已跳过。")
            return fallback
        except Exception as exc:
            if self._log:
                self._log(f"{task_name} 失败: {exc}")
            return fallback

    def check_close_above_ma(
        self, history_data: pd.DataFrame, streak_days: int, ma_period: int
    ) -> bool:
        return self._build_analysis_service().check_close_above_ma(
            history_data,
            streak_days=streak_days,
            ma_period=ma_period,
        )

    def _resolve_analysis_config(
        self,
        *,
        streak_days: Optional[int] = None,
        ma_period: Optional[int] = None,
        limit_up_lookback_days: Optional[int] = None,
        volume_lookback_days: Optional[int] = None,
        volume_expand_enabled: Optional[bool] = None,
        volume_expand_factor: Optional[float] = None,
    ) -> HistoryAnalysisConfig:
        return HistoryAnalysisConfig.from_filter_settings(
            self.get_settings(),
            trend_days=streak_days,
            ma_period=ma_period,
            limit_up_lookback_days=limit_up_lookback_days,
            volume_lookback_days=volume_lookback_days,
            volume_expand_enabled=volume_expand_enabled,
            volume_expand_factor=volume_expand_factor,
        )

    def _build_analysis_service(
        self,
        *,
        streak_days: Optional[int] = None,
        ma_period: Optional[int] = None,
        limit_up_lookback_days: Optional[int] = None,
        volume_lookback_days: Optional[int] = None,
        volume_expand_enabled: Optional[bool] = None,
        volume_expand_factor: Optional[float] = None,
    ) -> HistoryAnalysisService:
        config = self._resolve_analysis_config(
            streak_days=streak_days,
            ma_period=ma_period,
            limit_up_lookback_days=limit_up_lookback_days,
            volume_lookback_days=volume_lookback_days,
            volume_expand_enabled=volume_expand_enabled,
            volume_expand_factor=volume_expand_factor,
        )
        return HistoryAnalysisService(config)

    def _limit_up_threshold(self, board: str = "", stock_name: str = "") -> float:
        return self._build_analysis_service().limit_up_threshold(
            board=board,
            stock_name=stock_name,
        )

    @staticmethod
    def _calculate_limit_up_streak(mask: pd.Series) -> int:
        """计算从最新交易日往前数的连续涨停天数。"""
        streak = 0
        for flag in reversed(mask.tolist()):
            if bool(flag):
                streak += 1
            else:
                break
        return streak

    def _calculate_trade_score(
        self,
        result: Dict[str, Any],
        streak_days: int,
        ma_period: int,
        volume_enabled: bool,
    ) -> tuple[int, str]:
        return self._build_analysis_service(
            streak_days=streak_days,
            ma_period=ma_period,
            volume_expand_enabled=volume_enabled,
        ).calculate_trade_score(result)

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
        return self._build_analysis_service(
            streak_days=streak_days,
            ma_period=ma_period,
            limit_up_lookback_days=limit_up_lookback_days,
            volume_lookback_days=volume_lookback_days,
            volume_expand_enabled=volume_expand_enabled,
            volume_expand_factor=volume_expand_factor,
        ).analyze_history(
            history_data,
            board=board,
            stock_name=stock_name,
            stock_code=stock_code,
        )

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

    def _apply_strong_followthrough_failure(
        self,
        result: Dict[str, Any],
        analysis: Dict[str, Any],
    ) -> bool:
        """当开启"承接强势"过滤时，未命中形态的股票直接淘汰。"""
        if not getattr(self, "strong_ft_enabled", False):
            return False
        ft = analysis.get("strong_followthrough") or {}
        if ft.get("has_strong_followthrough"):
            return False
        reason = self._build_strong_ft_failure_reason(ft)
        analysis["summary"] = (
            f"{analysis['summary']}；{reason}" if analysis.get("summary") else reason
        )
        result["reasons"].append(reason)
        return True

    def _build_strong_ft_failure_reason(self, ft: Dict[str, Any]) -> str:
        """把 followthrough 结果翻译成人类友好的失败原因。"""
        if ft.get("limit_up_is_today"):
            return f"{ft.get('limit_up_date')} 刚涨停，次日走势还未出现，无法判断承接"
        if not ft.get("limit_up_date"):
            return f"近{self.limit_up_lookback_days}日未找到可承接的涨停日"
        parts = [f"{ft['limit_up_date']} 涨停后"]
        if not ft.get("is_pullback_day"):
            parts.append("次日未回落（未形成承接形态）")
        if not ft.get("pullback_within_limit"):
            parts.append(
                f"回撤过深（{ft.get('pullback_pct', 0):.1f}% > {self.strong_ft_max_pullback_pct:.1f}%）"
            )
        if not ft.get("volume_shrunk"):
            parts.append(
                f"未缩量（次日量比 {ft.get('pullback_volume_ratio', 0):.0%} > {self.strong_ft_max_volume_ratio:.0%}）"
            )
        if not ft.get("holds_above_pullback_low"):
            parts.append("后续跌破回落日最低价")
        elif ft.get("hold_days", 0) < ft.get("min_hold_days", 0):
            parts.append(f"站稳天数不足（{ft.get('hold_days', 0)} < {ft.get('min_hold_days', 0)}）")
        return "；".join(parts)

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
        if self._apply_strong_followthrough_failure(result, analysis):
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

    @staticmethod
    def _build_local_cache_history_plan(reason: str = "local-cache-only") -> HistoryRequestPlan:
        return HistoryRequestPlan(
            mode="cache_only",
            provider_sequence=("local-cache",),
            mirror_urls=(),
            reason=reason,
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
        cancel_token: Optional[CancelToken] = None,
    ) -> List[Dict[str, Any]]:
        # 兼容旧接口：将 should_stop 回调与 CancelToken 合并成同一个谓词
        should_stop = coerce_should_stop(cancel_token, should_stop)
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

        # 提交前再检查一次：用户可能在加载股票池阶段就点了停止
        if self._should_stop_scan(should_stop, log):
            return results

        executor = DaemonThreadPoolExecutor(max_workers=workers)
        try:
            future_to_meta = self._submit_scan_tasks(executor, assigned_jobs, available_mirrors, history_plan)
            if log:
                log("【阶段 3/3】开始逐只拉取历史日线并计算结果...")

            pending = set(future_to_meta)
            while pending:
                # 更短的轮询周期，让取消信号更快生效；同时在每轮起点主动检查一次
                if self._should_stop_scan(should_stop, log):
                    for fut in pending:
                        fut.cancel()
                    break
                done, pending = wait(pending, timeout=0.5, return_when=FIRST_COMPLETED)
                if not done:
                    if self._should_stop_scan(should_stop, log):
                        for fut in pending:
                            fut.cancel()
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
                        for p in pending:
                            p.cancel()
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
                except Exception as exc:
                    logger.debug("预取数据 %s 异常: %s", key, exc)

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
        cache_only: bool = False,
    ) -> None:
        """批量预取涨停池股票的历史数据到本地缓存。
        已有缓存的跳过，缺失的并行拉取，确保后续分类时不再逐只走网络。
        
        参数:
            cache_only: True=只使用本地缓存，不发起网络请求；False=允许网络请求补全缓存
        """
        from stock_data import _is_history_cache_fresh
        need_fetch: List[str] = []
        cached_count = 0
        for code in codes:
            c = str(code).strip().zfill(6)
            # 检查缓存是否新鲜（至少需要10行数据）
            if _is_history_cache_fresh(c, min(10, days)):
                cached_count += 1
                continue
            # 再检查 SQLite 有没有足够行数
            from stock_store import load_history as _load_h
            cached = _load_h(c, limit=days)
            if cached is not None and not cached.empty and len(cached) >= min(10, days):
                cached_count += 1
            else:
                need_fetch.append(c)

        if self._log:
            self._log(f"涨停分类：缓存命中 {cached_count}/{len(codes)}，需预取 {len(need_fetch)} 只")

        if not need_fetch:
            if progress_callback:
                progress_callback(len(codes), len(codes), "全部已有缓存")
            return

        # 如果只使用缓存，跳过网络请求
        if cache_only:
            if self._log:
                self._log(f"涨停分类：cache-only 模式，跳过 {len(need_fetch)} 只无缓存股票的网络请求")
            if progress_callback:
                progress_callback(len(codes) - len(need_fetch), len(codes), f"cache-only: {len(need_fetch)}只无缓存")
            return

        total = len(need_fetch)
        if self._log:
            self._log(f"涨停分类：需预取 {total}/{len(codes)} 只股票的历史数据")

        completed = 0
        completed_lock = threading.Lock()

        def _fetch_one(code: str) -> None:
            nonlocal completed
            try:
                self.fetcher.get_history_data(code, days=days, force_refresh=False)
            except Exception as exc:
                logger.debug("预取历史 %s 失败: %s", code, exc)
            finally:
                with completed_lock:
                    completed += 1
                if progress_callback:
                    progress_callback(completed, total, f"预取 {code}")

        # 根据股票数量动态调整并发数，上限不超过历史接口并发限制
        history_limit = self.fetcher.history_request_concurrency_limit()
        workers = min(max(4, total // 3), history_limit, 8)
        workers = max(1, min(workers, total))

        executor = DaemonThreadPoolExecutor(max_workers=workers, thread_name_prefix="zt-prefetch")
        try:
            futures = [executor.submit(_fetch_one, c) for c in need_fetch]
            from concurrent.futures import as_completed
            for fut in as_completed(futures):
                try:
                    fut.result(timeout=15.0)  # 单只股票最多15秒
                except Exception as exc:
                    logger.debug("预取 future 异常: %s", exc)
        finally:
            executor.shutdown(wait=True)  # 等待所有任务完成，不取消

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
            except Exception as exc:
                logger.debug("分时数据获取失败 %s: %s", code, exc)
                intraday_payload = {}
            try:
                history_df = fut_history.result()
            except Exception as exc:
                logger.debug("历史数据获取失败 %s: %s", code, exc)
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

    def _extract_pre_limit_up_features(
        self,
        code: str,
        limit_up_date_idx: int,
        df: pd.DataFrame,
        close: pd.Series,
        volume: pd.Series,
        amount: pd.Series,
        change_pct: pd.Series,
    ) -> Optional[Dict[str, Any]]:
        """提取某只股票在涨停日前一天（T-1）的特征快照。

        返回特征字典，数据不足则返回 None。
        """
        t = limit_up_date_idx  # 涨停日在 df 中的行索引
        if t < 6:
            return None  # 至少需要前6天数据

        feat: Dict[str, Any] = {}

        # --- T-1 日（涨停前一天）的特征 ---
        prev = t - 1

        # 涨跌幅
        feat["change_pct_t1"] = float(change_pct.iloc[prev]) if not pd.isna(change_pct.iloc[prev]) else None

        # 收盘价
        prev_close = float(close.iloc[prev]) if not pd.isna(close.iloc[prev]) else None
        feat["close_t1"] = prev_close

        # 成交量 / 成交额
        feat["volume_t1"] = float(volume.iloc[prev]) if not pd.isna(volume.iloc[prev]) else None
        feat["amount_t1"] = float(amount.iloc[prev]) if not pd.isna(amount.iloc[prev]) else None

        # 量比：T-1 成交量 / 前5日均量
        vol_window = volume.iloc[max(0, prev - 5):prev].dropna()
        if feat["volume_t1"] and not vol_window.empty and float(vol_window.mean()) > 0:
            feat["vol_ratio_t1"] = round(feat["volume_t1"] / float(vol_window.mean()), 2)
        else:
            feat["vol_ratio_t1"] = None

        # 额比：T-1 成交额 / 前5日均额
        amt_window = amount.iloc[max(0, prev - 5):prev].dropna()
        if feat["amount_t1"] and not amt_window.empty and float(amt_window.mean()) > 0:
            feat["amt_ratio_t1"] = round(feat["amount_t1"] / float(amt_window.mean()), 2)
        else:
            feat["amt_ratio_t1"] = None

        # 缩量比：前3日均量 / 前5日均量（判断是否蓄势）
        vol_3 = volume.iloc[max(0, prev - 3):prev].dropna()
        vol_5 = volume.iloc[max(0, prev - 5):prev].dropna()
        if not vol_3.empty and not vol_5.empty and float(vol_5.mean()) > 0:
            feat["shrink_ratio_t1"] = round(float(vol_3.mean()) / float(vol_5.mean()), 2)
        else:
            feat["shrink_ratio_t1"] = None

        # 均线距离
        ma5 = close.rolling(5, min_periods=5).mean()
        ma10 = close.rolling(10, min_periods=10).mean()
        ma20 = close.rolling(20, min_periods=20).mean()

        ma5_val = float(ma5.iloc[prev]) if not pd.isna(ma5.iloc[prev]) else None
        ma10_val = float(ma10.iloc[prev]) if not pd.isna(ma10.iloc[prev]) else None
        ma20_val = float(ma20.iloc[prev]) if not pd.isna(ma20.iloc[prev]) else None

        if prev_close and ma5_val and ma5_val > 0:
            feat["dist_ma5_pct"] = round((prev_close / ma5_val - 1) * 100, 2)
        else:
            feat["dist_ma5_pct"] = None
        if prev_close and ma10_val and ma10_val > 0:
            feat["dist_ma10_pct"] = round((prev_close / ma10_val - 1) * 100, 2)
        else:
            feat["dist_ma10_pct"] = None

        # 多头排列
        feat["ma_bullish"] = bool(
            ma5_val is not None and ma10_val is not None and ma20_val is not None
            and ma5_val > ma10_val > ma20_val
        )

        # 站上 MA5
        feat["above_ma5"] = bool(prev_close is not None and ma5_val is not None and prev_close > ma5_val)

        # 回踩MA5：收盘接近或略低于MA5（距MA5在 -3%~+1% 之间），且前几日曾在MA5之上
        feat["ma5_pullback"] = False
        if prev_close is not None and ma5_val is not None and ma5_val > 0:
            dist = (prev_close / ma5_val - 1) * 100
            if -3.0 <= dist <= 1.0:
                # 检查前3~5日是否曾站上MA5（确认是回踩而非下跌趋势）
                was_above = False
                for lookback in range(2, min(6, prev + 1)):
                    idx_back = prev - lookback
                    if idx_back >= 0 and not pd.isna(close.iloc[idx_back]) and not pd.isna(ma5.iloc[idx_back]):
                        if float(close.iloc[idx_back]) > float(ma5.iloc[idx_back]) * 1.01:
                            was_above = True
                            break
                feat["ma5_pullback"] = was_above

        # 5日涨幅
        if prev >= 5 and not pd.isna(close.iloc[prev - 5]) and close.iloc[prev - 5] > 0:
            feat["trend_5d"] = round((prev_close / float(close.iloc[prev - 5]) - 1) * 100, 2) if prev_close else None
        else:
            feat["trend_5d"] = None

        # 10日涨幅
        if prev >= 10 and not pd.isna(close.iloc[prev - 10]) and close.iloc[prev - 10] > 0:
            feat["trend_10d"] = round((prev_close / float(close.iloc[prev - 10]) - 1) * 100, 2) if prev_close else None
        else:
            feat["trend_10d"] = None

        # 60日位置分位
        window = close.iloc[max(0, prev - 59):prev + 1].dropna()
        if len(window) >= 10 and prev_close is not None:
            feat["position_60d"] = round(float((window < prev_close).sum()) / len(window) * 100, 1)
        else:
            feat["position_60d"] = None

        # 近10日振幅（波动率）
        recent_close = close.iloc[max(0, prev - 10):prev].dropna()
        if len(recent_close) >= 5 and recent_close.mean() > 0:
            feat["volatility_10d"] = round(float(recent_close.std() / recent_close.mean() * 100), 2)
        else:
            feat["volatility_10d"] = None

        # 换手率
        turnover = pd.to_numeric(df.get("turnover_rate"), errors="coerce") if "turnover_rate" in df.columns else pd.Series(dtype=float)
        feat["turnover_t1"] = float(turnover.iloc[prev]) if not turnover.empty and prev < len(turnover) and not pd.isna(turnover.iloc[prev]) else None

        return feat

    def analyze_pre_limit_up_profile(
        self,
        lookback_days: int = 5,
        trade_date: Optional[str] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, Any]:
        """回溯分析最近 N 个交易日涨停股在涨停前的特征。

        步骤：
        1. 获取最近 N 天的涨停池
        2. 对每只首板涨停股，拉取历史数据，提取涨停前 T-1 日特征
        3. 汇总统计各特征的分布（中位数、均值、分位）

        返回:
            feature_samples: 每只涨停股的特征样本列表
            profile: 聚合的特征画像（中位数/均值/分位）
            sample_count: 样本数量
            trade_dates: 回溯的交易日期列表
        """
        if self._log:
            self._log(f"涨停画像：回溯最近 {lookback_days} 个交易日涨停股特征...")

        # 获取最近 N 个交易日
        from datetime import datetime as _dt
        base_trade_date = str(trade_date or "").strip() or _dt.now().strftime("%Y%m%d")
        trade_dates = self.fetcher._recent_trade_dates(base_trade_date, lookback_days)
        if not trade_dates:
            return {"feature_samples": [], "profile": {}, "sample_count": 0, "trade_dates": []}

        # 收集所有首板涨停股（加总超时保护，防止东财不可达时无限阻塞）
        import time as _time
        _pool_deadline = _time.time() + 45.0  # 涨停池获取总时限 45 秒
        all_first_board: List[Dict[str, Any]] = []
        for d in trade_dates:
            if _time.time() > _pool_deadline:
                if self._log:
                    self._log(f"涨停画像：获取涨停池超时（已超 45s），使用已获取的数据继续")
                break
            try:
                pool = self.fetcher.get_limit_up_pool(d)
            except Exception as e:
                if self._log:
                    self._log(f"涨停画像：获取 {d} 涨停池失败: {e}，跳过该日")
                continue
            if pool is None or pool.empty:
                continue
            if "连板数" in pool.columns:
                first = pool[pool["连板数"] == 1]
            else:
                first = pool
            for _, row in first.iterrows():
                code = str(row.get("代码", "")).strip().zfill(6)
                name = str(row.get("名称", ""))
                industry = str(row.get("所属行业", ""))
                if "ST" in name.upper():
                    continue
                all_first_board.append({
                    "code": code, "name": name, "industry": industry,
                    "limit_up_date": d,
                })

        if not all_first_board:
            return {"feature_samples": [], "profile": {}, "sample_count": 0, "trade_dates": trade_dates}

        if self._log:
            self._log(f"涨停画像：共 {len(all_first_board)} 只首板涨停股，正在提取涨停前特征...")

        # 预取历史数据（只使用本地缓存，不发起网络请求）
        codes = list({r["code"] for r in all_first_board})
        self._prefetch_history_for_pool(codes, days=65, progress_callback=progress_callback, cache_only=True)
        local_cache_plan = self._build_local_cache_history_plan(reason="predict-profile-cache-only")

        # 逐只提取特征（只从本地缓存读取）
        feature_samples: List[Dict[str, Any]] = []
        total = len(all_first_board)
        prepared_history: Dict[str, Optional[Tuple[pd.DataFrame, pd.Series, pd.Series, pd.Series, pd.Series]]] = {}
        for idx, rec in enumerate(all_first_board):
            code = rec["code"]
            limit_date = rec["limit_up_date"]

            if code not in prepared_history:
                try:
                    # 只使用本地缓存，不发起网络请求
                    history = self.fetcher.get_history_data(
                        code,
                        days=65,
                        force_refresh=False,
                        request_plan=local_cache_plan,
                    )
                except Exception as exc:
                    logger.debug("涨停分类获取历史 %s 失败: %s", code, exc)
                    history = None

                if history is None or history.empty or len(history) < 10:
                    prepared_history[code] = None
                else:
                    df = history.sort_values("date").reset_index(drop=True)
                    df["date"] = df["date"].astype(str).str.strip().str.replace("-", "")
                    close = pd.to_numeric(df["close"], errors="coerce")
                    volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)
                    amount = pd.to_numeric(df.get("amount"), errors="coerce") if "amount" in df.columns else pd.Series(dtype=float)
                    change_pct = pd.to_numeric(df.get("change_pct"), errors="coerce") if "change_pct" in df.columns else pd.Series(dtype=float)
                    prepared_history[code] = (df, close, volume, amount, change_pct)

            prepared = prepared_history.get(code)
            if prepared is None:
                continue

            df, close, volume, amount, change_pct = prepared

            # 找到涨停日在历史中的位置
            match_idx = df.index[df["date"] == limit_date].tolist()
            if not match_idx:
                continue
            t = match_idx[0]

            feat = self._extract_pre_limit_up_features(code, t, df, close, volume, amount, change_pct)
            if feat is None:
                continue
            feat["code"] = code
            feat["name"] = rec["name"]
            feat["industry"] = rec["industry"]
            feat["limit_up_date"] = limit_date
            feature_samples.append(feat)

            if progress_callback:
                progress_callback(idx + 1, total, f"画像 {code} {rec['name']}")

        # 汇总统计
        profile = self._aggregate_profile(feature_samples)

        if self._log:
            self._log(f"涨停画像：成功提取 {len(feature_samples)} 个样本的涨停前特征")

        return {
            "feature_samples": feature_samples,
            "profile": profile,
            "sample_count": len(feature_samples),
            "trade_dates": trade_dates,
        }

    @staticmethod
    def _aggregate_profile(samples: List[Dict[str, Any]]) -> Dict[str, Any]:
        """汇总特征样本，计算各指标的中位数/均值/分位数分布。"""
        if not samples:
            return {}

        numeric_keys = [
            "change_pct_t1", "vol_ratio_t1", "amt_ratio_t1", "shrink_ratio_t1",
            "dist_ma5_pct", "dist_ma10_pct", "trend_5d", "trend_10d",
            "position_60d", "volatility_10d", "turnover_t1",
        ]
        bool_keys = ["ma_bullish", "above_ma5", "ma5_pullback"]

        profile: Dict[str, Any] = {}
        for key in numeric_keys:
            values = [s[key] for s in samples if s.get(key) is not None]
            if not values:
                profile[key] = {"median": None, "mean": None, "p25": None, "p75": None, "count": 0}
                continue
            sorted_v = sorted(values)
            n = len(sorted_v)
            profile[key] = {
                "median": round(sorted_v[n // 2], 2),
                "mean": round(sum(sorted_v) / n, 2),
                "p25": round(sorted_v[max(0, n // 4)], 2),
                "p75": round(sorted_v[max(0, n * 3 // 4)], 2),
                "min": round(sorted_v[0], 2),
                "max": round(sorted_v[-1], 2),
                "count": n,
            }

        for key in bool_keys:
            true_count = sum(1 for s in samples if s.get(key))
            profile[key] = {
                "true_count": true_count,
                "total": len(samples),
                "ratio": round(true_count / max(len(samples), 1) * 100, 1),
            }

        return profile

    def predict_limit_up_candidates(
        self,
        trade_date: str,
        lookback_days: int = 5,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, Any]:
        """基于涨停对比 + 五日承接数据预测明日涨停候选。

        步骤：
        1. 回看最近 N 日涨停对比，统计昨日首板晋级率等环境数据
        2. 保留涨停候选：对今日涨停股按封板质量 + 近期晋级环境评分
        3. 五日承接候选：从今日强势股中挑出"涨停→回落缩量→站稳"的股票

        返回字段沿用旧结构，便于 GUI 直接复用：
            profile: 兼容旧 UI，现固定为空
            continuation_candidates: 保留涨停/连板候选
            first_board_candidates: 五日承接候选
            hot_industries: 今日涨停行业分布
            summary: 文字摘要
        """
        # 阶段1：回看最近 N 日涨停对比环境
        if self._log:
            self._log(f"涨停预测：阶段1 - 统计最近 {lookback_days} 日涨停对比环境...")
        if progress_callback:
            progress_callback(0, 1, "统计最近涨停对比环境...")

        profile: Dict[str, Any] = {}
        feature_samples: List[Dict[str, Any]] = []
        compare_context = self._build_compare_market_context(trade_date, lookback_days)

        # 阶段2：获取今日涨停池 + 全市场行情
        if self._log:
            self._log(f"涨停预测：阶段2 - 获取 {trade_date} 涨停池 + 全市场行情...")
        if progress_callback:
            progress_callback(0, 1, "获取今日涨停池...")

        # 并行获取涨停池和全市场行情快照
        today_pool_df: Optional[pd.DataFrame] = None
        spot_df: Optional[pd.DataFrame] = None
        zt_codes: set = set()

        def _fetch_pool():
            nonlocal today_pool_df
            today_pool_df = self.fetcher.get_limit_up_pool(trade_date)

        def _fetch_spot():
            nonlocal spot_df
            spot_df = self._fetch_spot_snapshot()

        # 使用线程池并行获取两个数据源。这里不能用 `with`，否则退出上下文时会 wait=True，
        # 即使 result(timeout=...) 超时了，仍然会继续等待后台任务跑完。
        executor = DaemonThreadPoolExecutor(max_workers=2, thread_name_prefix="stage2")
        try:
            future_pool = executor.submit(_fetch_pool)
            future_spot = executor.submit(_fetch_spot)

            try:
                # 涨停池最多 15 秒（底层 _gupiao_request_with_retry 有 20s deadline 兜底）
                future_pool.result(timeout=15.0)
            except FutureTimeoutError as e:
                if self._log:
                    self._log(f"涨停预测：获取涨停池超时 (get_limit_up_pool): {e}")
            except Exception as e:
                if self._log:
                    self._log(f"涨停预测：获取涨停池失败 (get_limit_up_pool): {e}")

            try:
                # 全市场行情：东财约5秒，新浪约30秒
                from stock_data import _eastmoney_circuit_breaker_open
                _spot_timeout = 45.0 if _eastmoney_circuit_breaker_open() else 20.0
                future_spot.result(timeout=_spot_timeout)
            except FutureTimeoutError as e:
                if self._log:
                    self._log(f"涨停预测：获取全市场行情超时 (5000+只股票): {e}")
                    self._log("涨停预测：将跳过首板候选筛选，继续执行连板延续分析")
            except Exception as e:
                if self._log:
                    self._log(f"涨停预测：获取全市场行情失败: {e}")
                    self._log("涨停预测：将跳过首板候选筛选，继续执行连板延续分析")
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        if today_pool_df is None or today_pool_df.empty:
            non_trading = False
            try:
                from datetime import datetime as _dt2
                from src.utils.trade_calendar import _get_trade_calendar, _is_trading_day
                parsed = _dt2.strptime(str(trade_date).strip(), "%Y%m%d").date()
                non_trading = not _is_trading_day(parsed, _get_trade_calendar())
            except Exception:
                pass
            summary = (
                f"{trade_date} 非交易日（周末/节假日），无涨停池数据"
                if non_trading
                else f"{trade_date} 未获取到涨停池数据"
            )
            result = {
                "trade_date": trade_date,
                "profile": profile,
                "profile_samples": feature_samples,
                "continuation_candidates": [],
                "first_board_candidates": [],
                "hot_industries": {},
                "summary": summary,
            }
            try:
                save_last_limit_up_prediction(result)
            except Exception:
                pass
            return result

        all_pool_records = self._parse_full_pool(today_pool_df)
        if self._log:
            self._log(f"涨停预测：解析涨停池完成，共 {len(all_pool_records)} 只")
        hot_industries = self._count_pool_industries(today_pool_df)
        if self._log:
            self._log(f"涨停预测：统计热门行业完成，共 {len(hot_industries)} 个行业")

        if not today_pool_df.empty and "代码" in today_pool_df.columns:
            zt_codes = set(today_pool_df["代码"].astype(str).str.strip().str.zfill(6))
            if self._log:
                self._log(f"涨停预测：提取涨停股代码 {len(zt_codes)} 只")

        # 阶段3：统一预取所有需要的历史数据（一次搞定）
        if self._log:
            self._log("涨停预测：阶段3 - 统一预取历史数据...")

        # 收集所有需要历史数据的股票代码
        pool_codes = [r["code"] for r in all_pool_records]
        candidate_codes: List[str] = []
        if spot_df is not None and not spot_df.empty:
            if self._log:
                self._log(f"涨停预测：开始筛选强势股（全市场 {len(spot_df)} 只）...")
            strong = self._filter_strong_stocks(spot_df, zt_codes)
            if self._log:
                self._log(f"涨停预测：筛选强势股完成，共 {len(strong)} 只")
            pullback = self._filter_ma5_pullback_stocks(spot_df, zt_codes)
            if self._log:
                self._log(f"涨停预测：筛选回踩MA5完成，共 {len(pullback)} 只")
            seen = set()
            for rec in strong + pullback:
                if rec["code"] not in seen:
                    seen.add(rec["code"])
                    candidate_codes.append(rec["code"])
        else:
            if self._log:
                self._log("涨停预测：无全市场行情，跳过强势股筛选（首板候选将不可用）")

        all_codes = list(set(pool_codes + candidate_codes))
        if self._log:
            self._log(f"涨停预测：统一预取 {len(all_codes)} 只股票历史数据"
                      f"（涨停池{len(pool_codes)} + 候选{len(candidate_codes)}）")
        # 只使用本地缓存，不发起网络请求
        self._prefetch_history_for_pool(all_codes, days=65, progress_callback=progress_callback, cache_only=True)
        if self._log:
            self._log("涨停预测：阶段3完成 - 历史数据预取结束")

        # 阶段4：保留涨停 / 连板延续候选评分
        if self._log:
            self._log(f"涨停预测：阶段4 - 分析 {len(all_pool_records)} 只涨停股的保留涨停潜力...")

        continuation_candidates = []
        for idx, rec in enumerate(all_pool_records):
            score_info = self._score_continuation_by_compare(rec, hot_industries, compare_context)
            if score_info["score"] >= 40:
                continuation_candidates.append(score_info)
            if progress_callback:
                progress_callback(idx + 1, len(all_pool_records),
                                  f"保留涨停分析 {rec['code']} {rec.get('name', '')}")
        continuation_candidates.sort(key=lambda x: -x["score"])

        # 阶段5：五日承接候选（历史数据 + 行情都已缓存）
        if self._log:
            self._log("涨停预测：阶段5 - 识别五日承接候选...")

        first_board_candidates = self._scan_followthrough_candidates_cached(
            hot_industries, spot_df, zt_codes, progress_callback, lookback_days=lookback_days,
        )

        # 阶段6：首板涨停候选（最近 N 日未涨停、今日量价启动）
        if self._log:
            self._log("涨停预测：阶段6 - 识别首板涨停候选...")
        fresh_first_board_candidates = self._scan_fresh_first_board_candidates_cached(
            spot_df, zt_codes, hot_industries, compare_context, progress_callback,
        )

        # 摘要
        summary_lines = [
            f"预测日期：基于 {trade_date} 数据预测次日涨停候选",
            f"环境样本：最近 {compare_context.get('pair_count', 0)} 组首板晋级对比",
            f"今日涨停总数：{len(all_pool_records)} 只",
            f"保留涨停候选：{len(continuation_candidates)} 只（得分>=40）",
            f"五日承接候选：{len(first_board_candidates)} 只（得分>=50）",
            f"首板涨停候选：{len(fresh_first_board_candidates)} 只（10日未涨停，得分>=50）",
        ]
        latest_cont_rate = compare_context.get("latest_continuation_rate")
        avg_cont_rate = compare_context.get("avg_continuation_rate")
        if latest_cont_rate is not None:
            summary_lines.append(f"昨日首板最新晋级率：{latest_cont_rate:.1f}%")
        if avg_cont_rate is not None:
            summary_lines.append(f"近{compare_context.get('pair_count', 0)}组平均晋级率：{avg_cont_rate:.1f}%")
        if hot_industries:
            top3 = sorted(hot_industries.items(), key=lambda x: -x[1])[:3]
            summary_lines.append(f"热门行业：{'、'.join(f'{k}({v})' for k, v in top3)}")

        result = {
            "trade_date": trade_date,
            "profile": profile,
            "profile_samples": feature_samples,
            "continuation_candidates": continuation_candidates,
            "first_board_candidates": first_board_candidates,
            "fresh_first_board_candidates": fresh_first_board_candidates,
            "hot_industries": hot_industries,
            "compare_context": compare_context,
            "summary": "\n".join(summary_lines),
        }
        try:
            save_last_limit_up_prediction(result)
        except Exception:
            pass
        return result

    def _build_compare_market_context(
        self,
        trade_date: str,
        lookback_days: int,
    ) -> Dict[str, Any]:
        """从最近几组涨停对比中提炼市场环境。"""
        window_days = max(2, int(lookback_days or 2) + 1)
        trade_dates = self.fetcher._recent_trade_dates(trade_date, window_days)
        pair_stats: List[Dict[str, Any]] = []

        for idx in range(1, len(trade_dates)):
            prev_date = trade_dates[idx - 1]
            cur_date = trade_dates[idx]
            try:
                compare = self.fetcher.compare_limit_up_pools(cur_date, prev_date)
            except Exception as exc:
                logger.debug("涨停预测获取涨停对比 %s/%s 失败: %s", cur_date, prev_date, exc)
                continue

            yesterday_first = compare.get("yesterday_first", []) or []
            continued = compare.get("continued_codes", []) or []
            lost = compare.get("lost_codes", []) or []
            first_count = len(yesterday_first)
            rate = round(len(continued) / first_count * 100, 1) if first_count else None
            pair_stats.append({
                "today_date": cur_date,
                "yesterday_date": prev_date,
                "yesterday_first_count": first_count,
                "continued_count": len(continued),
                "lost_count": len(lost),
                "continuation_rate": rate,
                "today_first_count": len(compare.get("today_first", []) or []),
            })

        valid_rates = [item["continuation_rate"] for item in pair_stats if item.get("continuation_rate") is not None]
        avg_rate = round(sum(valid_rates) / len(valid_rates), 1) if valid_rates else None
        latest_rate = pair_stats[-1]["continuation_rate"] if pair_stats else None
        latest_first_count = pair_stats[-1]["today_first_count"] if pair_stats else 0

        return {
            "trade_dates": trade_dates,
            "pair_stats": pair_stats,
            "pair_count": len(pair_stats),
            "avg_continuation_rate": avg_rate,
            "latest_continuation_rate": latest_rate,
            "latest_first_count": latest_first_count,
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

        # 1. 连板数基础分
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

        # 4. 换手率
        if turnover is not None:
            if 3 <= turnover <= 15:
                score += 5
                reasons.append(f"换手{turnover:.1f}%适中+5")
            elif turnover > 30:
                score -= 5
                reasons.append(f"换手{turnover:.1f}%过高-5")

        # 5. 量能和均线（历史数据已预取到缓存，直接读取）
        try:
            # 只使用本地缓存，不发起网络请求
            history = self.fetcher.get_history_data(
                code, days=65, force_refresh=False,
                request_plan=self._build_local_cache_history_plan(reason="predict-continuation-cache-only"),
            )
        except Exception as exc:
            logger.debug("预测续板获取历史 %s 失败: %s", code, exc)
            history = None
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

            if (latest_ma5 is not None and latest_ma10 is not None and latest_ma20 is not None
                    and latest_ma5 > latest_ma10 > latest_ma20):
                score += 10
                reasons.append("多头排列+10")

            # 量能
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

    def _score_continuation_by_compare(
        self,
        rec: Dict[str, Any],
        hot_industries: Dict[str, int],
        compare_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """结合最近涨停对比环境，对今日涨停股评估次日保留涨停概率。"""
        base = self._score_continuation(rec, hot_industries)
        score = float(base.get("score", 0))
        reasons = [r for r in str(base.get("reasons", "")).split(" / ") if r]

        boards = int(rec.get("consecutive_boards") or 1)
        latest_rate = compare_context.get("latest_continuation_rate")
        avg_rate = compare_context.get("avg_continuation_rate")

        ref_rate = latest_rate if latest_rate is not None else avg_rate
        if ref_rate is not None:
            if boards == 1:
                if ref_rate >= 35:
                    score += 15
                    reasons.append(f"首板晋级环境强({ref_rate:.1f}%)+15")
                elif ref_rate >= 25:
                    score += 8
                    reasons.append(f"首板晋级环境尚可({ref_rate:.1f}%)+8")
                elif ref_rate < 15:
                    score -= 10
                    reasons.append(f"首板晋级环境弱({ref_rate:.1f}%)-10")
            else:
                if ref_rate >= 30:
                    score += 8
                    reasons.append(f"连板接力环境偏强({ref_rate:.1f}%)+8")
                elif ref_rate < 12:
                    score -= 5
                    reasons.append(f"接力环境偏冷({ref_rate:.1f}%)-5")

        if boards == 1:
            pattern = self.classify_limit_up_pattern(
                rec["code"],
                stock_name=rec.get("name", ""),
            ).get("pattern", "")
            if pattern in {"回踩MA5涨停", "趋势加速涨停", "突破平台涨停"}:
                score += 8
                reasons.append(f"{pattern}+8")
            elif pattern == "暴量涨停":
                score -= 5
                reasons.append("暴量首板次日分歧-5")

        final_score = max(0, min(100, int(round(score))))
        base["score"] = final_score
        base["reasons"] = " / ".join(reasons[:8])
        base["predict_type"] = "保留涨停"
        return base

    def _scan_followthrough_candidates_cached(
        self,
        hot_industries: Dict[str, int],
        spot_df: Optional[pd.DataFrame],
        zt_codes: set,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        *,
        lookback_days: int = 5,
    ) -> List[Dict[str, Any]]:
        """从今日强势股中识别五日承接候选。"""
        if spot_df is None or spot_df.empty:
            return []

        strong_stocks = self._filter_strong_stocks(spot_df, zt_codes)
        ma5_pullback_stocks = self._filter_ma5_pullback_stocks(spot_df, zt_codes)

        seen_codes = set()
        merged: List[Dict[str, Any]] = []
        for rec in strong_stocks + ma5_pullback_stocks:
            if rec["code"] in seen_codes:
                continue
            seen_codes.add(rec["code"])
            merged.append(rec)

        if not merged:
            return []

        candidates: List[Dict[str, Any]] = []
        total = len(merged)
        for idx, rec in enumerate(merged):
            score_info = self._score_followthrough_candidate(
                rec,
                hot_industries,
                lookback_days=lookback_days,
            )
            if score_info["score"] >= 50:
                candidates.append(score_info)
            if progress_callback:
                progress_callback(idx + 1, total, f"五日承接 {rec['code']} {rec.get('name', '')}")

        candidates.sort(key=lambda x: -x["score"])
        return candidates[:50]

    def _score_followthrough_candidate(
        self,
        rec: Dict[str, Any],
        hot_industries: Dict[str, int],
        *,
        lookback_days: int = 5,
    ) -> Dict[str, Any]:
        """对"近期爆量后回落到 MA5 附近"的股票评分。"""
        code = rec["code"]
        name = rec.get("name", "")
        change_pct = rec.get("change_pct")
        turnover = rec.get("turnover")
        industry = rec.get("industry", "")
        score = 0.0
        reasons: List[str] = []

        try:
            history = self.fetcher.get_history_data(
                code,
                days=65,
                force_refresh=False,
                request_plan=self._build_local_cache_history_plan(reason="predict-followthrough-cache-only"),
            )
        except Exception as exc:
            logger.debug("预测五日承接获取历史 %s 失败: %s", code, exc)
            history = None

        latest_close = rec.get("close")
        ma5_val = None
        dist_ma5_pct = None
        recent_burst_ratio = None
        recent_burst_amount_ratio = None
        burst_date = ""
        days_since_burst = None
        trend_10d = None
        latest_low = None
        touched_ma5 = False
        recent_above_ma5 = False
        trend_ok = False
        pullback_day = False

        if history is not None and not history.empty and len(history) >= 10:
            df = history.sort_values("date").reset_index(drop=True)
            close = pd.to_numeric(df["close"], errors="coerce")
            low = pd.to_numeric(df.get("low"), errors="coerce") if "low" in df.columns else pd.Series(dtype=float)
            volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)
            amount = pd.to_numeric(df.get("amount"), errors="coerce") if "amount" in df.columns else pd.Series(dtype=float)

            t = len(df) - 1
            latest_close = float(close.iloc[t]) if not pd.isna(close.iloc[t]) else latest_close
            prev_close = float(close.iloc[t - 1]) if t >= 1 and not pd.isna(close.iloc[t - 1]) else None
            latest_low = float(low.iloc[t]) if not low.empty and not pd.isna(low.iloc[t]) else None

            ma5 = close.rolling(5, min_periods=5).mean()
            ma10 = close.rolling(10, min_periods=10).mean()
            ma20 = close.rolling(20, min_periods=20).mean()
            ma5_val = float(ma5.iloc[t]) if not pd.isna(ma5.iloc[t]) else None
            ma10_val = float(ma10.iloc[t]) if not pd.isna(ma10.iloc[t]) else None
            ma20_val = float(ma20.iloc[t]) if not pd.isna(ma20.iloc[t]) else None

            if latest_close is not None and ma5_val is not None and ma5_val > 0:
                dist_ma5_pct = round((latest_close / ma5_val - 1) * 100, 2)
            if latest_low is not None and ma5_val is not None and ma5_val > 0:
                touched_ma5 = latest_low <= ma5_val * 1.01 and latest_low >= ma5_val * 0.97

            burst_window = max(3, min(int(lookback_days or 5), 7))
            burst_start = max(5, t - burst_window)
            best_burst_idx = None
            best_burst_score = 0.0
            for idx in range(burst_start, t):
                if pd.isna(volume.iloc[idx]):
                    continue
                prev_vol = volume.iloc[idx - 5:idx].dropna()
                vol_ratio_i = None
                amt_ratio_i = None
                if not prev_vol.empty and float(prev_vol.mean()) > 0:
                    vol_ratio_i = float(volume.iloc[idx]) / float(prev_vol.mean())
                if not amount.empty and idx < len(amount) and not pd.isna(amount.iloc[idx]):
                    prev_amt = amount.iloc[idx - 5:idx].dropna()
                    if not prev_amt.empty and float(prev_amt.mean()) > 0:
                        amt_ratio_i = float(amount.iloc[idx]) / float(prev_amt.mean())

                score_i = max(
                    vol_ratio_i if vol_ratio_i is not None else 0.0,
                    amt_ratio_i if amt_ratio_i is not None else 0.0,
                )
                if score_i > best_burst_score:
                    best_burst_score = score_i
                    best_burst_idx = idx
                    recent_burst_ratio = round(vol_ratio_i, 2) if vol_ratio_i is not None else None
                    recent_burst_amount_ratio = round(amt_ratio_i, 2) if amt_ratio_i is not None else None

            if best_burst_idx is not None:
                burst_date = str(df.iloc[best_burst_idx].get("date", "") or "")
                days_since_burst = t - best_burst_idx

            if t >= 10 and not pd.isna(close.iloc[t - 10]) and close.iloc[t - 10] > 0 and latest_close is not None:
                trend_10d = round((latest_close / float(close.iloc[t - 10]) - 1) * 100, 1)

            if prev_close is not None and latest_close is not None:
                pullback_day = latest_close <= prev_close
            if ma5_val is not None and ma10_val is not None:
                trend_ok = ma5_val >= ma10_val or (latest_close is not None and latest_close >= ma10_val)

            if ma5_val is not None:
                for lb in range(1, min(4, t + 1)):
                    idx_b = t - lb
                    if idx_b >= 0 and not pd.isna(close.iloc[idx_b]) and not pd.isna(ma5.iloc[idx_b]):
                        if float(close.iloc[idx_b]) > float(ma5.iloc[idx_b]) * 1.01:
                            recent_above_ma5 = True
                            break

        # 1. 先看最近几天是否出现过爆量
        if recent_burst_ratio is not None:
            if recent_burst_ratio >= 2.5:
                score += 28
                reasons.append(f"近期爆量{recent_burst_ratio:.1f}x+28")
            elif recent_burst_ratio >= 1.8:
                score += 18
                reasons.append(f"近期放量{recent_burst_ratio:.1f}x+18")
            elif recent_burst_ratio >= 1.5:
                score += 8
                reasons.append(f"近期量能活跃{recent_burst_ratio:.1f}x+8")
            else:
                score -= 12
                reasons.append(f"近期无明显爆量{recent_burst_ratio:.1f}x-12")
        else:
            reasons.append("近几日量能数据不足")

        if recent_burst_amount_ratio is not None and recent_burst_amount_ratio >= 2.0:
            score += 8
            reasons.append(f"近期额比{recent_burst_amount_ratio:.1f}x+8")

        if days_since_burst is not None:
            if 1 <= days_since_burst <= 3:
                score += 12
                reasons.append(f"爆量后{days_since_burst}日回踩+12")
            elif 4 <= days_since_burst <= 5:
                score += 6
                reasons.append(f"爆量后{days_since_burst}日回踩+6")
            elif days_since_burst >= 6:
                score -= 5
                reasons.append(f"距爆量已{days_since_burst}日-5")

        # 2. 当日价格回落而非继续冲高
        if change_pct is not None:
            if -4.0 <= change_pct <= 1.5:
                score += 20
                reasons.append(f"当日回落{change_pct:.1f}%+20")
            elif 1.5 < change_pct <= 3.5:
                score += 8
                reasons.append(f"回落不深{change_pct:.1f}%+8")
            elif change_pct < -6.0:
                score -= 15
                reasons.append(f"回落过深{change_pct:.1f}%-15")
        if pullback_day:
            score += 8
            reasons.append("收盘低于昨收+8")

        # 3. 靠近 MA5 才叫五日承接
        if dist_ma5_pct is not None:
            if -1.5 <= dist_ma5_pct <= 1.0:
                score += 25
                reasons.append(f"贴近MA5 {dist_ma5_pct:+.1f}%+25")
            elif -3.0 <= dist_ma5_pct <= 2.0:
                score += 12
                reasons.append(f"靠近MA5 {dist_ma5_pct:+.1f}%+12")
            elif dist_ma5_pct < -4.0:
                score -= 12
                reasons.append(f"跌破MA5过深{dist_ma5_pct:+.1f}%-12")
            elif dist_ma5_pct > 4.0:
                score -= 8
                reasons.append(f"离MA5过远{dist_ma5_pct:+.1f}%-8")

        if touched_ma5:
            score += 10
            reasons.append("日内触及MA5+10")

        # 4. 前面最好本来就在 MA5 上方，说明是强势回踩不是纯下跌
        if recent_above_ma5:
            score += 8
            reasons.append("前几日曾强于MA5+8")
        if trend_ok:
            score += 8
            reasons.append("趋势未坏+8")
        elif dist_ma5_pct is not None and dist_ma5_pct < 0:
            score -= 6
            reasons.append("回落时趋势偏弱-6")

        if trend_10d is not None:
            if 5 <= trend_10d <= 25:
                score += 6
                reasons.append(f"10日仍强{trend_10d:.1f}%+6")
            elif trend_10d < -5:
                score -= 6
                reasons.append(f"10日转弱{trend_10d:.1f}%-6")

        if change_pct is not None:
            if change_pct > 6:
                score -= 8
                reasons.append(f"当日仍过强{change_pct:.1f}%-8")

        if industry and hot_industries.get(industry, 0) >= 3:
            score += 10
            reasons.append(f"热门板块({hot_industries[industry]}只)+10")
        elif industry and hot_industries.get(industry, 0) >= 2:
            score += 5
            reasons.append(f"板块联动({hot_industries[industry]}只)+5")

        if turnover is not None:
            if 3 <= turnover <= 20:
                score += 5
                reasons.append(f"换手{turnover:.1f}%适中+5")
            elif turnover > 35:
                score -= 5
                reasons.append(f"换手{turnover:.1f}%过高-5")

        final_score = max(0, min(100, int(round(score))))
        return {
            "code": code,
            "name": name,
            "industry": industry,
            "close": latest_close,
            "change_pct": change_pct,
            "turnover": turnover,
            "ma5": ma5_val,
            "dist_ma5_pct": dist_ma5_pct,
            "volume_ratio": recent_burst_ratio,
            "amount_ratio": recent_burst_amount_ratio,
            "burst_date": burst_date,
            "days_since_burst": days_since_burst,
            "trend_10d": trend_10d,
            "touched_ma5": touched_ma5,
            "score": final_score,
            "reasons": " / ".join(reasons[:8]),
            "predict_type": "五日承接",
        }

    @staticmethod
    def _limit_up_threshold_pct(code: str) -> float:
        """A股各板块涨停阈值（百分比）。ST/退市单独处理，本预测已排除 ST。"""
        c = (code or "").strip()
        if c.startswith(("30", "68")):
            return 19.5
        if c.startswith(("43", "83", "87", "88", "92")):
            return 29.5
        return 9.5

    def _scan_fresh_first_board_candidates_cached(
        self,
        spot_df: Optional[pd.DataFrame],
        zt_codes: set,
        hot_industries: Dict[str, int],
        compare_context: Dict[str, Any],
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        *,
        cooldown_days: int = 10,
    ) -> List[Dict[str, Any]]:
        """从全市场强势股中识别"近期未涨停、明日有望首封"的候选。

        与 `_scan_followthrough_candidates_cached` 区别：
        - 承接候选：最近曾涨停过、回落到 MA5 附近的股票
        - 首板候选：最近 N 日未出现过涨停，今日量价启动、逼近涨停的"新生力量"
        """
        if spot_df is None or spot_df.empty:
            return []

        merged: List[Dict[str, Any]] = []
        seen: set = set()
        for rec in self._filter_strong_stocks(spot_df, zt_codes):
            chg = rec.get("change_pct")
            if chg is None or chg < 4.0 or chg >= 9.5:
                continue
            if rec["code"] in seen:
                continue
            seen.add(rec["code"])
            merged.append(rec)

        if not merged:
            return []

        candidates: List[Dict[str, Any]] = []
        total = len(merged)
        for idx, rec in enumerate(merged):
            score_info = self._score_fresh_first_board(
                rec, hot_industries, compare_context, cooldown_days=cooldown_days,
            )
            if score_info is not None and score_info["score"] >= 50:
                candidates.append(score_info)
            if progress_callback:
                progress_callback(idx + 1, total, f"首板筛选 {rec['code']} {rec.get('name', '')}")

        candidates.sort(key=lambda x: -x["score"])
        return candidates[:50]

    def _score_fresh_first_board(
        self,
        rec: Dict[str, Any],
        hot_industries: Dict[str, int],
        compare_context: Dict[str, Any],
        *,
        cooldown_days: int = 10,
    ) -> Optional[Dict[str, Any]]:
        """对"近期未涨停、今日量价启动"的强势股评分。

        强制条件：最近 cooldown_days 个交易日内不存在涨停过。命中冷却期返回 None。
        """
        code = rec["code"]
        name = rec.get("name", "")
        change_pct = rec.get("change_pct")
        turnover = rec.get("turnover")
        industry = rec.get("industry", "")

        try:
            history = self.fetcher.get_history_data(
                code, days=65, force_refresh=False,
                request_plan=self._build_local_cache_history_plan(reason="predict-fresh-first-board-cache-only"),
            )
        except Exception as exc:
            logger.debug("预测首板获取历史 %s 失败: %s", code, exc)
            history = None

        if history is None or history.empty or len(history) < 11:
            return None

        df = history.sort_values("date").reset_index(drop=True)
        close = pd.to_numeric(df["close"], errors="coerce")
        volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)

        t = len(df) - 1
        latest_close = float(close.iloc[t]) if not pd.isna(close.iloc[t]) else rec.get("close")

        # ---- 冷却期判定：最近 cooldown_days 交易日内不能有涨停 ----
        threshold = self._limit_up_threshold_pct(code)
        cooldown_start = max(1, t - cooldown_days + 1)
        last_zt_offset: Optional[int] = None
        for i in range(cooldown_start, t + 1):
            if pd.isna(close.iloc[i]) or pd.isna(close.iloc[i - 1]) or float(close.iloc[i - 1]) <= 0:
                continue
            chg_i = (float(close.iloc[i]) / float(close.iloc[i - 1]) - 1) * 100
            if chg_i >= threshold - 0.3:
                last_zt_offset = t - i
                break
        if last_zt_offset is not None:
            return None  # 已涨停过，让承接/连板分支处理

        score = 0.0
        reasons: List[str] = []

        # 1. 当日涨幅靠近涨停
        if change_pct is not None:
            if change_pct >= 8.0:
                score += 28
                reasons.append(f"涨{change_pct:.1f}%逼近涨停+28")
            elif change_pct >= 6.0:
                score += 18
                reasons.append(f"涨{change_pct:.1f}%放量上攻+18")
            elif change_pct >= 4.0:
                score += 10
                reasons.append(f"涨{change_pct:.1f}%突破+10")

        # 2. 量比放大
        vol_ratio = None
        if len(volume) >= 6 and not pd.isna(volume.iloc[t]):
            vol_window = volume.iloc[max(0, t - 5):t].dropna()
            if not vol_window.empty and float(vol_window.mean()) > 0:
                vol_ratio = round(float(volume.iloc[t]) / float(vol_window.mean()), 2)
        if vol_ratio is not None:
            if vol_ratio >= 2.5:
                score += 22
                reasons.append(f"量比{vol_ratio:.1f}x爆量+22")
            elif vol_ratio >= 1.8:
                score += 14
                reasons.append(f"量比{vol_ratio:.1f}x放量+14")
            elif vol_ratio >= 1.3:
                score += 6
                reasons.append(f"量比{vol_ratio:.1f}x温和放量+6")
            elif vol_ratio < 1.0:
                score -= 10
                reasons.append(f"量比{vol_ratio:.1f}x缩量-10")

        # 3. 均线位置：站上 MA5/MA10/MA20
        ma5 = close.rolling(5, min_periods=5).mean()
        ma10 = close.rolling(10, min_periods=10).mean()
        ma20 = close.rolling(20, min_periods=20).mean()
        ma5_val = float(ma5.iloc[t]) if not pd.isna(ma5.iloc[t]) else None
        ma10_val = float(ma10.iloc[t]) if not pd.isna(ma10.iloc[t]) else None
        ma20_val = float(ma20.iloc[t]) if not pd.isna(ma20.iloc[t]) else None
        dist_ma5_pct = None
        if ma5_val and ma5_val > 0 and latest_close is not None:
            dist_ma5_pct = round((latest_close / ma5_val - 1) * 100, 2)

        if (
            latest_close is not None and ma5_val and ma10_val and ma20_val
            and latest_close >= ma5_val >= ma10_val >= ma20_val
        ):
            score += 14
            reasons.append("多头排列+14")
        elif (
            latest_close is not None and ma5_val and ma10_val
            and latest_close >= ma5_val >= ma10_val
        ):
            score += 8
            reasons.append("站上MA5/10+8")
        elif latest_close is not None and ma5_val and latest_close < ma5_val * 0.99:
            score -= 8
            reasons.append("跌破MA5-8")

        # 4. 60日位置：避开高位接盘
        position_60d = None
        if t >= 60:
            window60 = close.iloc[t - 60:t + 1].dropna()
            if not window60.empty:
                hi = float(window60.max())
                lo = float(window60.min())
                if hi > lo and latest_close is not None:
                    position_60d = round((latest_close - lo) / (hi - lo) * 100, 1)
        if position_60d is not None:
            if position_60d >= 92:
                score -= 10
                reasons.append(f"60日位置{position_60d:.0f}%过高-10")
            elif position_60d <= 35:
                score += 8
                reasons.append(f"60日位置{position_60d:.0f}%低位+8")
            elif 35 < position_60d <= 70:
                score += 4
                reasons.append(f"60日位置{position_60d:.0f}%中位+4")

        # 5. 5日/10日趋势
        trend_5d = None
        if t >= 5 and not pd.isna(close.iloc[t - 5]) and float(close.iloc[t - 5]) > 0 and latest_close is not None:
            trend_5d = round((latest_close / float(close.iloc[t - 5]) - 1) * 100, 1)
        if trend_5d is not None:
            if trend_5d > 22:
                score -= 8
                reasons.append(f"5日已涨{trend_5d:.1f}%过急-8")
            elif 4 <= trend_5d <= 18:
                score += 6
                reasons.append(f"5日涨{trend_5d:.1f}%稳健+6")

        # 6. 行业共振
        if industry and hot_industries.get(industry, 0) >= 3:
            score += 12
            reasons.append(f"热门板块({hot_industries[industry]}只)+12")
        elif industry and hot_industries.get(industry, 0) >= 2:
            score += 6
            reasons.append(f"板块联动({hot_industries[industry]}只)+6")

        # 7. 换手率
        if turnover is not None:
            if 5 <= turnover <= 15:
                score += 6
                reasons.append(f"换手{turnover:.1f}%健康+6")
            elif 15 < turnover <= 25:
                score += 2
                reasons.append(f"换手{turnover:.1f}%偏高+2")
            elif turnover > 30:
                score -= 6
                reasons.append(f"换手{turnover:.1f}%过热-6")
            elif turnover < 1.5:
                score -= 4
                reasons.append(f"换手{turnover:.1f}%偏冷-4")

        # 8. 大盘环境调节：晋级率高时稍加分，低时减分
        latest_cont_rate = compare_context.get("latest_continuation_rate")
        if latest_cont_rate is not None:
            if latest_cont_rate >= 60:
                score += 5
                reasons.append(f"昨日晋级率{latest_cont_rate:.0f}%+5")
            elif latest_cont_rate < 25:
                score -= 5
                reasons.append(f"昨日晋级率{latest_cont_rate:.0f}%-5")

        final_score = max(0, min(100, int(round(score))))
        return {
            "code": code,
            "name": name,
            "industry": industry,
            "close": latest_close,
            "change_pct": change_pct,
            "turnover": turnover,
            "ma5": ma5_val,
            "dist_ma5_pct": dist_ma5_pct,
            "volume_ratio": vol_ratio,
            "trend_5d": trend_5d,
            "position_60d": position_60d,
            "cooldown_days": cooldown_days,
            "score": final_score,
            "reasons": " / ".join(reasons[:8]),
            "predict_type": "首板涨停",
        }

    def _scan_first_board_candidates_cached(
        self,
        today_pool_df: pd.DataFrame,
        hot_industries: Dict[str, int],
        profile: Dict[str, Any],
        spot_df: Optional[pd.DataFrame],
        zt_codes: set,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> List[Dict[str, Any]]:
        """用画像匹配候选股（行情和历史数据均已提前缓存）。"""
        if spot_df is None or spot_df.empty:
            return []

        strong_stocks = self._filter_strong_stocks(spot_df, zt_codes)
        ma5_pullback_stocks = self._filter_ma5_pullback_stocks(spot_df, zt_codes)

        seen_codes = set()
        merged: List[Dict[str, Any]] = []
        for rec in strong_stocks:
            if rec["code"] not in seen_codes:
                seen_codes.add(rec["code"])
                merged.append(rec)
        for rec in ma5_pullback_stocks:
            if rec["code"] not in seen_codes:
                seen_codes.add(rec["code"])
                merged.append(rec)

        if not merged:
            return []

        if self._log:
            self._log(f"涨停预测：强势股 {len(strong_stocks)} 只 + 回踩MA5 {len(ma5_pullback_stocks)} 只，"
                      f"合并去重后 {len(merged)} 只")

        # 历史数据已在阶段3统一预取，这里直接评分
        candidates = []
        total = len(merged)
        for idx, rec in enumerate(merged):
            score_info = self._score_first_board_by_profile(rec, hot_industries, profile)
            if score_info["score"] >= 50:
                candidates.append(score_info)
            if progress_callback:
                progress_callback(idx + 1, total, f"首板匹配 {rec['code']} {rec.get('name', '')}")

        candidates.sort(key=lambda x: -x["score"])
        return candidates[:50]

    def _fetch_spot_snapshot(self) -> Optional[pd.DataFrame]:
        """获取全市场实时行情快照（只调一次 API）。
        优先东财，东财熔断时自动回退到新浪。
        """
        import akshare as ak
        from stock_data import _retry_ak_call, _eastmoney_circuit_breaker_open
        # 东财可用时优先东财
        if not _eastmoney_circuit_breaker_open():
            try:
                if self._log:
                    self._log("涨停预测：正在获取全市场实时行情快照（东财）...")
                return _retry_ak_call(ak.stock_zh_a_spot_em)
            except Exception as e:
                if self._log:
                    self._log(f"涨停预测：东财实时行情失败: {e}，尝试新浪备选...")
        # 新浪备选
        try:
            if self._log:
                self._log("涨停预测：正在获取全市场实时行情快照（新浪，约30s）...")
            df = _retry_ak_call(ak.stock_zh_a_spot)
            if df is not None and not df.empty:
                # 新浪代码带交易所前缀（如 sh600000），去掉前缀统一为纯数字
                if "代码" in df.columns:
                    df["代码"] = df["代码"].astype(str).str.replace(r"^(sh|sz|bj)", "", regex=True).str.strip().str.zfill(6)
                return df
        except Exception as e2:
            if self._log:
                self._log(f"涨停预测：新浪实时行情也失败: {e2}")
        return None

    @staticmethod
    def _parse_spot_record(row, exclude_codes: set) -> Optional[Dict[str, Any]]:
        """从实时行情行中解析基础记录，返回 None 表示需跳过。"""
        code = str(row.get("代码", "")).strip().zfill(6)
        if code in exclude_codes:
            return None
        name = str(row.get("名称", ""))
        if "ST" in name.upper():
            return None
        close = float(row["最新价"]) if pd.notna(row.get("最新价")) else None
        if close is None or close <= 0:
            return None
        change_pct = float(row["涨跌幅"]) if pd.notna(row.get("涨跌幅")) else None
        amount_val = float(row["成交额"]) if pd.notna(row.get("成交额")) else None
        if amount_val is not None and amount_val < 5000_0000:
            return None
        volume_val = float(row["成交量"]) if pd.notna(row.get("成交量")) else None
        turnover = float(row["换手率"]) if pd.notna(row.get("换手率")) else None
        industry = str(
            row.get("所属行业", row.get("行业", row.get("板块", ""))) or ""
        ).strip()
        return {
            "code": code, "name": name, "change_pct": change_pct,
            "close": close, "volume": volume_val, "amount": amount_val,
            "turnover": turnover, "industry": industry,
        }

    def _filter_strong_stocks(
        self, spot_df: pd.DataFrame, exclude_codes: set
    ) -> List[Dict[str, Any]]:
        """从行情快照中筛选涨幅 3%~9.5% 的强势股。"""
        records = []
        for _, row in spot_df.iterrows():
            rec = self._parse_spot_record(row, exclude_codes)
            if rec is None:
                continue
            chg = rec.get("change_pct")
            if chg is None or chg < 3.0 or chg >= 9.5:
                continue
            records.append(rec)
        records.sort(key=lambda x: -(x.get("change_pct") or 0))
        return records[:100]

    def _filter_ma5_pullback_stocks(
        self, spot_df: pd.DataFrame, exclude_codes: set
    ) -> List[Dict[str, Any]]:
        """从行情快照中筛选涨跌幅 -3%~+3% 的回踩MA5候选。"""
        records = []
        for _, row in spot_df.iterrows():
            rec = self._parse_spot_record(row, exclude_codes)
            if rec is None:
                continue
            chg = rec.get("change_pct")
            if chg is None or chg < -3.0 or chg >= 3.0:
                continue
            records.append(rec)
        records.sort(key=lambda x: -(x.get("amount") or 0))
        return records[:200]

    def _score_first_board_by_profile(
        self,
        rec: Dict[str, Any],
        hot_industries: Dict[str, int],
        profile: Dict[str, Any],
    ) -> Dict[str, Any]:
        """用涨停前兆画像对强势股打分。

        核心思路：把当前股票的特征和画像中涨停股 T-1 日特征对比，
        越接近画像中位数/均值的，得分越高。
        """
        code = rec["code"]
        name = rec.get("name", "")
        score = 0.0
        reasons: List[str] = []
        change_pct = rec.get("change_pct", 0)
        turnover = rec.get("turnover")

        # 当日涨幅
        if change_pct is not None:
            if change_pct >= 8:
                score += 18
                reasons.append(f"涨{change_pct:.1f}%接近涨停+18")
            elif change_pct >= 6:
                score += 12
                reasons.append(f"涨{change_pct:.1f}%+12")
            elif change_pct >= 3:
                score += 6
                reasons.append(f"涨{change_pct:.1f}%+6")

        # 获取历史数据计算特征（已预取到缓存，直接读取）
        try:
            # 只使用本地缓存，不发起网络请求
            history = self.fetcher.get_history_data(
                code, days=65, force_refresh=False,
                request_plan=self._build_local_cache_history_plan(reason="predict-first-board-cache-only"),
            )
        except Exception as exc:
            logger.debug("预测首板获取历史 %s 失败: %s", code, exc)
            history = None

        industry = ""
        vol_ratio = None
        position_60d = None
        trend_10d = None
        ma_bullish = False

        if history is not None and not history.empty and len(history) >= 10:
            df = history.sort_values("date").reset_index(drop=True)
            close = pd.to_numeric(df["close"], errors="coerce")
            volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)
            amount = pd.to_numeric(df.get("amount"), errors="coerce") if "amount" in df.columns else pd.Series(dtype=float)
            latest_close = float(close.iloc[-1]) if not pd.isna(close.iloc[-1]) else None
            t = len(df) - 1  # 当前最新一行

            ma5 = close.rolling(5, min_periods=5).mean()
            ma10 = close.rolling(10, min_periods=10).mean()
            ma20 = close.rolling(20, min_periods=20).mean()
            ma5_val = float(ma5.iloc[t]) if not pd.isna(ma5.iloc[t]) else None
            ma10_val = float(ma10.iloc[t]) if not pd.isna(ma10.iloc[t]) else None
            ma20_val = float(ma20.iloc[t]) if not pd.isna(ma20.iloc[t]) else None

            # --- 量比匹配 ---
            if len(volume) >= 6 and not pd.isna(volume.iloc[t]):
                vol_window = volume.iloc[max(0, t - 5):t].dropna()
                if not vol_window.empty and float(vol_window.mean()) > 0:
                    vol_ratio = round(float(volume.iloc[t]) / float(vol_window.mean()), 2)
                    p = profile.get("vol_ratio_t1", {})
                    p_med = p.get("median")
                    p_p25 = p.get("p25")
                    p_p75 = p.get("p75")
                    if p_med is not None and p_p25 is not None and p_p75 is not None:
                        if p_p25 <= vol_ratio <= p_p75:
                            score += 15
                            reasons.append(f"量比{vol_ratio:.1f}x吻合画像[{p_p25:.1f}~{p_p75:.1f}]+15")
                        elif vol_ratio >= p_med * 0.6:
                            score += 8
                            reasons.append(f"量比{vol_ratio:.1f}x接近画像+8")
                    elif vol_ratio >= 1.5:
                        score += 8
                        reasons.append(f"放量{vol_ratio:.1f}x+8")

            # --- 额比匹配 ---
            if len(amount) >= 6 and not pd.isna(amount.iloc[t]):
                amt_window = amount.iloc[max(0, t - 5):t].dropna()
                if not amt_window.empty and float(amt_window.mean()) > 0:
                    amt_ratio = round(float(amount.iloc[t]) / float(amt_window.mean()), 2)
                    p = profile.get("amt_ratio_t1", {})
                    p_med = p.get("median")
                    if p_med is not None and amt_ratio >= p_med * 0.8:
                        score += 5
                        reasons.append(f"额比{amt_ratio:.1f}x匹配+5")

            # --- 均线匹配 ---
            if ma5_val is not None and ma10_val is not None and ma20_val is not None:
                if ma5_val > ma10_val > ma20_val:
                    ma_bullish = True
                    p_bull = profile.get("ma_bullish", {})
                    if p_bull.get("ratio", 0) >= 50:
                        score += 10
                        reasons.append(f"多头排列(画像{p_bull['ratio']:.0f}%)+10")
                    else:
                        score += 5
                        reasons.append("多头排列+5")

            # 站上MA5
            if latest_close is not None and ma5_val is not None and latest_close > ma5_val:
                p_above = profile.get("above_ma5", {})
                if p_above.get("ratio", 0) >= 60:
                    score += 5
                    reasons.append(f"站上MA5(画像{p_above['ratio']:.0f}%)+5")

            # --- MA5 距离匹配 ---
            if latest_close and ma5_val and ma5_val > 0:
                dist_ma5 = round((latest_close / ma5_val - 1) * 100, 2)
                p = profile.get("dist_ma5_pct", {})
                p_p25 = p.get("p25")
                p_p75 = p.get("p75")
                if p_p25 is not None and p_p75 is not None:
                    if p_p25 <= dist_ma5 <= p_p75:
                        score += 5
                        reasons.append(f"距MA5 {dist_ma5:+.1f}%吻合+5")

            # --- 回踩MA5检测 ---
            # 收盘接近或略低于MA5（-3%~+1%），且前几日曾站上MA5
            if latest_close and ma5_val and ma5_val > 0:
                dist_ma5_now = (latest_close / ma5_val - 1) * 100
                if -3.0 <= dist_ma5_now <= 1.0:
                    was_above_ma5 = False
                    for lb in range(2, min(6, t + 1)):
                        idx_b = t - lb
                        if idx_b >= 0 and not pd.isna(close.iloc[idx_b]) and not pd.isna(ma5.iloc[idx_b]):
                            if float(close.iloc[idx_b]) > float(ma5.iloc[idx_b]) * 1.01:
                                was_above_ma5 = True
                                break
                    if was_above_ma5:
                        # 回踩MA5，这是涨停前常见形态
                        p_pb = profile.get("ma5_pullback", {})
                        pb_ratio = p_pb.get("ratio", 0)
                        if pb_ratio >= 20:
                            score += 15
                            reasons.append(f"回踩MA5(画像{pb_ratio:.0f}%)+15")
                        else:
                            score += 10
                            reasons.append(f"回踩MA5(距{dist_ma5_now:+.1f}%)+10")

            # --- 60日位置匹配 ---
            if len(close) >= 20 and latest_close is not None:
                window = close.tail(min(60, len(close))).dropna()
                if len(window) >= 10:
                    position_60d = round(float((window < latest_close).sum()) / len(window) * 100, 1)
                    p = profile.get("position_60d", {})
                    p_med = p.get("median")
                    p_p25 = p.get("p25")
                    p_p75 = p.get("p75")
                    if p_med is not None and p_p25 is not None and p_p75 is not None:
                        if p_p25 <= position_60d <= p_p75:
                            score += 8
                            reasons.append(f"位置{position_60d:.0f}%吻合画像[{p_p25:.0f}~{p_p75:.0f}]+8")
                        elif position_60d < 30:
                            score += 5
                            reasons.append(f"低位{position_60d:.0f}%+5")

            # --- 10日趋势 ---
            if t >= 10 and not pd.isna(close.iloc[t - 10]) and close.iloc[t - 10] > 0:
                trend_10d = round((float(close.iloc[t]) / float(close.iloc[t - 10]) - 1) * 100, 1)

            # --- 缩量蓄势匹配 ---
            if len(volume) >= 6:
                vol_3 = volume.iloc[max(0, t - 3):t].dropna()
                vol_5 = volume.iloc[max(0, t - 5):t].dropna()
                if not vol_3.empty and not vol_5.empty and float(vol_5.mean()) > 0:
                    shrink = round(float(vol_3.mean()) / float(vol_5.mean()), 2)
                    p = profile.get("shrink_ratio_t1", {})
                    p_med = p.get("median")
                    if p_med is not None and shrink <= p_med and vol_ratio is not None and vol_ratio >= 1.5:
                        score += 10
                        reasons.append(f"缩量蓄势后放量(缩{shrink:.2f}/量比{vol_ratio:.1f}x)+10")

        # 板块热度
        if industry and hot_industries.get(industry, 0) >= 3:
            score += 10
            reasons.append(f"热门板块({hot_industries[industry]}只)+10")
        elif industry and hot_industries.get(industry, 0) >= 2:
            score += 5
            reasons.append(f"板块有{hot_industries[industry]}只+5")

        # 换手率
        if turnover is not None:
            p = profile.get("turnover_t1", {})
            p_p25 = p.get("p25")
            p_p75 = p.get("p75")
            if p_p25 is not None and p_p75 is not None:
                if p_p25 <= turnover <= p_p75:
                    score += 5
                    reasons.append(f"换手{turnover:.1f}%吻合画像+5")
            elif 3 <= turnover <= 20:
                score += 3
                reasons.append(f"换手{turnover:.1f}%适中+3")
            if turnover > 40:
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
            "vol_ratio": vol_ratio,
            "position_60d": position_60d,
            "trend_10d": trend_10d,
            "ma_bullish": ma_bullish,
            "score": final_score,
            "reasons": " / ".join(reasons[:8]),
            "predict_type": "首板候选",
        }
