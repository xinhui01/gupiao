from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from src.models.analysis_models import HistoryAnalysisConfig

GROWTH_BOARDS = {"创业板", "科创板"}
SPECIAL_TREATMENT_PREFIXES = ("ST", "*ST")
LIMIT_UP_TOLERANCE = 0.2


class HistoryAnalysisService:
    def __init__(self, config: HistoryAnalysisConfig):
        self.config = config

    def check_close_above_ma(
        self,
        history_data: pd.DataFrame,
        streak_days: Optional[int] = None,
        ma_period: Optional[int] = None,
    ) -> bool:
        if history_data is None or history_data.empty or "close" not in history_data.columns:
            return False

        effective_streak_days = max(1, int(streak_days or self.config.trend_days))
        effective_ma_period = max(1, int(ma_period or self.config.ma_period))
        df = self._sort_history(history_data)
        required_rows = effective_streak_days + effective_ma_period - 1
        if len(df) < required_rows:
            return False

        close = pd.to_numeric(df["close"], errors="coerce")
        ma = close.rolling(window=effective_ma_period, min_periods=effective_ma_period).mean()
        recent_close = close.tail(effective_streak_days)
        recent_ma = ma.tail(effective_streak_days)
        if recent_close.isna().any() or recent_ma.isna().any():
            return False
        return bool((recent_close.values > recent_ma.values).all())

    def limit_up_threshold(self, board: str = "", stock_name: str = "") -> float:
        normalized_name = str(stock_name or "").upper().strip()
        if any(normalized_name.startswith(prefix) for prefix in SPECIAL_TREATMENT_PREFIXES):
            return 5.0
        if str(board or "").strip() in GROWTH_BOARDS:
            return 20.0
        return 10.0

    def analyze_history(
        self,
        history_data: pd.DataFrame,
        *,
        board: str = "",
        stock_name: str = "",
        stock_code: str = "",
    ) -> Dict[str, Any]:
        result = self._create_empty_analysis_result()
        if history_data is None or history_data.empty:
            result["summary"] = "无历史数据"
            return result
        if "date" not in history_data.columns or "close" not in history_data.columns:
            result["summary"] = "历史数据缺少 date/close"
            return result

        df = self._sort_history(history_data)
        close = pd.to_numeric(df["close"], errors="coerce")
        ma = close.rolling(
            window=self.config.ma_period,
            min_periods=self.config.ma_period,
        ).mean()
        self._populate_price_metrics(result, df, close, ma)
        volume = self._apply_volume_analysis(result, df)
        self._apply_limit_up_analysis(
            result,
            df,
            board=board,
            stock_name=stock_name,
            volume=volume,
        )
        result["summary"] = self._build_analysis_summary(result)
        score, score_breakdown = self.calculate_trade_score(result)
        result["score"] = score
        result["score_breakdown"] = score_breakdown
        if stock_code:
            result["stock_code"] = str(stock_code).strip().zfill(6)
        return result

    def calculate_trade_score(self, result: Dict[str, Any]) -> tuple[int, str]:
        score = 50.0
        reasons: List[str] = []

        if result.get("passed"):
            score += 18
            reasons.append(f"站上MA{self.config.ma_period}+18")
        else:
            score -= 10
            reasons.append(f"跌破MA{self.config.ma_period}-10")

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
            reasons.append(f"{self.config.trend_days}日内有涨停+4")

        if self.config.volume_expand_enabled and result.get("volume_expand"):
            score += 8
            reasons.append("放量有效+8")
        elif self.config.volume_expand_enabled and result.get("volume_expand_ratio") is not None:
            ratio = float(result["volume_expand_ratio"])
            if ratio < max(1.2, self.config.volume_expand_factor * 0.8):
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

    def _create_empty_analysis_result(self) -> Dict[str, Any]:
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
            "volume_lookback_days": self.config.volume_lookback_days,
            "volume_expand_enabled": self.config.volume_expand_enabled,
            "volume_expand_factor": self.config.volume_expand_factor,
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
    ) -> None:
        recent = df.tail(self.config.trend_days).copy()
        recent_close = pd.to_numeric(recent["close"], errors="coerce")
        recent_ma = ma.tail(self.config.trend_days)

        result["latest_date"] = str(df["date"].iloc[-1])
        result["latest_close"] = self._coerce_float(close.iloc[-1])
        result["latest_ma"] = self._coerce_float(ma.iloc[-1])
        ma10 = close.rolling(window=10, min_periods=10).mean()
        result["latest_ma10"] = self._coerce_float(ma10.iloc[-1])
        result["recent_closes"] = [self._coerce_float(value) for value in recent_close.tolist()]
        result["recent_ma"] = [self._coerce_float(value) for value in recent_ma.tolist()]

        if "change_pct" in df.columns:
            change_pct = pd.to_numeric(df["change_pct"], errors="coerce")
            result["latest_change_pct"] = self._coerce_float(change_pct.iloc[-1]) if not change_pct.empty else None

        _five_day_lookback = 5
        if len(df) >= _five_day_lookback and not pd.isna(close.iloc[-1]) and not pd.isna(close.iloc[-_five_day_lookback]):
            prev_close = close.iloc[-_five_day_lookback]
            if not pd.isna(prev_close) and float(prev_close) != 0.0:
                result["five_day_return"] = (float(close.iloc[-1]) / float(prev_close) - 1.0) * 100.0

        required_rows = self.config.trend_days + self.config.ma_period - 1
        if len(df) >= required_rows and not recent_close.isna().any() and not recent_ma.isna().any():
            result["passed"] = bool((recent_close.values > recent_ma.values).all())

    def _apply_volume_analysis(
        self,
        result: Dict[str, Any],
        df: pd.DataFrame,
    ) -> Optional[pd.Series]:
        if "volume" not in df.columns or len(df) < self.config.volume_lookback_days:
            return None

        volume = pd.to_numeric(df["volume"], errors="coerce")
        recent_volume = volume.tail(self.config.volume_lookback_days).dropna()
        if not recent_volume.empty:
            volume_min = float(recent_volume.min())
            volume_max = float(recent_volume.max())
            ratio = float(volume_max / volume_min) if volume_min > 0 else None
            result["volume_min"] = volume_min
            result["volume_max"] = volume_max
            result["volume_expand_ratio"] = ratio
            result["volume_expand"] = bool(
                self.config.volume_expand_enabled
                and ratio is not None
                and ratio >= self.config.volume_expand_factor
            )

        compare_window = volume.iloc[-(self.config.volume_lookback_days + 1):-1].dropna()
        if compare_window.empty:
            compare_window = recent_volume
        latest_volume = volume.iloc[-1] if not volume.empty else None
        if latest_volume is not None and not pd.isna(latest_volume) and not compare_window.empty:
            average_volume = float(compare_window.mean())
            if average_volume > 0:
                result["latest_volume_ratio"] = float(float(latest_volume) / average_volume * 100.0)
        return volume

    def _apply_limit_up_analysis(
        self,
        result: Dict[str, Any],
        df: pd.DataFrame,
        *,
        board: str,
        stock_name: str,
        volume: Optional[pd.Series],
    ) -> None:
        threshold = self.limit_up_threshold(board=board, stock_name=stock_name)
        result["limit_up_threshold"] = threshold
        if "change_pct" not in df.columns:
            return

        change_pct = pd.to_numeric(df["change_pct"], errors="coerce")
        full_limit_up_mask = (change_pct >= (threshold - LIMIT_UP_TOLERANCE)).fillna(False)
        recent_change_pct = change_pct.tail(max(self.config.limit_up_lookback_days, 1))
        recent_dates = df.tail(max(self.config.limit_up_lookback_days, 1))["date"].astype(str).tolist()
        hit_dates = [
            trade_date
            for trade_date, hit in zip(
                recent_dates,
                (recent_change_pct >= (threshold - LIMIT_UP_TOLERANCE)).tolist(),
            )
            if bool(hit)
        ]
        result["limit_up_hit_dates"] = hit_dates
        result["limit_up_within_days"] = bool(hit_dates)
        result["limit_up"] = bool(
            result["latest_change_pct"] is not None
            and result["latest_change_pct"] >= (threshold - LIMIT_UP_TOLERANCE)
        )
        result["limit_up_streak"] = self._calculate_limit_up_streak(full_limit_up_mask)

        broken_streak = self._calculate_broken_limit_up_streak(full_limit_up_mask)
        result["broken_limit_up"] = broken_streak > 0
        result["broken_streak_count"] = broken_streak
        result["after_two_limit_up"] = bool(result["broken_limit_up"] and broken_streak >= 2)

        if not result["broken_limit_up"] or volume is None or not self.config.volume_expand_enabled:
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
            result["volume_break_limit_up"] = bool(
                break_ratio >= self.config.volume_expand_factor
            )

    def _build_analysis_summary(self, result: Dict[str, Any]) -> str:
        if result["passed"]:
            summary = (
                f"最近{self.config.trend_days}日收盘全部高于MA{self.config.ma_period}，"
                f"最新收盘 {result['latest_close']:.2f} / MA{self.config.ma_period} {result['latest_ma']:.2f}"
            )
        else:
            summary = f"未满足最近{self.config.trend_days}日收盘全部高于MA{self.config.ma_period}"

        if self.config.volume_expand_enabled and result["volume_expand"]:
            ratio_text = (
                "-"
                if result["volume_expand_ratio"] is None
                else f"{result['volume_expand_ratio']:.2f}倍"
            )
            summary = f"{summary}；近{self.config.volume_lookback_days}日放量 {ratio_text}"
        elif not self.config.volume_expand_enabled:
            summary = f"{summary}；放量倍数检测已关闭"

        if result["limit_up_streak"] >= 2:
            summary = f"{summary}；连板 {result['limit_up_streak']} 板"
        if result["broken_limit_up"]:
            summary = f"{summary}；断板，前序连板 {result['broken_streak_count']} 板"
        if result["volume_break_limit_up"]:
            summary = f"{summary}；放量后断板"
        return summary

    @staticmethod
    def _calculate_limit_up_streak(mask: pd.Series) -> int:
        streak = 0
        for flag in reversed(mask.tolist()):
            if bool(flag):
                streak += 1
            else:
                break
        return streak

    @staticmethod
    def _calculate_broken_limit_up_streak(mask: pd.Series) -> int:
        if len(mask) < 2 or bool(mask.iloc[-1]) or not bool(mask.iloc[-2]):
            return 0
        broken_streak = 0
        idx = len(mask) - 2
        while idx >= 0 and bool(mask.iloc[idx]):
            broken_streak += 1
            idx -= 1
        return broken_streak

    @staticmethod
    def _sort_history(history_data: pd.DataFrame) -> pd.DataFrame:
        return history_data.sort_values("date").reset_index(drop=True)

    @staticmethod
    def _coerce_float(value: Any) -> Optional[float]:
        if value is None or pd.isna(value):
            return None
        return float(value)
