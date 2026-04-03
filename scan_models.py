from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class FilterSettings:
    trend_days: int = 5
    ma_period: int = 5
    limit_up_lookback_days: int = 5
    volume_lookback_days: int = 5
    volume_expand_enabled: bool = True
    volume_expand_factor: float = 2.0
    require_limit_up_within_days: bool = False

    def to_signature(self) -> Dict[str, Any]:
        return {
            "trend_days": int(self.trend_days),
            "ma_period": int(self.ma_period),
            "limit_up_lookback_days": int(self.limit_up_lookback_days),
            "volume_lookback_days": int(self.volume_lookback_days),
            "volume_expand_enabled": bool(self.volume_expand_enabled),
            "volume_expand_factor": float(self.volume_expand_factor),
            "require_limit_up_within_days": bool(self.require_limit_up_within_days),
        }


@dataclass(frozen=True)
class ScanRequest:
    filter_settings: FilterSettings
    max_stocks: int = 0
    scan_workers: int = 12
    allowed_boards: tuple[str, ...] = ()
    refresh_universe: bool = False
    ignore_result_snapshot: bool = False

    def to_signature(self) -> Dict[str, Any]:
        signature = self.filter_settings.to_signature()
        signature.update(
            {
                "allowed_boards": sorted(
                    {str(board).strip() for board in self.allowed_boards if str(board).strip()}
                ),
                "max_stocks": int(self.max_stocks),
            }
        )
        return signature
