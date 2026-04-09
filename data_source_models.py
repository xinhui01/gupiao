from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

DATA_SOURCE_OPTIONS: Dict[str, Tuple[str, ...]] = {
    "history": ("auto", "eastmoney", "tencent", "sina", "netease", "baidu", "sohu", "ths", "wscn"),
    "intraday": ("auto", "eastmoney", "sina"),
    "fund_flow": ("auto", "eastmoney", "ths"),
    "limit_up_reason": ("auto", "eastmoney"),
}


@dataclass(frozen=True)
class AppSourceSettings:
    history_source: str = "auto"
    intraday_source: str = "auto"
    fund_flow_source: str = "auto"
    limit_up_reason_source: str = "auto"


@dataclass(frozen=True)
class DataProviderPlan:
    mode: str = "network"
    provider_sequence: tuple[str, ...] = ()
    reason: str = ""

    @property
    def cache_only(self) -> bool:
        return self.mode == "cache_only"


@dataclass(frozen=True)
class HistoryRequestPlan(DataProviderPlan):
    mirror_urls: tuple[str, ...] = ()
