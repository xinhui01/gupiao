import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from typing import List, Dict, Any, Optional, Callable
from stock_data import StockDataFetcher


class StockFilter:
    def __init__(self):
        self.fetcher = StockDataFetcher()
        self._log: Optional[Callable[[str], None]] = None
        self.min_volume_ratio = 3.0
        self.min_inner_outer_ratio = 1.0
        self.min_main_net_inflow = 0
        self.trend_days = 5
        self.ma_period = 5
        self.min_price = 2.0
        self.max_price = 100.0

    def set_log_callback(self, cb: Optional[Callable[[str], None]]) -> None:
        self._log = cb
        self.fetcher.set_log_callback(cb)

    def check_volume_ratio(self, stock_data: Dict[str, Any]) -> bool:
        volume_ratio = stock_data.get('volume_ratio', 0)
        return volume_ratio >= self.min_volume_ratio

    def check_inner_outer_ratio(self, inner_outer_data: Dict[str, Any]) -> bool:
        if inner_outer_data is None:
            return False
        ratio = inner_outer_data.get('inner_outer_ratio', 0)
        return ratio >= self.min_inner_outer_ratio

    def check_main_net_inflow(self, flow_data: Dict[str, Any]) -> bool:
        if flow_data is None:
            return False
        net_inflow = flow_data.get('main_net_inflow', 0)
        return net_inflow >= self.min_main_net_inflow

    def check_close_above_ma(
        self, history_data: pd.DataFrame, streak_days: int, ma_period: int
    ) -> bool:
        if history_data is None or history_data.empty:
            return False
        if "close" not in history_data.columns:
            return False
        
        df = history_data.sort_values("date").reset_index(drop=True) if "date" in history_data.columns else history_data.reset_index(drop=True)
        need = streak_days + ma_period - 1
        if len(df) < need:
            return False
        
        ma = df["close"].rolling(window=ma_period, min_periods=ma_period).mean()
        recent_close = df["close"].tail(streak_days)
        recent_ma = ma.tail(streak_days)
        if recent_ma.isna().any():
            return False
        return bool((recent_close.values > recent_ma.values).all())

    def calculate_trend_strength(self, history_data: pd.DataFrame, days: int = 3) -> float:
        if history_data is None or len(history_data) < days:
            return 0.0
        
        recent_data = history_data.tail(days)
        
        if 'change_pct' not in recent_data.columns:
            return 0.0
        
        return recent_data['change_pct'].sum()

    def filter_stock(self, stock_code: str) -> Dict[str, Any]:
        result = {
            'code': stock_code,
            'passed': False,
            'reasons': [],
            'data': {}
        }
        
        realtime_data = self.fetcher.get_realtime_data(stock_code)
        if realtime_data is None:
            result['reasons'].append('无法获取实时数据')
            return result
        
        result['data']['realtime'] = realtime_data
        
        if realtime_data['price'] < self.min_price or realtime_data['price'] > self.max_price:
            result['reasons'].append(f"价格不在范围 [{self.min_price}, {self.max_price}] 内")
            return result
        
        if not self.check_volume_ratio(realtime_data):
            result['reasons'].append(f"量比 {realtime_data['volume_ratio']:.2f} < {self.min_volume_ratio}")
            return result
        result['reasons'].append(f"✓ 量比: {realtime_data['volume_ratio']:.2f}")
        
        inner_outer_data = self.fetcher.get_inner_outer_disk(stock_code)
        result['data']['inner_outer'] = inner_outer_data
        if not self.check_inner_outer_ratio(inner_outer_data):
            r = (
                inner_outer_data.get("inner_outer_ratio", 0)
                if inner_outer_data
                else 0.0
            )
            result["reasons"].append(
                f"内外盘比 {r:.2f} < {self.min_inner_outer_ratio}"
            )
            return result
        result['reasons'].append(f"✓ 内外盘比: {inner_outer_data['inner_outer_ratio']:.2f}")
        
        flow_data = self.fetcher.get_individual_flow(stock_code)
        result['data']['flow'] = flow_data
        if flow_data:
            if self.check_main_net_inflow(flow_data):
                result['reasons'].append(f"✓ 主力净流入: {flow_data['main_net_inflow']:.2f}万元")
            else:
                result['reasons'].append(f"主力净流入: {flow_data['main_net_inflow']:.2f}万元 (未达标)")
        
        hist_days = max(30, self.trend_days + self.ma_period + 5)
        history_data = self.fetcher.get_history_data(stock_code, days=hist_days)
        result['data']['history'] = history_data
        
        if self.check_close_above_ma(history_data, self.trend_days, self.ma_period):
            trend_strength = self.calculate_trend_strength(history_data, self.trend_days)
            result['reasons'].append(
                f"✓ 连续{self.trend_days}日收盘均在MA{self.ma_period}之上，累计涨跌 {trend_strength:.2f}%"
            )
        else:
            result['reasons'].append(
                f"不满足连续{self.trend_days}日收盘>MA{self.ma_period}"
            )
            return result
        
        result['passed'] = True
        return result

    def scan_all_stocks(
        self,
        max_stocks: int = 100,
        progress_callback=None,
        max_workers: Optional[int] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        refresh_universe: bool = False,
    ) -> List[Dict[str, Any]]:
        log = self._log
        if log:
            log("【阶段 1/3】构建股票池…")

        all_stocks = self.fetcher.get_all_stocks(skip_cache=refresh_universe)

        if all_stocks.empty:
            if log:
                log("列表为空，扫描终止。")
            return []

        has_spot_columns = (
            "price" in all_stocks.columns and "volume_ratio" in all_stocks.columns
        )

        if has_spot_columns:
            if log:
                log(
                    f"【阶段 2/3】东方财富全表模式：原始 {len(all_stocks)} 只，按现价、量比预筛…"
                )
            all_stocks = all_stocks[all_stocks["price"] > 0]
            all_stocks = all_stocks[
                all_stocks["volume_ratio"] >= self.min_volume_ratio
            ]
        else:
            if log:
                log(
                    f"【阶段 2/3】交易所列表模式：共 {len(all_stocks)} 只，"
                    f"无量比预筛；已随机打乱后依次检测（每只单独请求行情）…"
                )
            all_stocks = all_stocks.sample(frac=1, random_state=None).reset_index(
                drop=True
            )

        results: List[Dict[str, Any]] = []
        total = min(len(all_stocks), max_stocks)
        subset = all_stocks.head(max_stocks)
        tasks: List[tuple[str, str]] = []
        for _, row in subset.iterrows():
            code = str(row["code"]).strip().zfill(6)
            name = str(row.get("name", "") or "")
            tasks.append((code, name))

        if max_workers is None:
            try:
                max_workers = int(
                    os.environ.get("GUPPIAO_SCAN_WORKERS", "12").strip() or "12"
                )
            except ValueError:
                max_workers = 12
        workers = max(1, min(int(max_workers), 64))

        if log:
            log(
                f"将检测 {total} 只：【阶段 3/3】并发 {workers} 线程拉取行情/资金/K线（过大易限流）…"
            )

        executor = ThreadPoolExecutor(max_workers=workers)
        try:
            future_to_meta = {
                executor.submit(self.filter_stock, code): (code, name)
                for code, name in tasks
            }
            done_count = 0
            for fut in as_completed(future_to_meta):
                code, name = future_to_meta[fut]
                if should_stop and should_stop():
                    if log:
                        log("收到停止信号，正在取消未完成任务…")
                    break
                try:
                    filter_result = fut.result()
                except Exception as e:
                    if log:
                        log(f"  {code} 检测异常: {e}")
                    filter_result = {
                        "code": code,
                        "passed": False,
                        "reasons": [str(e)],
                        "data": {},
                    }
                done_count += 1
                if log:
                    log(f"  → {done_count}/{total} {code} {name}")
                if progress_callback:
                    try:
                        progress_callback(done_count, total, code, name)
                    except StopIteration:
                        raise
                if filter_result.get("passed") and filter_result.get("data", {}).get(
                    "realtime"
                ):
                    filter_result["data"]["realtime"]["name"] = name
                    results.append(filter_result)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        results.sort(key=lambda x: str(x.get("code", "")))

        if log:
            log(f"阶段 3/3 结束，符合条件共 {len(results)} 只。")

        return results

    def get_stock_detail(self, stock_code: str) -> Dict[str, Any]:
        detail = {
            'code': stock_code,
            'realtime': None,
            'flow': None,
            'inner_outer': None,
            'history': None
        }
        
        detail['realtime'] = self.fetcher.get_realtime_data(stock_code)
        detail['flow'] = self.fetcher.get_individual_flow(stock_code)
        detail['inner_outer'] = self.fetcher.get_inner_outer_disk(stock_code)
        detail['history'] = self.fetcher.get_history_data(stock_code, days=10)
        
        return detail
