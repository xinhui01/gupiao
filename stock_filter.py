from __future__ import annotations

import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from typing import List, Dict, Any, Optional, Callable
import threading
import weakref

import pandas as pd

from stock_data import StockDataFetcher
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
        self.trend_days = 5
        self.ma_period = 5
        self.limit_up_lookback_days = 5
        self.volume_lookback_days = 5
        self.volume_expand_enabled = True
        self.volume_expand_factor = 2.0
        self.require_limit_up_within_days = False

    def set_log_callback(self, cb: Optional[Callable[[str], None]]) -> None:
        self._log = cb
        self.fetcher.set_log_callback(cb)

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
        result = {
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
            "summary": "",
        }
        if history_data is None or history_data.empty:
            result["summary"] = "无历史数据"
            return result
        if "date" not in history_data.columns or "close" not in history_data.columns:
            result["summary"] = "历史数据缺少 date/close"
            return result

        df = history_data.sort_values("date").reset_index(drop=True)
        close = pd.to_numeric(df["close"], errors="coerce")
        ma = close.rolling(window=ma_period, min_periods=ma_period).mean()
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

        if "volume" in df.columns and len(df) >= volume_days:
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

        threshold = self._limit_up_threshold(board=board, stock_name=stock_name)
        result["limit_up_threshold"] = threshold
        if "change_pct" in df.columns:
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
            streak = 0
            for flag in reversed(full_limit_up_mask.tolist()):
                if bool(flag):
                    streak += 1
                else:
                    break
            result["limit_up_streak"] = streak

            broken_streak = 0
            if len(full_limit_up_mask) >= 2 and (not bool(full_limit_up_mask.iloc[-1])) and bool(full_limit_up_mask.iloc[-2]):
                result["broken_limit_up"] = True
                idx = len(full_limit_up_mask) - 2
                while idx >= 0 and bool(full_limit_up_mask.iloc[idx]):
                    broken_streak += 1
                    idx -= 1
            result["broken_streak_count"] = broken_streak
            result["after_two_limit_up"] = bool(result["broken_limit_up"] and broken_streak >= 2)

            if result["broken_limit_up"] and "volume" in df.columns and volume_enabled:
                volume = pd.to_numeric(df["volume"], errors="coerce")
                break_volume = volume.iloc[-1] if len(volume) >= 1 else None
                streak_volumes = volume.iloc[-1 - broken_streak:-1] if broken_streak > 0 else pd.Series(dtype=float)
                streak_volumes = streak_volumes.dropna()
                if (
                    break_volume is not None
                    and not pd.isna(break_volume)
                    and float(break_volume) > 0
                    and not streak_volumes.empty
                ):
                    base_volume = float(streak_volumes.min())
                    if base_volume > 0:
                        break_ratio = float(break_volume) / base_volume
                        result["volume_break_limit_up"] = bool(break_ratio >= volume_factor)

        if result["passed"]:
            result["summary"] = (
                f"最近{streak_days}日收盘全部高于MA{ma_period}，"
                f"最新收盘 {result['latest_close']:.2f} / MA{ma_period} {result['latest_ma']:.2f}"
            )
        else:
            result["summary"] = f"未满足最近{streak_days}日收盘全部高于MA{ma_period}"

        if volume_enabled and result["volume_expand"]:
            ratio_text = "-" if result["volume_expand_ratio"] is None else f"{result['volume_expand_ratio']:.2f}倍"
            result["summary"] = f"{result['summary']}；近{volume_days}日放量 {ratio_text}"
        elif not volume_enabled:
            result["summary"] = f"{result['summary']}；放量倍数检测已关闭"

        if result["limit_up_streak"] >= 2:
            result["summary"] = f"{result['summary']}；连板 {result['limit_up_streak']} 板"
        if result["broken_limit_up"]:
            result["summary"] = f"{result['summary']}；断板，前序连板 {result['broken_streak_count']} 板"
        if result["volume_break_limit_up"]:
            result["summary"] = f"{result['summary']}；放量后断板"
        return result

    def filter_stock(
        self,
        stock_code: str,
        stock_name: str = "",
        board: str = "",
        exchange: str = "",
        history_mirror: Optional[str] = None,
        mirror_pool: Optional[List[str]] = None,
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

        hist_days = max(
            14,
            self.trend_days + self.ma_period + 4,
            self.limit_up_lookback_days + self.ma_period + 4,
            self.volume_lookback_days + 4,
        )
        history_data = self.fetcher.get_history_data(
            stock_code,
            days=hist_days,
            preferred_mirror=history_mirror,
            mirror_pool=mirror_pool,
        )
        result["data"]["history"] = history_data
        if history_data is None or history_data.empty:
            result["reasons"].append("无法获取历史数据")
            return result

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

        if self.require_limit_up_within_days and not analysis.get("limit_up_within_days"):
            analysis["summary"] = (
                f"{analysis['summary']}；未命中过去{self.limit_up_lookback_days}个交易日涨停条件"
                if analysis.get("summary")
                else f"未命中过去{self.limit_up_lookback_days}个交易日涨停条件"
            )
            result["reasons"].append(analysis["summary"])
            return result

        if analysis.get("passed"):
            result["passed"] = True
            result["reasons"].append(analysis["summary"])
        else:
            result["reasons"].append(analysis["summary"])
            return result

        return result

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

    def scan_all_stocks(
        self,
        max_stocks: int = 0,
        progress_callback=None,
        max_workers: Optional[int] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        refresh_universe: bool = False,
        allowed_boards: Optional[List[str]] = None,
        allowed_exchanges: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        log = self._log
        t0 = time.time()
        if log:
            log("【阶段 1/3】加载股票池...")

        all_stocks = self.fetcher.get_all_stocks(force_refresh=refresh_universe)
        if all_stocks.empty:
            if log:
                log("股票池为空，扫描终止。")
            return []

        total_universe = len(all_stocks)
        if allowed_boards and "board" in all_stocks.columns:
            allowed_board_set = {str(x).strip() for x in allowed_boards if str(x).strip()}
            if allowed_board_set:
                before = len(all_stocks)
                all_stocks = all_stocks[all_stocks["board"].astype(str).isin(allowed_board_set)]
                if log:
                    log(
                        f"板块过滤：保留 {len(all_stocks)}/{before} 只，目标板块 {', '.join(sorted(allowed_board_set))}"
                    )
        if allowed_exchanges and "exchange" in all_stocks.columns:
            allowed_exchange_set = {str(x).strip() for x in allowed_exchanges if str(x).strip()}
            if allowed_exchange_set:
                before = len(all_stocks)
                all_stocks = all_stocks[all_stocks["exchange"].astype(str).isin(allowed_exchange_set)]
                if log:
                    log(
                        f"交易所过滤：保留 {len(all_stocks)}/{before} 只，目标交易所 {', '.join(sorted(allowed_exchange_set))}"
                    )

        if max_stocks and max_stocks > 0:
            subset = all_stocks.head(max_stocks).reset_index(drop=True)
        else:
            subset = all_stocks.reset_index(drop=True)
        total = len(subset)
        if max_workers is None:
            try:
                max_workers = int(os.environ.get("GUPPIAO_SCAN_WORKERS", "12").strip() or "12")
            except ValueError:
                max_workers = 12
        workers = max(1, min(int(max_workers), 16))

        available_mirrors = self.fetcher.get_available_history_mirrors(force_refresh=True)
        if not available_mirrors:
            if log:
                failures = self.fetcher.get_last_history_probe_failures()
                if failures:
                    sample = "；".join(f"{host}: {detail}" for host, detail in list(failures.items())[:3])
                    log(f"历史接口镜像全部不可用，最近探测失败示例：{sample}")
                log("历史接口镜像不可用，已切换为仅使用本地历史缓存扫描；未缓存到本地的股票会被快速跳过。")

        rows = subset.to_dict("records")
        random.Random(int(time.time())).shuffle(rows)
        assigned_jobs = []
        mirror_counts: Dict[str, int] = {}
        for idx, row in enumerate(rows):
            mirror = available_mirrors[idx % len(available_mirrors)] if available_mirrors else None
            if mirror:
                mirror_counts[mirror] = mirror_counts.get(mirror, 0) + 1
            assigned_jobs.append((row, mirror))

        if log:
            log(
                f"【阶段 2/3】股票池 {total_universe} 只，本次扫描 {total} 只，最近{self.trend_days}日收盘 > MA{self.ma_period}，并发 {workers} 线程。"
            )
            log("说明：扫描阶段只拉历史日线，不拉实时、资金流或内外盘。")
            log("说明：单只股票历史数据会在少量镜像间快速切换，失败节点会短时冷却，避免长时间卡死。")
            if available_mirrors:
                mirror_summary = "，".join(
                    f"{mirror.split('//', 1)[-1].split('/', 1)[0]}={mirror_counts.get(mirror, 0)}"
                    for mirror in available_mirrors
                )
                log(f"历史镜像分区：{mirror_summary}")
            else:
                log("历史镜像分区：当前无可用镜像，本轮仅尝试读取 data/stock_store.sqlite3 中已缓存的历史数据。")

        results: List[Dict[str, Any]] = []
        completed = 0
        last_report = time.time()
        report_every = 25

        def _submit_tasks(executor: ThreadPoolExecutor):
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
                )
                future_to_meta[future] = (code, name, board, exchange, mirror)
            return future_to_meta

        executor = DaemonThreadPoolExecutor(max_workers=workers)
        try:
            future_to_meta = _submit_tasks(executor)
            if log:
                log("【阶段 3/3】开始逐只拉取历史日线并计算结果...")

            pending = set(future_to_meta)
            while pending:
                done, pending = wait(pending, timeout=2.0, return_when=FIRST_COMPLETED)
                if not done:
                    if should_stop and should_stop():
                        if log:
                            log("收到停止信号，正在取消未完成任务...")
                        break
                    now = time.time()
                    if log and now - last_report >= 10:
                        elapsed = now - t0
                        sample = [
                            (
                                f"{future_to_meta[f][0]}@{future_to_meta[f][4].split('//', 1)[-1].split('/', 1)[0]}"
                                if future_to_meta[f][4]
                                else f"{future_to_meta[f][0]}@cache-only"
                            )
                            for f in list(pending)[:3]
                        ]
                        sample_text = "、".join(sample) if sample else "-"
                        log(
                            f"进度 {completed}/{total}，命中 {len(results)} 只，已用时 {elapsed:.1f}s，"
                            f"仍在等待历史数据返回，示例代码 {sample_text}"
                        )
                        last_report = now
                    continue

                for fut in done:
                    code, name, board, exchange, mirror = future_to_meta[fut]
                    if should_stop and should_stop():
                        if log:
                            log("收到停止信号，正在取消未完成任务...")
                        pending.clear()
                        break
                    try:
                        filter_result = fut.result()
                    except Exception as e:
                        if log:
                            log(
                                f"  {code} {name} 检测异常[{mirror.split('//', 1)[-1].split('/', 1)[0] if mirror else 'cache-only'}]: {e}"
                            )
                        filter_result = {
                            "code": code,
                            "name": name,
                            "passed": False,
                            "reasons": [str(e)],
                            "data": {"board": board, "exchange": exchange},
                        }

                    completed += 1
                    analysis = filter_result.get("data", {}).get("analysis") or {}

                    if filter_result.get("passed") and log:
                        log(
                            f"  通过 {completed}/{total} {code} {name} "
                            f"最新收盘 {analysis.get('latest_close', 0):.2f} / MA{self.ma_period} {analysis.get('latest_ma', 0):.2f}"
                        )

                    if filter_result.get("passed"):
                        filter_result["name"] = name
                        filter_result.setdefault("data", {})
                        filter_result["data"].setdefault("board", board)
                        filter_result["data"].setdefault("exchange", exchange)
                        results.append(filter_result)

                    if progress_callback:
                        try:
                            progress_callback(completed, total, code, name)
                        except StopIteration:
                            raise

                    now = time.time()
                    if log and (completed % report_every == 0 or now - last_report >= 10):
                        elapsed = now - t0
                        log(
                            f"进度 {completed}/{total}，命中 {len(results)} 只，已用时 {elapsed:.1f}s，"
                            f"当前 {code} {name} @ {mirror.split('//', 1)[-1].split('/', 1)[0] if mirror else 'cache-only'}"
                        )
                        last_report = now
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        results.sort(key=self._result_sort_key, reverse=True)

        if log:
            elapsed = time.time() - t0
            log(f"【完成】扫描结束，命中 {len(results)} 只，用时 {elapsed:.1f}s。")

        return results

    def get_stock_detail(self, stock_code: str) -> Dict[str, Any]:
        code = str(stock_code).strip().zfill(6)
        history_days = max(30, self.trend_days + self.limit_up_lookback_days + self.ma_period + 4)
        history = self._call_with_timeout(
            lambda: self.fetcher.get_history_data(code, days=history_days),
            timeout_sec=15.0,
            fallback=None,
            task_name=f"详情历史 {code}",
        )
        name = ""
        board = ""
        exchange = ""
        universe = self._call_with_timeout(
            lambda: self.fetcher.get_all_stocks(),
            timeout_sec=8.0,
            fallback=None,
            task_name=f"详情股票池 {code}",
        )
        if universe is not None:
            try:
                match = universe[universe["code"].astype(str).str.zfill(6) == code]
                if not match.empty:
                    row = match.iloc[0]
                    name = str(row.get("name", "") or "")
                    board = str(row.get("board", "") or "")
                    exchange = str(row.get("exchange", "") or "")
            except Exception:
                pass
        analysis = self.analyze_history(
            history,
            self.trend_days,
            self.ma_period,
            self.limit_up_lookback_days,
            self.volume_lookback_days,
            self.volume_expand_enabled,
            self.volume_expand_factor,
            board=board,
            stock_name=name,
            stock_code=stock_code,
        )
        if history is not None and not history.empty:
            latest_row = history.iloc[-1]
            analysis["latest_volume"] = latest_row.get("volume")
            analysis["latest_amount"] = latest_row.get("amount")
            analysis["quote_time"] = str(latest_row.get("date", "") or "")
        else:
            analysis["latest_volume"] = None
            analysis["latest_amount"] = None

        fund_flow_df = self._call_with_timeout(
            lambda: self.fetcher.get_fund_flow_data(code, days=30, force_refresh=False),
            timeout_sec=10.0,
            fallback=None,
            task_name=f"详情资金流 {code}",
        )
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
        else:
            analysis["flow_date"] = ""
            analysis["main_force_amount"] = None
            analysis["big_order_amount"] = None
            analysis["super_big_order_amount"] = None
            analysis["main_force_ratio"] = None
            analysis["big_order_ratio"] = None
            analysis["super_big_order_ratio"] = None
            analysis["fund_flow_history"] = []
        return {
            "code": code,
            "name": name,
            "board": board,
            "exchange": exchange,
            "history": history,
            "analysis": analysis,
        }

    def get_stock_intraday(self, stock_code: str) -> Dict[str, Any]:
        code = str(stock_code).strip().zfill(6)
        intraday_df = self._call_with_timeout(
            lambda: self.fetcher.get_intraday_data(code),
            timeout_sec=12.0,
            fallback=None,
            task_name=f"分时 {code}",
        )
        prev_close = None
        history_df = self._call_with_timeout(
            lambda: self.fetcher.get_history_data(code, days=2),
            timeout_sec=6.0,
            fallback=None,
            task_name=f"分时昨收 {code}",
        )
        if history_df is not None and not history_df.empty and "close" in history_df.columns:
            close_series = pd.to_numeric(history_df["close"], errors="coerce").dropna()
            if not close_series.empty:
                prev_close = float(close_series.iloc[-1])
        return {
            "code": code,
            "intraday": intraday_df,
            "prev_close": prev_close,
        }
