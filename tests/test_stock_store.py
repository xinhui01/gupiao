"""stock_store.py 的单元测试——使用临时 SQLite 数据库。"""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd


class StockStoreTestCase(unittest.TestCase):
    """每个用例独享一个临时数据库目录。"""

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._patch_dir = mock.patch("stock_store._DATA_DIR", Path(self._tmp))
        self._patch_db = mock.patch("stock_store._DB_PATH", Path(self._tmp) / "test.sqlite3")
        self._patch_dir.start()
        self._patch_db.start()
        import stock_store
        stock_store.ensure_store_ready()

    def tearDown(self):
        self._patch_dir.stop()
        self._patch_db.stop()


class TestUniverse(StockStoreTestCase):
    def test_save_and_load_universe(self):
        from stock_store import save_universe, load_universe
        df = pd.DataFrame([
            {"code": "000001", "name": "平安银行", "exchange": "SZ", "board": "主板", "concepts": "金融"},
            {"code": "300001", "name": "特锐德", "exchange": "SZ", "board": "创业板", "concepts": "充电桩"},
        ])
        save_universe(df)
        loaded = load_universe()
        self.assertIsNotNone(loaded)
        self.assertEqual(len(loaded), 2)
        self.assertEqual(loaded.iloc[0]["name"], "平安银行")

    def test_save_universe_with_empty_df(self):
        from stock_store import save_universe, load_universe
        save_universe(pd.DataFrame())
        loaded = load_universe()
        self.assertIsNone(loaded)


class TestHistory(StockStoreTestCase):
    def test_save_and_load_history(self):
        from stock_store import save_history, load_history
        df = pd.DataFrame([
            {"date": "2026-04-01", "open": 10.0, "close": 10.5, "high": 11.0, "low": 9.8, "volume": 1000, "amount": 10500},
            {"date": "2026-04-02", "open": 10.5, "close": 11.0, "high": 11.2, "low": 10.3, "volume": 1200, "amount": 12600},
        ])
        save_history("000001", df)
        loaded = load_history("000001")
        self.assertIsNotNone(loaded)
        self.assertEqual(len(loaded), 2)
        self.assertAlmostEqual(loaded.iloc[0]["close"], 10.5)

    def test_load_history_with_limit(self):
        from stock_store import save_history, load_history
        df = pd.DataFrame([
            {"date": f"2026-04-{i:02d}", "open": 10.0, "close": 10.0 + i, "high": 11.0, "low": 9.0, "volume": 100}
            for i in range(1, 11)
        ])
        save_history("000001", df)
        loaded = load_history("000001", limit=3)
        self.assertEqual(len(loaded), 3)
        # limit 取最新 3 条再按日期升序
        self.assertIn("2026-04-10", loaded["date"].values)

    def test_load_history_nonexistent(self):
        from stock_store import load_history
        loaded = load_history("999999")
        self.assertIsNone(loaded)

    def test_save_history_upsert(self):
        from stock_store import save_history, load_history
        df1 = pd.DataFrame([{"date": "2026-04-01", "open": 10.0, "close": 10.5, "high": 11.0, "low": 9.8, "volume": 1000}])
        save_history("000001", df1)
        df2 = pd.DataFrame([{"date": "2026-04-01", "open": 10.0, "close": 99.9, "high": 11.0, "low": 9.8, "volume": 1000}])
        save_history("000001", df2)
        loaded = load_history("000001")
        self.assertEqual(len(loaded), 1)
        self.assertAlmostEqual(loaded.iloc[0]["close"], 99.9)


class TestHistoryMeta(StockStoreTestCase):
    def test_save_and_load_meta(self):
        from stock_store import save_history_meta, load_history_meta
        save_history_meta("000001", "2026-04-07", 100, "eastmoney")
        meta = load_history_meta("000001")
        self.assertIsNotNone(meta)
        self.assertEqual(meta["latest_trade_date"], "2026-04-07")
        self.assertEqual(meta["row_count"], 100)
        self.assertEqual(meta["source"], "eastmoney")

    def test_load_nonexistent_meta(self):
        from stock_store import load_history_meta
        self.assertIsNone(load_history_meta("999999"))


class TestFundFlow(StockStoreTestCase):
    def test_save_and_load_fund_flow(self):
        from stock_store import save_fund_flow, load_fund_flow
        df = pd.DataFrame([
            {"date": "2026-04-07", "close": 10.5, "change_pct": 2.0, "main_force_amount": 500, "main_force_ratio": 10.0},
        ])
        save_fund_flow("000001", df)
        loaded = load_fund_flow("000001")
        self.assertIsNotNone(loaded)
        self.assertEqual(len(loaded), 1)
        self.assertAlmostEqual(loaded.iloc[0]["main_force_ratio"], 10.0)


class TestAppConfig(StockStoreTestCase):
    def test_save_and_load_config(self):
        from stock_store import save_app_config, load_app_config
        data = {"columns": ["code", "name"], "visible": True}
        save_app_config("test_key", data)
        loaded = load_app_config("test_key")
        self.assertEqual(loaded["columns"], ["code", "name"])
        self.assertTrue(loaded["visible"])

    def test_load_missing_config_returns_default(self):
        from stock_store import load_app_config
        result = load_app_config("nonexistent", default={"fallback": True})
        self.assertEqual(result, {"fallback": True})

    def test_save_overwrite(self):
        from stock_store import save_app_config, load_app_config
        save_app_config("k", {"v": 1})
        save_app_config("k", {"v": 2})
        self.assertEqual(load_app_config("k")["v"], 2)


class TestWatchlist(StockStoreTestCase):
    def test_save_load_delete_watchlist(self):
        from stock_store import save_watchlist_item, load_watchlist, load_watchlist_item, delete_watchlist_item
        save_watchlist_item({"code": "000001", "name": "平安银行", "score": 85})
        items = load_watchlist()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["name"], "平安银行")

        item = load_watchlist_item("000001")
        self.assertIsNotNone(item)
        self.assertAlmostEqual(item["score"], 85.0)

        delete_watchlist_item("000001")
        items = load_watchlist()
        self.assertEqual(len(items), 0)

    def test_watchlist_upsert(self):
        from stock_store import save_watchlist_item, load_watchlist
        save_watchlist_item({"code": "000001", "name": "平安银行", "score": 50})
        save_watchlist_item({"code": "000001", "name": "平安银行(更新)", "score": 90})
        items = load_watchlist()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["name"], "平安银行(更新)")


class TestScanSnapshot(StockStoreTestCase):
    def test_save_and_load_snapshot(self):
        from stock_store import save_scan_snapshot, load_scan_snapshot
        payload = {
            "scan_date": "2026-04-07",
            "complete": True,
            "row_count": 5,
            "saved_at": "2026-04-07 16:00:00",
            "results": [{"code": "000001"}],
        }
        save_scan_snapshot("sig1", payload)
        loaded = load_scan_snapshot("sig1")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded["row_count"], 5)

    def test_load_incomplete_snapshot_returns_none(self):
        from stock_store import save_scan_snapshot, load_scan_snapshot
        payload = {"scan_date": "2026-04-07", "complete": False, "row_count": 0, "saved_at": "2026-04-07 16:00:00"}
        save_scan_snapshot("sig2", payload)
        loaded = load_scan_snapshot("sig2")
        self.assertIsNone(loaded)

    def test_load_latest_snapshot(self):
        from stock_store import save_scan_snapshot, load_latest_scan_snapshot
        for i in range(3):
            payload = {
                "scan_date": f"2026-04-0{i+1}",
                "complete": True,
                "row_count": i + 1,
                "saved_at": f"2026-04-0{i+1} 16:00:00",
            }
            save_scan_snapshot(f"sig_{i}", payload)
        latest = load_latest_scan_snapshot()
        self.assertIsNotNone(latest)
        self.assertEqual(latest["row_count"], 3)


class TestIntradayCache(StockStoreTestCase):
    def test_save_and_load_intraday(self):
        from stock_store import save_intraday_cache, load_intraday_cache
        save_intraday_cache("000001", "2026-04-07", '{"rows":[]}', '{"auction":"data"}', 10)
        cached = load_intraday_cache("000001", "2026-04-07")
        self.assertIsNotNone(cached)
        self.assertEqual(cached["row_count"], 10)
        self.assertIn("rows", cached["data_json"])

    def test_load_nonexistent_intraday(self):
        from stock_store import load_intraday_cache
        self.assertIsNone(load_intraday_cache("999999", "2026-04-07"))


class TestCoverageSummary(StockStoreTestCase):
    def test_coverage_with_data(self):
        from stock_store import save_universe, save_history, history_coverage_summary
        uni_df = pd.DataFrame([
            {"code": "000001", "name": "A"},
            {"code": "000002", "name": "B"},
        ])
        save_universe(uni_df)
        hist_df = pd.DataFrame([{"date": "2026-04-07", "open": 10, "close": 10.5, "high": 11, "low": 9.5, "volume": 100}])
        save_history("000001", hist_df)

        summary = history_coverage_summary()
        self.assertEqual(summary["universe_count"], 2)
        self.assertEqual(summary["covered_count"], 1)
        self.assertAlmostEqual(summary["coverage_ratio"], 0.5)


class TestClearTables(StockStoreTestCase):
    def test_clear_universe(self):
        from stock_store import save_universe, load_universe, clear_universe
        df = pd.DataFrame([{"code": "000001", "name": "A"}])
        save_universe(df)
        self.assertIsNotNone(load_universe())
        clear_universe()
        self.assertIsNone(load_universe())

    def test_clear_history(self):
        from stock_store import save_history, load_history, clear_history
        df = pd.DataFrame([{"date": "2026-04-07", "open": 10, "close": 10.5, "high": 11, "low": 9.5, "volume": 100}])
        save_history("000001", df)
        self.assertIsNotNone(load_history("000001"))
        clear_history()
        self.assertIsNone(load_history("000001"))


if __name__ == "__main__":
    unittest.main()
