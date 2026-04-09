"""评分系统 (_calculate_trade_score) 和边界条件测试。"""
import unittest

import pandas as pd

from stock_filter import StockFilter


def _build_filter() -> StockFilter:
    sf = StockFilter.__new__(StockFilter)
    sf.trend_days = 5
    sf.ma_period = 5
    sf.limit_up_lookback_days = 5
    sf.volume_lookback_days = 5
    sf.volume_expand_enabled = True
    sf.volume_expand_factor = 2.0
    sf.require_limit_up_within_days = False
    sf._log = None
    return sf


class TestScoreRange(unittest.TestCase):
    """测试评分始终在 0~100 之间。"""

    def test_score_clamps_to_zero(self):
        """极端差数据不会出现负分。"""
        sf = _build_filter()
        result = {
            "passed": False,
            "five_day_return": -20,
            "latest_change_pct": -9,
            "limit_up_streak": 0,
            "limit_up_within_days": False,
            "volume_expand": False,
            "volume_expand_ratio": 1.0,
            "latest_volume_ratio": 50,
            "broken_limit_up": True,
            "after_two_limit_up": True,
            "volume_break_limit_up": True,
        }
        score, breakdown = sf._calculate_trade_score(result, 5, 5, True)
        self.assertGreaterEqual(score, 0)
        self.assertLessEqual(score, 100)

    def test_score_clamps_to_hundred(self):
        """极端好数据不会超过 100。"""
        sf = _build_filter()
        result = {
            "passed": True,
            "five_day_return": 30,
            "latest_change_pct": 10.0,
            "limit_up_streak": 5,
            "limit_up_within_days": True,
            "volume_expand": True,
            "volume_expand_ratio": 5.0,
            "latest_volume_ratio": 300,
            "broken_limit_up": False,
            "after_two_limit_up": False,
            "volume_break_limit_up": False,
        }
        score, breakdown = sf._calculate_trade_score(result, 5, 5, True)
        self.assertEqual(score, 100)

    def test_base_score_is_50_with_neutral_data(self):
        """中性数据基准分 50 附近。"""
        sf = _build_filter()
        result = {
            "passed": True,
            "five_day_return": 3,
            "latest_change_pct": 2,
            "limit_up_streak": 0,
            "limit_up_within_days": False,
            "volume_expand": False,
            "volume_expand_ratio": 1.5,
            "latest_volume_ratio": 100,
            "broken_limit_up": False,
            "after_two_limit_up": False,
            "volume_break_limit_up": False,
        }
        score, _ = sf._calculate_trade_score(result, 5, 5, True)
        # passed (+18) + 量能偏弱(-4) = 50+18-4 = 64
        self.assertGreaterEqual(score, 50)
        self.assertLessEqual(score, 80)


class TestScoreBreakdown(unittest.TestCase):
    def test_limit_up_streak_bonus(self):
        sf = _build_filter()
        result = {
            "passed": True,
            "five_day_return": 10,
            "latest_change_pct": 10.0,
            "limit_up_streak": 3,
            "limit_up_within_days": True,
            "volume_expand": False,
            "volume_expand_ratio": None,
            "latest_volume_ratio": None,
            "broken_limit_up": False,
            "after_two_limit_up": False,
            "volume_break_limit_up": False,
        }
        score, breakdown = sf._calculate_trade_score(result, 5, 5, False)
        self.assertIn("高连板+10", breakdown)
        self.assertIn("当日涨停+14", breakdown)

    def test_two_board_streak_bonus(self):
        sf = _build_filter()
        result = {
            "passed": True,
            "five_day_return": 8,
            "latest_change_pct": 10.0,
            "limit_up_streak": 2,
            "limit_up_within_days": True,
            "volume_expand": False,
            "volume_expand_ratio": None,
            "latest_volume_ratio": None,
            "broken_limit_up": False,
            "after_two_limit_up": False,
            "volume_break_limit_up": False,
        }
        score, breakdown = sf._calculate_trade_score(result, 5, 5, False)
        self.assertIn("二连板+7", breakdown)

    def test_broken_limit_up_penalties(self):
        sf = _build_filter()
        result = {
            "passed": False,
            "five_day_return": 5,
            "latest_change_pct": -2,
            "limit_up_streak": 0,
            "limit_up_within_days": True,
            "volume_expand": False,
            "volume_expand_ratio": None,
            "latest_volume_ratio": None,
            "broken_limit_up": True,
            "after_two_limit_up": True,
            "volume_break_limit_up": True,
        }
        score, breakdown = sf._calculate_trade_score(result, 5, 5, False)
        self.assertIn("断板-10", breakdown)
        self.assertIn("二板后断板-6", breakdown)
        self.assertIn("放量断板-5", breakdown)


class TestLimitUpThreshold(unittest.TestCase):
    def test_st_stock_threshold(self):
        sf = _build_filter()
        self.assertAlmostEqual(sf._limit_up_threshold(board="主板", stock_name="ST测试"), 5.0)
        self.assertAlmostEqual(sf._limit_up_threshold(board="主板", stock_name="*ST测试"), 5.0)

    def test_gem_star_threshold(self):
        sf = _build_filter()
        self.assertAlmostEqual(sf._limit_up_threshold(board="创业板"), 20.0)
        self.assertAlmostEqual(sf._limit_up_threshold(board="科创板"), 20.0)

    def test_main_board_threshold(self):
        sf = _build_filter()
        self.assertAlmostEqual(sf._limit_up_threshold(board="主板"), 10.0)


class TestAnalyzeHistoryEdgeCases(unittest.TestCase):
    def test_empty_dataframe(self):
        sf = _build_filter()
        result = sf.analyze_history(pd.DataFrame())
        self.assertFalse(result["passed"])
        self.assertEqual(result["score"], 0)
        self.assertEqual(result["summary"], "无历史数据")

    def test_missing_columns(self):
        sf = _build_filter()
        df = pd.DataFrame([{"foo": 1}])
        result = sf.analyze_history(df)
        self.assertFalse(result["passed"])
        self.assertIn("缺少", result["summary"])

    def test_none_input(self):
        sf = _build_filter()
        result = sf.analyze_history(None)
        self.assertFalse(result["passed"])
        self.assertEqual(result["summary"], "无历史数据")

    def test_insufficient_data(self):
        sf = _build_filter()
        df = pd.DataFrame([{"date": "2026-04-01", "close": 10.0}])
        result = sf.analyze_history(df)
        self.assertFalse(result["passed"])
        # 数据不足时仍然会打分（基础 50 - 各项扣分），分数应在合理区间
        self.assertGreaterEqual(result["score"], 0)
        self.assertLessEqual(result["score"], 100)


class TestVolumeAnalysis(unittest.TestCase):
    def test_volume_expand_detection(self):
        sf = _build_filter()
        sf.volume_lookback_days = 3
        sf.volume_expand_factor = 2.0
        dates = [f"2026-04-{i:02d}" for i in range(1, 12)]
        # 前面低量，最后几天暴量
        volumes = [100] * 8 + [100, 300, 100]
        df = pd.DataFrame({"date": dates, "close": [10 + i * 0.1 for i in range(11)], "volume": volumes, "change_pct": [1.0] * 11})
        result = sf.analyze_history(df)
        self.assertTrue(result["volume_expand"])
        self.assertGreater(result["volume_expand_ratio"], 2.0)

    def test_volume_ratio_high(self):
        sf = _build_filter()
        sf.volume_lookback_days = 3
        dates = [f"2026-04-{i:02d}" for i in range(1, 12)]
        volumes = [100] * 10 + [300]
        df = pd.DataFrame({"date": dates, "close": [10 + i * 0.1 for i in range(11)], "volume": volumes, "change_pct": [1.0] * 11})
        result = sf.analyze_history(df)
        self.assertIsNotNone(result["latest_volume_ratio"])
        self.assertGreater(result["latest_volume_ratio"], 150)


if __name__ == "__main__":
    unittest.main()
