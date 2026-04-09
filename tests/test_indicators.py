"""stock_indicators.py 技术指标测试。"""
import unittest

import numpy as np
import pandas as pd

from stock_indicators import calc_macd, calc_kdj, calc_rsi, calc_boll, enrich_with_indicators


def _make_close_series(n: int = 60) -> pd.Series:
    """生成模拟收盘价序列。"""
    np.random.seed(42)
    prices = 10.0 + np.cumsum(np.random.randn(n) * 0.3)
    return pd.Series(prices.clip(min=1.0))


def _make_ohlcv_df(n: int = 60) -> pd.DataFrame:
    np.random.seed(42)
    close = 10.0 + np.cumsum(np.random.randn(n) * 0.3)
    close = close.clip(min=1.0)
    high = close + np.abs(np.random.randn(n) * 0.2)
    low = close - np.abs(np.random.randn(n) * 0.2)
    low = low.clip(min=0.5)
    open_ = close + np.random.randn(n) * 0.1
    dates = pd.date_range("2026-01-01", periods=n, freq="B")
    return pd.DataFrame({
        "date": dates.strftime("%Y-%m-%d"),
        "open": open_,
        "close": close,
        "high": high,
        "low": low,
        "volume": np.random.randint(1000, 10000, n),
    })


class TestMACD(unittest.TestCase):
    def test_macd_keys(self):
        close = _make_close_series()
        result = calc_macd(close)
        self.assertIn("dif", result)
        self.assertIn("dea", result)
        self.assertIn("macd", result)

    def test_macd_length(self):
        close = _make_close_series(100)
        result = calc_macd(close)
        self.assertEqual(len(result["dif"]), 100)

    def test_macd_values_finite(self):
        close = _make_close_series(100)
        result = calc_macd(close)
        # EWM 会从第1行就开始有值
        self.assertTrue(np.isfinite(result["dif"].iloc[-1]))
        self.assertTrue(np.isfinite(result["dea"].iloc[-1]))


class TestKDJ(unittest.TestCase):
    def test_kdj_keys(self):
        df = _make_ohlcv_df()
        result = calc_kdj(df["high"], df["low"], df["close"])
        self.assertIn("k", result)
        self.assertIn("d", result)
        self.assertIn("j", result)

    def test_kdj_initial_value(self):
        df = _make_ohlcv_df()
        result = calc_kdj(df["high"], df["low"], df["close"])
        self.assertAlmostEqual(result["k"].iloc[0], 50.0)
        self.assertAlmostEqual(result["d"].iloc[0], 50.0)

    def test_kdj_range(self):
        """K/D 值通常在 0~100 之间（J 可能超出）。"""
        df = _make_ohlcv_df(200)
        result = calc_kdj(df["high"], df["low"], df["close"])
        k = result["k"].dropna()
        self.assertTrue((k >= -10).all())
        self.assertTrue((k <= 110).all())


class TestRSI(unittest.TestCase):
    def test_rsi_keys(self):
        close = _make_close_series()
        result = calc_rsi(close)
        self.assertIn("rsi_6", result)
        self.assertIn("rsi_12", result)
        self.assertIn("rsi_24", result)

    def test_rsi_range(self):
        """RSI 应在 0~100 之间。"""
        close = _make_close_series(200)
        result = calc_rsi(close)
        rsi6 = result["rsi_6"].dropna()
        self.assertTrue((rsi6 >= 0).all())
        self.assertTrue((rsi6 <= 100).all())

    def test_custom_periods(self):
        close = _make_close_series()
        result = calc_rsi(close, periods=(5, 10))
        self.assertIn("rsi_5", result)
        self.assertIn("rsi_10", result)
        self.assertNotIn("rsi_6", result)


class TestBOLL(unittest.TestCase):
    def test_boll_keys(self):
        close = _make_close_series()
        result = calc_boll(close)
        self.assertIn("mid", result)
        self.assertIn("upper", result)
        self.assertIn("lower", result)

    def test_upper_above_mid_above_lower(self):
        close = _make_close_series(60)
        result = calc_boll(close)
        valid = result["mid"].dropna().index
        for i in valid:
            self.assertGreaterEqual(result["upper"].iloc[i], result["mid"].iloc[i])
            self.assertLessEqual(result["lower"].iloc[i], result["mid"].iloc[i])


class TestEnrich(unittest.TestCase):
    def test_enrich_adds_all_columns(self):
        df = _make_ohlcv_df()
        enriched = enrich_with_indicators(df)
        for col in ["macd_dif", "macd_dea", "macd_bar", "kdj_k", "kdj_d", "kdj_j",
                     "rsi_6", "rsi_12", "rsi_24", "boll_mid", "boll_upper", "boll_lower"]:
            self.assertIn(col, enriched.columns, f"缺少列: {col}")
        self.assertEqual(len(enriched), len(df))

    def test_enrich_does_not_modify_original(self):
        df = _make_ohlcv_df()
        original_cols = list(df.columns)
        _ = enrich_with_indicators(df)
        self.assertEqual(list(df.columns), original_cols)


if __name__ == "__main__":
    unittest.main()
