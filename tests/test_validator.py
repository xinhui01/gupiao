"""stock_validator.py 数据校验测试。"""
import unittest

import pandas as pd

from stock_validator import (
    validate_ohlc,
    validate_change_pct,
    detect_date_gaps,
    validate_history,
    has_issues,
)


class TestValidateOHLC(unittest.TestCase):
    def test_normal_data_no_issues(self):
        df = pd.DataFrame([
            {"date": "2026-04-01", "open": 10.0, "close": 10.5, "high": 10.8, "low": 9.8},
            {"date": "2026-04-02", "open": 10.5, "close": 11.0, "high": 11.2, "low": 10.3},
        ])
        issues = validate_ohlc(df)
        self.assertEqual(len(issues), 0)

    def test_high_less_than_open_close(self):
        df = pd.DataFrame([
            {"date": "2026-04-01", "open": 10.0, "close": 10.5, "high": 9.0, "low": 8.0},
        ])
        issues = validate_ohlc(df)
        self.assertTrue(any("high < max" in i["issue"] for i in issues))

    def test_low_greater_than_open_close(self):
        df = pd.DataFrame([
            {"date": "2026-04-01", "open": 10.0, "close": 10.5, "high": 11.0, "low": 10.8},
        ])
        issues = validate_ohlc(df)
        self.assertTrue(any("low > min" in i["issue"] for i in issues))

    def test_negative_price(self):
        df = pd.DataFrame([
            {"date": "2026-04-01", "open": -1.0, "close": 10.0, "high": 10.0, "low": -1.0},
        ])
        issues = validate_ohlc(df)
        self.assertTrue(any("<=0" in i["issue"] for i in issues))

    def test_empty_df(self):
        self.assertEqual(validate_ohlc(pd.DataFrame()), [])

    def test_none_input(self):
        self.assertEqual(validate_ohlc(None), [])


class TestValidateChangePct(unittest.TestCase):
    def test_normal_change(self):
        df = pd.DataFrame([
            {"date": "2026-04-01", "change_pct": 5.0},
            {"date": "2026-04-02", "change_pct": -3.0},
        ])
        self.assertEqual(len(validate_change_pct(df)), 0)

    def test_abnormal_change(self):
        df = pd.DataFrame([
            {"date": "2026-04-01", "change_pct": 25.0},
            {"date": "2026-04-02", "change_pct": -30.0},
        ])
        issues = validate_change_pct(df, max_change_pct=22.0)
        self.assertEqual(len(issues), 2)


class TestDetectDateGaps(unittest.TestCase):
    def test_no_gaps(self):
        df = pd.DataFrame({"date": ["2026-04-01", "2026-04-02", "2026-04-03"]})
        self.assertEqual(len(detect_date_gaps(df)), 0)

    def test_normal_weekend_no_gap(self):
        df = pd.DataFrame({"date": ["2026-04-03", "2026-04-07"]})  # Fri to Mon = 4 days
        self.assertEqual(len(detect_date_gaps(df, max_gap_calendar_days=7)), 0)

    def test_big_gap_detected(self):
        df = pd.DataFrame({"date": ["2026-03-01", "2026-04-01"]})  # 31 day gap
        gaps = detect_date_gaps(df, max_gap_calendar_days=7)
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]["gap_days"], 31)


class TestValidateHistory(unittest.TestCase):
    def test_combined_validation(self):
        df = pd.DataFrame([
            {"date": "2026-03-01", "open": 10.0, "close": 10.5, "high": 10.8, "low": 9.8, "change_pct": 5.0},
            {"date": "2026-04-01", "open": -1.0, "close": 11.0, "high": 11.2, "low": 10.0, "change_pct": 50.0},
        ])
        report = validate_history(df, stock_code="000001")
        self.assertTrue(has_issues(report))
        self.assertTrue(len(report["ohlc"]) > 0)
        self.assertTrue(len(report["change_pct"]) > 0)
        self.assertTrue(len(report["date_gaps"]) > 0)


if __name__ == "__main__":
    unittest.main()
