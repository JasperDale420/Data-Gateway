"""Unit tests for Data Quality Analyzer."""

from datetime import date

import pytest

from gateway.core.quality import (
    BarQuality,
    QualityAnalyzer,
    QualityCode,
    QualityIssue,
    QuoteQuality,
    SymbolQuality,
    get_quality_analyzer,
)


class TestQualityCode:
    """Test QualityCode enum."""

    def test_codes_exist(self):
        assert QualityCode.MISSING_BARS.value == "Q001"
        assert QualityCode.CROSSED_QUOTE.value == "Q002"
        assert QualityCode.STALE_QUOTE.value == "Q003"
        assert QualityCode.ZERO_VOLUME.value == "Q004"
        assert QualityCode.PRICE_ANOMALY.value == "Q005"
        assert QualityCode.TIMESTAMP_OUT_OF_SEQ.value == "Q006"


class TestBarQuality:
    """Test BarQuality dataclass."""

    def test_to_dict(self):
        quality = BarQuality(
            expected=390,
            received=388,
            coverage=0.995,
            gaps=[],
        )
        d = quality.to_dict()
        assert d["expected"] == 390
        assert d["received"] == 388
        assert d["coverage"] == 0.995


class TestQuoteQuality:
    """Test QuoteQuality dataclass."""

    def test_to_dict(self):
        quality = QuoteQuality(
            total=125000,
            crossed=12,
            crossed_pct=0.0001,
            stale=45,
            stale_pct=0.0004,
            avg_spread_bps=2.5,
        )
        d = quality.to_dict()
        assert d["total"] == 125000
        assert d["crossed"] == 12
        assert d["avg_spread_bps"] == 2.5


class TestQualityAnalyzer:
    """Test QualityAnalyzer."""

    def setup_method(self):
        self.analyzer = QualityAnalyzer()

    # Analyze bars
    def test_analyze_bars_full_coverage(self):
        bars = [{"close": 100, "volume": 1000}] * 390
        quality = self.analyzer.analyze_bars(bars, expected_count=390)
        assert quality.expected == 390
        assert quality.received == 390
        assert quality.coverage == 1.0

    def test_analyze_bars_partial_coverage(self):
        bars = [{"close": 100, "volume": 1000}] * 388
        quality = self.analyzer.analyze_bars(bars, expected_count=390)
        assert quality.coverage == pytest.approx(0.9949, rel=0.01)

    def test_analyze_bars_detects_gaps(self):
        bars = [
            {"timestamp": "2024-01-15T10:00:00Z", "close": 100},
            {"timestamp": "2024-01-15T10:01:00Z", "close": 101},
            # Gap here
            {"timestamp": "2024-01-15T10:05:00Z", "close": 105},
            {"timestamp": "2024-01-15T10:06:00Z", "close": 106},
        ]
        quality = self.analyzer.analyze_bars(bars, expected_count=10, timeframe="1Min")
        assert len(quality.gaps) >= 1

    # Analyze quotes
    def test_analyze_quotes_crossed(self):
        quotes = [
            {"bid_price": 100.05, "ask_price": 100.00},  # Crossed!
            {"bid_price": 100.00, "ask_price": 100.05},  # Normal
        ]
        quality = self.analyzer.analyze_quotes(quotes)
        assert quality.total == 2
        assert quality.crossed == 1

    def test_analyze_quotes_spread(self):
        quotes = [
            {"bid_price": 100.00, "ask_price": 100.10},
            {"bid_price": 100.05, "ask_price": 100.15},
        ]
        quality = self.analyzer.analyze_quotes(quotes)
        assert quality.avg_spread_bps > 0

    # Analyze trades
    def test_analyze_trades(self):
        trades = [
            {"size": 100},
            {"size": 200},
            {"size": 50},  # Odd lot
        ]
        quality = self.analyzer.analyze_trades(trades)
        assert quality.total == 3
        assert quality.odd_lots == 1
        assert quality.avg_size == pytest.approx(116.67, rel=0.01)

    # Detect issues
    def test_detect_zero_volume(self):
        bars = [
            {"timestamp": "2024-01-15T10:00:00Z", "close": 100, "volume": 0},
        ]
        issues = self.analyzer.detect_issues(bars=bars)
        zero_vol_issues = [i for i in issues if i.code == QualityCode.ZERO_VOLUME]
        assert len(zero_vol_issues) == 1

    def test_detect_price_anomaly(self):
        bars = [
            {"timestamp": "2024-01-15T10:00:00Z", "close": 100, "volume": 1000},
            {"timestamp": "2024-01-15T10:01:00Z", "close": 150, "volume": 1000},  # 50% jump!
        ]
        issues = self.analyzer.detect_issues(bars=bars)
        anomaly_issues = [i for i in issues if i.code == QualityCode.PRICE_ANOMALY]
        assert len(anomaly_issues) == 1

    def test_detect_crossed_quote(self):
        quotes = [
            {"timestamp": "2024-01-15T10:00:00Z", "bid_price": 105, "ask_price": 100},
        ]
        issues = self.analyzer.detect_issues(quotes=quotes)
        crossed_issues = [i for i in issues if i.code == QualityCode.CROSSED_QUOTE]
        assert len(crossed_issues) == 1

    # Quality issue to_dict
    def test_quality_issue_to_dict(self):
        issue = QualityIssue(
            code=QualityCode.MISSING_BARS,
            message="Missing bars in sequence",
            timestamp="10:30:00",
        )
        d = issue.to_dict()
        assert d["code"] == "Q001"
        assert d["message"] == "Missing bars in sequence"

    # Symbol quality to_dict
    def test_symbol_quality_to_dict(self):
        quality = SymbolQuality(
            symbol="AAPL",
            date=date(2024, 1, 15),
            session="regular",
            bars=BarQuality(expected=390, received=390, coverage=1.0),
        )
        d = quality.to_dict()
        assert d["symbol"] == "AAPL"
        assert d["session"] == "regular"
        assert "bars" in d

    # Singleton
    def test_singleton(self):
        a1 = get_quality_analyzer()
        a2 = get_quality_analyzer()
        assert a1 is a2
