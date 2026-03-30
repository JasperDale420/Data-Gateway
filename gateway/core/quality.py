"""Data Quality Metrics.

Per-symbol quality analysis and issue detection as specified in PRD (lines 918-984).
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any

_TIMEFRAME_TO_MINUTES: dict[str, int] = {
    "1Min": 1,
    "1m": 1,
    "5Min": 5,
    "5m": 5,
    "15Min": 15,
    "15m": 15,
    "30Min": 30,
    "30m": 30,
    "1Hour": 60,
    "1h": 60,
    "1D": 390,
    "1d": 390,
}


# ─────────────────────────────────────────────────────────────────────────────
# Quality Issue Codes (per PRD)
# ─────────────────────────────────────────────────────────────────────────────


class QualityCode(str, Enum):
    """Data quality issue codes."""

    MISSING_BARS = "Q001"  # Missing bars in sequence
    CROSSED_QUOTE = "Q002"  # Bid > Ask
    STALE_QUOTE = "Q003"  # Unchanged > 60s
    ZERO_VOLUME = "Q004"  # Zero volume during market hours
    PRICE_ANOMALY = "Q005"  # > 20% move
    TIMESTAMP_OUT_OF_SEQ = "Q006"  # Out of order timestamps


# ─────────────────────────────────────────────────────────────────────────────
# Quality Metrics
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class GapInfo:
    """Information about a data gap."""

    start: str  # Time string
    end: str
    missing: int  # Number of missing bars

    def to_dict(self) -> dict[str, Any]:
        return {
            "start": self.start,
            "end": self.end,
            "missing": self.missing,
        }


@dataclass
class BarQuality:
    """Quality metrics for bar data."""

    expected: int
    received: int
    coverage: float
    gaps: list[GapInfo] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "expected": self.expected,
            "received": self.received,
            "coverage": self.coverage,
            "gaps": [g.to_dict() for g in self.gaps],
        }


@dataclass
class QuoteQuality:
    """Quality metrics for quote data."""

    total: int
    crossed: int  # Bid > Ask
    crossed_pct: float
    stale: int  # Unchanged > 60s
    stale_pct: float
    avg_spread_bps: float  # Average spread in basis points

    def to_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "crossed": self.crossed,
            "crossed_pct": self.crossed_pct,
            "stale": self.stale,
            "stale_pct": self.stale_pct,
            "avg_spread_bps": self.avg_spread_bps,
        }


@dataclass
class TradeQuality:
    """Quality metrics for trade data."""

    total: int
    odd_lots: int  # Trades with size not multiple of 100
    odd_lot_pct: float
    avg_size: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "odd_lots": self.odd_lots,
            "odd_lot_pct": self.odd_lot_pct,
            "avg_size": self.avg_size,
        }


@dataclass
class QualityIssue:
    """A single quality issue."""

    code: QualityCode
    message: str
    timestamp: str | None = None
    details: dict | None = None

    def to_dict(self) -> dict[str, Any]:
        result = {
            "code": self.code.value,
            "message": self.message,
        }
        if self.timestamp:
            result["timestamp"] = self.timestamp
        if self.details:
            result["details"] = self.details
        return result


@dataclass
class SymbolQuality:
    """Complete quality report for a symbol/date."""

    symbol: str
    date: date
    session: str  # "regular", "extended"
    bars: BarQuality | None = None
    quotes: QuoteQuality | None = None
    trades: TradeQuality | None = None
    issues: list[QualityIssue] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "symbol": self.symbol,
            "date": self.date.isoformat(),
            "session": self.session,
        }
        if self.bars:
            result["bars"] = self.bars.to_dict()
        if self.quotes:
            result["quotes"] = self.quotes.to_dict()
        if self.trades:
            result["trades"] = self.trades.to_dict()
        if self.issues:
            result["issues"] = [i.to_dict() for i in self.issues]
        return result


# ─────────────────────────────────────────────────────────────────────────────
# Quality Analyzer
# ─────────────────────────────────────────────────────────────────────────────


class QualityAnalyzer:
    """Analyzes data quality for bars, quotes, and trades.

    Detects:
    - Missing bars/gaps in data
    - Crossed quotes (bid > ask)
    - Stale quotes (unchanged > 60s)
    - Zero volume during market hours
    - Price anomalies (> 20% moves)
    - Out-of-sequence timestamps

    Example:
        analyzer = QualityAnalyzer()
        quality = analyzer.analyze_bars(bars, expected_count=390)
        print(quality.coverage)  # 0.995
    """

    # Regular market hours (9:30 AM - 4:00 PM ET)
    REGULAR_SESSION_MINUTES = 390  # 6.5 hours

    # Thresholds
    STALE_THRESHOLD_SECONDS = 60
    PRICE_ANOMALY_THRESHOLD = 0.20  # 20%

    def analyze_bars(
        self,
        bars: list[dict],
        expected_count: int | None = None,
        timeframe: str = "1Min",
        issues_out: list[QualityIssue] | None = None,
    ) -> BarQuality:
        """Analyze bar data quality.

        Args:
            bars: List of bar dictionaries
            expected_count: Expected number of bars (defaults to session length)
            timeframe: Bar timeframe for determining expected count

        Returns:
            BarQuality with coverage and gap information
        """
        if expected_count is None:
            expected_count = self._get_expected_bar_count(timeframe)

        received = len(bars)
        coverage = received / expected_count if expected_count > 0 else 0.0

        # Detect gaps
        gaps = self._detect_bar_gaps(bars, timeframe)
        if issues_out is not None:
            issues_out.extend(self._detect_bar_issues(bars))

        return BarQuality(
            expected=expected_count,
            received=received,
            coverage=round(coverage, 4),
            gaps=gaps,
        )

    def analyze_quotes(
        self,
        quotes: list[dict],
        issues_out: list[QualityIssue] | None = None,
    ) -> QuoteQuality:
        """Analyze quote data quality."""
        total = len(quotes)
        crossed = 0
        stale = 0
        spreads = []

        prev_quote = None
        prev_time = None

        for quote in quotes:
            bid = quote.get("bid_price", quote.get("bp", 0))
            ask = quote.get("ask_price", quote.get("ap", 0))
            curr_time = self._parse_timestamp(quote.get("timestamp", quote.get("t")))

            # Check for crossed quote
            if bid > ask:
                crossed += 1
                if issues_out is not None:
                    issues_out.append(
                        QualityIssue(
                            code=QualityCode.CROSSED_QUOTE,
                            message=f"Crossed quote: bid {bid} > ask {ask}",
                            timestamp=quote.get("timestamp", quote.get("t")),
                        )
                    )

            # Calculate spread
            if ask > 0 and bid > 0:
                mid = (bid + ask) / 2
                spread_bps = ((ask - bid) / mid) * 10000 if mid > 0 else 0
                spreads.append(spread_bps)

            # Check for stale quote
            if prev_quote is not None and prev_time is not None:
                if curr_time and prev_time:
                    if quote.get("bid_price") == prev_quote.get("bid_price") and quote.get(
                        "ask_price"
                    ) == prev_quote.get("ask_price"):
                        delta = (curr_time - prev_time).total_seconds()
                        if delta > self.STALE_THRESHOLD_SECONDS:
                            stale += 1

            prev_quote = quote
            prev_time = curr_time

        avg_spread = sum(spreads) / len(spreads) if spreads else 0.0

        return QuoteQuality(
            total=total,
            crossed=crossed,
            crossed_pct=round(crossed / total, 6) if total > 0 else 0.0,
            stale=stale,
            stale_pct=round(stale / total, 6) if total > 0 else 0.0,
            avg_spread_bps=round(avg_spread, 2),
        )

    def analyze_trades(self, trades: list[dict]) -> TradeQuality:
        """Analyze trade data quality."""
        total = len(trades)
        odd_lots = 0
        total_size = 0

        for trade in trades:
            size = trade.get("size", trade.get("s")) or 0
            if not isinstance(size, int | float):
                size = 0
            total_size += size

            # Odd lot: not a multiple of 100 (standard round lot)
            if size and size % 100 != 0:
                odd_lots += 1

        avg_size = total_size / total if total > 0 else 0.0

        return TradeQuality(
            total=total,
            odd_lots=odd_lots,
            odd_lot_pct=round(odd_lots / total, 4) if total > 0 else 0.0,
            avg_size=round(avg_size, 2),
        )

    def detect_issues(
        self,
        bars: list[dict] | None = None,
        quotes: list[dict] | None = None,
        trades: list[dict] | None = None,
    ) -> list[QualityIssue]:
        """Detect quality issues in data.

        Args:
            bars: Optional list of bars
            quotes: Optional list of quotes
            trades: Optional list of trades

        Returns:
            List of detected quality issues
        """
        issues: list[QualityIssue] = []

        if bars:
            issues.extend(self._detect_bar_issues(bars))

        if quotes:
            issues.extend(self._detect_quote_issues(quotes))

        return issues

    def _detect_bar_gaps(self, bars: list[dict], timeframe: str) -> list[GapInfo]:
        """Detect gaps in bar data."""
        gaps: list[GapInfo] = []

        if len(bars) < 2:
            return gaps

        # Determine expected interval
        interval_minutes = self._timeframe_to_minutes(timeframe)

        # Sort only when needed; most feeds arrive in timestamp order.
        sorted_bars = (
            bars
            if self._bars_are_sorted_by_timestamp(bars)
            else sorted(bars, key=lambda b: b.get("timestamp", b.get("t", "")))
        )

        prev_ts = self._parse_timestamp(sorted_bars[0].get("timestamp", sorted_bars[0].get("t")))
        for bar in sorted_bars[1:]:
            curr_ts = self._parse_timestamp(bar.get("timestamp", bar.get("t")))

            if prev_ts and curr_ts:
                delta_minutes = (curr_ts - prev_ts).total_seconds() / 60
                expected_delta = interval_minutes

                if delta_minutes > expected_delta * 1.5:  # Allow 50% tolerance
                    missing = int(delta_minutes / interval_minutes) - 1
                    if missing > 0:
                        gaps.append(
                            GapInfo(
                                start=prev_ts.strftime("%H:%M:%S"),
                                end=curr_ts.strftime("%H:%M:%S"),
                                missing=missing,
                            )
                        )
            prev_ts = curr_ts

        return gaps

    def _detect_bar_issues(self, bars: list[dict]) -> list[QualityIssue]:
        """Detect issues in bar data."""
        issues = []
        prev_close = None
        prev_ts = None

        for bar in bars:
            close = bar.get("close", bar.get("c", 0))
            ts_str = bar.get("timestamp", bar.get("t"))
            ts = self._parse_timestamp(ts_str)

            # Zero volume
            volume = bar.get("volume", bar.get("v", 0))
            if volume == 0:
                issues.append(
                    QualityIssue(
                        code=QualityCode.ZERO_VOLUME,
                        message="Zero volume bar during market hours",
                        timestamp=ts_str,
                    )
                )

            # Price anomaly
            if prev_close and prev_close > 0 and close > 0:
                change = abs(close - prev_close) / prev_close
                if change > self.PRICE_ANOMALY_THRESHOLD:
                    issues.append(
                        QualityIssue(
                            code=QualityCode.PRICE_ANOMALY,
                            message=f"Price change {change:.1%} exceeds threshold",
                            timestamp=ts_str,
                            details={"prev": prev_close, "current": close},
                        )
                    )

            # Timestamp out of sequence
            if prev_ts and ts and ts < prev_ts:
                issues.append(
                    QualityIssue(
                        code=QualityCode.TIMESTAMP_OUT_OF_SEQ,
                        message="Timestamp out of sequence",
                        timestamp=ts_str,
                    )
                )

            prev_close = close
            prev_ts = ts

        return issues

    def _detect_quote_issues(self, quotes: list[dict]) -> list[QualityIssue]:
        """Detect issues in quote data."""
        issues = []

        for quote in quotes:
            bid = quote.get("bid_price", quote.get("bp", 0))
            ask = quote.get("ask_price", quote.get("ap", 0))
            ts_str = quote.get("timestamp", quote.get("t"))

            if bid > ask:
                issues.append(
                    QualityIssue(
                        code=QualityCode.CROSSED_QUOTE,
                        message=f"Crossed quote: bid {bid} > ask {ask}",
                        timestamp=ts_str,
                    )
                )

        return issues

    def _get_expected_bar_count(self, timeframe: str) -> int:
        """Get expected bar count for regular session."""
        interval = self._timeframe_to_minutes(timeframe)
        return self.REGULAR_SESSION_MINUTES // interval if interval > 0 else 390

    def _timeframe_to_minutes(self, timeframe: str) -> int:
        """Convert timeframe string to minutes."""
        return _TIMEFRAME_TO_MINUTES.get(timeframe, 1)

    def _bars_are_sorted_by_timestamp(self, bars: list[dict]) -> bool:
        """Fast-path check for common in-order ISO timestamp payloads."""
        prev_ts = bars[0].get("timestamp", bars[0].get("t", ""))
        if not isinstance(prev_ts, str):
            return False
        for bar in bars[1:]:
            curr_ts = bar.get("timestamp", bar.get("t", ""))
            if not isinstance(curr_ts, str):
                return False
            if curr_ts < prev_ts:
                return False
            prev_ts = curr_ts
        return True

    def _parse_timestamp(self, ts: Any) -> datetime | None:
        """Parse timestamp to datetime."""
        if ts is None:
            return None
        if isinstance(ts, datetime):
            return ts
        if isinstance(ts, str):
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────────────────────

_analyzer: QualityAnalyzer | None = None


def get_quality_analyzer() -> QualityAnalyzer:
    """Get singleton QualityAnalyzer instance."""
    global _analyzer
    if _analyzer is None:
        _analyzer = QualityAnalyzer()
    return _analyzer
