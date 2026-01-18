"""Unit tests for As-Of Query Handler."""

from datetime import UTC, datetime

from gateway.core.asof import (
    AsOfMeta,
    AsOfQueryHandler,
    AsOfResult,
    get_asof_handler,
)


class TestAsOfMeta:
    """Test AsOfMeta dataclass."""

    def test_to_dict(self):
        meta = AsOfMeta(
            as_of=datetime(2024, 1, 15, 16, 0, 0, tzinfo=UTC),
            pit_guaranteed=True,
            data_version="original",
            provider_support=True,
        )
        d = meta.to_dict()
        assert "2024-01-15" in d["as_of"]
        assert d["pit_guaranteed"] is True
        assert d["data_version"] == "original"


class TestAsOfResult:
    """Test AsOfResult dataclass."""

    def test_to_dict(self):
        result = AsOfResult(
            data=[{"price": 100}],
            meta=AsOfMeta(
                as_of=datetime(2024, 1, 15, tzinfo=UTC),
                pit_guaranteed=True,
                data_version="original",
                provider_support=True,
            ),
        )
        d = result.to_dict()
        assert "data" in d
        assert "meta" in d


class TestAsOfQueryHandler:
    """Test AsOfQueryHandler."""

    def setup_method(self):
        self.handler = AsOfQueryHandler()

    # Provider support
    def test_supports_as_of_alpaca_bars(self):
        assert self.handler.supports_as_of("alpaca", "bars") is True

    def test_supports_as_of_alpaca_quotes(self):
        assert self.handler.supports_as_of("alpaca", "quotes") is True

    def test_supports_as_of_alpaca_options(self):
        # Options not supported per PRD
        assert self.handler.supports_as_of("alpaca", "options") is False

    def test_supports_as_of_yfinance(self):
        # yfinance doesn't support as-of
        assert self.handler.supports_as_of("yfinance", "bars") is False

    def test_supports_as_of_uw_flow(self):
        assert self.handler.supports_as_of("uw", "flow") is True

    def test_supports_as_of_uw_chain(self):
        assert self.handler.supports_as_of("uw", "chain") is False

    # Apply filter
    def test_apply_as_of_filter(self):
        data = [
            {"timestamp": "2024-01-15T10:00:00Z", "price": 100},
            {"timestamp": "2024-01-15T12:00:00Z", "price": 101},
            {"timestamp": "2024-01-15T14:00:00Z", "price": 102},
            {"timestamp": "2024-01-15T16:00:00Z", "price": 103},
        ]

        as_of = datetime(2024, 1, 15, 13, 0, 0, tzinfo=UTC)
        filtered = self.handler.apply_as_of_filter(data, as_of)

        # Should only include first 2 records
        assert len(filtered) == 2
        assert filtered[-1]["price"] == 101

    def test_apply_as_of_filter_all_included(self):
        data = [
            {"timestamp": "2024-01-15T10:00:00Z", "price": 100},
            {"timestamp": "2024-01-15T12:00:00Z", "price": 101},
        ]

        as_of = datetime(2024, 1, 15, 16, 0, 0, tzinfo=UTC)
        filtered = self.handler.apply_as_of_filter(data, as_of)

        assert len(filtered) == 2

    def test_apply_as_of_filter_none_included(self):
        data = [
            {"timestamp": "2024-01-15T14:00:00Z", "price": 102},
            {"timestamp": "2024-01-15T16:00:00Z", "price": 103},
        ]

        as_of = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
        filtered = self.handler.apply_as_of_filter(data, as_of)

        assert len(filtered) == 0

    # Build metadata
    def test_build_meta_supported_provider(self):
        meta = self.handler.build_meta(
            provider="alpaca",
            data_type="bars",
            as_of=datetime(2024, 1, 15, 16, 0, 0, tzinfo=UTC),
        )
        assert meta.pit_guaranteed is True
        assert meta.data_version == "original"
        assert meta.provider_support is True

    def test_build_meta_unsupported_provider(self):
        meta = self.handler.build_meta(
            provider="yfinance",
            data_type="bars",
            as_of=datetime(2024, 1, 15, 16, 0, 0, tzinfo=UTC),
        )
        assert meta.pit_guaranteed is False
        assert meta.data_version == "current"
        assert meta.provider_support is False

    # Wrap result
    def test_wrap_result_with_filter(self):
        data = [
            {"timestamp": "2024-01-15T10:00:00Z", "price": 100},
            {"timestamp": "2024-01-15T16:00:00Z", "price": 103},
        ]

        result = self.handler.wrap_result(
            data=data,
            provider="alpaca",
            data_type="bars",
            as_of=datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        )

        # Should filter data since provider supports as-of
        assert len(result.data) == 1
        assert result.meta.pit_guaranteed is True

    # Singleton
    def test_singleton(self):
        h1 = get_asof_handler()
        h2 = get_asof_handler()
        assert h1 is h2
