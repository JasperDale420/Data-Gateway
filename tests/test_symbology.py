"""Unit tests for Symbology Service."""

from datetime import date
from decimal import Decimal

from gateway.core.symbology import (
    _SYMBOL_RESOLVE_CACHE_MAX,
    ResolvedSymbol,
    SymbolResolver,
    get_symbol_resolver,
)


class TestSymbolResolver:
    """Test SymbolResolver functionality."""

    def setup_method(self):
        self.resolver = SymbolResolver()

    # Stock symbols
    def test_resolve_stock_symbol(self):
        result = self.resolver.resolve("AAPL")
        assert result.type == "stock"
        assert result.symbol == "AAPL"
        assert result.provider_formats["alpaca"] == "AAPL"

    def test_resolve_lowercase_stock(self):
        result = self.resolver.resolve("msft")
        assert result.type == "stock"
        assert result.symbol == "MSFT"

    # OCC option format
    def test_resolve_occ_call(self):
        result = self.resolver.resolve("AAPL250117C00200000")
        assert result.type == "option"
        assert result.underlying == "AAPL"
        assert result.expiration == date(2025, 1, 17)
        assert result.strike == Decimal("200")
        assert result.option_type == "call"

    def test_resolve_occ_put(self):
        result = self.resolver.resolve("MSFT250221P00350500")
        assert result.type == "option"
        assert result.underlying == "MSFT"
        assert result.expiration == date(2025, 2, 21)
        assert result.strike == Decimal("350.5")
        assert result.option_type == "put"

    # Human-readable options
    def test_resolve_human_option_full(self):
        result = self.resolver.resolve("AAPL 2025-01-17 $200 Call")
        assert result.type == "option"
        assert result.underlying == "AAPL"
        assert result.expiration == date(2025, 1, 17)
        assert result.strike == Decimal("200")
        assert result.option_type == "call"

    def test_resolve_human_option_short(self):
        result = self.resolver.resolve("AAPL 2025-01-17 $200 C")
        assert result.type == "option"
        assert result.option_type == "call"

    # Crypto
    def test_resolve_crypto(self):
        result = self.resolver.resolve("BTC/USD")
        assert result.type == "crypto"
        assert result.symbol == "BTC/USD"

    # Forex
    def test_resolve_forex(self):
        result = self.resolver.resolve("EUR/USD")
        assert result.type == "forex"
        assert result.symbol == "EUR/USD"

    # OCC generation
    def test_to_occ(self):
        occ = self.resolver.to_occ(
            underlying="AAPL",
            expiry=date(2025, 1, 17),
            strike=Decimal("200"),
            opt_type="C",
        )
        assert occ == "AAPL250117C00200000"

    def test_to_occ_decimal_strike(self):
        occ = self.resolver.to_occ(
            underlying="MSFT",
            expiry=date(2025, 2, 21),
            strike=Decimal("350.5"),
            opt_type="P",
        )
        assert occ == "MSFT250221P00350500"

    # Human readable
    def test_to_human(self):
        human = self.resolver.to_human("AAPL250117C00200000")
        assert "AAPL" in human
        assert "2025-01-17" in human
        assert "200" in human
        assert "Call" in human

    # Provider formats
    def test_provider_formats_option(self):
        result = self.resolver.resolve("AAPL250117C00200000")
        assert "alpaca" in result.provider_formats
        assert "uw" in result.provider_formats
        assert "occ" in result.provider_formats
        assert result.provider_formats["human"] == "AAPL 2025-01-17 $200 Call"

    # Validation
    def test_validate_valid_stock(self):
        is_valid, error = self.resolver.validate("AAPL")
        assert is_valid is True
        assert error is None

    def test_validate_invalid_symbol(self):
        is_valid, error = self.resolver.validate("INVALID123")
        assert is_valid is False
        assert error is not None

    # Singleton
    def test_singleton(self):
        r1 = get_symbol_resolver()
        r2 = get_symbol_resolver()
        assert r1 is r2

    # to_dict
    def test_resolved_symbol_to_dict(self):
        result = self.resolver.resolve("AAPL250117C00200000")
        d = result.to_dict()
        assert d["type"] == "option"
        assert d["underlying"] == "AAPL"
        assert d["strike"] == 200.0

    def test_resolve_cache_returns_isolated_provider_formats(self):
        result_one = self.resolver.resolve("AAPL")
        result_one.provider_formats["custom"] = "mutated"

        result_two = self.resolver.resolve("AAPL")

        assert "custom" not in result_two.provider_formats

    def test_resolve_cache_is_bounded(self):
        for i in range(_SYMBOL_RESOLVE_CACHE_MAX + 5):
            key = f"sym-{i}"
            self.resolver._cache_resolved(
                key,
                ResolvedSymbol(
                    input=key,
                    type="stock",
                    symbol="AAPL",
                    provider_formats={"alpaca": "AAPL"},
                ),
            )

        assert len(self.resolver._resolve_cache) <= _SYMBOL_RESOLVE_CACHE_MAX
