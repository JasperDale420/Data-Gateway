"""Symbology Service.

Symbol resolution and format conversion between OCC, human-readable,
and provider-specific formats as specified in PRD (lines 459-523).
"""

import re
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import structlog

logger = structlog.get_logger()
_SYMBOL_RESOLVE_CACHE_MAX = 4096


# ─────────────────────────────────────────────────────────────────────────────
# Resolved Symbol
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class ResolvedSymbol:
    """Result of symbol resolution."""

    input: str
    type: str  # "stock", "option", "crypto", "forex"
    symbol: str  # Normalized form
    underlying: str | None = None
    expiration: date | None = None
    strike: Decimal | None = None
    option_type: str | None = None  # "call", "put"
    provider_formats: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "input": self.input,
            "type": self.type,
            "symbol": self.symbol,
        }
        if self.underlying:
            result["underlying"] = self.underlying
        if self.expiration:
            result["expiration"] = self.expiration.isoformat()
        if self.strike is not None:
            result["strike"] = float(self.strike)
        if self.option_type:
            result["option_type"] = self.option_type
        if self.provider_formats:
            result["provider_formats"] = self.provider_formats
        return result


# ─────────────────────────────────────────────────────────────────────────────
# Symbol Resolver
# ─────────────────────────────────────────────────────────────────────────────


class SymbolResolver:
    """Resolves and converts between symbol formats.

    Supports:
    - Stock symbols (AAPL)
    - OCC option format (AAPL250117C00200000)
    - Human-readable options (AAPL 2025-01-17 $200 C)
    - Crypto pairs (BTC/USD)
    - Forex pairs (EUR/USD)

    Example:
        resolver = SymbolResolver()
        result = resolver.resolve("AAPL250117C00200000")
        print(result.underlying)  # "AAPL"
        print(result.strike)      # Decimal("200")
    """

    # Regex patterns per PRD
    STOCK_PATTERN = re.compile(r"^[A-Z]{1,5}$")
    OCC_PATTERN = re.compile(r"^([A-Z]{1,6})(\d{6})([CP])(\d{8})$")
    CRYPTO_PATTERN = re.compile(r"^([A-Z]{2,5})/([A-Z]{3,4})$")
    FOREX_PATTERN = re.compile(r"^([A-Z]{3})/([A-Z]{3})$")

    # Known forex currency codes (ISO 4217)
    FOREX_CURRENCIES = {
        "USD",
        "EUR",
        "GBP",
        "JPY",
        "CHF",
        "CAD",
        "AUD",
        "NZD",
        "CNY",
        "HKD",
        "SGD",
        "SEK",
        "NOK",
        "DKK",
        "MXN",
        "ZAR",
        "TRY",
        "RUB",
        "INR",
        "BRL",
        "KRW",
        "TWD",
        "THB",
        "PLN",
    }

    # Human-readable option patterns
    HUMAN_OPTION_PATTERNS = [
        # "AAPL 2025-01-17 $200 C" or "AAPL 2025-01-17 $200 Call"
        re.compile(
            r"^([A-Z]{1,5})\s+(\d{4}-\d{2}-\d{2})\s+\$?([\d.]+)\s+(C|P|Call|Put)$",
            re.IGNORECASE,
        ),
        # "AAPL Jan17 200C" or "AAPL Jan17 $200 C"
        re.compile(
            r"^([A-Z]{1,5})\s+([A-Za-z]{3})(\d{2})\s+\$?([\d.]+)\s*([CP])$",
            re.IGNORECASE,
        ),
    ]

    # Month abbreviation to number
    MONTH_MAP = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }

    def __init__(self) -> None:
        self._resolve_cache: dict[str, ResolvedSymbol] = {}

    def resolve(self, symbol: str) -> ResolvedSymbol:
        """Resolve any symbol format to normalized structure.

        Args:
            symbol: Symbol in any supported format

        Returns:
            ResolvedSymbol with normalized data and provider formats
        """
        symbol = symbol.strip().upper()
        if cached := self._resolve_cache.get(symbol):
            return self._clone_resolved(cached)

        # Try stock first
        if self.STOCK_PATTERN.match(symbol):
            resolved = ResolvedSymbol(
                input=symbol,
                type="stock",
                symbol=symbol,
                provider_formats=self._get_stock_provider_formats(symbol),
            )
            self._cache_resolved(symbol, resolved)
            return self._clone_resolved(resolved)

        # Try OCC option
        if match := self.OCC_PATTERN.match(symbol):
            resolved = self._parse_occ_option(symbol, match)
            self._cache_resolved(symbol, resolved)
            return self._clone_resolved(resolved)

        # Try forex first (exact 3-letter currency codes, must be known forex currency)
        if match := self.FOREX_PATTERN.match(symbol):
            base, quote = match.groups()
            # Only classify as forex if base is a known forex currency code
            if base in self.FOREX_CURRENCIES:
                resolved = ResolvedSymbol(
                    input=symbol,
                    type="forex",
                    symbol=f"{base}/{quote}",
                    provider_formats={
                        "alpaca": f"{base}/{quote}",
                        "generic": f"{base}{quote}",
                    },
                )
                self._cache_resolved(symbol, resolved)
                return self._clone_resolved(resolved)

        # Try crypto
        if match := self.CRYPTO_PATTERN.match(symbol):
            base, quote = match.groups()
            resolved = ResolvedSymbol(
                input=symbol,
                type="crypto",
                symbol=f"{base}/{quote}",
                provider_formats={
                    "alpaca": f"{base}/{quote}",
                    "generic": f"{base}{quote}",
                },
            )
            self._cache_resolved(symbol, resolved)
            return self._clone_resolved(resolved)

        # Try human-readable option
        resolved = self._parse_human_option(symbol)
        if resolved.type != "unknown":
            self._cache_resolved(symbol, resolved)
            return self._clone_resolved(resolved)
        return resolved

    def _cache_resolved(self, symbol: str, resolved: ResolvedSymbol) -> None:
        if len(self._resolve_cache) >= _SYMBOL_RESOLVE_CACHE_MAX:
            self._resolve_cache.clear()
        self._resolve_cache[symbol] = resolved

    def _clone_resolved(self, resolved: ResolvedSymbol) -> ResolvedSymbol:
        return replace(resolved, provider_formats=dict(resolved.provider_formats))

    def _parse_occ_option(self, symbol: str, match: re.Match) -> ResolvedSymbol:
        """Parse OCC format option symbol."""
        underlying = match.group(1)
        date_str = match.group(2)  # YYMMDD
        opt_type = match.group(3)  # C or P
        strike_str = match.group(4)  # 8 digits, implied 3 decimals

        # Parse date
        year = 2000 + int(date_str[:2])
        month = int(date_str[2:4])
        day = int(date_str[4:6])
        expiry = date(year, month, day)

        # Parse strike (last 3 digits are decimals)
        strike = Decimal(strike_str) / 1000

        option_type = "call" if opt_type == "C" else "put"

        return ResolvedSymbol(
            input=symbol,
            type="option",
            symbol=symbol,
            underlying=underlying,
            expiration=expiry,
            strike=strike,
            option_type=option_type,
            provider_formats=self._get_option_provider_formats(underlying, expiry, strike, opt_type),
        )

    def _parse_human_option(self, symbol: str) -> ResolvedSymbol:
        """Parse human-readable option format."""
        original = symbol
        symbol = symbol.strip()

        # Try pattern 1: "AAPL 2025-01-17 $200 Call"
        if match := self.HUMAN_OPTION_PATTERNS[0].match(symbol):
            underlying = match.group(1).upper()
            expiry = date.fromisoformat(match.group(2))
            strike = Decimal(match.group(3))
            opt_type_str = match.group(4).upper()
            opt_type = "C" if opt_type_str in ("C", "CALL") else "P"
            option_type = "call" if opt_type == "C" else "put"

            occ = self.to_occ(underlying, expiry, strike, opt_type)

            return ResolvedSymbol(
                input=original,
                type="option",
                symbol=occ,
                underlying=underlying,
                expiration=expiry,
                strike=strike,
                option_type=option_type,
                provider_formats=self._get_option_provider_formats(underlying, expiry, strike, opt_type),
            )

        # Try pattern 2: "AAPL Jan17 200C"
        if match := self.HUMAN_OPTION_PATTERNS[1].match(symbol):
            underlying = match.group(1).upper()
            month_str = match.group(2).lower()
            day = int(match.group(3))
            strike = Decimal(match.group(4))
            opt_type = match.group(5).upper()

            # Assume current or next year
            month = self.MONTH_MAP.get(month_str, 1)
            current_year = datetime.now(UTC).year
            expiry = date(current_year, month, day)
            if expiry < date.today():
                expiry = date(current_year + 1, month, day)

            option_type = "call" if opt_type == "C" else "put"
            occ = self.to_occ(underlying, expiry, strike, opt_type)

            return ResolvedSymbol(
                input=original,
                type="option",
                symbol=occ,
                underlying=underlying,
                expiration=expiry,
                strike=strike,
                option_type=option_type,
                provider_formats=self._get_option_provider_formats(underlying, expiry, strike, opt_type),
            )

        # Unknown format - return as stock with warning
        logger.warning("unknown_symbol_format", symbol=original)
        return ResolvedSymbol(
            input=original,
            type="unknown",
            symbol=original,
        )

    def to_occ(
        self,
        underlying: str,
        expiry: date,
        strike: Decimal,
        opt_type: str,
    ) -> str:
        """Convert option components to OCC format.

        Args:
            underlying: Underlying symbol (e.g., "AAPL")
            expiry: Expiration date
            strike: Strike price
            opt_type: "C" for call, "P" for put

        Returns:
            OCC format string (e.g., "AAPL250117C00200000")
        """
        # Pad underlying to 6 chars (left-justified)
        underlying = underlying.upper().ljust(6)[:6]

        # Format date as YYMMDD
        date_str = expiry.strftime("%y%m%d")

        # Format strike as 8 digits (5.3 format)
        strike_int = int(strike * 1000)
        strike_str = f"{strike_int:08d}"

        return f"{underlying.strip()}{date_str}{opt_type.upper()}{strike_str}"

    def to_human(self, occ_or_resolved: str | ResolvedSymbol) -> str:
        """Convert OCC or resolved symbol to human-readable format.

        Args:
            occ_or_resolved: OCC string or ResolvedSymbol

        Returns:
            Human-readable string (e.g., "AAPL 2025-01-17 $200 Call")
        """
        if isinstance(occ_or_resolved, str):
            resolved = self.resolve(occ_or_resolved)
        else:
            resolved = occ_or_resolved

        if resolved.type != "option":
            return resolved.symbol

        opt_type_str = "Call" if resolved.option_type == "call" else "Put"
        return f"{resolved.underlying} {resolved.expiration.isoformat()} ${resolved.strike} {opt_type_str}"

    def to_provider_format(self, symbol: str, provider: str) -> str:
        """Get symbol in provider-specific format.

        Args:
            symbol: Symbol in any format
            provider: Provider name ("alpaca", "uw", etc.)

        Returns:
            Symbol formatted for the specific provider
        """
        resolved = self.resolve(symbol)
        return resolved.provider_formats.get(provider, resolved.symbol)

    def validate(self, symbol: str) -> tuple[bool, str | None]:
        """Validate symbol format.

        Args:
            symbol: Symbol to validate

        Returns:
            (is_valid, error_message or None)
        """
        try:
            resolved = self.resolve(symbol)
            if resolved.type == "unknown":
                return False, f"Unknown symbol format: {symbol}"
            return True, None
        except Exception as e:
            return False, str(e)

    def _get_stock_provider_formats(self, symbol: str) -> dict[str, str]:
        """Get provider-specific formats for a stock."""
        return {
            "alpaca": symbol,
            "yfinance": symbol,
            "uw": symbol,
        }

    def _get_option_provider_formats(
        self,
        underlying: str,
        expiry: date,
        strike: Decimal,
        opt_type: str,
    ) -> dict[str, str]:
        """Get provider-specific formats for an option."""
        occ = self.to_occ(underlying, expiry, strike, opt_type)
        option_label = "Call" if opt_type == "C" else "Put"
        human_format = f"{underlying} {expiry.isoformat()} ${strike} {option_label}"

        # UW format: AAPL_250117C200
        uw_date = expiry.strftime("%y%m%d")
        uw_strike = str(int(strike)) if strike == int(strike) else str(strike)
        uw_format = f"{underlying}_{uw_date}{opt_type}{uw_strike}"

        return {
            "alpaca": occ,
            "uw": uw_format,
            "occ": occ,
            "human": human_format,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────────────────────

_resolver: SymbolResolver | None = None


def get_symbol_resolver() -> SymbolResolver:
    """Get singleton SymbolResolver instance."""
    global _resolver
    if _resolver is None:
        _resolver = SymbolResolver()
    return _resolver
