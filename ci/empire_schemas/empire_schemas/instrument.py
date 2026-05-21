"""Instrument key validation utilities.

Extracted from Heber's envelope model — provides regex-based validation
of instrument keys per PRD Section 6.2.
"""

import re

INSTRUMENT_KEY_PATTERNS = {
    "equity": re.compile(r"^equity:[A-Z0-9]+(?:[.-][A-Z0-9]+)*$"),
    "crypto": re.compile(r"^crypto:[A-Z]{2,10}-[A-Z]{2,10}$"),
    "forex": re.compile(r"^forex:[A-Z]{3}-[A-Z]{3}$"),
    "option": re.compile(r"^option:OCC:[A-Z]{1,6}\d{6}[CP]\d{8}$"),
}


def validate_instrument_key(instrument_key: str, instrument_type: str) -> bool:
    """Validate instrument_key format per PRD Section 6.2.

    Args:
        instrument_key: The instrument key to validate
        instrument_type: One of equity, crypto, forex, option

    Returns:
        True if valid, False otherwise

    Examples:
        >>> validate_instrument_key("equity:AAPL", "equity")
        True
        >>> validate_instrument_key("crypto:BTC-USD", "crypto")
        True
        >>> validate_instrument_key("option:OCC:AAPL260116C00200000", "option")
        True
    """
    pattern = INSTRUMENT_KEY_PATTERNS.get(instrument_type)
    if pattern is None:
        return False
    return bool(pattern.match(instrument_key))
