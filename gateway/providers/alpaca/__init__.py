"""Alpaca Markets data provider."""

from gateway.providers.alpaca._base import AlpacaBaseMixin
from gateway.providers.alpaca.corporate import AlpacaCorporateMixin
from gateway.providers.alpaca.crypto import AlpacaCryptoMixin
from gateway.providers.alpaca.forex import AlpacaForexMixin
from gateway.providers.alpaca.market import AlpacaMarketMixin
from gateway.providers.alpaca.news import AlpacaNewsMixin
from gateway.providers.alpaca.options import AlpacaOptionsMixin
from gateway.providers.alpaca.trading import AlpacaTradingMixin


class AlpacaProvider(
    AlpacaMarketMixin,
    AlpacaOptionsMixin,
    AlpacaCryptoMixin,
    AlpacaForexMixin,
    AlpacaNewsMixin,
    AlpacaCorporateMixin,
    AlpacaTradingMixin,
    AlpacaBaseMixin,  # Must be last — provides DataProvider ABC + __init__
):
    """Alpaca Markets data provider with REST and WebSocket support."""

    pass
