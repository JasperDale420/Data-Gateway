"""Unusual Whales data provider — composed from domain-specific mixins."""

from gateway.providers.uw._base import DEFAULT_UW_MAX_INFLIGHT_CALLS, UWBaseMixin
from gateway.providers.uw.congress_ext import UWCongressExtMixin
from gateway.providers.uw.crossasset import UWCrossAssetMixin
from gateway.providers.uw.earnings import UWEarningsMixin
from gateway.providers.uw.flow import UWFlowMixin
from gateway.providers.uw.fundamentals import UWFundamentalsMixin
from gateway.providers.uw.institutional import UWInstitutionalMixin
from gateway.providers.uw.market import UWMarketMixin
from gateway.providers.uw.options import UWOptionsMixin
from gateway.providers.uw.options_ext import UWOptionsExtMixin
from gateway.providers.uw.predictions import UWPredictionsMixin
from gateway.providers.uw.private_markets import UWPrivateMarketsMixin
from gateway.providers.uw.volatility_ext import UWVolatilityExtMixin


class UnusualWhalesProvider(
    UWFlowMixin,
    UWOptionsMixin,
    UWOptionsExtMixin,
    UWEarningsMixin,
    UWInstitutionalMixin,
    UWMarketMixin,
    UWVolatilityExtMixin,
    UWFundamentalsMixin,
    UWCongressExtMixin,
    UWCrossAssetMixin,
    UWPredictionsMixin,
    UWPrivateMarketsMixin,
    UWBaseMixin,  # MUST be last — provides DataProvider ABC + shared helpers
):
    """Unusual Whales data provider for flow, options, earnings, institutional, and market data.

    Inherits from domain mixins with UWBaseMixin last in MRO so that
    DataProvider.__init__ and shared helpers are resolved correctly.
    """

    pass


__all__ = [
    "UnusualWhalesProvider",
    "DEFAULT_UW_MAX_INFLIGHT_CALLS",
]
