import sys
from datetime import datetime

# Hack to avoid importing local 'unusualwhales' stub from Kairos if present in sys.path
paths_to_remove = [p for p in sys.path if "Kairos" in p]
for p in paths_to_remove:
    sys.path.remove(p)

from unusualwhales.client import UnusualWhalesClient  # noqa: E402
from unusualwhales.api.endpoints import Endpoints  # noqa: E402
from dataingestion.shared.config import get_settings  # noqa: E402
from dataingestion.features.uw_data.schemas import (  # noqa: E402
    Flow,
    FlowResponse,
    DarkPoolTrade,
    DarkPoolResponse,
    MarketTide,
    TideResponse,
    InsiderTransaction,
    InsiderResponse,
    CongressTrade,
    CongressResponse,
    NewsHeadline,
    NewsResponse,
    ShortInterest,
    ShortInterestResponse,
    GreekExposure,
    GreekExposureResponse,
    OptionContract,
    OptionContractResponse,
    MaxPain,
    MaxPainResponse,
    InstitutionHolding,
    InstitutionResponse,
    ETFHolding,
    ETFHoldingsResponse,
    ScreenerResult,
    ScreenerResponse,
    Seasonality,
    SeasonalityResponse,
    NetFlow,
    NetFlowResponse,
    GroupFlow,
    GroupFlowResponse,
    DarkPoolRecentResponse,
    ShortVolume,
    ShortVolumeResponse,
    FailToDeliver,
    FailToDeliverResponse,
    OptionContractHistoryResponse,
    OptionScreenerResult,
    OptionScreenerResponse,
    EarningsResult,
    EarningsResponse,
    CongressLateReport,
    CongressLateReportResponse,
    PoliticianPortfolio,
    PoliticianPortfolioResponse,
    StockInfo,
    StockInfoResponse,
    StockCandle,
    StockOHLCResponse,
    UserAlert,
    AlertResponse,
)  # noqa: E402
from dataingestion.shared.storage import data_lake  # noqa: E402

settings = get_settings()


class UWIngestor:
    def __init__(self):
        self._client = None

    @property
    def client(self):
        if not self._client:
            self._client = UnusualWhalesClient(
                token=settings.UNUSUAL_WHALES_API_KEY,
                base_url="https://api.unusualwhales.com",
            )
        return self._client

    async def get_flow(self, ticker: str) -> FlowResponse:
        """Fetch option flow for a ticker."""
        try:
            # Use the asyncio method from the generated SDK
            response = (
                await Endpoints.flow()
                .get_ticker_order_flow()
                .asyncio(client=self.client, ticker_symbol=ticker, limit=100)
            )

            result_flow = []
            if response and hasattr(response, "data"):
                # The response might be a FlowAlert object or list of them
                # Based on get_ticker_order_flow.py, it returns FlowAlert
                # But wait, the return type is Union[ErrorMessage, FlowAlert, str]
                # And FlowAlert likely contains a list of results?
                # Let's assume response is the data we want or has a 'results' field
                # Actually, looking at the SDK code:
                # response_200 = FlowAlert.from_dict(response.json())
                # So response is a FlowAlert object.
                # I need to check FlowAlert definition to see where the list of trades is.
                # For now, I'll wrap it in a try/except and print the structure if it fails during verification

                # Naive mapping:
                data_list = response.results if hasattr(response, "results") else []
                for item in data_list:
                    # Map item attributes to Flow model
                    # item is likely a model object
                    result_flow.append(
                        Flow(
                            ticker=ticker,
                            premium=getattr(item, "premium", 0),
                            size=getattr(item, "size", 0),
                            price=getattr(item, "price", 0),
                            put_call=getattr(item, "put_call", "UNKNOWN"),
                            timestamp=getattr(item, "timestamp", None),
                            # Add other fields mapping
                        )
                    )

            # Write to Data Lake
            if result_flow:
                date_str = datetime.now().strftime("%Y%m%d")
                # Convert list of Pydantic models to DataFrame
                import pandas as pd

                df = pd.DataFrame([f.model_dump() for f in result_flow])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="options_flow",
                    asset_class="equity_options",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"flow_{date_str}",
                )

            return FlowResponse(ticker=ticker, flow=result_flow)
        except Exception as e:
            print(f"Error fetching flow: {e}")
            return FlowResponse(ticker=ticker, flow=[])

    async def get_darkpool(self, ticker: str) -> DarkPoolResponse:
        """Fetch dark pool trades."""
        try:
            response = (
                await Endpoints.darkpool()
                .get_trades_by_ticker()
                .asyncio(client=self.client, ticker=ticker)
            )

            result_trades = []
            if response and hasattr(response, "results"):
                for item in response.results:
                    result_trades.append(
                        DarkPoolTrade(
                            ticker=ticker,
                            price=getattr(item, "price", 0),
                            size=getattr(item, "size", 0),
                            timestamp=getattr(
                                item, "timestamp", datetime.now()
                            ),  # Fallback
                            exchange=getattr(item, "exchange", None),
                        )
                    )

            # Write to Data Lake
            if result_trades:
                date_str = datetime.now().strftime("%Y%m%d")
                # Convert list of Pydantic models to DataFrame
                import pandas as pd

                df = pd.DataFrame([t.model_dump() for t in result_trades])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="darkpool",
                    asset_class="equity",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"darkpool_{date_str}",
                )

            return DarkPoolResponse(ticker=ticker, trades=result_trades)
        except Exception as e:
            print(f"Error fetching darkpool: {e}")
            return DarkPoolResponse(ticker=ticker, trades=[])

    async def get_market_tide(self) -> TideResponse:
        """Fetch market tide data."""
        try:
            response = (
                await Endpoints.market().get_market_tide().asyncio(client=self.client)
            )
            # Response is likely a MarketTideResults object or similar
            # Let's assume it has a .results attribute or is iterable
            # Based on SDK patterns, it usually returns a model.
            # If I can't be sure, I'll inspect it in verification.
            # For now, let's assume .results or direct list
            data = getattr(response, "results", []) if response else []

            tide_list = []
            for item in data:
                tide_list.append(
                    MarketTide(
                        timestamp=getattr(item, "timestamp", datetime.now()),
                        call_premium=getattr(item, "call_premium", 0),
                        put_premium=getattr(item, "put_premium", 0),
                        net_premium=getattr(item, "net_premium", 0),
                        call_volume=getattr(item, "call_volume", 0),
                        put_volume=getattr(item, "put_volume", 0),
                    )
                )

            if tide_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([t.model_dump() for t in tide_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="market_tide",
                    asset_class="market",
                    symbol="MARKET",
                    date=datetime.now(),
                    filename=f"market_tide_{date_str}",
                )

            return TideResponse(tide=tide_list)
        except Exception as e:
            print(f"Error fetching market tide: {e}")
            return TideResponse(tide=[])

    async def get_sector_tide(self, sector: str) -> TideResponse:
        """Fetch sector tide data."""
        try:
            # Not in SDK, use raw request
            # URL: /api/market/{sector}/sector-tide
            # URL: /api/market/{sector}/sector-tide
            url = f"{self.client._base_url}/api/market/{sector}/sector-tide"
            # http_client = self.client.get_httpx_client()
            # Need to handle async request manually if client doesn't expose async request easily
            # The client has get_async_httpx_client if available?
            # Inspecting client showed 'get_async_httpx_client'
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Sector tide failed: {response.status_code}")
                return TideResponse(tide=[])

            data = response.json().get("data", [])  # Assuming 'data' or 'results'
            # If list is direct
            if isinstance(data, list):
                pass
            elif isinstance(data, dict) and "results" in data:
                data = data["results"]

            tide_list = []
            for item in data:
                # Item is dict
                tide_list.append(
                    MarketTide(
                        timestamp=(
                            datetime.fromisoformat(
                                item.get("timestamp").replace("Z", "+00:00")
                            )
                            if item.get("timestamp")
                            else datetime.now()
                        ),
                        call_premium=item.get("call_premium", 0),
                        put_premium=item.get("put_premium", 0),
                        net_premium=item.get("net_premium", 0),
                        call_volume=item.get("call_volume", 0),
                        put_volume=item.get("put_volume", 0),
                    )
                )

            if tide_list:
                date_str = datetime.now().strftime("%Y%m%d")
                safe_sector = sector.replace(" ", "_").replace("/", "-")

                import pandas as pd

                df = pd.DataFrame([t.model_dump() for t in tide_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="sector_tide",
                    asset_class="sector",
                    symbol=safe_sector,
                    date=datetime.now(),
                    filename=f"sector_tide_{safe_sector}_{date_str}",
                )

            return TideResponse(tide=tide_list)
        except Exception as e:
            print(f"Error fetching sector tide: {e}")
            return TideResponse(tide=[])

    async def get_etf_tide(self, ticker: str) -> TideResponse:
        """Fetch ETF tide data."""
        try:
            response = (
                await Endpoints.market()
                .get_market_tide_by_etf()
                .asyncio(client=self.client, ticker=ticker)
            )
            data = getattr(response, "results", []) if response else []

            tide_list = []
            for item in data:
                tide_list.append(
                    MarketTide(
                        timestamp=getattr(item, "timestamp", datetime.now()),
                        call_premium=getattr(item, "call_premium", 0),
                        put_premium=getattr(item, "put_premium", 0),
                        net_premium=getattr(item, "net_premium", 0),
                        call_volume=getattr(item, "call_volume", 0),
                        put_volume=getattr(item, "put_volume", 0),
                    )
                )

            if tide_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([t.model_dump() for t in tide_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="etf_tide",
                    asset_class="etf",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"etf_tide_{date_str}",
                )

            return TideResponse(tide=tide_list)
        except Exception as e:
            print(f"Error fetching ETF tide: {e}")
            return TideResponse(tide=[])

    async def get_insider_transactions(self) -> InsiderResponse:
        """Fetch insider transactions."""
        try:
            # Use market().get_insider_trades()
            response = (
                await Endpoints.market()
                .get_insider_trades()
                .asyncio(client=self.client, limit=100)
            )
            data = getattr(response, "results", []) if response else []

            tx_list = []
            for item in data:
                tx_list.append(
                    InsiderTransaction(
                        ticker=getattr(item, "ticker", "UNKNOWN"),
                        filing_date=getattr(item, "filing_date", datetime.now()),
                        transaction_date=getattr(item, "transaction_date", None),
                        insider_name=getattr(item, "insider_name", "UNKNOWN"),
                        insider_title=getattr(item, "insider_title", None),
                        transaction_type=getattr(item, "transaction_type", "UNKNOWN"),
                        shares=getattr(item, "shares", 0),
                        price=getattr(item, "price", 0),
                        value=getattr(item, "value", 0),
                    )
                )

            if tx_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([t.model_dump() for t in tx_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="insider",
                    asset_class="equity",
                    symbol="INSIDER_FEED",
                    date=datetime.now(),
                    filename=f"transactions_{date_str}",
                )

            return InsiderResponse(transactions=tx_list)
        except Exception as e:
            print(f"Error fetching insider transactions: {e}")
            return InsiderResponse(transactions=[])

    async def get_congress_trades(self) -> CongressResponse:
        """Fetch congress trades."""
        try:
            # Use congress().get_trades()
            response = (
                await Endpoints.congress()
                .get_trades()
                .asyncio(client=self.client, limit=100)
            )
            data = getattr(response, "results", []) if response else []

            trade_list = []
            for item in data:
                trade_list.append(
                    CongressTrade(
                        ticker=getattr(item, "ticker", "UNKNOWN"),
                        politician=getattr(item, "politician", "UNKNOWN"),
                        party=getattr(item, "party", None),
                        chamber=getattr(item, "chamber", None),
                        transaction_date=getattr(item, "transaction_date", None),
                        filing_date=getattr(item, "filing_date", datetime.now()),
                        transaction_type=getattr(item, "transaction_type", "UNKNOWN"),
                        amount_min=getattr(item, "amount_min", 0),
                        amount_max=getattr(item, "amount_max", 0),
                    )
                )

            if trade_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([t.model_dump() for t in trade_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="congress",
                    asset_class="equity",
                    symbol="CONGRESS_FEED",
                    date=datetime.now(),
                    filename=f"trades_{date_str}",
                )

            return CongressResponse(trades=trade_list)
        except Exception as e:
            print(f"Error fetching congress trades: {e}")
            return CongressResponse(trades=[])

    async def get_news_headlines(self) -> NewsResponse:
        """Fetch news headlines."""
        try:
            # Not in SDK, use raw request
            # URL: /api/news/headlines
            url = f"{self.client._base_url}/api/news/headlines"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"News failed: {response.status_code}")
                return NewsResponse(headlines=[])

            data = response.json().get("data", [])
            if isinstance(data, dict) and "results" in data:
                data = data["results"]

            news_list = []
            for item in data:
                # item is dict
                news_list.append(
                    NewsHeadline(
                        id=item.get("id", "UNKNOWN"),
                        headline=item.get("headline", "UNKNOWN"),
                        url=item.get("url"),
                        source=item.get("source", "UNKNOWN"),
                        timestamp=(
                            datetime.fromisoformat(
                                item.get("timestamp").replace("Z", "+00:00")
                            )
                            if item.get("timestamp")
                            else datetime.now()
                        ),
                        tickers=item.get("tickers", []),
                        sentiment=item.get("sentiment", 0),
                    )
                )

            if news_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([n.model_dump() for n in news_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="news",
                    asset_class="headlines",
                    symbol="NEWS_FEED",
                    date=datetime.now(),
                    filename=f"headlines_{date_str}",
                )

            return NewsResponse(headlines=news_list)
        except Exception as e:
            print(f"Error fetching news: {e}")
            return NewsResponse(headlines=[])

    async def get_short_interest(self, ticker: str) -> ShortInterestResponse:
        """Fetch short interest data."""
        try:
            # Not in SDK, use raw request
            # URL: /api/shorts/{ticker}/data
            url = f"{self.client._base_url}/api/shorts/{ticker}/data"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Shorts failed: {response.status_code}")
                return ShortInterestResponse(data=[])

            data = response.json().get("data", [])
            # Might be a single object
            if isinstance(data, dict):
                data = [data]

            short_list = []
            for item in data:
                short_list.append(
                    ShortInterest(
                        ticker=ticker,
                        date=(
                            datetime.fromisoformat(
                                item.get("date").replace("Z", "+00:00")
                            )
                            if item.get("date")
                            else datetime.now()
                        ),
                        short_interest=item.get("short_interest", 0),
                        float_shares=item.get("float_shares"),
                        days_to_cover=item.get("days_to_cover"),
                    )
                )

            if short_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([s.model_dump() for s in short_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="shorts",
                    asset_class="equity",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"short_interest_{date_str}",
                )

            return ShortInterestResponse(data=short_list)
            return ShortInterestResponse(data=short_list)
        except Exception as e:
            print(f"Error fetching short interest: {e}")
            return ShortInterestResponse(data=[])

    async def get_greek_exposure(self, ticker: str) -> GreekExposureResponse:
        """Fetch greek exposure."""
        try:
            url = f"{self.client._base_url}/api/stock/{ticker}/greek-exposure"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Greek exposure failed: {response.status_code}")
                return GreekExposureResponse(data=[])

            data = response.json().get("data", [])
            if isinstance(data, dict):
                data = [data]

            res_list = []
            for item in data:
                res_list.append(
                    GreekExposure(
                        ticker=ticker,
                        timestamp=(
                            datetime.fromisoformat(
                                item.get("timestamp").replace("Z", "+00:00")
                            )
                            if item.get("timestamp")
                            else datetime.now()
                        ),
                        price=float(item.get("price", 0)),
                        gamma_per_one_percent_move_oi=(
                            float(item.get("gamma_per_one_percent_move_oi"))
                            if item.get("gamma_per_one_percent_move_oi")
                            else None
                        ),
                        delta_per_one_percent_move_oi=(
                            float(item.get("delta_per_one_percent_move_oi"))
                            if item.get("delta_per_one_percent_move_oi")
                            else None
                        ),
                        charm_per_one_percent_move_oi=(
                            float(item.get("charm_per_one_percent_move_oi"))
                            if item.get("charm_per_one_percent_move_oi")
                            else None
                        ),
                        vanna_per_one_percent_move_oi=(
                            float(item.get("vanna_per_one_percent_move_oi"))
                            if item.get("vanna_per_one_percent_move_oi")
                            else None
                        ),
                        gamma_per_one_percent_move_vol=(
                            float(item.get("gamma_per_one_percent_move_vol"))
                            if item.get("gamma_per_one_percent_move_vol")
                            else None
                        ),
                        delta_per_one_percent_move_vol=(
                            float(item.get("delta_per_one_percent_move_vol"))
                            if item.get("delta_per_one_percent_move_vol")
                            else None
                        ),
                        charm_per_one_percent_move_vol=(
                            float(item.get("charm_per_one_percent_move_vol"))
                            if item.get("charm_per_one_percent_move_vol")
                            else None
                        ),
                        vanna_per_one_percent_move_vol=(
                            float(item.get("vanna_per_one_percent_move_vol"))
                            if item.get("vanna_per_one_percent_move_vol")
                            else None
                        ),
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="greeks",
                    asset_class="equity",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"greeks_{date_str}",
                )

            return GreekExposureResponse(data=res_list)
        except Exception as e:
            print(f"Error fetching greek exposure: {e}")
            return GreekExposureResponse(data=[])

    async def get_option_contracts(self, ticker: str) -> OptionContractResponse:
        """Fetch option contracts."""
        try:
            url = f"{self.client._base_url}/api/stock/{ticker}/option-contracts"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Option contracts failed: {response.status_code}")
                return OptionContractResponse(contracts=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                # Assuming item is dict with details
                # If it's just a list of strings (symbols), we need to parse them
                # Documentation says: "Returns all option contracts for the given ticker"
                # Let's assume it returns objects or strings.
                # If strings, we parse: SYMBOL YYMMDD T STRIKE
                # Regex provided in doc: ^(?<symbol>[\w]*)(?<expiry>(\d{2})(\d{2})(\d{2}))(?<type>[PC])(?<strike>\d{8})$
                # But that's for 'option-chains'. 'option-contracts' might be detailed.
                # Let's assume it returns a list of dicts for now, or strings if simple.
                # Use a safe fallback.
                if isinstance(item, str):
                    # Basic parsing todo
                    res_list.append(
                        OptionContract(
                            option_symbol=item,
                            underlying_symbol=ticker,
                            expiry="UNKNOWN",
                            strike=0.0,
                            option_type="UNKNOWN",
                        )
                    )
                elif isinstance(item, dict):
                    res_list.append(
                        OptionContract(
                            option_symbol=item.get("symbol", "UNKNOWN"),
                            underlying_symbol=ticker,
                            expiry=item.get("expiry", "UNKNOWN"),
                            strike=float(item.get("strike", 0)),
                            option_type=item.get("type", "UNKNOWN"),
                        )
                    )

            # Save logic omitted for brevity on large lists, or sample it?
            # Let's save it.
            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="contracts",
                    asset_class="equity_options",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"contracts_{date_str}",
                )

            return OptionContractResponse(contracts=res_list)
        except Exception as e:
            print(f"Error fetching option contracts: {e}")
            return OptionContractResponse(contracts=[])

    async def get_max_pain(self, ticker: str) -> MaxPainResponse:
        """Fetch max pain."""
        try:
            url = f"{self.client._base_url}/api/stock/{ticker}/max-pain"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Max pain failed: {response.status_code}")
                return MaxPainResponse(data=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    MaxPain(
                        ticker=ticker,
                        date=(
                            datetime.fromisoformat(
                                item.get("date").replace("Z", "+00:00")
                            )
                            if item.get("date")
                            else datetime.now()
                        ),
                        strike=float(
                            item.get("price", 0)
                        ),  # Max pain price is usually the field
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="max_pain",
                    asset_class="equity",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"max_pain_{date_str}",
                )

            return MaxPainResponse(data=res_list)
        except Exception as e:
            print(f"Error fetching max pain: {e}")
            return MaxPainResponse(data=[])

    async def get_institution_holdings(self, name: str) -> InstitutionResponse:
        """Fetch institution holdings."""
        try:
            # Name might need encoding if it contains spaces
            safe_name = name.replace(" ", "%20")
            url = f"{self.client._base_url}/api/institution/{safe_name}/holdings"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Institution holdings failed: {response.status_code}")
                return InstitutionResponse(institution=name, holdings=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    InstitutionHolding(
                        ticker=item.get("ticker", "UNKNOWN"),
                        shares=float(item.get("shares", 0)),
                        value=float(item.get("value", 0)),
                        portfolio_percent=(
                            float(item.get("portfolio_percent"))
                            if item.get("portfolio_percent")
                            else None
                        ),
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                safe_file_name = name.replace(" ", "_")
                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="institution_holdings",
                    asset_class="equity",
                    symbol=safe_file_name,
                    date=datetime.now(),
                    filename=f"holdings_{safe_file_name}_{date_str}",
                )

            return InstitutionResponse(institution=name, holdings=res_list)
        except Exception as e:
            print(f"Error fetching institution holdings: {e}")
            return InstitutionResponse(institution=name, holdings=[])

    async def get_etf_holdings(self, ticker: str) -> ETFHoldingsResponse:
        """Fetch ETF holdings."""
        try:
            url = f"{self.client._base_url}/api/etfs/{ticker}/holdings"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"ETF holdings failed: {response.status_code}")
                return ETFHoldingsResponse(etf=ticker, holdings=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    ETFHolding(
                        ticker=item.get("ticker", "UNKNOWN"),
                        name=item.get("name", "UNKNOWN"),
                        sector=item.get("sector"),
                        shares=float(item.get("shares", 0)),
                        weight=float(item.get("weight", 0)),
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="etf_holdings",
                    asset_class="etf",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"holdings_{ticker}_{date_str}",
                )

            return ETFHoldingsResponse(etf=ticker, holdings=res_list)
        except Exception as e:
            print(f"Error fetching ETF holdings: {e}")
            return ETFHoldingsResponse(etf=ticker, holdings=[])

    async def get_screener_stocks(self) -> ScreenerResponse:
        """Fetch stock screener results (default/top)."""
        try:
            # Basic default screener call
            url = f"{self.client._base_url}/api/screener/stocks?limit=50&order=volume_desc"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Screener failed: {response.status_code}")
                return ScreenerResponse(results=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    ScreenerResult(
                        ticker=item.get("ticker", "UNKNOWN"),
                        price=float(item.get("price", 0)),
                        volume=float(item.get("volume", 0)),
                        market_cap=(
                            float(item.get("market_cap"))
                            if item.get("market_cap")
                            else None
                        ),
                        industry=item.get("industry"),
                        sector=item.get("sector"),
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="screener_stocks",
                    asset_class="equity",
                    symbol="SCREENER_TOP",
                    date=datetime.now(),
                    filename=f"screener_stocks_{date_str}",
                )

            return ScreenerResponse(results=res_list)
        except Exception as e:
            print(f"Error fetching screener stocks: {e}")
            return ScreenerResponse(results=[])

    async def get_seasonality(self, ticker: str) -> SeasonalityResponse:
        """Fetch seasonality data."""
        try:
            url = f"{self.client._base_url}/api/seasonality/{ticker}/monthly"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Seasonality failed: {response.status_code}")
                return SeasonalityResponse(ticker=ticker, monthly_data=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    Seasonality(
                        month=int(item.get("month", 0)),
                        avg_return=float(item.get("avg_return", 0)),
                        win_rate=float(item.get("win_rate", 0)),
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="seasonality",
                    asset_class="equity",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"seasonality_{date_str}",
                )

            return SeasonalityResponse(ticker=ticker, monthly_data=res_list)
        except Exception as e:
            print(f"Error fetching seasonality: {e}")
            return SeasonalityResponse(ticker=ticker, monthly_data=[])

    async def get_net_flow(self, ticker: str) -> NetFlowResponse:
        """Fetch net flow data."""
        try:
            url = f"{self.client._base_url}/api/net-flow/{ticker}"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Net flow failed: {response.status_code}")
                # Try alternative if 404? No, stick to pattern.
                return NetFlowResponse(ticker=ticker, data=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    NetFlow(
                        date=(
                            datetime.fromisoformat(item.get("date"))
                            if item.get("date")
                            else datetime.now()
                        ),
                        price=float(item.get("price", 0)),
                        total_market_net_premium=float(
                            item.get("total_market_net_premium", 0)
                        ),
                        total_market_net_volume=int(
                            item.get("total_market_net_volume", 0)
                        ),
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="net_flow",
                    asset_class="equity",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"net_flow_{ticker}_{date_str}",
                )

            return NetFlowResponse(ticker=ticker, data=res_list)
        except Exception as e:
            print(f"Error fetching net flow: {e}")
            return NetFlowResponse(ticker=ticker, data=[])

    async def get_group_flow(self, ticker: str) -> GroupFlowResponse:
        """Fetch group flow data."""
        try:
            url = f"{self.client._base_url}/api/group-flow/{ticker}"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Group flow failed: {response.status_code}")
                return GroupFlowResponse(ticker=ticker, data=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    GroupFlow(
                        date=(
                            datetime.fromisoformat(item.get("date"))
                            if item.get("date")
                            else datetime.now()
                        ),
                        sector=item.get("sector"),
                        industry=item.get("industry"),
                        net_premium=float(item.get("net_premium", 0)),
                        net_volume=int(item.get("net_volume", 0)),
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="group_flow",
                    asset_class="equity",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"group_flow_{ticker}_{date_str}",
                )

            return GroupFlowResponse(ticker=ticker, data=res_list)
        except Exception as e:
            print(f"Error fetching group flow: {e}")
            return GroupFlowResponse(ticker=ticker, data=[])

    async def get_recent_darkpool_orders(self) -> DarkPoolRecentResponse:
        """Fetch recent dark pool orders."""
        try:
            url = f"{self.client._base_url}/api/darkpool/recent"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Recent darkpool failed: {response.status_code}")
                return DarkPoolRecentResponse(trades=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    DarkPoolTrade(
                        ticker=item.get("ticker", "UNKNOWN"),
                        price=float(item.get("price", 0)),
                        size=int(item.get("size", 0)),
                        timestamp=(
                            datetime.fromisoformat(item.get("timestamp"))
                            if item.get("timestamp")
                            else datetime.now()
                        ),
                        exchange=item.get("exchange"),
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="darkpool_recent",
                    asset_class="equity",
                    symbol="MARKET",
                    date=datetime.now(),
                    filename=f"darkpool_recent_{date_str}",
                )

            return DarkPoolRecentResponse(trades=res_list)
        except Exception as e:
            print(f"Error fetching recent darkpool: {e}")
            return DarkPoolRecentResponse(trades=[])

    async def get_short_volume(self, ticker: str) -> ShortVolumeResponse:
        """Fetch short volume data."""
        try:
            url = f"{self.client._base_url}/api/shorts/{ticker}/volume"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Short volume failed: {response.status_code}")
                return ShortVolumeResponse(ticker=ticker, data=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    ShortVolume(
                        date=(
                            datetime.fromisoformat(item.get("date"))
                            if item.get("date")
                            else datetime.now()
                        ),
                        volume=int(item.get("volume", 0)),
                        short_volume=int(item.get("short_volume", 0)),
                        short_exempt_volume=(
                            int(item.get("short_exempt_volume"))
                            if item.get("short_exempt_volume")
                            else None
                        ),
                        market_share=(
                            float(item.get("market_share"))
                            if item.get("market_share")
                            else None
                        ),
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="short_volume",
                    asset_class="equity",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"short_volume_{ticker}_{date_str}",
                )

            return ShortVolumeResponse(ticker=ticker, data=res_list)
        except Exception as e:
            print(f"Error fetching short volume: {e}")
            return ShortVolumeResponse(ticker=ticker, data=[])

    async def get_ftds(self, ticker: str) -> FailToDeliverResponse:
        """Fetch FTD data."""
        try:
            url = f"{self.client._base_url}/api/shorts/{ticker}/ftds"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"FTDs failed: {response.status_code}")
                return FailToDeliverResponse(ticker=ticker, data=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    FailToDeliver(
                        date=(
                            datetime.fromisoformat(item.get("date"))
                            if item.get("date")
                            else datetime.now()
                        ),
                        quantity=int(item.get("quantity", 0)),
                        price=float(item.get("price")) if item.get("price") else None,
                        amount=(
                            float(item.get("amount")) if item.get("amount") else None
                        ),
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="ftds",
                    asset_class="equity",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"ftds_{ticker}_{date_str}",
                )

            return FailToDeliverResponse(ticker=ticker, data=res_list)
        except Exception as e:
            print(f"Error fetching FTDs: {e}")
            return FailToDeliverResponse(ticker=ticker, data=[])

    async def get_option_contract_history(
        self, contract_id: str
    ) -> OptionContractHistoryResponse:
        """Fetch option contract history."""
        try:
            url = f"{self.client._base_url}/api/option-contract/{contract_id}/history"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Contract history failed: {response.status_code}")
                return OptionContractHistoryResponse(contract_id=contract_id, data=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                # Reuse Flow model parsing
                res_list.append(
                    Flow(
                        id=item.get("id"),
                        ticker=item.get("ticker"),
                        expiry=item.get("expiry"),
                        strike=(
                            float(item.get("strike")) if item.get("strike") else None
                        ),
                        put_call=item.get("put_call"),
                        premium=(
                            float(item.get("premium")) if item.get("premium") else None
                        ),
                        size=int(item.get("size")) if item.get("size") else None,
                        price=float(item.get("price")) if item.get("price") else None,
                        timestamp=(
                            datetime.fromisoformat(item.get("timestamp"))
                            if item.get("timestamp")
                            else datetime.now()
                        ),
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                # Use a specific subdir or just flat?
                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="contract_history",
                    asset_class="option",
                    symbol=contract_id,
                    date=datetime.now(),
                    filename=f"contract_history_{contract_id}_{date_str}",
                )

            return OptionContractHistoryResponse(contract_id=contract_id, data=res_list)
        except Exception as e:
            print(f"Error fetching contract history: {e}")
            return OptionContractHistoryResponse(contract_id=contract_id, data=[])

    async def get_option_screener(self) -> OptionScreenerResponse:
        """Fetch option screener results (e.g. hottest chains)."""
        try:
            url = f"{self.client._base_url}/api/screener/options"  # Hypothetical endpoint, assuming 'options' screener at this path
            # If standard screener endpoint is used with params, we'd adjust here.
            # Assuming discrete endpoint based on task.

            # Fallback to general screener if specific one doesn't exist, but let's try a likely path or params.
            # Re-checking audit: it said '/api/screener/option-contracts' is missing.
            url = f"{self.client._base_url}/api/screener/option-contracts"

            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Option screener failed: {response.status_code}")
                return OptionScreenerResponse(contracts=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    OptionScreenerResult(
                        ticker=item.get("ticker"),
                        expiry=item.get("expiry"),
                        strike=float(item.get("strike")) if item.get("strike") else 0.0,
                        put_call=item.get("put_call", "C"),
                        avg_premium=(
                            float(item.get("avg_premium"))
                            if item.get("avg_premium")
                            else None
                        ),
                        volume=int(item.get("volume")) if item.get("volume") else None,
                        open_interest=(
                            int(item.get("open_interest"))
                            if item.get("open_interest")
                            else None
                        ),
                        implied_volatility=(
                            float(item.get("implied_volatility"))
                            if item.get("implied_volatility")
                            else None
                        ),
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="screener_options",
                    asset_class="option",
                    symbol="MARKET",
                    date=datetime.now(),
                    filename=f"screener_options_{date_str}",
                )

            return OptionScreenerResponse(contracts=res_list)
        except Exception as e:
            print(f"Error fetching option screener: {e}")
            return OptionScreenerResponse(contracts=[])

    async def get_earnings(self, ticker: str) -> EarningsResponse:
        """Fetch earnings data for a ticker."""
        try:
            url = f"{self.client._base_url}/api/earnings/{ticker}"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Earnings failed: {response.status_code}")
                return EarningsResponse(ticker=ticker, earnings=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    EarningsResult(
                        ticker=ticker,
                        date=(
                            datetime.fromisoformat(item.get("date"))
                            if item.get("date")
                            else datetime.now()
                        ),
                        eps_est=(
                            float(item.get("eps_est")) if item.get("eps_est") else None
                        ),
                        eps_act=(
                            float(item.get("eps_act")) if item.get("eps_act") else None
                        ),
                        rev_est=(
                            float(item.get("rev_est")) if item.get("rev_est") else None
                        ),
                        rev_act=(
                            float(item.get("rev_act")) if item.get("rev_act") else None
                        ),
                        time=item.get("time"),
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="earnings",
                    asset_class="equity",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"earnings_{ticker}_{date_str}",
                )

            return EarningsResponse(ticker=ticker, earnings=res_list)
        except Exception as e:
            print(f"Error fetching earnings: {e}")
            return EarningsResponse(ticker=ticker, earnings=[])

    async def get_congress_late_reports(self) -> CongressLateReportResponse:
        """Fetch congress late reports."""
        try:
            url = f"{self.client._base_url}/api/congress/late-reports"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Late reports failed: {response.status_code}")
                return CongressLateReportResponse(reports=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    CongressLateReport(
                        politician=item.get("politician", "UNKNOWN"),
                        party=item.get("party", "UNKNOWN"),
                        state=item.get("state", "UNKNOWN"),
                        chamber=item.get("chamber", "UNKNOWN"),
                        filed_date=(
                            datetime.fromisoformat(item.get("filed_date"))
                            if item.get("filed_date")
                            else datetime.now()
                        ),
                        published_date=(
                            datetime.fromisoformat(item.get("published_date"))
                            if item.get("published_date")
                            else datetime.now()
                        ),
                        days_late=int(item.get("days_late", 0)),
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="congress_late",
                    asset_class="congress",
                    symbol="MARKET",
                    date=datetime.now(),
                    filename=f"congress_late_{date_str}",
                )

            return CongressLateReportResponse(reports=res_list)
        except Exception as e:
            print(f"Error fetching late reports: {e}")
            return CongressLateReportResponse(reports=[])

    async def get_politician_portfolio(
        self, politician: str
    ) -> PoliticianPortfolioResponse:
        """Fetch politician portfolio (Enterprise)."""
        try:
            url = f"{self.client._base_url}/api/politician-portfolios/{politician}"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Portfolio failed: {response.status_code}")
                # Likely 403 Forbidden for non-enterprise, return empty gracefully
                return PoliticianPortfolioResponse(
                    data=PoliticianPortfolio(politician=politician, portfolio=[])
                )

            # Assuming structure, this is speculative for Enterprise
            data = response.json().get("data", [])
            # Data might be a list of holdings

            portfolio = PoliticianPortfolio(politician=politician, portfolio=data)

            if data:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                # Flatten portfolio if list of dicts
                df = pd.DataFrame(data)

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="politician_portfolio",
                    asset_class="congress",
                    symbol=politician,
                    date=datetime.now(),
                    filename=f"politician_portfolio_{politician}_{date_str}",
                )

            return PoliticianPortfolioResponse(data=portfolio)
        except Exception as e:
            print(f"Error fetching politician portfolio: {e}")
            return PoliticianPortfolioResponse(
                data=PoliticianPortfolio(politician=politician, portfolio=[])
            )

    async def get_stock_info(self, ticker: str) -> StockInfoResponse:
        """Fetch stock info."""
        try:
            url = f"{self.client._base_url}/api/stock/{ticker}/info"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Stock info failed: {response.status_code}")
                # Return empty generic
                return StockInfoResponse(data=StockInfo(ticker=ticker))

            data = response.json().get("data", {})

            info = StockInfo(
                ticker=ticker,
                company_name=data.get("company_name"),
                exchange=data.get("exchange"),
                sector=data.get("sector"),
                industry=data.get("industry"),
            )

            if data:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([info.model_dump()])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="stock_info",
                    asset_class="equity",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"stock_info_{ticker}_{date_str}",
                )

            return StockInfoResponse(data=info)
        except Exception as e:
            print(f"Error fetching stock info: {e}")
            return StockInfoResponse(data=StockInfo(ticker=ticker))

    async def get_stock_ohlc(self, ticker: str) -> StockOHLCResponse:
        """Fetch stock OHLC."""
        try:
            url = f"{self.client._base_url}/api/stock/{ticker}/ohlc"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Stock OHLC failed: {response.status_code}")
                return StockOHLCResponse(ticker=ticker, candles=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    StockCandle(
                        timestamp=(
                            datetime.fromisoformat(item.get("timestamp"))
                            if item.get("timestamp")
                            else datetime.now()
                        ),
                        open=float(item.get("open")) if item.get("open") else 0.0,
                        high=float(item.get("high")) if item.get("high") else 0.0,
                        low=float(item.get("low")) if item.get("low") else 0.0,
                        close=float(item.get("close")) if item.get("close") else 0.0,
                        volume=int(item.get("volume")) if item.get("volume") else 0,
                    )
                )

            if res_list:
                date_str = datetime.now().strftime("%Y%m%d")
                import pandas as pd

                df = pd.DataFrame([r.model_dump() for r in res_list])

                data_lake.save_bronze(
                    data=df,
                    provider="unusual_whales",
                    slice_name="stock_ohlc",
                    asset_class="equity",
                    symbol=ticker,
                    date=datetime.now(),
                    filename=f"stock_ohlc_{ticker}_{date_str}",
                )

            return StockOHLCResponse(ticker=ticker, candles=res_list)
        except Exception as e:
            print(f"Error fetching stock OHLC: {e}")
            return StockOHLCResponse(ticker=ticker, candles=[])

    async def get_user_alerts(self) -> AlertResponse:
        """Fetch user alerts."""
        try:
            url = f"{self.client._base_url}/api/alerts"
            async_http = self.client.get_async_httpx_client()
            response = await async_http.get(url, headers=self.client._headers)

            if response.status_code != 200:
                print(f"Alerts failed: {response.status_code}")
                return AlertResponse(alerts=[])

            data = response.json().get("data", [])

            res_list = []
            for item in data:
                res_list.append(
                    UserAlert(
                        id=item.get("id", "UNKNOWN"),
                        name=item.get("name", "UNKNOWN"),
                        conditions=item.get("conditions", {}),
                        created_at=(
                            datetime.fromisoformat(item.get("created_at"))
                            if item.get("created_at")
                            else datetime.now()
                        ),
                        active=bool(item.get("active", True)),
                    )
                )

            # No data lake storage needed for personal config

            return AlertResponse(alerts=res_list)
        except Exception as e:
            print(f"Error fetching alerts: {e}")
            return AlertResponse(alerts=[])


uw_ingestor = UWIngestor()
