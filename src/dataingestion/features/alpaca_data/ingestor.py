from typing import List, Optional, Dict
from datetime import datetime
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import (
    StockBarsRequest,
    CryptoBarsRequest,
    OptionBarsRequest,
    OptionTradesRequest,
)
from alpaca.data.timeframe import TimeFrame
from dataingestion.shared.config import get_settings
from dataingestion.features.alpaca_data.schemas import (
    Bar,
    BarResponse,
    OptionBar,
    OptionBarResponse,
    OptionTrade,
    OptionTradeResponse,
)
from dataingestion.shared.storage import data_lake

settings = get_settings()


class AlpacaIngestor:
    def __init__(self):
        self._stock_client = None
        self._crypto_client = None
        self._option_client = None

    @property
    def stock_client(self):
        if not self._stock_client:
            self._stock_client = StockHistoricalDataClient(
                api_key=settings.APCA_API_KEY_ID,
                secret_key=settings.APCA_API_SECRET_KEY,
            )
        return self._stock_client

    @property
    def crypto_client(self):
        if not self._crypto_client:
            self._crypto_client = CryptoHistoricalDataClient(
                api_key=settings.APCA_API_KEY_ID,
                secret_key=settings.APCA_API_SECRET_KEY,
            )
        return self._crypto_client

    @property
    def option_client(self):
        if not self._option_client:
            self._option_client = OptionHistoricalDataClient(
                api_key=settings.APCA_API_KEY_ID,
                secret_key=settings.APCA_API_SECRET_KEY,
            )
        return self._option_client

    @property
    def trading_client(self):
        from alpaca.trading.client import TradingClient

        return TradingClient(
            api_key=settings.APCA_API_KEY_ID,
            secret_key=settings.APCA_API_SECRET_KEY,
            paper=True,  # Default to paper for now as per user request
        )

    def get_account(self):
        return self.trading_client.get_account()

    def get_option_contracts(
        self,
        underlying_symbol: str,
        limit: int = 100,
        expiration_date: str = None,
        type: str = None,  # "call" or "put"
    ):
        from alpaca.trading.requests import GetOptionContractsRequest
        from alpaca.trading.enums import ContractType

        params = {
            "underlying_symbols": [underlying_symbol],
            "status": "active",
            "limit": limit,
        }
        if expiration_date:
            # Check format or pass as is? Alpaca expects YYYY-MM-DD
            params["expiration_date"] = expiration_date
        if type:
            params["type"] = ContractType.CALL if type.lower() == "call" else ContractType.PUT

        req = GetOptionContractsRequest(**params)
        return self.trading_client.get_option_contracts(req)

    def get_option_snapshot(self, symbol: str):
        from alpaca.data.requests import OptionSnapshotRequest
        req = OptionSnapshotRequest(symbol_or_symbols=symbol)
        # Verify which client to use: option_client (historical) usually has getting latest quote?
        # Actually in alpaca-py, OptionHistoricalDataClient has get_option_snapshot
        res = self.option_client.get_option_snapshot(req)
        return res[symbol] if symbol in res else None

    def get_latest_quote(self, symbol: str):
        from alpaca.data.requests import StockLatestQuoteRequest
        
        req = StockLatestQuoteRequest(symbol_or_symbols=symbol)
        res = self.stock_client.get_stock_latest_quote(req)
        return res[symbol] if symbol in res else None

    def submit_order(
        self,
        symbol: str,
        qty: float,
        side: str,
        type: str,
        time_in_force: str,
        limit_price: float = None,
    ):
        from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce

        side_enum = OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL
        tif_enum = TimeInForce.DAY  # Default

        if type.lower() == "market":
            req = MarketOrderRequest(
                symbol=symbol, qty=qty, side=side_enum, time_in_force=tif_enum
            )
        elif type.lower() == "limit":
            req = LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side_enum,
                time_in_force=tif_enum,
                limit_price=limit_price,
            )
        else:
            raise ValueError(f"Unsupported order type: {type}")

        return self.trading_client.submit_order(req)

    def get_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: Optional[datetime] = None,
    ) -> BarResponse:
        """Fetch historical bars."""
        # Map timeframe string to Alpaca TimeFrame
        tf_map = {
            "1Min": TimeFrame.Minute,
            "1Hour": TimeFrame.Hour,
            "1Day": TimeFrame.Day,
        }
        tf = tf_map.get(timeframe, TimeFrame.Day)

        request_params = {
            "symbol_or_symbols": symbol,
            "timeframe": tf,
            "start": start,
            "end": end,
        }

        # Determine if crypto or stock based on symbol (naive check, can be improved)
        if symbol in ["BTC/USD", "ETH/USD"]:
            req = CryptoBarsRequest(**request_params)
            bars = self.crypto_client.get_crypto_bars(req)
        else:
            req = StockBarsRequest(**request_params)
            bars = self.stock_client.get_stock_bars(req)

        data = bars.df.reset_index() if not bars.df.empty else []

        # Write to Data Lake
        if not bars.df.empty:
            start_str = start.strftime("%Y%m%d")
            data_lake.save_bronze(
                data=bars.df,
                provider="alpaca",
                slice_name="market_data",
                asset_class="crypto" if "USD" in symbol else "equity",
                symbol=symbol,
                date=start,
                filename=f"bars_{timeframe}_{start_str}",
            )

        result_bars = []
        if not bars.df.empty:
            for _, row in data.iterrows():
                result_bars.append(
                    Bar(
                        t=row["timestamp"],
                        o=row["open"],
                        h=row["high"],
                        l=row["low"],
                        c=row["close"],
                        v=row["volume"],
                        n=row.get("trade_count"),
                        vw=row.get("vwap"),
                    )
                )

        return BarResponse(symbol=symbol, bars=result_bars)

    def get_corporate_actions(
        self, symbol: str, start: datetime, end: datetime
    ) -> List[Dict]:
        """Fetch corporate actions (Splits/Dividends) from Alpaca."""
        from alpaca.data.requests import CorporateActionsRequest

        request_params = {"symbol_or_symbols": [symbol], "start": start, "end": end}

        try:
            req = CorporateActionsRequest(**request_params)
            actions = self.stock_client.get_corporate_actions(req)

            if actions.df.empty:
                return []

            # Write to Data Lake
            # Corporate actions are smaller, saves as JSON
            data_dict = actions.df.reset_index().to_dict(orient="records")
            data_lake.save_bronze(
                data=data_dict,
                provider="alpaca",
                slice_name="corporate_actions",
                asset_class="equity",
                symbol=symbol,
                date=start,
                filename="actions",
            )

            return data_dict

        except Exception:
            return []

    def get_option_bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: Optional[datetime] = None,
    ) -> OptionBarResponse:
        """Fetch historical option bars."""
        tf_map = {
            "1Min": TimeFrame.Minute,
            "1Hour": TimeFrame.Hour,
            "1Day": TimeFrame.Day,
        }
        tf = tf_map.get(timeframe, TimeFrame.Day)

        request_params = {
            "symbol_or_symbols": symbol,
            "timeframe": tf,
            "start": start,
            "end": end,
        }

        req = OptionBarsRequest(**request_params)
        bars = self.option_client.get_option_bars(req)

        data = bars.df.reset_index() if not bars.df.empty else []

        # Write to Data Lake
        if not bars.df.empty:
            start_str = start.strftime("%Y%m%d")
            data_lake.save_bronze(
                data=bars.df,
                provider="alpaca",
                slice_name="options",
                asset_class="equity_options",
                symbol=symbol,
                date=start,
                filename=f"bars_{timeframe}_{start_str}",
            )

        result_bars = []
        if not bars.df.empty:
            for _, row in data.iterrows():
                result_bars.append(
                    OptionBar(
                        t=row["timestamp"],
                        o=row["open"],
                        h=row["high"],
                        l=row["low"],
                        c=row["close"],
                        v=row["volume"],
                        n=row.get("trade_count"),
                        vw=row.get("vwap"),
                    )
                )

        return OptionBarResponse(symbol=symbol, bars=result_bars)

    def get_option_trades(
        self, symbol: str, start: datetime, end: Optional[datetime] = None
    ) -> OptionTradeResponse:
        """Fetch historical option trades."""
        request_params = {"symbol_or_symbols": symbol, "start": start, "end": end}

        req = OptionTradesRequest(**request_params)
        trades = self.option_client.get_option_trades(req)

        data = trades.df.reset_index() if not trades.df.empty else []

        # Write to Data Lake
        if not trades.df.empty:
            start_str = start.strftime("%Y%m%d")
            data_lake.save_bronze(
                data=trades.df,
                provider="alpaca",
                slice_name="options_trades",
                asset_class="equity_options",
                symbol=symbol,
                date=start,
                filename=f"trades_{start_str}",
            )

        result_trades = []
        if not trades.df.empty:
            for _, row in data.iterrows():
                result_trades.append(
                    OptionTrade(
                        t=row["timestamp"],
                        x=row["exchange"],
                        p=row["price"],
                        s=row["size"],
                        c=row["condition"],
                    )
                )

        return OptionTradeResponse(symbol=symbol, trades=result_trades)


alpaca_ingestor = AlpacaIngestor()
