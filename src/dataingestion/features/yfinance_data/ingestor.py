import yfinance as yf
from datetime import datetime, timezone
from dataingestion.shared.storage import data_lake
from dataingestion.features.yfinance_data.schemas import YFinanceResponse, YFinanceBar


class YFinanceIngestor:
    def get_history(self, query):
        # Fetch from yfinance
        ticker = yf.Ticker(query.ticker)
        try:
            history = ticker.history(period=query.period, interval=query.interval)
        except Exception as e:
            raise Exception(f"Failed to fetch data from yfinance: {e}")

        if history.empty:
            return YFinanceResponse(
                ticker=query.ticker,
                period=query.period,
                interval=query.interval,
                bars=[],
            )

        # Process dataframe
        history = history.reset_index()
        history.columns = [c.lower() for c in history.columns]

        # Rename 'Date' to 'timestamp' if present (1d interval uses Date, others Datetime)
        if "date" in history.columns:
            history.rename(columns={"date": "timestamp"}, inplace=True)
        elif "datetime" in history.columns:
            history.rename(columns={"datetime": "timestamp"}, inplace=True)

        # Write to Data Lake
        partition_date = datetime.now(timezone.utc)

        data_lake.save_bronze(
            data=history,
            provider="yfinance",
            slice_name="market_data",
            asset_class="equity",
            symbol=query.ticker,
            date=partition_date,
            filename=f"bars_{query.interval}",
        )

        # Convert to response model
        bars = []
        for _, row in history.iterrows():
            bars.append(
                YFinanceBar(
                    timestamp=row["timestamp"],
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=int(row["volume"]),
                    dividends=row.get("dividends", 0.0),
                    stock_splits=row.get("stock_splits", 0.0),
                )
            )

        return YFinanceResponse(
            ticker=query.ticker, period=query.period, interval=query.interval, bars=bars
        )


yfinance_ingestor = YFinanceIngestor()
