from coinbase.rest import RESTClient
import pandas as pd
from dataingestion.shared.config import get_settings
from dataingestion.shared.storage import data_lake


class CoinbaseIngestor:
    def __init__(self):
        self.settings = get_settings()
        if self.settings.COINBASE_API_KEY and self.settings.COINBASE_API_SECRET:
            self.client = RESTClient(
                api_key=self.settings.COINBASE_API_KEY,
                api_secret=self.settings.COINBASE_API_SECRET,
            )
        else:
            self.client = None

    def get_candles(self, query):
        if not self.client:
            print("Coinbase credentials missing")
            return []

        try:
            # Calculate timestamps logic if needed or pass as is
            # query.start is datetime
            start_ts = int(query.start.timestamp())
            end_ts = int(query.end.timestamp())

            candles = self.client.get_candles(
                product_id=query.product_id,
                start=str(start_ts),
                end=str(end_ts),
                granularity=query.granularity,
            )

            if candles and "candles" in candles:
                data = candles["candles"]
                df = pd.DataFrame(data)

                # Write to Data Lake
                if not df.empty:
                    partition_date = query.start

                    data_lake.save_bronze(
                        data=df,
                        provider="coinbase",
                        slice_name="market_data",
                        asset_class="crypto",
                        symbol=query.product_id,
                        date=partition_date,
                        filename=f"candles_{query.granularity}_{int(partition_date.timestamp())}",
                    )

                return df.to_dict(orient="records")

            return []

        except Exception as e:
            print(f"Error fetching Coinbase candles for {query.product_id}: {e}")
            return []


coinbase_ingestor = CoinbaseIngestor()
