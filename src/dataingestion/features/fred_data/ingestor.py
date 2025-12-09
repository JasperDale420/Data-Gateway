from fredapi import Fred
from datetime import datetime
from dataingestion.shared.config import get_settings
from dataingestion.shared.storage import data_lake


class FredIngestor:
    def __init__(self):
        self.settings = get_settings()
        if self.settings.FRED_API_KEY:
            self.fred = Fred(api_key=self.settings.FRED_API_KEY)
        else:
            self.fred = None

    def ingest(self, query):
        if not self.fred:
            print("FRED API Key missing")
            return None

        try:
            start = query.start_date
            end = query.end_date
            # FredAPI returns a pandas Series by default
            df = self.fred.get_series(
                query.series_id, observation_start=start, observation_end=end
            )

            if df is None or df.empty:
                return None

            # Convert Series to DataFrame
            df = df.to_frame(name="value")
            df.index.name = "date"
            df.reset_index(inplace=True)

            # Write to Data Lake
            if not df.empty:
                partition_date = datetime.now()

                data_lake.save_bronze(
                    data=df,
                    provider="fred",
                    slice_name="macro",
                    asset_class="economy",
                    symbol=query.series_id,
                    date=partition_date,
                    filename=f"series_{query.series_id}",
                )

            return df

        except Exception as e:
            print(f"Error fetching FRED series {query.series_id}: {e}")
            return None


fred_ingestor = FredIngestor()
