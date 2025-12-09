from newsapi import NewsApiClient
from datetime import datetime
from dataingestion.shared.config import get_settings
from dataingestion.shared.storage import data_lake


class NewsApiIngestor:
    def __init__(self):
        self.settings = get_settings()
        if self.settings.NEWS_API_KEY:
            self.client = NewsApiClient(api_key=self.settings.NEWS_API_KEY)
        else:
            self.client = None

    def ingest(self, query):
        if not self.client:
            print("NewsAPI Key missing")
            return []

        try:
            # Sanitize inputs
            # NewsAPI expects ISO strings or YYYY-MM-DD
            from_str = (
                query.from_param.strftime("%Y-%m-%d") if query.from_param else None
            )
            to_str = query.to.strftime("%Y-%m-%d") if query.to else None

            response = self.client.get_everything(
                q=query.q,
                from_param=from_str,
                to=to_str,
                language=query.language,
                sort_by=query.sort_by,
                page_size=query.page_size,
            )

            articles = response.get("articles", [])

            # Write to Data Lake
            if articles:
                partition_date = datetime.now()
                # Sanitize query
                safe_q = query.q.replace(" ", "_").replace("/", "-")

                data_lake.save_bronze(
                    data=articles,
                    provider="newsapi",
                    slice_name="news",
                    asset_class="headlines",
                    symbol=safe_q,
                    date=partition_date,
                    filename=f"news_{safe_q}",
                )

            return articles

        except Exception as e:
            print(f"Error fetching NewsAPI for {query.q}: {e}")
            return []


news_ingestor = NewsApiIngestor()
