from pydantic import BaseModel
from datetime import datetime


class CoinbaseCandleQuery(BaseModel):
    product_id: str
    start: datetime
    end: datetime
    granularity: (
        str  # ONE_MINUTE, FIVE_MINUTE, FIFTEEN_MINUTE, ONE_HOUR, SIX_HOUR, ONE_DAY
    )
