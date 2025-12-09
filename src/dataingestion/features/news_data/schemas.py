from pydantic import BaseModel
from typing import Optional
from datetime import date


class NewsQuery(BaseModel):
    q: str
    sources: Optional[str] = None
    domains: Optional[str] = None
    from_param: Optional[date] = None  # 'from' is reserved
    to: Optional[date] = None
    language: str = "en"
    sort_by: str = "relevancy"
    page_size: int = 100
