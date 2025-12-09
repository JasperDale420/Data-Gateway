from pydantic_settings import BaseSettings
from typing import Optional
from functools import lru_cache


class Settings(BaseSettings):
    APP_NAME: str = "Data Ingestion Service"
    DEBUG: bool = False

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # Storage
    DATA_DIR: str = "../data"

    # SEC API
    SEC_USER_AGENT: str = "Company Name AdminContact@company.com"

    # Alpaca
    APCA_API_KEY_ID: str = ""
    APCA_API_SECRET_KEY: str = ""
    APCA_API_BASE_URL: str = "https://paper-api.alpaca.markets"

    # Unusual Whales
    UNUSUAL_WHALES_API_KEY: str = ""

    FRED_API_KEY: Optional[str] = None
    NEWS_API_KEY: Optional[str] = None
    COINBASE_API_KEY: Optional[str] = None
    COINBASE_API_SECRET: Optional[str] = None

    class Config:
        # Load .env from the dataingestion project root, not just CWD
        # This allows other projects (like GOATmoat) to use the config without duplicating .env
        import os
        from pathlib import Path

        # Find the .env file relative to this file
        # this file is in src/dataingestion/shared/config.py
        # .env is in / (project root)
        # So we go up 4 levels: shared -> dataingestion -> src -> root
        base_dir = Path(__file__).resolve().parent.parent.parent.parent
        env_file = str(base_dir / ".env")
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings():
    return Settings()
