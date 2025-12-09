# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - 2025-12-08

### Added
- **Unusual Whales API Integration**: Complete implementation of Phases 7-10 (Endpoints: NetFlow, GroupFlow, DarkPool, MarketTide, Insider, Congress, News, Shorts, Greeks, Options, Eras, Tickers, Alerts).
- **Data Ingestion**: Specific ingestors for SEC, YFinance, FRED, NewsAPI, Coinbase, Alpaca, and Unusual Whales.
- **Storage**: Forensic Data Lake structure (Bronze/Silver/Gold layers) with Parquet/ZSTD support.
- **API**: FastAPI router with caching (Redis) and Pydantic schemas.
- **Infrastructure**: Docker Compose setup for API and Redis.
- **Tooling**: Makefile for `format`, `lint`, `test`, `ci`.

### Fixed
- Multiple linting errors in `ingestor.py`, `storage.py`, `gemini_adapter.py`.
- Syntax errors in `feature_processor.py`.
- API verification tests for SEC endpoints.
