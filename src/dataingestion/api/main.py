from contextlib import asynccontextmanager
from fastapi import FastAPI
from dataingestion.shared.config import get_settings
from dataingestion.shared.cache import cache_service
from dataingestion.features.sec_data.router import router as sec_router
from dataingestion.features.alpaca_data.router import router as alpaca_router
from dataingestion.features.uw_data.router import router as uw_router
from dataingestion.features.llm_gateway.router import router as llm_router
from dataingestion.features.yfinance_data.router import router as yfinance_router
from dataingestion.features.fred_data.router import router as fred_router
from dataingestion.features.news_data.router import router as news_router
from dataingestion.features.coinbase_data.router import router as coinbase_router

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await cache_service.connect()
    yield
    # Shutdown
    await cache_service.close()


app = FastAPI(title=settings.APP_NAME, lifespan=lifespan)

app.include_router(sec_router)
app.include_router(alpaca_router)
app.include_router(uw_router)
app.include_router(llm_router)
app.include_router(yfinance_router)
app.include_router(fred_router)
app.include_router(news_router)
app.include_router(coinbase_router)


@app.get("/health")
async def health_check():
    return {"status": "ok", "redis": cache_service.redis is not None}
