import asyncio
from datetime import UTC, datetime, timedelta

from gateway.config import get_settings
from gateway.core.backfill import BackfillEngine, BackfillRequest
from gateway.core.globals import set_registry
from gateway.core.rate_limiter import get_provider_rate_limiters
from gateway.core.registry import ProviderRegistry


async def main():
    settings = get_settings()
    rate_limiters = get_provider_rate_limiters()
    await rate_limiters.start()

    # Initialize provider registry properly
    registry = ProviderRegistry()
    await registry.load_from_config(settings.providers_config_path)
    set_registry(registry)

    engine = BackfillEngine()
    engine.configure(provider_registry=registry, sink_registry=None)

    req = BackfillRequest(
        provider="alpaca",
        feed="crypto_quotes",
        symbols=["BTC/USD"],
        start=datetime.now(UTC).date() - timedelta(days=3),
        end=datetime.now(UTC).date() - timedelta(days=1),
        timeframe="1Day",
        chunk_days=1,
    )
    job = engine.submit(req)
    print(f"Successfully submitted job {job.job_id} for {job.request.feed}")

    # Monitor status
    while True:
        status = job.status.value
        print(
            f"Job status: {status} (Chunks complete: {job.symbols_progress['BTC/USD'].chunks_complete}/{job.symbols_progress['BTC/USD'].chunks_total})"
        )
        if status in ("completed", "failed", "cancelled"):
            if status == "failed":
                print(f"Errors: {job.errors}")
            break
        await asyncio.sleep(2)

    await registry.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
