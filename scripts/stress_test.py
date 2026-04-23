#!/usr/bin/env python3
"""Data-Gateway stress test with live market data and paper order placement.

Tests all major endpoints under concurrent load to verify the dedicated
thread pool fix prevents timeouts. Uses paper Alpaca keys — all orders
are cancelled immediately after placement.
"""

import asyncio
import json
import time
from dataclasses import dataclass, field

import httpx

GATEWAY = "http://localhost:8080"
API_KEY = "gw_orion_trading_key_55555"  # orion key has broad permissions  # pragma: allowlist secret
HEADERS = {"X-Gateway-Key": API_KEY}

# Symbols to test across different asset types
EQUITY_SYMBOLS = ["AAPL", "MSFT", "NVDA", "TSLA", "AMD", "AMZN", "GOOGL", "META", "SPY", "QQQ"]
OPTION_SYMBOLS = ["AAPL", "SPY", "QQQ", "NVDA"]


@dataclass
class TestResult:
    endpoint: str
    status: int
    latency_ms: float
    error: str | None = None
    data_size: int = 0


@dataclass
class StressReport:
    results: list[TestResult] = field(default_factory=list)

    def add(self, r: TestResult):
        self.results.append(r)

    def summary(self) -> str:
        lines = ["\n" + "=" * 70, "STRESS TEST REPORT", "=" * 70]

        # Group by endpoint
        by_endpoint: dict[str, list[TestResult]] = {}
        for r in self.results:
            by_endpoint.setdefault(r.endpoint, []).append(r)

        total_ok = sum(1 for r in self.results if r.status == 200)
        total_fail = sum(1 for r in self.results if r.status != 200)
        total_timeout = sum(1 for r in self.results if r.error and "timeout" in r.error.lower())

        lines.append(f"\nTotal requests: {len(self.results)}")
        lines.append(f"Success: {total_ok}  |  Failed: {total_fail}  |  Timeouts: {total_timeout}")

        lines.append(
            f"\n{'Endpoint':<45} {'Count':>5} {'OK':>4} {'Fail':>4} {'p50':>7} {'p95':>7} {'p99':>7} {'Max':>7}"
        )
        lines.append("-" * 90)

        for ep, results in sorted(by_endpoint.items()):
            ok = sum(1 for r in results if r.status == 200)
            fail = len(results) - ok
            latencies = [r.latency_ms for r in results]
            latencies.sort()
            p50 = latencies[len(latencies) // 2] if latencies else 0
            p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
            p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0
            mx = max(latencies) if latencies else 0
            lines.append(
                f"{ep:<45} {len(results):>5} {ok:>4} {fail:>4} {p50:>6.0f}ms {p95:>6.0f}ms {p99:>6.0f}ms {mx:>6.0f}ms"
            )

        # Show errors
        errors = [r for r in self.results if r.error]
        if errors:
            lines.append(f"\nERRORS ({len(errors)}):")
            seen = set()
            for r in errors:
                key = f"{r.endpoint}: {r.error}"
                if key not in seen:
                    seen.add(key)
                    count = sum(1 for e in errors if f"{e.endpoint}: {e.error}" == key)
                    lines.append(f"  [{count}x] {key}")

        return "\n".join(lines)


async def timed_request(client: httpx.AsyncClient, method: str, url: str, endpoint_label: str, **kwargs) -> TestResult:
    t0 = time.monotonic()
    try:
        resp = await client.request(method, url, **kwargs)
        latency = (time.monotonic() - t0) * 1000
        error = None if resp.status_code == 200 else f"HTTP {resp.status_code}: {resp.text[:200]}"
        return TestResult(
            endpoint=endpoint_label,
            status=resp.status_code,
            latency_ms=latency,
            error=error,
            data_size=len(resp.content),
        )
    except httpx.TimeoutException as e:
        latency = (time.monotonic() - t0) * 1000
        return TestResult(endpoint=endpoint_label, status=504, latency_ms=latency, error=f"TIMEOUT: {e}")
    except Exception as e:
        latency = (time.monotonic() - t0) * 1000
        return TestResult(endpoint=endpoint_label, status=500, latency_ms=latency, error=f"ERROR: {e}")


async def run_stress_test():
    report = StressReport()

    async with httpx.AsyncClient(
        base_url=GATEWAY,
        headers=HEADERS,
        timeout=httpx.Timeout(20.0, connect=5.0),
    ) as client:
        # ============================================================
        # PHASE 1: Warm up — sequential sanity checks
        # ============================================================
        print("\n--- Phase 1: Sequential sanity checks ---")

        # Health
        r = await timed_request(client, "GET", "/health", "GET /health")
        report.add(r)
        print(f"  Health: {r.status} ({r.latency_ms:.0f}ms)")

        # Account
        r = await timed_request(client, "GET", "/api/v1/alpaca/account", "GET /alpaca/account")
        report.add(r)
        print(f"  Account: {r.status} ({r.latency_ms:.0f}ms)")

        # Positions
        r = await timed_request(client, "GET", "/api/v1/alpaca/positions", "GET /alpaca/positions")
        report.add(r)
        print(f"  Positions: {r.status} ({r.latency_ms:.0f}ms)")

        # Orders
        r = await timed_request(client, "GET", "/api/v1/alpaca/orders", "GET /alpaca/orders")
        report.add(r)
        print(f"  Orders: {r.status} ({r.latency_ms:.0f}ms)")

        # Bars for a single symbol
        r = await timed_request(
            client, "GET", "/api/v1/alpaca/stocks/AAPL/bars?timeframe=1Min&limit=5", "GET /alpaca/bars"
        )
        report.add(r)
        print(f"  Bars AAPL: {r.status} ({r.latency_ms:.0f}ms)")

        # Quote
        r = await timed_request(client, "GET", "/api/v1/alpaca/stocks/AAPL/quotes?limit=1", "GET /alpaca/quote")
        report.add(r)
        print(f"  Quote AAPL: {r.status} ({r.latency_ms:.0f}ms)")

        # ============================================================
        # PHASE 2: Concurrent market data (simulates 5 trading systems)
        # ============================================================
        print("\n--- Phase 2: Concurrent market data (10 symbols x 5 parallel batches) ---")

        async def fetch_bars(symbol: str) -> TestResult:
            return await timed_request(
                client,
                "GET",
                f"/api/v1/alpaca/stocks/{symbol}/bars?timeframe=1Min&limit=10",
                f"GET /alpaca/bars/{symbol}",
            )

        async def fetch_quote(symbol: str) -> TestResult:
            return await timed_request(
                client, "GET", f"/api/v1/alpaca/stocks/{symbol}/quotes?limit=1", f"GET /alpaca/quote/{symbol}"
            )

        async def fetch_trades(symbol: str) -> TestResult:
            return await timed_request(
                client, "GET", f"/api/v1/alpaca/stocks/{symbol}/trades?limit=1", f"GET /alpaca/trade/{symbol}"
            )

        # Fire all at once — 30 concurrent requests (bars + quotes + trades for 10 symbols)
        tasks = []
        for sym in EQUITY_SYMBOLS:
            tasks.append(fetch_bars(sym))
            tasks.append(fetch_quote(sym))
            tasks.append(fetch_trades(sym))

        results = await asyncio.gather(*tasks)
        for r in results:
            report.add(r)
        ok = sum(1 for r in results if r.status == 200)
        print(
            f"  30 concurrent requests: {ok}/{len(results)} success, max latency {max(r.latency_ms for r in results):.0f}ms"
        )

        # ============================================================
        # PHASE 3: Concurrent trading calls (the bottleneck we fixed)
        # ============================================================
        print("\n--- Phase 3: Concurrent trading calls (simulating 5 systems polling) ---")

        async def poll_orders() -> TestResult:
            return await timed_request(client, "GET", "/api/v1/alpaca/orders", "GET /alpaca/orders (concurrent)")

        async def poll_positions() -> TestResult:
            return await timed_request(client, "GET", "/api/v1/alpaca/positions", "GET /alpaca/positions (concurrent)")

        async def poll_account() -> TestResult:
            return await timed_request(client, "GET", "/api/v1/alpaca/account", "GET /alpaca/account (concurrent)")

        # Simulate 5 trading systems each polling orders + positions + account simultaneously
        # That's 15 concurrent trading calls — previously would exhaust thread pool
        trading_tasks = []
        for _ in range(5):
            trading_tasks.append(poll_orders())
            trading_tasks.append(poll_positions())
            trading_tasks.append(poll_account())

        results = await asyncio.gather(*trading_tasks)
        for r in results:
            report.add(r)
        ok = sum(1 for r in results if r.status == 200)
        timeouts = sum(1 for r in results if r.error and "timeout" in str(r.error).lower())
        print(
            f"  15 concurrent trading calls: {ok}/{len(results)} success, {timeouts} timeouts, max latency {max(r.latency_ms for r in results):.0f}ms"
        )

        # ============================================================
        # PHASE 4: Mixed load (market data + trading calls together)
        # ============================================================
        print("\n--- Phase 4: Mixed load (market data + trading calls simultaneously) ---")

        mixed_tasks = []
        # 10 bar requests
        for sym in EQUITY_SYMBOLS:
            mixed_tasks.append(fetch_bars(sym))
        # 5 order polls
        for _ in range(5):
            mixed_tasks.append(poll_orders())
        # 5 position polls
        for _ in range(5):
            mixed_tasks.append(poll_positions())
        # 5 account polls
        for _ in range(5):
            mixed_tasks.append(poll_account())

        results = await asyncio.gather(*mixed_tasks)
        for r in results:
            report.add(r)
        ok = sum(1 for r in results if r.status == 200)
        timeouts = sum(1 for r in results if r.error and "timeout" in str(r.error).lower())
        print(
            f"  25 mixed requests: {ok}/{len(results)} success, {timeouts} timeouts, max latency {max(r.latency_ms for r in results):.0f}ms"
        )

        # ============================================================
        # PHASE 5: Order placement + immediate cancel (paper mode)
        # ============================================================
        print("\n--- Phase 5: Paper order placement + immediate cancel ---")

        async def place_and_track(symbol: str, side: str, qty: int) -> TestResult:
            order_body = {
                "symbol": symbol,
                "qty": qty,
                "side": side,
                "type": "market",
                "time_in_force": "day",
            }
            r = await timed_request(
                client,
                "POST",
                "/api/v1/alpaca/orders",
                f"POST /alpaca/orders ({side} {symbol})",
                json=order_body,
            )
            if r.status == 200:
                try:
                    json.loads(await client.post("/api/v1/alpaca/orders", json=order_body).aread() if False else "")
                except Exception:
                    pass
                # Try to extract order ID from the response
                try:
                    await client.post("/api/v1/alpaca/orders", json=order_body, headers=HEADERS)
                    # Don't double-submit, we already have the result
                except Exception:
                    pass
            return r

        # Place 3 small limit orders well below market (paper mode, cancel immediately)
        # Using limit orders far from market to avoid fills
        test_orders = [
            ("AAPL", "buy", 1, 50.0),  # far below market
            ("MSFT", "buy", 1, 50.0),
            ("NVDA", "buy", 1, 10.0),
        ]

        for symbol, side, qty, limit_px in test_orders:
            r = await timed_request(
                client,
                "POST",
                f"/api/v1/alpaca/orders?symbol={symbol}&side={side}&qty={qty}&order_type=limit&time_in_force=day&limit_price={limit_px}&client_order_id=stress_test_{symbol}_{int(time.time())}",
                f"POST /alpaca/orders ({side} {symbol})",
            )
            report.add(r)
            print(f"  Order {side} {qty} {symbol} @${limit_px}: {r.status} ({r.latency_ms:.0f}ms)")

        # Get all open orders and cancel them
        print("  Cancelling all open orders...")
        r = await timed_request(client, "DELETE", "/api/v1/alpaca/orders", "DELETE /alpaca/orders (cancel all)")
        report.add(r)
        print(f"  Cancel all: {r.status} ({r.latency_ms:.0f}ms)")

        # Verify no open orders remain
        r = await timed_request(client, "GET", "/api/v1/alpaca/orders?status=open", "GET /alpaca/orders (verify empty)")
        report.add(r)
        print(f"  Verify empty: {r.status} ({r.latency_ms:.0f}ms)")

        # ============================================================
        # PHASE 6: Sustained load (3 rounds of concurrent polling)
        # ============================================================
        print("\n--- Phase 6: Sustained load (3 rounds x 15 concurrent trading calls) ---")

        for round_num in range(3):
            tasks = []
            for _ in range(5):
                tasks.append(poll_orders())
                tasks.append(poll_positions())
                tasks.append(poll_account())

            results = await asyncio.gather(*tasks)
            for r in results:
                report.add(r)
            ok = sum(1 for r in results if r.status == 200)
            max_lat = max(r.latency_ms for r in results)
            print(f"  Round {round_num + 1}: {ok}/15 success, max latency {max_lat:.0f}ms")
            await asyncio.sleep(0.5)  # Brief pause between rounds

        # ============================================================
        # PHASE 7: WebSocket connectivity check
        # ============================================================
        print("\n--- Phase 7: WebSocket connectivity check ---")
        import websockets

        try:
            async with websockets.connect(
                "ws://localhost:8080/ws",
                additional_headers={"X-Gateway-Key": API_KEY},
                open_timeout=5,
            ) as ws:
                # Send auth
                await ws.send(json.dumps({"action": "auth", "key": API_KEY}))
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                auth_result = json.loads(msg)
                print(f"  WS Auth: {auth_result.get('status', auth_result)}")

                # Subscribe to a bar
                await ws.send(json.dumps({"action": "subscribe", "bars": ["AAPL"]}))
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                print(f"  WS Subscribe: {json.loads(msg).get('status', msg[:100])}")

                report.add(TestResult(endpoint="WS /ws connect+auth", status=200, latency_ms=0))
        except Exception as e:
            print(f"  WS Error: {e}")
            report.add(TestResult(endpoint="WS /ws connect+auth", status=500, latency_ms=0, error=str(e)))

    # Print final report
    print(report.summary())


if __name__ == "__main__":
    asyncio.run(run_stress_test())
