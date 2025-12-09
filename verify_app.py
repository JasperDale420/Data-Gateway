import sys
# Hack to remove conflicting local package from another project
sys.path = [p for p in sys.path if "Kairos" not in p]

from dataingestion.api.main import app

print("Verifying app routes...")
routes = [route.path for route in app.routes]
expected_routes = [
    "/sec/filings/{ticker}",
    "/alpaca/bars/{symbol}",
    "/uw/flow/{ticker}",
    "/yfinance/history/{ticker}",
    "/fred/series/{series_id}",
    "/news/headlines"
]

missing = []
for expected in expected_routes:
    found = False
    for route in routes:
        if route == expected:
            found = True
            break
    if not found:
        # Check if it's a partial match or parameterized
        # FastAPI stores paths like /sec/filings/{ticker}
        if expected in routes:
            found = True
        else:
            missing.append(expected)

if missing:
    print(f"❌ Missing routes: {missing}")
    print(f"Available routes: {routes}")
    sys.exit(1)
else:
    print("✅ All routers registered successfully.")
