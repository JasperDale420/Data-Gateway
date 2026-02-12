#!/usr/bin/env python3
"""
Simple test to run trading bot components.
"""

import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

print("=" * 70)
print("TRADING BOT TEST RUN")
print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# Test 1: Check Alpaca connection
print("\n1. Testing Alpaca connection...")
try:
    from core.trading_client import TradingClientWrapper

    trading_client = TradingClientWrapper(paper=True)
    account = trading_client.get_account()
    print("✅ Alpaca connected!")
    print(f"   Equity: ${account['equity']:,.2f}")
    print(f"   Buying Power: ${account['buying_power']:,.2f}")
    print(f"   Cash: ${account['cash']:,.2f}")
except Exception as e:
    print(f"❌ Alpaca failed: {e}")

# Test 2: Check Data-Gateway
print("\n2. Testing Data-Gateway...")
try:
    import requests

    response = requests.get("http://localhost:8080/health", timeout=5)
    if response.status_code == 200:
        print(f"✅ Data-Gateway: {response.json()}")
    else:
        print(f"❌ Data-Gateway status: {response.status_code}")
except Exception as e:
    print(f"❌ Data-Gateway failed: {e}")

# Test 3: Test strategy with sample data
print("\n3. Testing strategy...")
try:
    from strategies.mean_reversion import MeanReversionStrategy

    # Create sample data
    np.random.seed(42)
    dates = pd.date_range("2026-01-01", periods=50, freq="D")
    prices = 100 + np.cumsum(np.random.randn(50) * 2)

    df = pd.DataFrame(
        {
            "open": prices * 0.99,
            "high": prices * 1.01,
            "low": prices * 0.98,
            "close": prices,
            "volume": np.random.randint(1000000, 5000000, 50),
        },
        index=dates,
    )

    strategy = MeanReversionStrategy()
    df_with_indicators = strategy.calculate_indicators(df)
    signal = strategy.generate_signal(df_with_indicators, "SPY")

    if signal:
        print(f"✅ Signal: {signal.signal.value}")
        print(f"   Confidence: {signal.confidence:.2%}")
        print(f"   Price: ${signal.price:.2f}")
        print(f"   Stop Loss: ${signal.metadata['stop_loss']:.2f}")
        print(f"   Take Profit: ${signal.metadata['take_profit']:.2f}")

        # Test position sizing
        shares, risk_metrics = strategy.calculate_position_size(
            account_equity=100000, risk_per_trade=0.02, entry_price=signal.price, stop_loss=signal.metadata["stop_loss"]
        )

        print(f"   Position size: {shares} shares")
        print(f"   Risk amount: ${risk_metrics['actual_risk']:,.2f}")
    else:
        print("✅ Strategy: No signal (HOLD)")

except Exception as e:
    print(f"❌ Strategy failed: {e}")
    import traceback

    traceback.print_exc()

# Test 4: Try to get real market data
print("\n4. Testing market data fetch...")
try:
    from core.gateway_client import DataGatewayClient

    client = DataGatewayClient()

    # Try to get SPY data
    bars = client.get_stock_bars("SPY", "1Day", 5)
    if "bars" in bars and bars["bars"]:
        print(f"✅ Market data: {len(bars['bars'])} bars retrieved")
        latest = bars["bars"][-1]
        print(f"   Latest close: ${latest.get('close', 'N/A')}")
    else:
        print("⚠️  No bars data returned (might be auth issue)")

except Exception as e:
    print(f"❌ Market data failed: {e}")

print("\n" + "=" * 70)
print("TEST COMPLETE")
print("=" * 70)
print("\nNext: If all tests pass, the bot is ready for daily trading.")
print("Run: ./setup_everything.sh to set up automation.")
