#!/usr/bin/env python3
"""Quick test of trading bot components."""

import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


def test_alpaca():
    """Test Alpaca connection."""
    print("Testing Alpaca connection...")
    try:
        from core.trading_client import TradingClientWrapper

        client = TradingClientWrapper(paper=True)
        account = client.get_account()
        print("✅ Alpaca connected!")
        print(f"   Account ID: {account['id']}")
        print(f"   Equity: ${account['equity']:,.2f}")
        print(f"   Buying Power: ${account['buying_power']:,.2f}")
        print(f"   Cash: ${account['cash']:,.2f}")
        return True
    except Exception as e:
        print(f"❌ Alpaca failed: {e}")
        return False


def test_gateway():
    """Test Data-Gateway connection."""
    print("\nTesting Data-Gateway connection...")
    try:
        import requests

        response = requests.get("http://localhost:8080/health", timeout=10)
        if response.status_code == 200:
            print(f"✅ Data-Gateway health: {response.json()}")
            return True
        else:
            print(f"❌ Data-Gateway status: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Data-Gateway failed: {e}")
        return False


def test_strategy():
    """Test strategy calculation."""
    print("\nTesting strategy...")
    try:
        import numpy as np
        import pandas as pd
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
        signal = strategy.generate_signal(df_with_indicators, "TEST")

        print("✅ Strategy working!")
        print(f"   Signal: {signal.signal.value if signal else 'HOLD'}")
        if signal:
            print(f"   Confidence: {signal.confidence:.2%}")

        return True
    except Exception as e:
        print(f"❌ Strategy failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    print("=" * 60)
    print("QUICK TRADING BOT TEST")
    print("=" * 60)

    results = []
    results.append(test_alpaca())
    results.append(test_gateway())
    results.append(test_strategy())

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    if all(results):
        print("✅ ALL TESTS PASSED!")
        print("\nTrading bot is ready for development.")
        print("Next: Run integration tests and start backtesting.")
    else:
        print("⚠️  Some tests failed.")
        print("\nIssues to fix before proceeding.")

    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
