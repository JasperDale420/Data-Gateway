#!/usr/bin/env python3
"""
Test connectivity to Data-Gateway and Alpaca paper trading.
"""

import os
import sys

import requests
from dotenv import load_dotenv

# Load environment variables from Data-Gateway .env
load_dotenv("/Users/jacobmcmillan/Empire/Data-Gateway/.env")


def test_data_gateway():
    """Test Data-Gateway health endpoint."""
    print("Testing Data-Gateway connectivity...")
    try:
        response = requests.get("http://localhost:8080/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Data-Gateway health: {data}")
            return True
        else:
            print(f"✗ Data-Gateway returned status: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Data-Gateway connection failed: {e}")
        return False


def test_alpaca_credentials():
    """Verify Alpaca credentials are available."""
    print("\nTesting Alpaca credentials...")
    api_key = os.getenv("APCA_API_KEY_ID")
    api_secret = os.getenv("APCA_API_SECRET_KEY")
    base_url = os.getenv("APCA_API_BASE_URL")

    if api_key and api_secret:
        print(f"✓ Alpaca API Key ID: {api_key[:10]}...")
        print(f"✓ Alpaca API Secret Key: {api_secret[:10]}...")
        print(f"✓ Alpaca Base URL: {base_url}")

        # Test Alpaca connection directly using alpaca-py
        try:
            from alpaca.trading.client import TradingClient

            trading_client = TradingClient(api_key, api_secret, paper=True)
            account = trading_client.get_account()
            print("✓ Alpaca paper trading account connected:")
            print(f"  Account ID: {account.id}")
            print(f"  Status: {account.status}")
            print(f"  Buying Power: ${float(account.buying_power):,.2f}")
            print(f"  Equity: ${float(account.equity):,.2f}")
            print(f"  Cash: ${float(account.cash):,.2f}")
            return True
        except Exception as e:
            print(f"✗ Alpaca connection failed: {e}")
            return False
    else:
        print("✗ Alpaca credentials not found in .env")
        return False


def test_data_gateway_endpoints():
    """Test key Data-Gateway endpoints."""
    print("\nTesting Data-Gateway endpoints...")

    test_client_key = os.getenv("DATA_GATEWAY_TEST_API_KEY")
    if not test_client_key:
        print("✗ DATA_GATEWAY_TEST_API_KEY not set, skipping authenticated endpoint checks")
        return

    endpoints = [
        ("/api/v1/alpaca/stocks/SPY/bars?timeframe=1Day&limit=5", "SPY bars"),
        ("/api/v1/alpaca/account", "Alpaca account"),
        ("/api/v1/providers", "Available providers"),
    ]

    headers = {"X-API-Key": test_client_key, "Content-Type": "application/json"}

    for endpoint, description in endpoints:
        try:
            url = f"http://localhost:8080{endpoint}"
            response = requests.get(url, headers=headers, timeout=5)
            if response.status_code == 200:
                print(f"✓ {description}: OK")
            else:
                print(f"✗ {description}: Status {response.status_code}")
        except Exception as e:
            print(f"✗ {description}: Failed - {e}")


def main():
    print("=" * 60)
    print("Trading Bot Connectivity Test")
    print("=" * 60)

    # Test Data-Gateway
    gateway_ok = test_data_gateway()

    # Test Alpaca
    alpaca_ok = test_alpaca_credentials()

    # Test Data-Gateway endpoints if gateway is up
    if gateway_ok:
        test_data_gateway_endpoints()

    print("\n" + "=" * 60)
    print("Summary:")
    print(f"Data-Gateway: {'✓ OK' if gateway_ok else '✗ FAILED'}")
    print(f"Alpaca Paper Trading: {'✓ OK' if alpaca_ok else '✗ FAILED'}")

    if gateway_ok and alpaca_ok:
        print("\n✅ All connectivity tests passed! Ready to build trading bot.")
        return 0
    else:
        print("\n❌ Some connectivity tests failed. Please check configuration.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
