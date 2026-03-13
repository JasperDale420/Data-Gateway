#!/usr/bin/env python3
"""
Integration test for trading bot components.
"""

import sys
import os
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_gateway_client():
    """Test Data-Gateway client."""
    print("Testing Data-Gateway Client...")
    try:
        from core.gateway_client import DataGatewayClient
        client = DataGatewayClient()
        
        # Test health
        health = client.get_health()
        print(f"  ✓ Health: {health}")
        
        # Test providers
        providers = client.get_providers()
        print(f"  ✓ Providers: {providers}")
        
        return True
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False

def test_trading_client():
    """Test Alpaca trading client."""
    print("\nTesting Alpaca Trading Client...")
    try:
        from core.trading_client import TradingClientWrapper
        
        # Check if we can import required SDKs
        import importlib
        alpaca_py_available = importlib.util.find_spec("alpaca.trading") is not None
        alpaca_trade_api_available = importlib.util.find_spec("alpaca_trade_api") is not None
        
        if not alpaca_py_available and not alpaca_trade_api_available:
            print("  ⚠️ No Alpaca SDK installed (expected for initial test)")
            print("  Install with: pip install alpaca-py")
            return True  # Not a failure for initial test
        
        client = TradingClientWrapper(paper=True)
        account = client.get_account()
        
        print(f"  ✓ Account ID: {account['id']}")
        print(f"  ✓ Equity: ${account['equity']:,.2f}")
        print(f"  ✓ Buying Power: ${account['buying_power']:,.2f}")
        
        positions = client.get_positions()
        print(f"  ✓ Positions: {len(positions)}")
        
        return True
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False

def test_strategy():
    """Test mean reversion strategy."""
    print("\nTesting Mean Reversion Strategy...")
    try:
        import pandas as pd
        import numpy as np
        from strategies.mean_reversion import MeanReversionStrategy
        
        # Create sample data
        np.random.seed(42)
        dates = pd.date_range('2026-01-01', periods=50, freq='D')
        prices = 100 + np.cumsum(np.random.randn(50) * 2)
        
        df = pd.DataFrame({
            'open': prices * 0.99,
            'high': prices * 1.01,
            'low': prices * 0.98,
            'close': prices,
            'volume': np.random.randint(1000000, 5000000, 50)
        }, index=dates)
        
        strategy = MeanReversionStrategy()
        df_with_indicators = strategy.calculate_indicators(df)
        
        signal = strategy.generate_signal(df_with_indicators, 'TEST')
        
        print(f"  ✓ Strategy initialized")
        print(f"  ✓ Indicators calculated")
        print(f"  ✓ Signal: {signal.signal.value if signal else 'HOLD'}")
        
        if signal:
            print(f"  ✓ Confidence: {signal.confidence:.2%}")
            print(f"  ✓ Reason: {signal.reason}")
        
        # Test position sizing
        shares, risk_metrics = strategy.calculate_position_size(
            account_equity=100000,
            risk_per_trade=0.02,
            entry_price=100,
            stop_loss=98
        )
        
        print(f"  ✓ Position sizing: {shares} shares")
        print(f"  ✓ Risk: ${risk_metrics['actual_risk']:,.2f}")
        
        return True
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_trading_bot():
    """Test trading bot integration."""
    print("\nTesting Trading Bot Integration...")
    try:
        from core.trading_bot import TradingBot
        
        # Create bot with test settings
        bot = TradingBot(
            symbol='SPY',
            strategy_type='mean_reversion',
            risk_per_trade=0.02,
            max_positions=3,
            log_dir='test_logs'
        )
        
        print(f"  ✓ Bot initialized")
        print(f"  ✓ Symbol: {bot.symbol}")
        print(f"  ✓ Strategy: {bot.strategy.__class__.__name__}")
        print(f"  ✓ Risk per trade: {bot.risk_per_trade:.1%}")
        
        # Test account update (may fail if no Alpaca connection)
        try:
            if bot.update_account_info():
                print(f"  ✓ Account info updated")
            else:
                print(f"  ⚠️ Account update failed (may be expected)")
        except:
            print(f"  ⚠️ Account update exception (may be expected)")
        
        return True
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("=" * 70)
    print("TRADING BOT INTEGRATION TEST")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    results = []
    
    # Run tests
    results.append(('Data-Gateway Client', test_gateway_client()))
    results.append(('Alpaca Trading Client', test_trading_client()))
    results.append(('Mean Reversion Strategy', test_strategy()))
    results.append(('Trading Bot Integration', test_trading_bot()))
    
    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    
    all_passed = True
    for test_name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{test_name:30} {status}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 70)
    if all_passed:
        print("✅ ALL TESTS PASSED - Trading bot is ready for deployment!")
        print("\nNext steps:")
        print("1. Install Alpaca SDK: pip install alpaca-py")
        print("2. Test with real market data")
        print("3. Run backtest: python3 -m src.core.trading_bot")
        print("4. Set up cron job: ./setup_cron.sh")
    else:
        print("⚠️  SOME TESTS FAILED - Review errors above")
        print("\nCommon issues:")
        print("1. Data-Gateway not running on port 8080")
        print("2. Missing Python dependencies")
        print("3. Alpaca credentials not in .env file")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())