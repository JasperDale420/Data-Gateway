#!/usr/bin/env python3
"""
Run the trading bot manually.
"""

import os
import sys
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from core.trading_bot import TradingBot


def main():
    print("=" * 70)
    print("TRADING BOT - MANUAL EXECUTION")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Create and run the trading bot
    bot = TradingBot(
        symbol="SPY",
        strategy_type="mean_reversion",
        risk_per_trade=0.02,  # 2% risk per trade
        max_positions=3,
        log_dir="logs",
    )

    # Run one iteration
    bot.run_iteration()

    print("\n" + "=" * 70)
    print("EXECUTION COMPLETE")
    print("=" * 70)
    print(f"Logs saved to: {bot.log_dir}")
    print(f"Performance log: {bot.performance_log}")

    # Show account summary
    if bot.account_info:
        print("\nACCOUNT SUMMARY:")
        print(f"  Equity: ${bot.account_info['equity']:,.2f}")
        print(f"  Cash: ${bot.account_info['cash']:,.2f}")
        print(f"  Positions: {len(bot.positions)}")

    # Show any signals generated
    if hasattr(bot, "signals") and bot.signals:
        print(f"\nSIGNALS GENERATED: {len(bot.signals)}")
    else:
        print("\nNo signals generated this iteration")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
