"""
Main trading bot engine.
"""

import json
import logging
import os

# Fix relative imports
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.gateway_client import DataGatewayClient
from core.trading_client import TradingClientWrapper
from strategies.mean_reversion import MeanReversionStrategy, Signal, TradeSignal


class TradingBot:
    """
    Main trading bot that connects Data-Gateway, trading execution, and strategies.
    """

    def __init__(
        self,
        symbol: str = "SPY",
        strategy_type: str = "mean_reversion",
        risk_per_trade: float = 0.02,  # 2% risk per trade
        max_positions: int = 5,
        log_dir: str = "logs",
    ):
        """
        Initialize trading bot.

        Args:
            symbol: Primary trading symbol
            strategy_type: Trading strategy to use
            risk_per_trade: Maximum risk per trade (0.02 = 2%)
            max_positions: Maximum number of concurrent positions
            log_dir: Directory for logs
        """
        self.symbol = symbol
        self.risk_per_trade = risk_per_trade
        self.max_positions = max_positions
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)

        # Setup logging
        self.setup_logging()

        # Initialize clients
        self.logger.info("Initializing clients...")
        self.data_client = DataGatewayClient()
        self.trading_client = TradingClientWrapper(paper=True)

        # Initialize strategy
        self.strategy = self._init_strategy(strategy_type)

        # State tracking
        self.account_info = None
        self.positions = []
        self.orders = []
        self.signals = []

        # Performance tracking
        self.performance_log = self.log_dir / "performance.csv"
        self._init_performance_log()

    def setup_logging(self):
        """Setup logging configuration."""
        log_file = self.log_dir / f"trading_bot_{datetime.now().strftime('%Y%m%d')}.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
        )
        self.logger = logging.getLogger("TradingBot")

    def _init_strategy(self, strategy_type: str):
        """Initialize trading strategy."""
        if strategy_type == "mean_reversion":
            return MeanReversionStrategy()
        else:
            raise ValueError(f"Unknown strategy type: {strategy_type}")

    def _init_performance_log(self):
        """Initialize performance log file."""
        if not self.performance_log.exists():
            pd.DataFrame(
                columns=[
                    "timestamp",
                    "equity",
                    "cash",
                    "buying_power",
                    "positions_count",
                    "total_position_value",
                    "daily_pnl",
                    "total_pnl",
                ]
            ).to_csv(self.performance_log, index=False)

    def update_account_info(self):
        """Update account information from broker."""
        try:
            self.account_info = self.trading_client.get_account()
            self.positions = self.trading_client.get_positions()
            self.orders = self.trading_client.get_orders(status="open")

            self.logger.info(f"Account equity: ${self.account_info['equity']:,.2f}")
            self.logger.info(f"Cash: ${self.account_info['cash']:,.2f}")
            self.logger.info(f"Positions: {len(self.positions)}")
            self.logger.info(f"Open orders: {len(self.orders)}")

            return True
        except Exception as e:
            self.logger.error(f"Failed to update account info: {e}")
            return False

    def get_market_data(self, lookback_days: int = 30) -> pd.DataFrame | None:
        """
        Get market data for analysis.

        Args:
            lookback_days: Number of days of historical data to fetch

        Returns:
            DataFrame with market data or None if failed
        """
        try:
            # Calculate bar limit (assuming 1Day bars)
            limit = lookback_days + 20  # Extra for indicator calculation

            # Fetch data from Data-Gateway
            bars_data = self.data_client.get_stock_bars(symbol=self.symbol, timeframe="1Day", limit=limit)

            if "bars" not in bars_data or not bars_data["bars"]:
                self.logger.warning(f"No bars data returned for {self.symbol}")
                return None

            # Convert to DataFrame
            bars = bars_data["bars"]
            df = pd.DataFrame(bars)

            # Ensure required columns
            required_cols = ["open", "high", "low", "close", "volume"]
            if not all(col in df.columns for col in required_cols):
                self.logger.error("Missing required columns in bars data")
                return None

            # Convert timestamp to datetime
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df.set_index("timestamp", inplace=True)

            self.logger.info(f"Retrieved {len(df)} bars for {self.symbol}")
            return df

        except Exception as e:
            self.logger.error(f"Failed to get market data: {e}")
            return None

    def analyze_market(self, df: pd.DataFrame) -> TradeSignal | None:
        """
        Analyze market data and generate trading signal.

        Args:
            df: Market data DataFrame

        Returns:
            TradeSignal if conditions met, None otherwise
        """
        try:
            # Calculate indicators
            df_with_indicators = self.strategy.calculate_indicators(df)

            # Generate signal
            signal = self.strategy.generate_signal(df_with_indicators, self.symbol)

            if signal:
                self.logger.info(f"Signal generated: {signal.signal.value} (confidence: {signal.confidence:.2%})")
                self.logger.info(f"Reason: {signal.reason}")

            return signal

        except Exception as e:
            self.logger.error(f"Failed to analyze market: {e}")
            return None

    def execute_trade(self, signal: TradeSignal) -> bool:
        """
        Execute trade based on signal.

        Args:
            signal: Trade signal to execute

        Returns:
            True if trade executed successfully, False otherwise
        """
        try:
            # Check if we already have a position
            current_position = next((p for p in self.positions if p["symbol"] == signal.symbol), None)

            if current_position:
                self.logger.info(f"Already have position in {signal.symbol}: {current_position['qty']} shares")

                # Check if we should exit based on signal
                if (signal.signal == Signal.SELL and current_position["side"] == "long") or (
                    signal.signal == Signal.BUY and current_position["side"] == "short"
                ):
                    # Exit position (opposite signal)
                    exit_qty = abs(current_position["qty"])
                    exit_side = "sell" if current_position["side"] == "long" else "buy"

                    self.logger.info(f"Exiting position: {exit_side.upper()} {exit_qty} {signal.symbol}")

                    order = self.trading_client.submit_order(
                        symbol=signal.symbol, qty=exit_qty, side=exit_side, order_type="market"
                    )

                    self.logger.info(f"Exit order submitted: {order['id']}")
                    return True
                else:
                    self.logger.info("Signal aligns with current position, holding")
                    return False

            # Check max positions limit
            if len(self.positions) >= self.max_positions:
                self.logger.warning(f"Max positions ({self.max_positions}) reached, skipping trade")
                return False

            # Calculate position size
            shares, risk_metrics = self.strategy.calculate_position_size(
                account_equity=self.account_info["equity"],
                risk_per_trade=self.risk_per_trade,
                entry_price=signal.price,
                stop_loss=signal.metadata["stop_loss"],
            )

            if shares == 0:
                self.logger.warning("Position size calculation resulted in 0 shares")
                return False

            # Submit order
            order_side = "buy" if signal.signal == Signal.BUY else "sell"

            self.logger.info(
                f"Submitting {order_side.upper()} order for {shares} shares of {signal.symbol} at ${signal.price:.2f}"
            )
            self.logger.info(f"Risk: ${risk_metrics['actual_risk']:,.2f} ({risk_metrics['actual_risk_percent']:.2f}%)")
            self.logger.info(f"Stop loss: ${signal.metadata['stop_loss']:.2f}")
            self.logger.info(f"Take profit: ${signal.metadata['take_profit']:.2f}")

            order = self.trading_client.submit_order(
                symbol=signal.symbol, qty=shares, side=order_side, order_type="market"
            )

            self.logger.info(f"Order submitted: {order['id']}")

            # Log trade
            self.log_trade(signal, order, risk_metrics)

            return True

        except Exception as e:
            self.logger.error(f"Failed to execute trade: {e}")
            return False

    def log_trade(self, signal: TradeSignal, order: dict[str, Any], risk_metrics: dict[str, Any]):
        """Log trade details."""
        trade_log = self.log_dir / "trades.jsonl"

        trade_record = {
            "timestamp": datetime.now().isoformat(),
            "symbol": signal.symbol,
            "signal": signal.signal.value,
            "order_id": order["id"],
            "price": signal.price,
            "shares": risk_metrics["shares"],
            "position_value": risk_metrics["position_value"],
            "risk_amount": risk_metrics["actual_risk"],
            "risk_percent": risk_metrics["actual_risk_percent"],
            "stop_loss": signal.metadata["stop_loss"],
            "take_profit": signal.metadata["take_profit"],
            "confidence": signal.confidence,
            "reason": signal.reason,
            "account_equity": self.account_info["equity"],
        }

        with open(trade_log, "a") as f:
            f.write(json.dumps(trade_record) + "\n")

    def log_performance(self):
        """Log current performance metrics."""
        try:
            if not self.account_info:
                return

            performance_data = {
                "timestamp": datetime.now().isoformat(),
                "equity": self.account_info["equity"],
                "cash": self.account_info["cash"],
                "buying_power": self.account_info["buying_power"],
                "positions_count": len(self.positions),
                "total_position_value": sum(p["market_value"] for p in self.positions),
                "daily_pnl": self.account_info.get("last_equity", 0) - self.account_info.get("equity", 0),
                "total_pnl": self.account_info.get("equity", 0) - 100000,  # Assuming $100k starting
            }

            # Append to CSV
            df = pd.DataFrame([performance_data])
            df.to_csv(self.performance_log, mode="a", header=False, index=False)

        except Exception as e:
            self.logger.error(f"Failed to log performance: {e}")

    def run_iteration(self):
        """
        Run one iteration of the trading bot.
        This is the main loop that will be called by the cron job.
        """
        self.logger.info("=" * 60)
        self.logger.info(f"Trading Bot Iteration - {datetime.now()}")
        self.logger.info("=" * 60)

        # Step 1: Update account info
        if not self.update_account_info():
            self.logger.error("Failed to update account info, skipping iteration")
            return

        # Step 2: Get market data
        market_data = self.get_market_data(lookback_days=30)
        if market_data is None:
            self.logger.error("Failed to get market data, skipping iteration")
            return

        # Step 3: Analyze market and generate signal
        signal = self.analyze_market(market_data)

        # Step 4: Execute trade if signal exists
        if signal:
            self.execute_trade(signal)

        # Step 5: Log performance
        self.log_performance()

        # Step 6: Clean up old orders (optional)
        # self.cleanup_old_orders()

        self.logger.info("Iteration complete")
        self.logger.info("=" * 60)

    def run_backtest(self, historical_data: pd.DataFrame, initial_capital: float = 100000):
        """
        Run backtest on historical data.

        Args:
            historical_data: DataFrame with historical price data
            initial_capital: Initial capital for backtest

        Returns:
            Backtest results dictionary
        """
        self.logger.info("Starting backtest...")

        # This is a simplified backtest - in production, use a proper backtesting library
        results = {
            "initial_capital": initial_capital,
            "final_capital": initial_capital,
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "max_drawdown": 0,
            "sharpe_ratio": 0,
            "trades": [],
        }

        # TODO: Implement proper backtesting logic
        # This would involve simulating trades on historical data

        self.logger.info("Backtest complete (simplified)")
        return results


# Main execution
if __name__ == "__main__":
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

    print("\nTrading bot iteration complete.")
    print(f"Logs saved to: {bot.log_dir}")
    print(f"Performance log: {bot.performance_log}")
