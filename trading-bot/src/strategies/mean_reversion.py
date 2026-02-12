"""
Mean reversion trading strategy.
Based on Bollinger Bands and RSI indicators.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class Signal(Enum):
    """Trading signals."""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass
class TradeSignal:
    """Container for trade signals."""
    signal: Signal
    symbol: str
    price: float
    confidence: float
    reason: str
    metadata: Dict[str, Any]


class MeanReversionStrategy:
    """
    Mean reversion strategy using Bollinger Bands and RSI.
    
    Strategy logic:
    - Buy when price crosses below lower Bollinger Band and RSI < 30 (oversold)
    - Sell when price crosses above upper Bollinger Band and RSI > 70 (overbought)
    - Use ATR for stop loss and take profit levels
    """
    
    def __init__(
        self,
        bb_period: int = 20,
        bb_std: float = 2.0,
        rsi_period: int = 14,
        atr_period: int = 14,
        oversold_rsi: float = 30.0,
        overbought_rsi: float = 70.0,
        min_confidence: float = 0.6
    ):
        """
        Initialize mean reversion strategy.
        
        Args:
            bb_period: Bollinger Bands period
            bb_std: Bollinger Bands standard deviations
            rsi_period: RSI period
            atr_period: ATR period for volatility measurement
            oversold_rsi: RSI level considered oversold
            overbought_rsi: RSI level considered overbought
            min_confidence: Minimum confidence threshold for signals
        """
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.rsi_period = rsi_period
        self.atr_period = atr_period
        self.oversold_rsi = oversold_rsi
        self.overbought_rsi = overbought_rsi
        self.min_confidence = min_confidence
        
        # State tracking
        self.position = None  # 'long', 'short', or None
        self.entry_price = None
        self.stop_loss = None
        self.take_profit = None
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate technical indicators for the strategy.
        
        Args:
            df: DataFrame with columns ['open', 'high', 'low', 'close', 'volume']
            
        Returns:
            DataFrame with added indicator columns
        """
        df = df.copy()
        
        # Bollinger Bands
        df['bb_middle'] = df['close'].rolling(window=self.bb_period).mean()
        df['bb_std'] = df['close'].rolling(window=self.bb_period).std()
        df['bb_upper'] = df['bb_middle'] + (df['bb_std'] * self.bb_std)
        df['bb_lower'] = df['bb_middle'] - (df['bb_std'] * self.bb_std)
        df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_middle']
        df['bb_position'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'])
        
        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.rsi_period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_period).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # ATR (Average True Range)
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df['atr'] = true_range.rolling(window=self.atr_period).mean()
        
        # Price position relative to bands
        df['below_lower_band'] = df['close'] < df['bb_lower']
        df['above_upper_band'] = df['close'] > df['bb_upper']
        df['within_bands'] = ~(df['below_lower_band'] | df['above_upper_band'])
        
        # RSI conditions
        df['rsi_oversold'] = df['rsi'] < self.oversold_rsi
        df['rsi_overbought'] = df['rsi'] > self.overbought_rsi
        df['rsi_neutral'] = ~(df['rsi_oversold'] | df['rsi_overbought'])
        
        return df
    
    def generate_signal(self, df: pd.DataFrame, symbol: str) -> Optional[TradeSignal]:
        """
        Generate trading signal based on current market data.
        
        Args:
            df: DataFrame with calculated indicators (from calculate_indicators)
            symbol: Trading symbol
            
        Returns:
            TradeSignal if conditions met, None otherwise
        """
        if len(df) < self.bb_period:
            return None
        
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        
        # Initialize signal components
        signal = Signal.HOLD
        confidence = 0.0
        reason_parts = []
        
        # Buy signal conditions (oversold bounce)
        buy_conditions = []
        buy_weights = []
        
        # Condition 1: Price below lower Bollinger Band
        if latest['below_lower_band']:
            buy_conditions.append("price below lower Bollinger Band")
            buy_weights.append(0.3)
        
        # Condition 2: RSI oversold
        if latest['rsi_oversold']:
            buy_conditions.append("RSI oversold")
            buy_weights.append(0.3)
        
        # Condition 3: RSI crossing up from oversold
        if prev['rsi_oversold'] and not latest['rsi_oversold']:
            buy_conditions.append("RSI crossing up from oversold")
            buy_weights.append(0.2)
        
        # Condition 4: Price crossing above lower band
        if prev['below_lower_band'] and not latest['below_lower_band']:
            buy_conditions.append("price crossing above lower band")
            buy_weights.append(0.2)
        
        # Sell signal conditions (overbought rejection)
        sell_conditions = []
        sell_weights = []
        
        # Condition 1: Price above upper Bollinger Band
        if latest['above_upper_band']:
            sell_conditions.append("price above upper Bollinger Band")
            sell_weights.append(0.3)
        
        # Condition 2: RSI overbought
        if latest['rsi_overbought']:
            sell_conditions.append("RSI overbought")
            sell_weights.append(0.3)
        
        # Condition 3: RSI crossing down from overbought
        if prev['rsi_overbought'] and not latest['rsi_overbought']:
            sell_conditions.append("RSI crossing down from overbought")
            sell_weights.append(0.2)
        
        # Condition 4: Price crossing below upper band
        if prev['above_upper_band'] and not latest['above_upper_band']:
            sell_conditions.append("price crossing below upper band")
            sell_weights.append(0.2)
        
        # Calculate confidence scores
        buy_confidence = sum(buy_weights) if buy_weights else 0
        sell_confidence = sum(sell_weights) if sell_weights else 0
        
        # Generate signal
        if buy_confidence >= self.min_confidence and buy_confidence > sell_confidence:
            signal = Signal.BUY
            confidence = buy_confidence
            reason_parts = buy_conditions
        elif sell_confidence >= self.min_confidence and sell_confidence > buy_confidence:
            signal = Signal.SELL
            confidence = sell_confidence
            reason_parts = sell_conditions
        
        if signal != Signal.HOLD:
            # Calculate stop loss and take profit based on ATR
            atr = latest['atr']
            current_price = latest['close']
            
            if signal == Signal.BUY:
                stop_loss = current_price - (atr * 1.5)
                take_profit = current_price + (atr * 2.5)
            else:  # SELL (for short selling)
                stop_loss = current_price + (atr * 1.5)
                take_profit = current_price - (atr * 2.5)
            
            metadata = {
                'current_price': current_price,
                'bb_upper': latest['bb_upper'],
                'bb_middle': latest['bb_middle'],
                'bb_lower': latest['bb_lower'],
                'rsi': latest['rsi'],
                'atr': atr,
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'risk_reward_ratio': 2.5 / 1.5  # 1.67:1
            }
            
            return TradeSignal(
                signal=signal,
                symbol=symbol,
                price=current_price,
                confidence=confidence,
                reason=", ".join(reason_parts),
                metadata=metadata
            )
        
        return None
    
    def calculate_position_size(
        self,
        account_equity: float,
        risk_per_trade: float,
        entry_price: float,
        stop_loss: float
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate position size based on risk management.
        
        Args:
            account_equity: Total account equity
            risk_per_trade: Maximum risk per trade (e.g., 0.02 for 2%)
            entry_price: Entry price
            stop_loss: Stop loss price
            
        Returns:
            Tuple of (shares_to_trade, risk_metrics)
        """
        # Calculate risk per share
        if entry_price > stop_loss:  # Long position
            risk_per_share = entry_price - stop_loss
        else:  # Short position
            risk_per_share = stop_loss - entry_price
        
        # Calculate maximum risk amount
        max_risk_amount = account_equity * risk_per_trade
        
        # Calculate shares to trade
        shares = max_risk_amount / risk_per_share if risk_per_share > 0 else 0
        
        # Round down to whole shares
        shares = int(shares)
        
        # Calculate actual risk
        actual_risk = shares * risk_per_share
        actual_risk_percent = (actual_risk / account_equity) * 100
        
        risk_metrics = {
            'max_risk_amount': max_risk_amount,
            'risk_per_share': risk_per_share,
            'shares': shares,
            'actual_risk': actual_risk,
            'actual_risk_percent': actual_risk_percent,
            'position_value': shares * entry_price
        }
        
        return shares, risk_metrics


# Example usage
if __name__ == "__main__":
    # Create sample data for testing
    np.random.seed(42)
    dates = pd.date_range('2026-01-01', periods=100, freq='D')
    prices = 100 + np.cumsum(np.random.randn(100) * 2)
    
    df = pd.DataFrame({
        'open': prices * 0.99,
        'high': prices * 1.01,
        'low': prices * 0.98,
        'close': prices,
        'volume': np.random.randint(1000000, 5000000, 100)
    }, index=dates)
    
    # Test the strategy
    strategy = MeanReversionStrategy()
    df_with_indicators = strategy.calculate_indicators(df)
    
    print("Testing Mean Reversion Strategy")
    print("=" * 50)
    
    # Show latest indicators
    latest = df_with_indicators.iloc[-1]
    print(f"Latest close: ${latest['close']:.2f}")
    print(f"Bollinger Bands: ${latest['bb_lower']:.2f} - ${latest['bb_upper']:.2f}")
    print(f"RSI: {latest['rsi']:.1f}")
    print(f"ATR: ${latest['atr']:.2f}")
    
    # Generate signal
    signal = strategy.generate_signal(df_with_indicators, 'SPY')
    
    if signal:
        print(f"\nSignal: {signal.signal.value}")
        print(f"Confidence: {signal.confidence:.2%}")
        print(f"Reason: {signal.reason}")
        print(f"Price: ${signal.price:.2f}")
        print(f"Stop Loss: ${signal.metadata['stop_loss']:.2f}")
        print(f"Take Profit: ${signal.metadata['take_profit']:.2f}")
        print(f"Risk/Reward: {signal.metadata['risk_reward_ratio']:.2f}:1")
        
        # Test position sizing
        account_equity = 100000  # $100k account
        shares, risk_metrics = strategy.calculate_position_size(
            account_equity=account_equity,
            risk_per_trade=0.02,  # 2% risk per trade
            entry_price=signal.price,
            stop_loss=signal.metadata['stop_loss']
        )
        
        print(f"\nPosition Sizing (2% risk):")
        print(f"  Shares to trade: {shares}")
        print(f"  Position value: ${risk_metrics['position_value']:,.2f}")
        print(f"  Actual risk: ${risk_metrics['actual_risk']:,.2f} ({risk_metrics['actual_risk_percent']:.2f}%)")
    else:
        print("\nNo signal generated (HOLD)")