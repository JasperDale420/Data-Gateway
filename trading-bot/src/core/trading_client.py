"""
Alpaca trading client for order execution.
"""

import os
from typing import Dict, Any, Optional, List
from decimal import Decimal
from dotenv import load_dotenv

# Try to import alpaca-py (newer SDK)
try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import (
        MarketOrderRequest,
        LimitOrderRequest,
        StopOrderRequest,
        StopLimitOrderRequest,
        GetOrdersRequest
    )
    from alpaca.trading.enums import (
        OrderSide,
        TimeInForce,
        OrderClass,
        OrderStatus
    )
    ALPACA_PY_AVAILABLE = True
except ImportError:
    ALPACA_PY_AVAILABLE = False
    print("Warning: alpaca-py not available, using legacy API")

# Fallback to legacy API
if not ALPACA_PY_AVAILABLE:
    try:
        import alpaca_trade_api as tradeapi
        LEGACY_API_AVAILABLE = True
    except ImportError:
        LEGACY_API_AVAILABLE = False
        print("Warning: alpaca_trade_api not available")


class TradingClientWrapper:
    """Wrapper for Alpaca trading API."""
    
    def __init__(self, paper: bool = True):
        """
        Initialize Alpaca trading client.
        
        Args:
            paper: Use paper trading (default: True)
        """
        # Load environment variables from Data-Gateway .env
        load_dotenv('/Users/jacobmcmillan/Empire/Data-Gateway/.env')
        
        self.api_key = os.getenv('APCA_API_KEY_ID')
        self.api_secret = os.getenv('APCA_API_SECRET_KEY')
        self.base_url = os.getenv('APCA_API_BASE_URL')
        self.paper = paper
        
        if not self.api_key or not self.api_secret:
            raise ValueError("Alpaca API credentials not found in .env")
        
        self._init_client()
    
    def _init_client(self):
        """Initialize the appropriate Alpaca client."""
        if ALPACA_PY_AVAILABLE:
            self.client = TradingClient(
                api_key=self.api_key,
                secret_key=self.api_secret,
                paper=self.paper
            )
            self.use_legacy = False
            print("Using alpaca-py (new SDK)")
        elif LEGACY_API_AVAILABLE:
            self.client = tradeapi.REST(
                self.api_key,
                self.api_secret,
                self.base_url,
                api_version='v2'
            )
            self.use_legacy = True
            print("Using legacy alpaca_trade_api")
        else:
            raise ImportError("No Alpaca SDK available. Install alpaca-py or alpaca-trade-api")
    
    def get_account(self) -> Dict[str, Any]:
        """Get account information."""
        if self.use_legacy:
            account = self.client.get_account()
            return {
                'id': account.id,
                'status': account.status,
                'currency': account.currency,
                'buying_power': float(account.buying_power),
                'cash': float(account.cash),
                'portfolio_value': float(account.portfolio_value),
                'equity': float(account.equity),
                'last_equity': float(account.last_equity),
                'initial_margin': float(account.initial_margin),
                'maintenance_margin': float(account.maintenance_margin),
                'daytrade_count': account.daytrade_count,
                'trading_blocked': account.trading_blocked,
                'transfers_blocked': account.transfers_blocked,
                'account_blocked': account.account_blocked,
                'shorting_enabled': account.shorting_enabled
            }
        else:
            account = self.client.get_account()
            return {
                'id': account.id,
                'status': account.status.value,
                'currency': account.currency,
                'buying_power': float(account.buying_power),
                'cash': float(account.cash),
                'portfolio_value': float(account.portfolio_value),
                'equity': float(account.equity),
                'last_equity': float(account.last_equity),
                'initial_margin': float(account.initial_margin),
                'maintenance_margin': float(account.maintenance_margin),
                'daytrade_count': account.daytrade_count,
                'trading_blocked': account.trading_blocked,
                'transfers_blocked': account.transfers_blocked,
                'account_blocked': account.account_blocked,
                'shorting_enabled': account.shorting_enabled
            }
    
    def get_positions(self) -> List[Dict[str, Any]]:
        """Get current positions."""
        if self.use_legacy:
            positions = self.client.list_positions()
            return [
                {
                    'symbol': pos.symbol,
                    'qty': float(pos.qty),
                    'side': pos.side,
                    'market_value': float(pos.market_value),
                    'cost_basis': float(pos.cost_basis),
                    'unrealized_pl': float(pos.unrealized_pl),
                    'unrealized_plpc': float(pos.unrealized_plpc),
                    'current_price': float(pos.current_price),
                    'lastday_price': float(pos.lastday_price),
                    'change_today': float(pos.change_today)
                }
                for pos in positions
            ]
        else:
            positions = self.client.get_all_positions()
            return [
                {
                    'symbol': pos.symbol,
                    'qty': float(pos.qty),
                    'side': pos.side.value,
                    'market_value': float(pos.market_value),
                    'cost_basis': float(pos.cost_basis),
                    'unrealized_pl': float(pos.unrealized_pl),
                    'unrealized_plpc': float(pos.unrealized_plpc),
                    'current_price': float(pos.current_price),
                    'lastday_price': float(pos.lastday_price),
                    'change_today': float(pos.change_today)
                }
                for pos in positions
            ]
    
    def submit_order(
        self,
        symbol: str,
        qty: float,
        side: str,
        order_type: str = 'market',
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None,
        time_in_force: str = 'day'
    ) -> Dict[str, Any]:
        """
        Submit an order.
        
        Args:
            symbol: Stock symbol
            qty: Quantity to trade
            side: 'buy' or 'sell'
            order_type: 'market', 'limit', 'stop', 'stop_limit'
            limit_price: Limit price (for limit/stop_limit orders)
            stop_price: Stop price (for stop/stop_limit orders)
            time_in_force: 'day', 'gtc', 'opg', 'cls', 'ioc', 'fok'
        
        Returns:
            Order information
        """
        side_enum = OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL
        
        if order_type == 'market':
            order_request = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side_enum,
                time_in_force=time_in_force
            )
        elif order_type == 'limit':
            if not limit_price:
                raise ValueError("limit_price required for limit orders")
            order_request = LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side_enum,
                time_in_force=time_in_force,
                limit_price=limit_price
            )
        elif order_type == 'stop':
            if not stop_price:
                raise ValueError("stop_price required for stop orders")
            order_request = StopOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side_enum,
                time_in_force=time_in_force,
                stop_price=stop_price
            )
        elif order_type == 'stop_limit':
            if not limit_price or not stop_price:
                raise ValueError("limit_price and stop_price required for stop_limit orders")
            order_request = StopLimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side_enum,
                time_in_force=time_in_force,
                limit_price=limit_price,
                stop_price=stop_price
            )
        else:
            raise ValueError(f"Unsupported order_type: {order_type}")
        
        # Submit order
        if self.use_legacy:
            # Legacy API has different interface
            if order_type == 'market':
                order = self.client.submit_order(
                    symbol=symbol,
                    qty=qty,
                    side=side,
                    type='market',
                    time_in_force=time_in_force
                )
            elif order_type == 'limit':
                order = self.client.submit_order(
                    symbol=symbol,
                    qty=qty,
                    side=side,
                    type='limit',
                    time_in_force=time_in_force,
                    limit_price=limit_price
                )
            elif order_type == 'stop':
                order = self.client.submit_order(
                    symbol=symbol,
                    qty=qty,
                    side=side,
                    type='stop',
                    time_in_force=time_in_force,
                    stop_price=stop_price
                )
            elif order_type == 'stop_limit':
                order = self.client.submit_order(
                    symbol=symbol,
                    qty=qty,
                    side=side,
                    type='stop_limit',
                    time_in_force=time_in_force,
                    limit_price=limit_price,
                    stop_price=stop_price
                )
        else:
            order = self.client.submit_order(order_request)
        
        return self._format_order(order)
    
    def _format_order(self, order) -> Dict[str, Any]:
        """Format order object to dictionary."""
        if self.use_legacy:
            return {
                'id': order.id,
                'client_order_id': order.client_order_id,
                'symbol': order.symbol,
                'qty': float(order.qty),
                'filled_qty': float(order.filled_qty),
                'side': order.side,
                'type': order.type,
                'status': order.status,
                'time_in_force': order.time_in_force,
                'limit_price': float(order.limit_price) if order.limit_price else None,
                'stop_price': float(order.stop_price) if order.stop_price else None,
                'filled_avg_price': float(order.filled_avg_price) if order.filled_avg_price else None,
                'submitted_at': order.submitted_at,
                'updated_at': order.updated_at
            }
        else:
            return {
                'id': order.id,
                'client_order_id': order.client_order_id,
                'symbol': order.symbol,
                'qty': float(order.qty),
                'filled_qty': float(order.filled_qty),
                'side': order.side.value,
                'type': order.order_type.value,
                'status': order.status.value,
                'time_in_force': order.time_in_force.value,
                'limit_price': float(order.limit_price) if order.limit_price else None,
                'stop_price': float(order.stop_price) if order.stop_price else None,
                'filled_avg_price': float(order.filled_avg_price) if order.filled_avg_price else None,
                'submitted_at': order.submitted_at.isoformat() if order.submitted_at else None,
                'updated_at': order.updated_at.isoformat() if order.updated_at else None
            }
    
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        if self.use_legacy:
            self.client.cancel_order(order_id)
        else:
            self.client.cancel_order_by_id(order_id)
        return True
    
    def get_orders(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get orders with optional status filter."""
        if self.use_legacy:
            orders = self.client.list_orders(status=status)
            return [self._format_order(order) for order in orders]
        else:
            request = GetOrdersRequest(status=status) if status else None
            orders = self.client.get_orders(request)
            return [self._format_order(order) for order in orders]


# Example usage
if __name__ == "__main__":
    # Test the trading client
    try:
        trading_client = TradingClientWrapper(paper=True)
        
        print("Testing Alpaca trading client...")
        
        # Get account info
        account = trading_client.get_account()
        print(f"Account ID: {account['id']}")
        print(f"Status: {account['status']}")
        print(f"Buying Power: ${account['buying_power']:,.2f}")
        print(f"Cash: ${account['cash']:,.2f}")
        print(f"Equity: ${account['equity']:,.2f}")
        
        # Get positions
        positions = trading_client.get_positions()
        print(f"\nPositions: {len(positions)}")
        for pos in positions[:3]:  # Show first 3 positions
            print(f"  {pos['symbol']}: {pos['qty']} shares, ${pos['market_value']:,.2f}")
        
        # Get orders
        orders = trading_client.get_orders()
        print(f"\nOpen orders: {len(orders)}")
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nNote: Make sure Alpaca SDK is installed:")
        print("  pip install alpaca-py  # or alpaca-trade-api")