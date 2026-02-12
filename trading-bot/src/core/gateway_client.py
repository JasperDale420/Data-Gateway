"""
Data-Gateway client for market data access.
"""

import requests
import json
from typing import Dict, Any, Optional, List
import time


class DataGatewayClient:
    """Client for interacting with the Data-Gateway."""
    
    def __init__(self, base_url: str = "http://localhost:8080", api_key: str = None):
        """
        Initialize Data-Gateway client.
        
        Args:
            base_url: Data-Gateway base URL
            api_key: Client API key (from config/clients.yaml)
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key or "gw_test_dev_key_67890"  # Default test key
        self.session = requests.Session()
        self.session.headers.update({
            'X-API-Key': self.api_key,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make authenticated request to Data-Gateway."""
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            if hasattr(e.response, 'text'):
                print(f"Response: {e.response.text}")
            raise
    
    def get_health(self) -> Dict[str, Any]:
        """Get gateway health status."""
        return self._request('GET', '/health')
    
    def get_providers(self) -> List[str]:
        """Get list of available data providers."""
        response = self._request('GET', '/api/v1/providers')
        return response.get('providers', [])
    
    def get_stock_bars(self, symbol: str, timeframe: str = "1Day", limit: int = 100) -> Dict[str, Any]:
        """
        Get stock bars from Alpaca.
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            timeframe: Bar timeframe (1Min, 5Min, 15Min, 1Day, etc.)
            limit: Number of bars to return
        """
        endpoint = f"/api/v1/alpaca/stocks/{symbol}/bars"
        params = {
            'timeframe': timeframe,
            'limit': limit
        }
        return self._request('GET', endpoint, params=params)
    
    def get_account(self) -> Dict[str, Any]:
        """Get Alpaca account information."""
        return self._request('GET', '/api/v1/alpaca/account')
    
    def get_unusual_whales_flow(self, symbol: str, limit: int = 50) -> Dict[str, Any]:
        """
        Get unusual options flow from Unusual Whales.
        
        Args:
            symbol: Stock symbol
            limit: Number of flow events to return
        """
        endpoint = f"/api/v1/uw/flow/{symbol}"
        params = {'limit': limit}
        return self._request('GET', endpoint, params=params)
    
    def get_quotes(self, symbol: str) -> Dict[str, Any]:
        """Get latest quotes for a symbol."""
        endpoint = f"/api/v1/alpaca/stocks/{symbol}/quotes"
        return self._request('GET', endpoint)
    
    def get_trades(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """Get recent trades for a symbol."""
        endpoint = f"/api/v1/alpaca/stocks/{symbol}/trades"
        params = {'limit': limit}
        return self._request('GET', endpoint, params=params)


# Example usage
if __name__ == "__main__":
    # Test the client
    client = DataGatewayClient()
    
    print("Testing Data-Gateway client...")
    
    # Test health
    health = client.get_health()
    print(f"Health: {health}")
    
    # Test providers
    providers = client.get_providers()
    print(f"Available providers: {providers}")
    
    # Test account (if authenticated)
    try:
        account = client.get_account()
        print(f"Account: {account.keys()}")
    except:
        print("Account endpoint requires proper authentication")
    
    # Test stock bars
    try:
        bars = client.get_stock_bars('SPY', '1Day', 5)
        print(f"SPY bars: {len(bars.get('bars', []))} bars retrieved")
    except Exception as e:
        print(f"Failed to get bars: {e}")