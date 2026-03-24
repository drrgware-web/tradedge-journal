#!/usr/bin/env python3
"""
Scanner Fetcher v2.0
Fetches technical data from Yahoo Finance via Cloudflare Worker.
Phase 1 of the daily scanner workflow.

Usage:
    python scanner_fetcher.py                    # Default mode (top500)
    python scanner_fetcher.py --mode full        # All stocks
    python scanner_fetcher.py --mode top500      # Top 500 by volume
    python scanner_fetcher.py --mode priority    # Priority 100 stocks
    python scanner_fetcher.py --mode test        # First 10 stocks
    python scanner_fetcher.py --symbol RELIANCE  # Single stock
"""

import json
import os
import sys
import time
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import traceback

try:
    import requests
except ImportError:
    print("requests not installed - run: pip install requests")
    sys.exit(1)

try:
    import yfinance as yf
except ImportError:
    yf = None
    print("yfinance not installed - using Worker API only")

# ============================================================================
# CONFIGURATION
# ============================================================================

YAHOO_WORKER = "https://spring-fire-41a0.drrgware.workers.dev"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)  # Go up from scripts/ to repo root
DATA_DIR = os.path.join(REPO_ROOT, "data")
STOCK_DETAILS_DIR = os.path.join(DATA_DIR, "stock_details")
NSE_SYMBOLS_PATH = os.path.join(SCRIPT_DIR, "nse_symbols.json")

YAHOO_DELAY = 0.15  # 150ms between requests
MAX_RETRIES = 3

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# ============================================================================
# SYMBOL LOADING
# ============================================================================

def load_symbols() -> List[str]:
    """Load NSE symbols list."""
    symbols = []
    
    if os.path.exists(NSE_SYMBOLS_PATH):
        with open(NSE_SYMBOLS_PATH, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                symbols = [s["symbol"] if isinstance(s, dict) else s for s in data]
            elif isinstance(data, dict):
                symbols = list(data.keys())
    
    print(f"Loaded {len(symbols)} symbols")
    return symbols

def get_priority_symbols(all_symbols: List[str]) -> List[str]:
    """Get priority symbols (NIFTY 100 + high volume)."""
    # NIFTY 100 stocks (subset)
    nifty_100 = [
        "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
        "SBIN", "BHARTIARTL", "ITC", "KOTAKBANK", "LT", "AXISBANK",
        "HCLTECH", "ASIANPAINT", "MARUTI", "SUNPHARMA", "TITAN", "WIPRO",
        "ULTRACEMCO", "BAJFINANCE", "NESTLEIND", "TECHM", "DMART", "NTPC",
        "TATAMOTORS", "POWERGRID", "ONGC", "TATASTEEL", "M&M", "JSWSTEEL",
        "BAJAJFINSV", "ADANIENT", "ADANIPORTS", "COALINDIA", "HINDALCO",
        "GRASIM", "CIPLA", "DRREDDY", "EICHERMOT", "BRITANNIA", "DIVISLAB",
        "APOLLOHOSP", "SBILIFE", "BAJAJ-AUTO", "HDFCLIFE", "INDUSINDBK",
        "HEROMOTOCO", "DABUR", "SHREECEM", "TATACONSUM", "ADANIGREEN"
    ]
    
    # Filter to existing symbols
    priority = [s for s in nifty_100 if s in all_symbols]
    
    # Add remaining from all_symbols up to 100
    remaining = [s for s in all_symbols if s not in priority][:100 - len(priority)]
    
    return priority + remaining

# ============================================================================
# YAHOO FINANCE FETCHER
# ============================================================================

class YahooFetcher:
    """Fetches stock data from Yahoo Finance."""
    
    def __init__(self, use_worker: bool = True):
        self.use_worker = use_worker
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
    def fetch_stock(self, symbol: str) -> Optional[Dict]:
        """Fetch stock data for a single symbol."""
        yahoo_symbol = f"{symbol}.NS"
        
        # Try Worker API first
        if self.use_worker:
            data = self._fetch_via_worker(yahoo_symbol)
            if data:
                return self._process_data(symbol, data)
        
        # Fallback to yfinance
        if yf:
            data = self._fetch_via_yfinance(yahoo_symbol)
            if data:
                return self._process_data(symbol, data)
        
        return None
    
    def _fetch_via_worker(self, yahoo_symbol: str) -> Optional[Dict]:
        """Fetch via Cloudflare Worker."""
        try:
            response = self.session.get(
                f"{YAHOO_WORKER}/quote/{yahoo_symbol}",
                params={"range": "1y", "interval": "1d"},
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            print(f"Worker error for {yahoo_symbol}: {e}")
        
        return None
    
    def _fetch_via_yfinance(self, yahoo_symbol: str) -> Optional[Dict]:
        """Fetch via yfinance library."""
        try:
            ticker = yf.Ticker(yahoo_symbol)
            hist = ticker.history(period="1y")
            
            if hist.empty:
                return None
            
            info = ticker.info
            
            return {
                "history": hist.to_dict(),
                "info": info
            }
        except Exception as e:
            print(f"yfinance error for {yahoo_symbol}: {e}")
        
        return None
    
    def _process_data(self, symbol: str, raw_data: Dict) -> Dict:
        """Process raw Yahoo data into structured format."""
        data = {
            "symbol": symbol,
            "technical": {},
            "updated_at": datetime.now().isoformat()
        }
        
        try:
            # Extract from Worker format
            if "chart" in raw_data:
                result = raw_data["chart"]["result"][0]
                meta = result.get("meta", {})
                indicators = result.get("indicators", {})
                quotes = indicators.get("quote", [{}])[0]
                timestamps = result.get("timestamp", [])
                
                closes = quotes.get("close", [])
                volumes = quotes.get("volume", [])
                highs = quotes.get("high", [])
                lows = quotes.get("low", [])
                
                # Filter out None values
                closes = [c for c in closes if c is not None]
                volumes = [v for v in volumes if v is not None]
                
                if closes:
                    current = closes[-1]
                    prev_close = closes[-2] if len(closes) > 1 else current
                    
                    data["technical"] = {
                        "close": round(current, 2),
                        "prev_close": round(prev_close, 2),
                        "change": round(current - prev_close, 2),
                        "change_pct": round(((current - prev_close) / prev_close) * 100, 2) if prev_close else 0,
                        "high_52w": round(max(closes[-252:]) if len(closes) >= 252 else max(closes), 2),
                        "low_52w": round(min(closes[-252:]) if len(closes) >= 252 else min(closes), 2),
                        "volume": volumes[-1] if volumes else 0,
                        "avg_volume": int(sum(volumes[-20:]) / len(volumes[-20:])) if len(volumes) >= 20 else 0,
                        "volume_ratio": round(volumes[-1] / (sum(volumes[-20:]) / 20), 2) if len(volumes) >= 20 and sum(volumes[-20:]) > 0 else 1,
                    }
                    
                    # Calculate 52w high proximity
                    high_52w = data["technical"]["high_52w"]
                    low_52w = data["technical"]["low_52w"]
                    if high_52w > low_52w:
                        data["technical"]["high_52w_proximity"] = round(
                            ((current - low_52w) / (high_52w - low_52w)) * 100, 1
                        )
                    
                    # Calculate returns
                    data["technical"]["returns"] = self._calculate_returns(closes)
                    
                    # Calculate basic indicators
                    data["technical"]["indicators"] = self._calculate_indicators(closes, volumes)
                    
                    # Price history (last 30 days)
                    data["technical"]["price_history"] = [round(c, 2) for c in closes[-30:]]
                    
            # Extract from yfinance format
            elif "history" in raw_data:
                hist = raw_data["history"]
                info = raw_data.get("info", {})
                
                closes = list(hist.get("Close", {}).values())
                volumes = list(hist.get("Volume", {}).values())
                
                if closes:
                    current = closes[-1]
                    prev_close = closes[-2] if len(closes) > 1 else current
                    
                    data["technical"] = {
                        "close": round(current, 2),
                        "prev_close": round(prev_close, 2),
                        "change": round(current - prev_close, 2),
                        "change_pct": round(((current - prev_close) / prev_close) * 100, 2) if prev_close else 0,
                        "high_52w": round(info.get("fiftyTwoWeekHigh", max(closes)), 2),
                        "low_52w": round(info.get("fiftyTwoWeekLow", min(closes)), 2),
                        "volume": int(volumes[-1]) if volumes else 0,
                        "avg_volume": int(info.get("averageVolume", 0)),
                        "market_cap": info.get("marketCap", 0),
                    }
                    
                    data["technical"]["returns"] = self._calculate_returns(closes)
                    data["technical"]["indicators"] = self._calculate_indicators(closes, volumes)
                    data["technical"]["price_history"] = [round(c, 2) for c in closes[-30:]]
                    
        except Exception as e:
            print(f"Error processing data for {symbol}: {e}")
            traceback.print_exc()
        
        return data
    
    def _calculate_returns(self, closes: List[float]) -> Dict[str, float]:
        """Calculate returns for various periods."""
        if not closes or len(closes) < 2:
            return {}
        
        current = closes[-1]
        returns = {}
        
        periods = [
            ("1d", 1), ("1w", 5), ("1m", 21),
            ("3m", 63), ("6m", 126), ("1y", 252)
        ]
        
        for name, days in periods:
            if len(closes) > days:
                past = closes[-(days + 1)]
                if past > 0:
                    returns[name] = round(((current - past) / past) * 100, 2)
        
        return returns
    
    def _calculate_indicators(self, closes: List[float], volumes: List[float]) -> Dict:
        """Calculate basic technical indicators."""
        if not closes:
            return {}
        
        indicators = {}
        
        try:
            # Simple Moving Averages
            if len(closes) >= 20:
                indicators["sma_20"] = round(sum(closes[-20:]) / 20, 2)
            if len(closes) >= 50:
                indicators["sma_50"] = round(sum(closes[-50:]) / 50, 2)
            if len(closes) >= 200:
                indicators["sma_200"] = round(sum(closes[-200:]) / 200, 2)
            
            # RSI (14-period)
            if len(closes) >= 15:
                indicators["rsi"] = self._calculate_rsi(closes, 14)
            
            # Volatility (30-day)
            if len(closes) >= 30:
                returns = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes[-30:]))]
                if returns:
                    import math
                    mean = sum(returns) / len(returns)
                    variance = sum((r - mean) ** 2 for r in returns) / len(returns)
                    indicators["volatility_30d"] = round(math.sqrt(variance) * math.sqrt(252) * 100, 2)
            
            # Price vs SMAs
            current = closes[-1]
            if "sma_20" in indicators:
                indicators["above_sma_20"] = current > indicators["sma_20"]
            if "sma_50" in indicators:
                indicators["above_sma_50"] = current > indicators["sma_50"]
            if "sma_200" in indicators:
                indicators["above_sma_200"] = current > indicators["sma_200"]
                
        except Exception as e:
            print(f"Error calculating indicators: {e}")
        
        return indicators
    
    def _calculate_rsi(self, closes: List[float], period: int = 14) -> float:
        """Calculate RSI."""
        if len(closes) < period + 1:
            return 50
        
        gains = []
        losses = []
        
        for i in range(1, len(closes)):
            change = closes[i] - closes[i-1]
            if change >= 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return round(rsi, 2)


# ============================================================================
# MAIN FETCHER
# ============================================================================

def fetch_all_stocks(
    symbols: List[str],
    mode: str = "top500"
) -> Dict[str, Any]:
    """Fetch technical data for all stocks."""
    
    # Filter based on mode
    if mode == "test":
        target_symbols = symbols[:10]
    elif mode == "priority":
        target_symbols = get_priority_symbols(symbols)[:100]
    elif mode == "top500":
        target_symbols = symbols[:500]
    else:  # full
        target_symbols = symbols
    
    print(f"Mode: {mode}, Processing: {len(target_symbols)} stocks")
    
    fetcher = YahooFetcher()
    results = {
        "processed": 0,
        "success": 0,
        "failed": 0,
        "errors": []
    }
    
    os.makedirs(STOCK_DETAILS_DIR, exist_ok=True)
    
    for i, symbol in enumerate(target_symbols):
        try:
            print(f"[{i+1}/{len(target_symbols)}] Fetching {symbol}...", end=" ")
            
            data = fetcher.fetch_stock(symbol)
            
            if data and data.get("technical"):
                # Load existing detail if available
                detail_path = os.path.join(STOCK_DETAILS_DIR, f"{symbol}.json")
                
                if os.path.exists(detail_path):
                    with open(detail_path, "r") as f:
                        existing = json.load(f)
                    # Merge technical data
                    existing["technical"] = data["technical"]
                    existing["updated_at"] = data["updated_at"]
                    data = existing
                else:
                    data["name"] = symbol
                    data["fundamentals"] = {}
                    data["oneil"] = {}
                    data["guru_ratings"] = []
                    data["surveillance"] = {}
                    data["ownership"] = {}
                    data["quarterly_results"] = []
                
                # Save
                with open(detail_path, "w") as f:
                    json.dump(data, f, indent=2, default=str)
                
                results["success"] += 1
                print("✓")
            else:
                results["failed"] += 1
                results["errors"].append({"symbol": symbol, "error": "No data"})
                print("✗")
                
            time.sleep(YAHOO_DELAY)
            
        except Exception as e:
            results["failed"] += 1
            results["errors"].append({"symbol": symbol, "error": str(e)})
            print(f"✗ {e}")
        
        results["processed"] += 1
        
        # Progress update
        if (i + 1) % 100 == 0:
            print(f"\nProgress: {i+1}/{len(target_symbols)} ({results['success']} success)")
    
    print(f"\n=== Complete: {results['success']} success, {results['failed']} failed ===")
    return results


def main():
    parser = argparse.ArgumentParser(description="Scanner Fetcher - Yahoo Finance Data")
    parser.add_argument("--mode", type=str, default="top500",
                       choices=["full", "top500", "priority", "test"],
                       help="Run mode")
    parser.add_argument("--symbol", type=str, help="Fetch single symbol")
    
    args = parser.parse_args()
    
    symbols = load_symbols()
    
    if args.symbol:
        # Single symbol mode
        fetcher = YahooFetcher()
        data = fetcher.fetch_stock(args.symbol)
        if data:
            print(json.dumps(data, indent=2, default=str))
        else:
            print(f"Failed to fetch {args.symbol}")
    else:
        # Batch mode
        results = fetch_all_stocks(symbols, args.mode)
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
