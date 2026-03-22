"""
═══════════════════════════════════════════════════════════════════════════════
 TradEdge ATR Calculator Module
 Add to your scanner scripts to calculate ATR for each stock
═══════════════════════════════════════════════════════════════════════════════

Usage:
    from atr_calculator import calculate_atr, add_atr_to_scanner_results
    
    # Single stock
    atr = calculate_atr(df)  # df with 'high', 'low', 'close' columns
    
    # Batch - add ATR to scanner results
    results = add_atr_to_scanner_results(stocks_list)
"""

import yfinance as yf
import pandas as pd
import numpy as np
from typing import Optional, List, Dict
import json
from datetime import datetime, timedelta
import time


def calculate_atr(df: pd.DataFrame, period: int = 14) -> Optional[float]:
    """
    Calculate Average True Range (ATR) from OHLC data.
    
    Args:
        df: DataFrame with 'High', 'Low', 'Close' columns
        period: ATR period (default 14)
    
    Returns:
        ATR value or None if insufficient data
    """
    if df is None or len(df) < period + 1:
        return None
    
    try:
        # Normalize column names
        df = df.copy()
        df.columns = [c.lower() for c in df.columns]
        
        high = df['high']
        low = df['low']
        close = df['close']
        
        # True Range = max(H-L, |H-Prev_C|, |L-Prev_C|)
        prev_close = close.shift(1)
        
        tr1 = high - low
        tr2 = abs(high - prev_close)
        tr3 = abs(low - prev_close)
        
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        # ATR = EMA of True Range (or SMA for simplicity)
        atr = true_range.rolling(window=period).mean().iloc[-1]
        
        return round(atr, 2) if pd.notna(atr) else None
        
    except Exception as e:
        print(f"ATR calculation error: {e}")
        return None


def calculate_atr_percentage(df: pd.DataFrame, period: int = 14) -> Optional[float]:
    """
    Calculate ATR as percentage of current price.
    Useful for comparing volatility across different priced stocks.
    """
    atr = calculate_atr(df, period)
    if atr is None or len(df) == 0:
        return None
    
    try:
        current_price = df['close'].iloc[-1] if 'close' in df.columns else df['Close'].iloc[-1]
        return round((atr / current_price) * 100, 2)
    except:
        return None


def fetch_atr_for_symbol(symbol: str, period: int = 14, days: int = 60) -> Dict:
    """
    Fetch OHLC data from Yahoo Finance and calculate ATR.
    
    Args:
        symbol: NSE stock symbol (e.g., 'RELIANCE')
        period: ATR period (default 14)
        days: Number of days of data to fetch
    
    Returns:
        Dict with atr, atr_pct, price, symbol
    """
    try:
        ticker = f"{symbol}.NS"
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        stock = yf.Ticker(ticker)
        df = stock.history(start=start_date, end=end_date)
        
        if df.empty:
            return {'symbol': symbol, 'atr': None, 'atr_pct': None, 'error': 'No data'}
        
        atr = calculate_atr(df, period)
        atr_pct = calculate_atr_percentage(df, period)
        current_price = round(df['Close'].iloc[-1], 2)
        
        return {
            'symbol': symbol,
            'price': current_price,
            'atr': atr,
            'atr_pct': atr_pct,
            'atr_period': period,
            'volatility': 'HIGH' if atr_pct and atr_pct > 3 else 'MEDIUM' if atr_pct and atr_pct > 1.5 else 'LOW'
        }
        
    except Exception as e:
        return {'symbol': symbol, 'atr': None, 'error': str(e)}


def add_atr_to_scanner_results(stocks: List[Dict], period: int = 14) -> List[Dict]:
    """
    Add ATR data to a list of stock dictionaries from scanner results.
    
    Args:
        stocks: List of stock dicts with 'symbol' key
        period: ATR period
    
    Returns:
        Same list with 'atr', 'atr_pct', 'volatility' added to each stock
    """
    total = len(stocks)
    print(f"Calculating ATR for {total} stocks...")
    
    for i, stock in enumerate(stocks):
        symbol = stock.get('symbol', '')
        if not symbol:
            continue
            
        if (i + 1) % 50 == 0:
            print(f"  Progress: {i+1}/{total}")
        
        try:
            atr_data = fetch_atr_for_symbol(symbol, period)
            stock['atr'] = atr_data.get('atr')
            stock['atr_pct'] = atr_data.get('atr_pct')
            stock['volatility'] = atr_data.get('volatility')
            
            # Rate limiting to avoid Yahoo blocking
            time.sleep(0.1)
            
        except Exception as e:
            stock['atr'] = None
            stock['atr_pct'] = None
            
    print(f"ATR calculation complete for {total} stocks")
    return stocks


def update_scanner_json_with_atr(input_file: str, output_file: str = None, period: int = 14):
    """
    Read scanner_results.json, add ATR to each stock, and save.
    
    Args:
        input_file: Path to scanner_results.json
        output_file: Output path (defaults to same file)
        period: ATR period
    """
    if output_file is None:
        output_file = input_file
    
    print(f"Loading {input_file}...")
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    stocks = data.get('stocks', [])
    stocks = add_atr_to_scanner_results(stocks, period)
    data['stocks'] = stocks
    data['atr_updated'] = datetime.now().isoformat()
    
    print(f"Saving to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print("Done!")


# ═══════════════════════════════════════════════════════════════════════════════
# MULTI-TIMEFRAME ATR (for advanced trailing)
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_multi_tf_atr(symbol: str) -> Dict:
    """
    Calculate ATR across multiple timeframes.
    Useful for position sizing and trailing stop decisions.
    """
    try:
        ticker = f"{symbol}.NS"
        stock = yf.Ticker(ticker)
        
        result = {'symbol': symbol}
        
        # Daily ATR
        daily = stock.history(period='3mo', interval='1d')
        if not daily.empty:
            result['atr_daily'] = calculate_atr(daily, 14)
            result['price'] = round(daily['Close'].iloc[-1], 2)
        
        # Weekly ATR
        weekly = stock.history(period='1y', interval='1wk')
        if not weekly.empty:
            result['atr_weekly'] = calculate_atr(weekly, 14)
        
        # Intraday ATR (for scalping)
        try:
            hourly = stock.history(period='5d', interval='1h')
            if not hourly.empty:
                result['atr_hourly'] = calculate_atr(hourly, 14)
        except:
            result['atr_hourly'] = None
        
        # Suggested trail stops based on ATR
        if result.get('atr_daily'):
            atr = result['atr_daily']
            result['trail_suggestions'] = {
                'tight': round(atr * 0.5, 2),      # 0.5x ATR - aggressive
                'normal': round(atr * 1.0, 2),    # 1x ATR - standard
                'loose': round(atr * 2.0, 2),     # 2x ATR - swing
                'wide': round(atr * 3.0, 2),      # 3x ATR - position
            }
        
        return result
        
    except Exception as e:
        return {'symbol': symbol, 'error': str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# CLI USAGE
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python atr_calculator.py RELIANCE          # Single stock ATR")
        print("  python atr_calculator.py RELIANCE --multi  # Multi-timeframe ATR")
        print("  python atr_calculator.py --update scanner_results.json  # Batch update")
        sys.exit(1)
    
    arg = sys.argv[1]
    
    if arg == '--update' and len(sys.argv) > 2:
        update_scanner_json_with_atr(sys.argv[2])
    elif '--multi' in sys.argv:
        symbol = arg.upper()
        result = calculate_multi_tf_atr(symbol)
        print(json.dumps(result, indent=2))
    else:
        symbol = arg.upper()
        result = fetch_atr_for_symbol(symbol)
        print(json.dumps(result, indent=2))
        
        # Also show trail suggestions
        if result.get('atr'):
            atr = result['atr']
            print(f"\n📊 Trail Stop Suggestions for {symbol}:")
            print(f"   Tight (0.5× ATR): ₹{round(atr * 0.5, 2)}")
            print(f"   Normal (1× ATR):  ₹{round(atr * 1.0, 2)}")
            print(f"   Loose (2× ATR):   ₹{round(atr * 2.0, 2)}")
            print(f"   Wide (3× ATR):    ₹{round(atr * 3.0, 2)}")
