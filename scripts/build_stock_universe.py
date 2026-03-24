#!/usr/bin/env python3
"""
build_stock_universe.py - Generate data/stock_universe.json for TradEdge Scanner Builder
 
Fetches all stock data from yfinance and creates a flat JSON array with
fundamentals, technicals, and holdings for each stock. This enables
client-side scanning in scanner-builder.html.

Usage:
  python build_stock_universe.py
  
Output:
  data/stock_universe.json

Requires:
  pip install yfinance pandas numpy requests
"""

import json
import os
import sys
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# CONFIG
# ============================================================

SYMBOLS_FILE = 'data/nse_symbols.json'      # Your 2803-stock universe
OUTPUT_FILE = 'data/stock_universe.json'
BATCH_SIZE = 50                               # Stocks per yfinance batch
SLEEP_BETWEEN_BATCHES = 2                     # Seconds

# Fallback: if nse_symbols.json not found, try loading from build_symbol_list
FALLBACK_SYMBOLS_URL = 'https://archives.nseindia.com/content/equities/EQUITY_L.csv'


def load_symbols():
    """Load stock symbols from nse_symbols.json or fallback."""
    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE) as f:
            data = json.load(f)
        # Handle different formats
        if isinstance(data, list):
            if isinstance(data[0], str):
                return data
            elif isinstance(data[0], dict):
                return [s.get('symbol', s.get('ticker', '')) for s in data if s.get('symbol') or s.get('ticker')]
        elif isinstance(data, dict):
            return list(data.keys())
    
    # Fallback: try scanner_results.json to get symbols
    if os.path.exists('data/scanner_results.json'):
        with open('data/scanner_results.json') as f:
            scan_data = json.load(f)
        symbols = set()
        results = scan_data.get('scan_results', scan_data)
        for key, val in results.items():
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, dict) and 'symbol' in item:
                        symbols.add(item['symbol'])
                    elif isinstance(item, str):
                        symbols.add(item)
            elif isinstance(val, dict) and 'stocks' in val:
                for item in val['stocks']:
                    if isinstance(item, dict) and 'symbol' in item:
                        symbols.add(item['symbol'])
        if symbols:
            logger.info(f"Loaded {len(symbols)} symbols from scanner_results.json")
            return sorted(symbols)
    
    logger.error(f"No symbol file found at {SYMBOLS_FILE}")
    sys.exit(1)


def fetch_stock_data(symbols):
    """Fetch stock data in batches using yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance not installed. Run: pip install yfinance")
        sys.exit(1)
    
    all_stocks = []
    total = len(symbols)
    
    for i in range(0, total, BATCH_SIZE):
        batch = symbols[i:i + BATCH_SIZE]
        batch_tickers = [s + '.NS' for s in batch]
        
        logger.info(f"Fetching batch {i // BATCH_SIZE + 1}/{(total + BATCH_SIZE - 1) // BATCH_SIZE} ({len(batch)} stocks)")
        
        try:
            # Download price history (6 months for EMA calculations)
            tickers_str = ' '.join(batch_tickers)
            data = yf.download(tickers_str, period='6mo', group_by='ticker', progress=False, threads=True)
            
            for j, sym in enumerate(batch):
                ticker = sym + '.NS'
                try:
                    # Get price data
                    if len(batch) == 1:
                        hist = data
                    else:
                        if ticker not in data.columns.get_level_values(0):
                            continue
                        hist = data[ticker]
                    
                    if hist.empty or len(hist) < 5:
                        continue
                    
                    close = hist['Close'].dropna()
                    if close.empty:
                        continue
                    
                    latest = close.iloc[-1]
                    prev = close.iloc[-2] if len(close) > 1 else latest
                    
                    # Calculate technicals
                    stock_data = build_stock_record(sym, hist, close, latest, prev)
                    
                    # Fetch fundamentals from yfinance info
                    try:
                        info = yf.Ticker(ticker).info
                        enrich_fundamentals(stock_data, info)
                    except Exception:
                        pass
                    
                    all_stocks.append(stock_data)
                    
                except Exception as e:
                    logger.debug(f"Error processing {sym}: {e}")
                    continue
            
        except Exception as e:
            logger.warning(f"Batch error: {e}")
        
        if i + BATCH_SIZE < total:
            time.sleep(SLEEP_BETWEEN_BATCHES)
    
    return all_stocks


def build_stock_record(symbol, hist, close, latest, prev):
    """Build a stock record with technical indicators."""
    high = hist['High'].dropna()
    low = hist['Low'].dropna()
    volume = hist['Volume'].dropna()
    
    # Change
    change_pct = ((latest - prev) / prev * 100) if prev > 0 else 0
    
    # EMAs
    ema_20 = close.ewm(span=20).mean().iloc[-1] if len(close) >= 20 else None
    ema_50 = close.ewm(span=50).mean().iloc[-1] if len(close) >= 50 else None
    ema_200 = close.ewm(span=200).mean().iloc[-1] if len(close) >= 200 else None
    ema_9 = close.ewm(span=9).mean().iloc[-1] if len(close) >= 9 else None
    ema_21 = close.ewm(span=21).mean().iloc[-1] if len(close) >= 21 else None
    
    # RSI (14)
    rsi = None
    if len(close) >= 15:
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] > 0 else 100
        rsi = 100 - (100 / (1 + rs))
    
    # Volume ratio
    vol_ratio = None
    if len(volume) >= 21:
        avg_vol = volume.iloc[-21:].mean()
        vol_ratio = volume.iloc[-1] / avg_vol if avg_vol > 0 else 1
    
    # 52-week high/low
    high_52w = high.max() if len(high) >= 1 else latest
    low_52w = low.min() if len(low) >= 1 else latest
    pct_from_high = ((high_52w - latest) / high_52w * 100) if high_52w > 0 else 0
    pct_from_low = ((latest - low_52w) / low_52w * 100) if low_52w > 0 else 0
    
    # 3-month change
    change_3m = None
    if len(close) >= 63:
        old = close.iloc[-63]
        change_3m = ((latest - old) / old * 100) if old > 0 else 0
    
    return {
        'symbol': symbol,
        'name': symbol,
        'price': round(float(latest), 2),
        'open': round(float(hist['Open'].iloc[-1]), 2) if not hist['Open'].empty else None,
        'high': round(float(hist['High'].iloc[-1]), 2) if not hist['High'].empty else None,
        'low': round(float(hist['Low'].iloc[-1]), 2) if not hist['Low'].empty else None,
        'change_pct': round(float(change_pct), 2),
        'change_pct_3m': round(float(change_3m), 2) if change_3m is not None else None,
        'rsi': round(float(rsi), 1) if rsi is not None else None,
        'ema_9': round(float(ema_9), 2) if ema_9 is not None else None,
        'ema_20': round(float(ema_20), 2) if ema_20 is not None else None,
        'ema_21': round(float(ema_21), 2) if ema_21 is not None else None,
        'ema_50': round(float(ema_50), 2) if ema_50 is not None else None,
        'ema_200': round(float(ema_200), 2) if ema_200 is not None else None,
        'volume': {
            'latest': int(volume.iloc[-1]) if not volume.empty else 0,
            'ratio': round(float(vol_ratio), 2) if vol_ratio is not None else None,
        },
        'breakout': {
            'high_52w': round(float(high_52w), 2),
            'low_52w': round(float(low_52w), 2),
            'pct_from_high': round(float(pct_from_high), 2),
            'pct_from_low': round(float(pct_from_low), 2),
        },
        'fundamentals': {},
        'fund_holdings': {},
    }


def enrich_fundamentals(stock, info):
    """Add fundamental data from yfinance info dict."""
    f = stock['fundamentals']
    
    # Market cap in crores
    mcap = info.get('marketCap', 0)
    f['market_cap_cr'] = round(mcap / 1e7, 2) if mcap else None
    
    # Valuation
    f['pe_ratio'] = info.get('trailingPE') or info.get('forwardPE')
    f['pb_ratio'] = info.get('priceToBook')
    f['ps_ratio'] = info.get('priceToSalesTrailing12Months')
    f['peg_ratio'] = info.get('pegRatio')
    
    # Profitability
    f['roe'] = round(info.get('returnOnEquity', 0) * 100, 2) if info.get('returnOnEquity') else None
    f['roce'] = None  # yfinance doesn't directly provide ROCE
    
    # Margins
    f['opm'] = round(info.get('operatingMargins', 0) * 100, 2) if info.get('operatingMargins') else None
    f['npm'] = round(info.get('profitMargins', 0) * 100, 2) if info.get('profitMargins') else None
    
    # Debt
    de = info.get('debtToEquity')
    f['debt_to_equity'] = round(de / 100, 2) if de else None
    
    # EPS
    f['eps'] = info.get('trailingEps')
    f['eps_growth'] = round(info.get('earningsGrowth', 0) * 100, 2) if info.get('earningsGrowth') else None
    
    # Revenue growth
    f['revenue_growth'] = round(info.get('revenueGrowth', 0) * 100, 2) if info.get('revenueGrowth') else None
    
    # Dividend
    f['dividend_yield'] = round(info.get('dividendYield', 0) * 100, 2) if info.get('dividendYield') else None
    
    # FCF
    f['fcf_per_share'] = info.get('freeCashflow')  # Will be total, not per share
    
    # Shares
    shares = info.get('sharesOutstanding', 0)
    f['shares_outstanding_cr'] = round(shares / 1e7, 2) if shares else None
    
    # Name & sector
    stock['name'] = info.get('shortName') or info.get('longName') or stock['symbol']
    stock['sector'] = info.get('sector', '')
    stock['industry'] = info.get('industry', '')
    
    # Holdings (if available)
    h = stock['fund_holdings']
    h['promoter_pct'] = info.get('heldPercentInsiders')
    h['institutional_pct'] = info.get('heldPercentInstitutions')
    
    # Round all numeric values
    for key in list(f.keys()):
        if isinstance(f[key], float):
            f[key] = round(f[key], 2)


def main():
    logger.info("TradEdge Stock Universe Builder")
    logger.info("=" * 50)
    
    # Load symbols
    symbols = load_symbols()
    logger.info(f"Loaded {len(symbols)} symbols")
    
    # Fetch data
    stocks = fetch_stock_data(symbols)
    logger.info(f"Successfully fetched data for {len(stocks)} stocks")
    
    # Write output
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    output = {
        'stocks': stocks,
        'count': len(stocks),
        'generated_at': datetime.now().isoformat(),
        'source': 'yfinance',
    }
    
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2, default=str)
    
    file_size = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    logger.info(f"Written {OUTPUT_FILE} ({file_size:.1f} MB, {len(stocks)} stocks)")
    logger.info("Done! Scanner Builder can now run client-side queries.")


if __name__ == '__main__':
    main()
