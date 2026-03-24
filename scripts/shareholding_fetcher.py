#!/usr/bin/env python3
"""
shareholding_fetcher.py - Fetch shareholding QoQ data from Screener.in

This script fetches shareholding pattern data including quarterly changes
for all stocks in the scanner universe and updates stock_details JSON files.

Usage:
    python shareholding_fetcher.py [--symbols SYMBOL1,SYMBOL2] [--all] [--limit N]

Features:
- Scrapes Screener.in shareholding table
- Extracts promoter, FII, DII, public holdings with QoQ changes
- Updates stock_details/{SYMBOL}.json files
- Rate limiting to avoid blocking
- Caching to reduce API calls

Author: TradEdge Scanner
"""

import json
import os
import re
import time
import argparse
import random
from datetime import datetime
from pathlib import Path

# Try to import requests, install if not available
try:
    import requests
except ImportError:
    print("Installing requests...")
    os.system("pip install requests --quiet")
    import requests

# Try to import BeautifulSoup
try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Installing beautifulsoup4...")
    os.system("pip install beautifulsoup4 --quiet")
    from bs4 import BeautifulSoup


# Configuration
STOCK_DETAILS_DIR = "data/stock_details"
SCANNER_RESULTS = "data/scanner_results.json"
CACHE_FILE = "data/shareholding_cache.json"
SCREENER_BASE = "https://www.screener.in/company"

# Rate limiting
MIN_DELAY = 1.5  # seconds between requests
MAX_DELAY = 3.0

# Headers to mimic browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def load_cache():
    """Load cached shareholding data."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {}


def save_cache(cache):
    """Save cache to file."""
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)


def fetch_shareholding(symbol, session=None):
    """
    Fetch shareholding data from Screener.in for a given symbol.
    
    Returns:
        dict with shareholding data or None if failed
    """
    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)
    
    url = f"{SCREENER_BASE}/{symbol}/"
    
    try:
        response = session.get(url, timeout=15)
        if response.status_code != 200:
            print(f"  ⚠ HTTP {response.status_code} for {symbol}")
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find shareholding section
        shp_section = None
        for section in soup.find_all('section', id='shareholding'):
            shp_section = section
            break
        
        if not shp_section:
            # Try alternative: look for shareholding table
            tables = soup.find_all('table')
            for table in tables:
                if 'Promoters' in table.get_text() and 'FII' in table.get_text():
                    shp_section = table.parent
                    break
        
        if not shp_section:
            print(f"  ⚠ No shareholding section found for {symbol}")
            return None
        
        # Parse shareholding data
        result = {
            'symbol': symbol,
            'updated': datetime.now().isoformat(),
            'promoter_pct': None,
            'promoter_chg': None,
            'fii_pct': None,
            'fii_chg': None,
            'dii_pct': None,
            'dii_chg': None,
            'public_pct': None,
            'public_chg': None,
            'govt_pct': None,
            'govt_chg': None,
        }
        
        # Find the shareholding table
        table = shp_section.find('table')
        if not table:
            tables = soup.find_all('table', class_='data-table')
            for t in tables:
                text = t.get_text().lower()
                if 'promoter' in text and ('fii' in text or 'institutional' in text):
                    table = t
                    break
        
        if table:
            rows = table.find_all('tr')
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    label = cells[0].get_text().strip().lower()
                    
                    # Get the latest value (usually last or second-to-last column)
                    values = [c.get_text().strip() for c in cells[1:]]
                    
                    # Find the latest non-empty value
                    latest_val = None
                    prev_val = None
                    for i, v in enumerate(reversed(values)):
                        v_clean = re.sub(r'[^\d.\-]', '', v)
                        if v_clean and v_clean != '-':
                            try:
                                if latest_val is None:
                                    latest_val = float(v_clean)
                                elif prev_val is None:
                                    prev_val = float(v_clean)
                                    break
                            except ValueError:
                                continue
                    
                    # Calculate QoQ change
                    qoq_chg = None
                    if latest_val is not None and prev_val is not None:
                        qoq_chg = round(latest_val - prev_val, 2)
                    
                    # Map to our fields
                    if 'promoter' in label and 'pledg' not in label:
                        result['promoter_pct'] = latest_val
                        result['promoter_chg'] = qoq_chg
                    elif 'fii' in label or 'foreign' in label:
                        result['fii_pct'] = latest_val
                        result['fii_chg'] = qoq_chg
                    elif 'dii' in label or 'domestic' in label:
                        result['dii_pct'] = latest_val
                        result['dii_chg'] = qoq_chg
                    elif 'public' in label or 'retail' in label:
                        result['public_pct'] = latest_val
                        result['public_chg'] = qoq_chg
                    elif 'govt' in label or 'government' in label:
                        result['govt_pct'] = latest_val
                        result['govt_chg'] = qoq_chg
        
        # Also try to extract from the summary/ratios section
        ratios = soup.find('ul', id='top-ratios')
        if ratios:
            for li in ratios.find_all('li'):
                text = li.get_text().lower()
                if 'promoter' in text:
                    match = re.search(r'(\d+\.?\d*)\s*%', li.get_text())
                    if match and result['promoter_pct'] is None:
                        result['promoter_pct'] = float(match.group(1))
        
        # Validate we got some data
        if result['promoter_pct'] is None and result['fii_pct'] is None:
            print(f"  ⚠ Could not parse shareholding data for {symbol}")
            return None
        
        return result
        
    except requests.exceptions.Timeout:
        print(f"  ⚠ Timeout for {symbol}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  ⚠ Request error for {symbol}: {e}")
        return None
    except Exception as e:
        print(f"  ⚠ Error parsing {symbol}: {e}")
        return None


def update_stock_json(symbol, shp_data):
    """Update stock_details JSON file with shareholding data."""
    json_path = os.path.join(STOCK_DETAILS_DIR, f"{symbol}.json")
    
    if not os.path.exists(json_path):
        print(f"  ⚠ Stock file not found: {json_path}")
        return False
    
    try:
        with open(json_path, 'r') as f:
            stock_data = json.load(f)
    except Exception as e:
        print(f"  ⚠ Error reading {json_path}: {e}")
        return False
    
    # Initialize fund_holdings if not exists
    if 'fund_holdings' not in stock_data:
        stock_data['fund_holdings'] = {}
    
    fh = stock_data['fund_holdings']
    
    # Update with new data
    if shp_data.get('promoter_pct') is not None:
        fh['promoter_pct'] = shp_data['promoter_pct']
    if shp_data.get('promoter_chg') is not None:
        fh['promoter_chg'] = shp_data['promoter_chg']
    
    if shp_data.get('fii_pct') is not None:
        fh['fii_pct'] = shp_data['fii_pct']
    if shp_data.get('fii_chg') is not None:
        fh['fii_chg'] = shp_data['fii_chg']
    
    if shp_data.get('dii_pct') is not None:
        fh['dii_pct'] = shp_data['dii_pct']
    if shp_data.get('dii_chg') is not None:
        fh['dii_chg'] = shp_data['dii_chg']
    
    if shp_data.get('public_pct') is not None:
        fh['public_pct'] = shp_data['public_pct']
    if shp_data.get('public_chg') is not None:
        fh['public_chg'] = shp_data['public_chg']
    
    # Calculate institutional if not present
    if fh.get('fii_pct') and fh.get('dii_pct'):
        fh['institutional_pct'] = round(fh['fii_pct'] + fh['dii_pct'], 2)
    
    # Add timestamp
    fh['shareholding_updated'] = datetime.now().strftime('%Y-%m-%d')
    
    # Save updated JSON
    try:
        with open(json_path, 'w') as f:
            json.dump(stock_data, f, indent=2)
        return True
    except Exception as e:
        print(f"  ⚠ Error writing {json_path}: {e}")
        return False


def get_symbols_to_update(args):
    """Get list of symbols to update based on arguments."""
    symbols = []
    
    if args.symbols:
        # Specific symbols provided
        symbols = [s.strip().upper() for s in args.symbols.split(',')]
    elif args.all or args.limit:
        # Load from scanner results
        if os.path.exists(SCANNER_RESULTS):
            try:
                with open(SCANNER_RESULTS, 'r') as f:
                    data = json.load(f)
                    all_stocks = data.get('all_stocks', [])
                    symbols = [s['symbol'] for s in all_stocks if 'symbol' in s]
            except Exception as e:
                print(f"Error loading scanner results: {e}")
        
        # Also check stock_details directory
        if os.path.exists(STOCK_DETAILS_DIR):
            for f in os.listdir(STOCK_DETAILS_DIR):
                if f.endswith('.json'):
                    sym = f[:-5]
                    if sym not in symbols:
                        symbols.append(sym)
        
        if args.limit:
            # Prioritize stocks without shareholding data
            without_data = []
            with_data = []
            
            for sym in symbols:
                json_path = os.path.join(STOCK_DETAILS_DIR, f"{sym}.json")
                if os.path.exists(json_path):
                    try:
                        with open(json_path, 'r') as f:
                            data = json.load(f)
                            if data.get('fund_holdings', {}).get('promoter_chg') is None:
                                without_data.append(sym)
                            else:
                                with_data.append(sym)
                    except:
                        without_data.append(sym)
            
            # Prioritize stocks without data
            symbols = without_data[:args.limit]
            if len(symbols) < args.limit:
                symbols.extend(with_data[:args.limit - len(symbols)])
    else:
        print("Please specify --symbols, --all, or --limit")
        return []
    
    return symbols


def main():
    parser = argparse.ArgumentParser(description='Fetch shareholding QoQ data from Screener.in')
    parser.add_argument('--symbols', type=str, help='Comma-separated list of symbols')
    parser.add_argument('--all', action='store_true', help='Update all stocks')
    parser.add_argument('--limit', type=int, help='Limit number of stocks to update')
    parser.add_argument('--no-cache', action='store_true', help='Ignore cache')
    args = parser.parse_args()
    
    print("=" * 60)
    print("📊 TradEdge Shareholding Fetcher")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    symbols = get_symbols_to_update(args)
    if not symbols:
        print("No symbols to update.")
        return
    
    print(f"Symbols to update: {len(symbols)}")
    print()
    
    # Load cache
    cache = {} if args.no_cache else load_cache()
    cache_ttl = 7 * 24 * 60 * 60  # 7 days
    now = time.time()
    
    # Create session for connection pooling
    session = requests.Session()
    session.headers.update(HEADERS)
    
    success = 0
    failed = 0
    cached = 0
    
    for i, symbol in enumerate(symbols, 1):
        print(f"[{i}/{len(symbols)}] {symbol}...", end=" ")
        
        # Check cache
        if symbol in cache:
            cache_time = cache[symbol].get('_cached_at', 0)
            if now - cache_time < cache_ttl:
                # Use cached data
                shp_data = cache[symbol]
                if update_stock_json(symbol, shp_data):
                    print("✓ (cached)")
                    cached += 1
                    continue
        
        # Fetch from Screener
        shp_data = fetch_shareholding(symbol, session)
        
        if shp_data:
            # Update stock JSON
            if update_stock_json(symbol, shp_data):
                print(f"✓ Promoter: {shp_data.get('promoter_pct')}% ({shp_data.get('promoter_chg'):+.2f}%)" if shp_data.get('promoter_chg') else f"✓ Promoter: {shp_data.get('promoter_pct')}%")
                success += 1
                
                # Save to cache
                shp_data['_cached_at'] = now
                cache[symbol] = shp_data
            else:
                print("⚠ Failed to update JSON")
                failed += 1
        else:
            print("✗ Failed")
            failed += 1
        
        # Rate limiting
        if i < len(symbols):
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            time.sleep(delay)
    
    # Save cache
    save_cache(cache)
    
    print()
    print("=" * 60)
    print(f"✓ Success: {success}")
    print(f"📦 Cached: {cached}")
    print(f"✗ Failed: {failed}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
