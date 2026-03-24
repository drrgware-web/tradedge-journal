#!/usr/bin/env python3
"""
screener_fetcher.py - Fetch comprehensive data from Screener.in

Fetches:
- Key metrics (P/E, P/B, ROE, ROCE, Debt/Equity, Dividend Yield, etc.)
- Shareholding pattern with QoQ changes
- Quarterly results (EPS, Sales, OPM)
- Piotroski score, Altman Z score
- And more...

Usage:
    python screener_fetcher.py [--symbols SYMBOL1,SYMBOL2] [--all] [--limit N]
    python screener_fetcher.py --batch 1  # Process batch 1 (~500 stocks)
    python screener_fetcher.py --batch 1 --limit 100  # Process 100 from batch 1

Batches:
    With ~2800 stocks, batches are:
    Batch 1: stocks 0-499
    Batch 2: stocks 500-999
    Batch 3: stocks 1000-1499
    Batch 4: stocks 1500-1999
    Batch 5: stocks 2000-2499
    Batch 6: stocks 2500+

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

try:
    import requests
except ImportError:
    os.system("pip install requests --quiet")
    import requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    os.system("pip install beautifulsoup4 --quiet")
    from bs4 import BeautifulSoup


# Configuration
STOCK_DETAILS_DIR = "data/stock_details"
SCANNER_RESULTS = "data/scanner_results.json"
NSE_SYMBOLS_FILE = "data/nse_symbols.json"
CACHE_FILE = "data/screener_cache.json"
SCREENER_BASE = "https://www.screener.in/company"

# Batch configuration
BATCH_SIZE = 500  # Stocks per batch

# Rate limiting
MIN_DELAY = 1.0  # Reduced for faster processing
MAX_DELAY = 2.0

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def load_cache():
    """Load cached data."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {}


def save_cache(cache):
    """Save cache to file."""
    os.makedirs(os.path.dirname(CACHE_FILE) if os.path.dirname(CACHE_FILE) else '.', exist_ok=True)
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)


def parse_number(text):
    """Parse number from text, handling Cr, %, etc."""
    if not text:
        return None
    text = text.strip()
    if text in ['-', '', 'N/A', 'NA']:
        return None
    
    # Remove commas
    text = text.replace(',', '')
    
    # Handle Cr (crores)
    multiplier = 1
    if 'Cr' in text:
        multiplier = 1
        text = text.replace('Cr', '').replace('₹', '').strip()
    elif 'L' in text or 'Lakh' in text:
        multiplier = 0.01  # Convert to Cr
        text = re.sub(r'L(akh)?', '', text).replace('₹', '').strip()
    
    # Remove % and ₹
    text = text.replace('%', '').replace('₹', '').strip()
    
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def fetch_screener_data(symbol, session=None):
    """
    Fetch comprehensive data from Screener.in for a given symbol.
    """
    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)
    
    url = f"{SCREENER_BASE}/{symbol}/"
    
    try:
        response = session.get(url, timeout=20)
        if response.status_code == 404:
            # Try with consolidated
            url = f"{SCREENER_BASE}/{symbol}/consolidated/"
            response = session.get(url, timeout=20)
        
        if response.status_code != 200:
            print(f"  ⚠ HTTP {response.status_code}")
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        result = {
            'symbol': symbol,
            'updated': datetime.now().isoformat(),
            'metrics': {},
            'shareholding': {},
            'quarters': []
        }
        
        # ========== 1. PARSE KEY METRICS (Ratios Table) ==========
        # Find the main ratios list
        ratios_ul = soup.find('ul', id='top-ratios')
        if ratios_ul:
            for li in ratios_ul.find_all('li'):
                spans = li.find_all('span')
                if len(spans) >= 2:
                    label = spans[0].get_text().strip()
                    value_span = spans[1]
                    value_text = value_span.get_text().strip()
                    value = parse_number(value_text)
                    
                    # Map to our field names
                    label_lower = label.lower()
                    
                    if 'market cap' in label_lower:
                        result['metrics']['market_cap_cr'] = value
                    elif 'current price' in label_lower:
                        result['metrics']['current_price'] = value
                    elif 'high' in label_lower and 'low' in label_lower:
                        # Parse "₹ 700 / 409" format
                        match = re.search(r'(\d+[\d,]*\.?\d*)\s*/\s*(\d+[\d,]*\.?\d*)', value_text)
                        if match:
                            result['metrics']['high_52w'] = parse_number(match.group(1))
                            result['metrics']['low_52w'] = parse_number(match.group(2))
                    elif 'stock p/e' in label_lower or label_lower == 'p/e':
                        result['metrics']['pe_ratio'] = value
                    elif 'book value' in label_lower:
                        result['metrics']['book_value'] = value
                    elif 'dividend yield' in label_lower:
                        result['metrics']['dividend_yield'] = value
                    elif 'roce' in label_lower:
                        result['metrics']['roce'] = value
                    elif 'roe' in label_lower:
                        result['metrics']['roe'] = value
                    elif 'face value' in label_lower:
                        result['metrics']['face_value'] = value
        
        # Find the extended ratios table
        ratios_table = None
        for table in soup.find_all('table'):
            table_text = table.get_text().lower()
            if 'debt to equity' in table_text or 'piotroski' in table_text:
                ratios_table = table
                break
        
        # Also check for the warehouse data (more reliable)
        warehouse_div = soup.find('div', {'id': 'company-info'})
        if warehouse_div:
            # Look for data attributes or script tags
            pass
        
        # Parse from the main page content - look for ratio pairs
        content = soup.find('section', id='top')
        if content:
            # Find all name-value pairs
            for li in content.find_all('li'):
                text = li.get_text()
                # Match patterns like "Label: Value" or spans
                name_span = li.find('span', class_='name')
                value_span = li.find('span', class_='value') or li.find('span', class_='number')
                
                if name_span and value_span:
                    label = name_span.get_text().strip().lower()
                    value = parse_number(value_span.get_text())
                    
                    if value is not None:
                        if 'price to earning' in label or 'p/e' in label:
                            result['metrics']['pe_ratio'] = value
                        elif 'price to book' in label or 'p/b' in label:
                            result['metrics']['pb_ratio'] = value
                        elif 'debt to equity' in label:
                            result['metrics']['debt_to_equity'] = value
                        elif 'roce' in label:
                            result['metrics']['roce'] = value
                        elif 'roe' in label:
                            result['metrics']['roe'] = value
                        elif 'dividend yield' in label:
                            result['metrics']['dividend_yield'] = value
                        elif 'promoter holding' in label:
                            result['shareholding']['promoter_pct'] = value
                        elif 'pledged' in label:
                            result['metrics']['pledged_pct'] = value
                        elif 'piotroski' in label:
                            result['metrics']['piotroski_score'] = value
                        elif 'altman' in label or 'z score' in label:
                            result['metrics']['altman_z_score'] = value
                        elif 'qoq sales' in label:
                            result['metrics']['sales_qoq'] = value
                        elif 'qoq profit' in label:
                            result['metrics']['profit_qoq'] = value
                        elif 'peg' in label:
                            result['metrics']['peg_ratio'] = value
                        elif 'industry p/e' in label:
                            result['metrics']['industry_pe'] = value
                        elif 'market cap to sales' in label:
                            result['metrics']['mcap_to_sales'] = value
                        elif 'ev/ebitda' in label or 'evebitda' in label:
                            result['metrics']['ev_ebitda'] = value
        
        # ========== 2. PARSE SHAREHOLDING ==========
        shp_section = soup.find('section', id='shareholding')
        if shp_section:
            shp_table = shp_section.find('table')
            if shp_table:
                rows = shp_table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        label = cells[0].get_text().strip().lower()
                        
                        # Get last two values for QoQ calculation
                        values = []
                        for cell in cells[1:]:
                            val = parse_number(cell.get_text())
                            if val is not None:
                                values.append(val)
                        
                        if len(values) >= 1:
                            latest = values[-1] if values else None
                            prev = values[-2] if len(values) >= 2 else None
                            qoq = round(latest - prev, 2) if latest and prev else None
                            
                            if 'promoter' in label and 'pledg' not in label:
                                result['shareholding']['promoter_pct'] = latest
                                result['shareholding']['promoter_chg'] = qoq
                            elif 'fii' in label or 'foreign' in label:
                                result['shareholding']['fii_pct'] = latest
                                result['shareholding']['fii_chg'] = qoq
                            elif 'dii' in label:
                                result['shareholding']['dii_pct'] = latest
                                result['shareholding']['dii_chg'] = qoq
                            elif 'public' in label:
                                result['shareholding']['public_pct'] = latest
                                result['shareholding']['public_chg'] = qoq
                            elif 'government' in label:
                                result['shareholding']['govt_pct'] = latest
                                result['shareholding']['govt_chg'] = qoq
        
        # ========== 3. PARSE QUARTERLY RESULTS ==========
        quarters_section = soup.find('section', id='quarters')
        if quarters_section:
            q_table = quarters_section.find('table')
            if q_table:
                headers = []
                header_row = q_table.find('thead')
                if header_row:
                    for th in header_row.find_all('th'):
                        headers.append(th.get_text().strip())
                
                rows = q_table.find_all('tr')
                quarter_data = {}
                
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        label = cells[0].get_text().strip().lower()
                        
                        for i, cell in enumerate(cells[1:], 1):
                            if i < len(headers):
                                qtr = headers[i]
                                if qtr not in quarter_data:
                                    quarter_data[qtr] = {'label': qtr}
                                
                                val = parse_number(cell.get_text())
                                
                                if 'sales' in label or 'revenue' in label:
                                    quarter_data[qtr]['sales'] = val
                                elif 'operating profit' in label or 'opm' in label:
                                    if '%' in cell.get_text():
                                        quarter_data[qtr]['opm'] = val
                                    else:
                                        quarter_data[qtr]['op_profit'] = val
                                elif 'net profit' in label:
                                    quarter_data[qtr]['net_profit'] = val
                                elif 'eps' in label:
                                    quarter_data[qtr]['eps'] = val
                
                # Convert to list, sorted by date (most recent first)
                result['quarters'] = list(quarter_data.values())[:8]
        
        # ========== 4. ADDITIONAL DATA FROM SCRIPT TAGS ==========
        # Screener sometimes embeds data in script tags
        for script in soup.find_all('script'):
            script_text = script.string or ''
            
            # Look for warehouse data
            if 'warehouse' in script_text.lower() or 'ratios' in script_text.lower():
                # Try to extract JSON data
                json_match = re.search(r'var\s+\w+\s*=\s*(\{[^;]+\});', script_text)
                if json_match:
                    try:
                        data = json.loads(json_match.group(1))
                        # Merge any found data
                        if isinstance(data, dict):
                            for key, val in data.items():
                                if key not in result['metrics'] and isinstance(val, (int, float)):
                                    result['metrics'][key] = val
                    except:
                        pass
        
        return result
        
    except requests.exceptions.Timeout:
        print(f"  ⚠ Timeout")
        return None
    except Exception as e:
        print(f"  ⚠ Error: {e}")
        return None


def update_stock_json(symbol, screener_data):
    """Update stock_details JSON file with Screener data."""
    json_path = os.path.join(STOCK_DETAILS_DIR, f"{symbol}.json")
    
    # Create directory if needed
    os.makedirs(STOCK_DETAILS_DIR, exist_ok=True)
    
    # Load existing data or create new
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r') as f:
                stock_data = json.load(f)
        except:
            stock_data = {'symbol': symbol}
    else:
        stock_data = {'symbol': symbol}
    
    # Initialize sections
    if 'fundamentals' not in stock_data:
        stock_data['fundamentals'] = {}
    if 'fund_holdings' not in stock_data:
        stock_data['fund_holdings'] = {}
    if 'technical' not in stock_data:
        stock_data['technical'] = {}
    
    metrics = screener_data.get('metrics', {})
    shp = screener_data.get('shareholding', {})
    
    # ========== Update Fundamentals ==========
    fund = stock_data['fundamentals']
    
    if metrics.get('market_cap_cr'):
        fund['market_cap_cr'] = metrics['market_cap_cr']
        # Determine mcap category
        mcap = metrics['market_cap_cr']
        if mcap >= 100000:
            fund['mcap_category'] = 'large_cap'
        elif mcap >= 20000:
            fund['mcap_category'] = 'mid_cap'
        elif mcap >= 5000:
            fund['mcap_category'] = 'small_cap'
        else:
            fund['mcap_category'] = 'micro_cap'
    
    if metrics.get('current_price'):
        stock_data['technical']['close'] = metrics['current_price']
        stock_data['price'] = metrics['current_price']
    
    if metrics.get('high_52w'):
        if 'breakout' not in stock_data:
            stock_data['breakout'] = {}
        stock_data['breakout']['high_52w'] = metrics['high_52w']
        stock_data['technical']['high_52w'] = metrics['high_52w']
    
    if metrics.get('low_52w'):
        if 'breakout' not in stock_data:
            stock_data['breakout'] = {}
        stock_data['breakout']['low_52w'] = metrics['low_52w']
        stock_data['technical']['low_52w'] = metrics['low_52w']
    
    if metrics.get('pe_ratio') is not None:
        fund['pe_ratio'] = metrics['pe_ratio']
    if metrics.get('pb_ratio') is not None:
        fund['pb_ratio'] = metrics['pb_ratio']
    if metrics.get('book_value') is not None:
        fund['book_value'] = metrics['book_value']
    if metrics.get('dividend_yield') is not None:
        fund['dividend_yield'] = metrics['dividend_yield']
    if metrics.get('roce') is not None:
        fund['roce'] = metrics['roce']
    if metrics.get('roe') is not None:
        fund['roe'] = metrics['roe']
    if metrics.get('debt_to_equity') is not None:
        fund['debt_to_equity'] = metrics['debt_to_equity']
    if metrics.get('face_value') is not None:
        fund['face_value'] = metrics['face_value']
    if metrics.get('piotroski_score') is not None:
        fund['piotroski_score'] = metrics['piotroski_score']
    if metrics.get('altman_z_score') is not None:
        fund['altman_z_score'] = metrics['altman_z_score']
    if metrics.get('peg_ratio') is not None:
        fund['peg_ratio'] = metrics['peg_ratio']
    if metrics.get('industry_pe') is not None:
        fund['industry_pe'] = metrics['industry_pe']
    if metrics.get('ev_ebitda') is not None:
        fund['ev_ebitda'] = metrics['ev_ebitda']
    if metrics.get('pledged_pct') is not None:
        fund['pledged_pct'] = metrics['pledged_pct']
    if metrics.get('sales_qoq') is not None:
        fund['sales_qoq'] = metrics['sales_qoq']
    if metrics.get('profit_qoq') is not None:
        fund['profit_qoq'] = metrics['profit_qoq']
    
    # ========== Update Shareholding ==========
    fh = stock_data['fund_holdings']
    
    if shp.get('promoter_pct') is not None:
        fh['promoter_pct'] = shp['promoter_pct']
    if shp.get('promoter_chg') is not None:
        fh['promoter_chg'] = shp['promoter_chg']
    if shp.get('fii_pct') is not None:
        fh['fii_pct'] = shp['fii_pct']
    if shp.get('fii_chg') is not None:
        fh['fii_chg'] = shp['fii_chg']
    if shp.get('dii_pct') is not None:
        fh['dii_pct'] = shp['dii_pct']
    if shp.get('dii_chg') is not None:
        fh['dii_chg'] = shp['dii_chg']
    if shp.get('public_pct') is not None:
        fh['public_pct'] = shp['public_pct']
    if shp.get('public_chg') is not None:
        fh['public_chg'] = shp['public_chg']
    
    # Calculate institutional total
    if fh.get('fii_pct') and fh.get('dii_pct'):
        fh['institutional_pct'] = round(fh['fii_pct'] + fh['dii_pct'], 2)
    elif fh.get('fii_pct'):
        fh['institutional_pct'] = fh['fii_pct']
    
    # ========== Update Quarters ==========
    if screener_data.get('quarters'):
        stock_data['quarters_screener'] = screener_data['quarters']
    
    # Add timestamp
    stock_data['screener_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    fh['shareholding_updated'] = datetime.now().strftime('%Y-%m-%d')
    
    # Save
    try:
        with open(json_path, 'w') as f:
            json.dump(stock_data, f, indent=2)
        return True
    except Exception as e:
        print(f"  ⚠ Error writing: {e}")
        return False


def get_all_symbols():
    """Get all symbols from various sources."""
    symbols = set()
    
    # 1. Load from nse_symbols.json (main universe)
    if os.path.exists(NSE_SYMBOLS_FILE):
        try:
            with open(NSE_SYMBOLS_FILE, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, str):
                            symbols.add(item)
                        elif isinstance(item, dict) and 'symbol' in item:
                            symbols.add(item['symbol'])
                elif isinstance(data, dict):
                    # Could be {symbol: data} format
                    symbols.update(data.keys())
        except Exception as e:
            print(f"Warning: Could not load {NSE_SYMBOLS_FILE}: {e}")
    
    # 2. Load from scanner results
    if os.path.exists(SCANNER_RESULTS):
        try:
            with open(SCANNER_RESULTS, 'r') as f:
                data = json.load(f)
                all_stocks = data.get('all_stocks', [])
                for s in all_stocks:
                    if isinstance(s, dict) and 'symbol' in s:
                        symbols.add(s['symbol'])
                    elif isinstance(s, str):
                        symbols.add(s)
        except:
            pass
    
    # 3. Load from existing stock_details
    if os.path.exists(STOCK_DETAILS_DIR):
        for f in os.listdir(STOCK_DETAILS_DIR):
            if f.endswith('.json'):
                symbols.add(f[:-5])
    
    return sorted(list(symbols))


def get_symbols_to_update(args):
    """Get list of symbols to update based on arguments."""
    
    if args.symbols:
        # Specific symbols provided
        return [s.strip().upper() for s in args.symbols.split(',')]
    
    # Get all symbols
    all_symbols = get_all_symbols()
    
    if not all_symbols:
        print("No symbols found. Please ensure nse_symbols.json or scanner_results.json exists.")
        return []
    
    print(f"Total universe: {len(all_symbols)} symbols")
    
    # Batch processing
    if args.batch:
        batch_num = int(args.batch)
        start_idx = (batch_num - 1) * BATCH_SIZE
        end_idx = start_idx + BATCH_SIZE
        batch_symbols = all_symbols[start_idx:end_idx]
        
        print(f"Batch {batch_num}: symbols {start_idx+1} to {min(end_idx, len(all_symbols))}")
        
        if args.limit:
            batch_symbols = batch_symbols[:args.limit]
        
        return batch_symbols
    
    # All symbols
    if args.all:
        if args.limit:
            return all_symbols[:args.limit]
        return all_symbols
    
    # Limit only (prioritize stocks without recent data)
    if args.limit:
        today = datetime.now().strftime('%Y-%m-%d')
        without_data = []
        with_old_data = []
        with_recent_data = []
        
        for sym in all_symbols:
            json_path = os.path.join(STOCK_DETAILS_DIR, f"{sym}.json")
            if os.path.exists(json_path):
                try:
                    with open(json_path, 'r') as f:
                        data = json.load(f)
                        updated = data.get('screener_updated', '')[:10]
                        if not updated:
                            without_data.append(sym)
                        elif updated < today:
                            with_old_data.append(sym)
                        else:
                            with_recent_data.append(sym)
                except:
                    without_data.append(sym)
            else:
                without_data.append(sym)
        
        # Priority: without data > old data > recent data
        result = []
        result.extend(without_data[:args.limit])
        if len(result) < args.limit:
            result.extend(with_old_data[:args.limit - len(result)])
        if len(result) < args.limit:
            result.extend(with_recent_data[:args.limit - len(result)])
        
        return result
    
    print("Please specify --symbols, --all, --batch, or --limit")
    return []


def main():
    parser = argparse.ArgumentParser(description='Fetch data from Screener.in')
    parser.add_argument('--symbols', type=str, help='Comma-separated list of symbols')
    parser.add_argument('--all', action='store_true', help='Update all stocks')
    parser.add_argument('--limit', type=int, help='Limit number of stocks')
    parser.add_argument('--batch', type=int, help='Batch number (1-6, each ~500 stocks)')
    parser.add_argument('--no-cache', action='store_true', help='Ignore cache')
    args = parser.parse_args()
    
    print("=" * 60)
    print("📊 TradEdge Screener Fetcher")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.batch:
        print(f"Mode: Batch {args.batch} (stocks {(args.batch-1)*BATCH_SIZE + 1} to {args.batch*BATCH_SIZE})")
    print()
    
    symbols = get_symbols_to_update(args)
    if not symbols:
        print("No symbols to update. Use --symbols, --all, --batch, or --limit")
        return
    
    print(f"Symbols to update: {len(symbols)}")
    print()
    
    cache = {} if args.no_cache else load_cache()
    cache_ttl = 24 * 60 * 60  # 24 hours for daily data
    now = time.time()
    
    session = requests.Session()
    session.headers.update(HEADERS)
    
    success = 0
    failed = 0
    cached = 0
    
    for i, symbol in enumerate(symbols, 1):
        print(f"[{i}/{len(symbols)}] {symbol}...", end=" ", flush=True)
        
        # Check cache
        if symbol in cache and not args.no_cache:
            cache_time = cache[symbol].get('_cached_at', 0)
            if now - cache_time < cache_ttl:
                if update_stock_json(symbol, cache[symbol]):
                    print("✓ (cached)")
                    cached += 1
                    continue
        
        # Fetch from Screener
        data = fetch_screener_data(symbol, session)
        
        if data and (data.get('metrics') or data.get('shareholding')):
            if update_stock_json(symbol, data):
                metrics = data.get('metrics', {})
                shp = data.get('shareholding', {})
                
                info_parts = []
                if metrics.get('pe_ratio'):
                    info_parts.append(f"P/E:{metrics['pe_ratio']:.1f}")
                if metrics.get('roce'):
                    info_parts.append(f"ROCE:{metrics['roce']:.1f}%")
                if shp.get('promoter_pct'):
                    chg = shp.get('promoter_chg')
                    chg_str = f"({chg:+.2f}%)" if chg else ""
                    info_parts.append(f"Promo:{shp['promoter_pct']:.1f}%{chg_str}")
                
                print(f"✓ {' | '.join(info_parts) if info_parts else 'OK'}")
                success += 1
                
                # Update cache
                data['_cached_at'] = now
                cache[symbol] = data
            else:
                print("⚠ Failed to save")
                failed += 1
        else:
            print("✗ No data")
            failed += 1
        
        # Rate limiting
        if i < len(symbols):
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        
        # Save cache periodically (every 50 stocks)
        if i % 50 == 0:
            save_cache(cache)
            print(f"  [Cache saved at {i} stocks]")
    
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
