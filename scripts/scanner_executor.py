#!/usr/bin/env python3
"""
scanner_executor.py - Execute scanner queries against stock data

This script evaluates ChartInk-style queries against stock_details JSON files
and returns matching stocks.

Usage:
    python scanner_executor.py --query "Market Cap > 500 AND ROCE > 10"
    python scanner_executor.py --preset accelerating_growth
    python scanner_executor.py --file my_query.txt

Output: JSON with matching stocks

Author: TradEdge Scanner
"""

import json
import os
import re
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional
import operator

# Configuration
STOCK_DETAILS_DIR = "data/stock_details"
SCANNER_RESULTS = "data/scanner_results.json"
OUTPUT_FILE = "data/scan_output.json"

# ============================================================
# FIELD MAPPINGS - Map ChartInk fields to JSON paths
# ============================================================

FIELD_MAP = {
    # Market Cap
    'market cap': 'fundamentals.market_cap_cr',
    'mcap': 'fundamentals.market_cap_cr',
    
    # Valuation
    'pe': 'fundamentals.pe_ratio',
    'p/e': 'fundamentals.pe_ratio',
    'pe ratio': 'fundamentals.pe_ratio',
    'pb': 'fundamentals.pb_ratio',
    'p/b': 'fundamentals.pb_ratio',
    'pb ratio': 'fundamentals.pb_ratio',
    'ps': 'fundamentals.ps_ratio',
    'p/s': 'fundamentals.ps_ratio',
    'peg': 'fundamentals.peg_ratio',
    'ev/ebitda': 'fundamentals.ev_ebitda',
    
    # Profitability
    'roe': 'fundamentals.roe',
    'roce': 'fundamentals.roce',
    'roa': 'fundamentals.roa',
    'opm': 'fundamentals.opm',
    'opm yearly': 'fundamentals.opm',
    'opm quarterly': 'fundamentals.opm_quarterly',
    'npm': 'fundamentals.npm',
    'npm yearly': 'fundamentals.npm',
    
    # Debt
    'de': 'fundamentals.debt_to_equity',
    'd/e': 'fundamentals.debt_to_equity',
    'debt to equity': 'fundamentals.debt_to_equity',
    'debt/equity': 'fundamentals.debt_to_equity',
    
    # Earnings
    'eps': 'fundamentals.eps',
    'eps quarterly': 'fundamentals.eps',
    'eps 1qtr back': 'fundamentals.eps_1qtr_back',
    'eps 4qtr back': 'fundamentals.eps_4qtr_back',
    'eps yearly': 'fundamentals.eps_yearly',
    'eps 1yr back': 'fundamentals.eps_1yr_back',
    'eps growth quarterly': 'fundamentals.eps_growth_quarterly',
    
    # Sales
    'sales quarterly': 'fundamentals.sales_quarterly',
    'sales 1qtr back': 'fundamentals.sales_1qtr_back',
    'sales 4qtr back': 'fundamentals.sales_4qtr_back',
    'sales growth quarterly yoy': 'fundamentals.sales_growth_yoy',
    'sales growth quarterly qoq': 'fundamentals.sales_growth_qoq',
    'sales 3yr cagr': 'fundamentals.sales_3yr_cagr',
    'quarterly sales all time high': 'fundamentals.quarterly_sales_ath',
    
    # Profit
    'net profit quarterly': 'fundamentals.net_profit_quarterly',
    'net profit 1qtr back': 'fundamentals.net_profit_1qtr_back',
    'net profit 4qtr back': 'fundamentals.net_profit_4qtr_back',
    'net profit 3yr cagr': 'fundamentals.net_profit_3yr_cagr',
    'quarterly profit all time high': 'fundamentals.quarterly_profit_ath',
    'operating profit growth quarterly yoy': 'fundamentals.op_growth_yoy',
    'operating profit growth quarterly qoq': 'fundamentals.op_growth_qoq',
    
    # Dividends
    'dividend yield': 'fundamentals.dividend_yield',
    
    # Cash Flow
    'fcf yield': 'fundamentals.fcf_yield',
    'free cash flow per share': 'fundamentals.fcf_per_share',
    'cash conversion cycle yearly': 'fundamentals.cash_conversion_cycle',
    'cash conversion cycle 5yr avg': 'fundamentals.cash_conversion_5yr_avg',
    
    # Holdings
    'fii holding': 'fund_holdings.fii_pct',
    'fii holding 1qtr back': 'fund_holdings.fii_pct_1qtr_back',
    'fii holding 4qtr back': 'fund_holdings.fii_pct_4qtr_back',
    'dii holding': 'fund_holdings.dii_pct',
    'dii holding 1qtr back': 'fund_holdings.dii_pct_1qtr_back',
    'dii holding 4qtr back': 'fund_holdings.dii_pct_4qtr_back',
    'retail holding': 'fund_holdings.public_pct',
    'retail holding 1qtr back': 'fund_holdings.public_pct_1qtr_back',
    'retail holding 4qtr back': 'fund_holdings.public_pct_4qtr_back',
    'promoter holding': 'fund_holdings.promoter_pct',
    'promoter holding 1qtr back': 'fund_holdings.promoter_pct_1qtr_back',
    'promoter holding 4qtr back': 'fund_holdings.promoter_pct_4qtr_back',
    'institutional holding': 'fund_holdings.institutional_pct',
    'shares outstanding': 'fundamentals.shares_outstanding_cr',
    
    # CapEx
    'gross block': 'fundamentals.gross_block',
    'gross block 1yr back': 'fundamentals.gross_block_1yr_back',
    'gross block 3yr back': 'fundamentals.gross_block_3yr_back',
    'net block': 'fundamentals.net_block',
    'net block 1yr back': 'fundamentals.net_block_1yr_back',
    'net block 3yr back': 'fundamentals.net_block_3yr_back',
    
    # Price
    'close': 'technical.close',
    'price': 'technical.close',
    'current price': 'technical.close',
    'open': 'technical.open',
    'high': 'technical.high',
    'low': 'technical.low',
    'volume': 'volume.latest',
    'high 52week': 'breakout.high_52w',
    'low 52week': 'breakout.low_52w',
    'price within fifty two week high': 'breakout.pct_from_high',
    'price within all time high': 'breakout.pct_from_ath',
    
    # Technical
    'rsi': 'rsi',
    'rsi(14)': 'rsi',
    
    # Piotroski/Altman
    'piotroski score': 'fundamentals.piotroski_score',
    'altman z score': 'fundamentals.altman_z_score',
}

# ============================================================
# PRESETS - Pre-built scanner queries
# ============================================================

PRESETS = {
    'accelerating_growth': {
        'name': 'Accelerating Growth',
        'query': 'market_cap > 400 AND sales_growth_yoy > 8 AND sales_growth_qoq > 8 AND op_growth_yoy > 8 AND op_growth_qoq > 8 AND roce > 8'
    },
    'smart_money_accumulation': {
        'name': 'Smart Money Accumulation',
        'query': 'fii_pct > fii_pct_1qtr_back AND fii_pct > 1 AND market_cap > 500 AND dii_pct > dii_pct_1qtr_back AND dii_pct > 1 AND public_pct < public_pct_1qtr_back'
    },
    'quality_at_highs': {
        'name': 'Quality Near Highs',
        'query': 'market_cap > 250 AND debt_to_equity < 0.6 AND public_pct < 25 AND (pct_from_high < 25 OR pct_from_ath < 25)'
    },
    'strong_fundamentals': {
        'name': 'Strong Fundamentals',
        'query': 'roce > 10 AND roe > 10 AND debt_to_equity < 1 AND opm > 10 AND npm > 6'
    },
    'blockbuster_earnings': {
        'name': 'Blockbuster Quarterly Earnings',
        'query': 'market_cap > 500 AND market_cap < 20000 AND eps_growth_quarterly > 25 AND debt_to_equity < 1 AND fcf_per_share > 0 AND sales_growth_qoq > 25'
    },
    'oshaughnessy': {
        'name': "O'Shaughnessy Scanner",
        'query': 'market_cap > 300 AND market_cap < 25000 AND peg_ratio < 1 AND ps_ratio < 2 AND debt_to_equity < 1 AND fcf_per_share > 1'
    },
    'graham_value': {
        'name': 'Graham Value',
        'query': 'pe_ratio < 15 AND pb_ratio < 1.5 AND debt_to_equity < 100 AND dividend_yield > 0 AND eps > 0 AND market_cap > 500'
    },
    'canslim': {
        'name': 'CANSLIM',
        'query': 'eps_growth_quarterly > 25 AND sales_growth_qoq > 25 AND roe > 15 AND institutional_pct > 10'
    },
    'volume_breakout': {
        'name': 'Volume Breakout',
        'query': 'volume_ratio > 2 AND change_pct > 0'
    },
    '52w_high_breakout': {
        'name': '52 Week High Breakout',
        'query': 'pct_from_high < 5 AND volume_ratio > 1.5'
    }
}

# ============================================================
# QUERY PARSER
# ============================================================

def normalize_field(field: str) -> str:
    """Normalize field name to internal format."""
    field = field.lower().strip()
    
    # Check direct mapping
    if field in FIELD_MAP:
        return FIELD_MAP[field]
    
    # Try without spaces
    field_no_space = field.replace(' ', '_')
    for key, val in FIELD_MAP.items():
        if key.replace(' ', '_') == field_no_space:
            return val
    
    # Return as-is for technical indicators
    return field


def get_nested_value(data: Dict, path: str) -> Optional[float]:
    """Get value from nested dict using dot notation."""
    keys = path.split('.')
    val = data
    
    for key in keys:
        if isinstance(val, dict):
            val = val.get(key)
        else:
            return None
        
        if val is None:
            return None
    
    # Convert to float if possible
    if isinstance(val, (int, float)):
        return float(val)
    elif isinstance(val, str):
        try:
            return float(val.replace(',', '').replace('%', ''))
        except:
            return None
    elif isinstance(val, bool):
        return 1.0 if val else 0.0
    
    return None


def tokenize_query(query: str) -> List[str]:
    """Tokenize query into components."""
    # Replace operators with standardized versions
    query = query.replace('>=', ' >= ')
    query = query.replace('<=', ' <= ')
    query = query.replace('>', ' > ')
    query = query.replace('<', ' < ')
    query = query.replace('=', ' = ')
    query = query.replace('  ', ' ')
    
    # Handle AND/OR
    query = re.sub(r'\bAND\b', ' AND ', query, flags=re.IGNORECASE)
    query = re.sub(r'\bOR\b', ' OR ', query, flags=re.IGNORECASE)
    
    # Tokenize
    tokens = []
    current = ''
    paren_depth = 0
    
    for char in query:
        if char == '(':
            if current.strip():
                tokens.append(current.strip())
                current = ''
            tokens.append('(')
            paren_depth += 1
        elif char == ')':
            if current.strip():
                tokens.append(current.strip())
                current = ''
            tokens.append(')')
            paren_depth -= 1
        elif char == ' ' and paren_depth == 0:
            if current.strip():
                tokens.append(current.strip())
            current = ''
        else:
            current += char
    
    if current.strip():
        tokens.append(current.strip())
    
    return tokens


def parse_condition(condition: str) -> Optional[tuple]:
    """Parse a single condition like 'Market Cap > 500'."""
    # Operators in order of precedence
    ops = ['>=', '<=', '!=', '>', '<', '=']
    
    for op in ops:
        if op in condition:
            parts = condition.split(op, 1)
            if len(parts) == 2:
                field = parts[0].strip()
                value = parts[1].strip()
                
                # Handle multiplication (e.g., "Sales 1Qtr Back * 1.15")
                if '*' in value:
                    val_parts = value.split('*')
                    base_field = val_parts[0].strip()
                    multiplier = float(val_parts[1].strip())
                    return (field, op, base_field, multiplier)
                
                # Try to convert value to float
                try:
                    value = float(value.replace(',', '').replace('%', ''))
                except:
                    # Value might be another field name
                    pass
                
                return (field, op, value, None)
    
    return None


def evaluate_condition(stock: Dict, condition: tuple) -> bool:
    """Evaluate a single condition against stock data."""
    field, op, value, multiplier = condition
    
    # Get field value
    field_path = normalize_field(field)
    stock_val = get_nested_value(stock, field_path)
    
    # Handle direct field names
    if stock_val is None:
        stock_val = get_nested_value(stock, field.replace(' ', '_').lower())
    
    # Some additional fallbacks
    if stock_val is None:
        # Try fundamentals
        stock_val = get_nested_value(stock, f"fundamentals.{field.replace(' ', '_').lower()}")
    if stock_val is None:
        # Try fund_holdings
        stock_val = get_nested_value(stock, f"fund_holdings.{field.replace(' ', '_').lower()}")
    if stock_val is None:
        # Try technical
        stock_val = get_nested_value(stock, f"technical.{field.replace(' ', '_').lower()}")
    
    if stock_val is None:
        return False
    
    # Get comparison value
    if isinstance(value, str):
        # Value is another field
        value_path = normalize_field(value)
        compare_val = get_nested_value(stock, value_path)
        if compare_val is None:
            return False
        if multiplier:
            compare_val *= multiplier
    else:
        compare_val = value
        if multiplier:
            compare_val *= multiplier
    
    # Evaluate
    ops_map = {
        '>': operator.gt,
        '<': operator.lt,
        '>=': operator.ge,
        '<=': operator.le,
        '=': operator.eq,
        '!=': operator.ne,
    }
    
    if op in ops_map:
        try:
            return ops_map[op](stock_val, compare_val)
        except:
            return False
    
    return False


def evaluate_query(stock: Dict, query: str) -> bool:
    """Evaluate a full query against stock data."""
    # Simple recursive descent parser
    tokens = tokenize_query(query)
    
    # Handle empty query
    if not tokens:
        return True
    
    # Build expression tree
    def parse_expr(tokens: List[str], pos: int = 0) -> tuple:
        """Parse expression recursively."""
        result = None
        current_op = 'AND'
        
        while pos < len(tokens):
            token = tokens[pos]
            
            if token.upper() == 'AND':
                current_op = 'AND'
                pos += 1
            elif token.upper() == 'OR':
                current_op = 'OR'
                pos += 1
            elif token == '(':
                # Find matching close paren
                depth = 1
                end = pos + 1
                while end < len(tokens) and depth > 0:
                    if tokens[end] == '(':
                        depth += 1
                    elif tokens[end] == ')':
                        depth -= 1
                    end += 1
                
                # Recursively evaluate inside parens
                inner_query = ' '.join(tokens[pos+1:end-1])
                inner_result = evaluate_query(stock, inner_query)
                
                if result is None:
                    result = inner_result
                elif current_op == 'AND':
                    result = result and inner_result
                else:
                    result = result or inner_result
                
                pos = end
            elif token == ')':
                pos += 1
            else:
                # Try to parse as condition
                # Look ahead to build full condition
                condition_parts = [token]
                pos += 1
                
                while pos < len(tokens) and tokens[pos].upper() not in ['AND', 'OR', '(', ')']:
                    condition_parts.append(tokens[pos])
                    pos += 1
                
                condition_str = ' '.join(condition_parts)
                parsed = parse_condition(condition_str)
                
                if parsed:
                    cond_result = evaluate_condition(stock, parsed)
                    
                    if result is None:
                        result = cond_result
                    elif current_op == 'AND':
                        result = result and cond_result
                    else:
                        result = result or cond_result
        
        return result if result is not None else True
    
    try:
        return parse_expr(tokens)
    except Exception as e:
        print(f"Query evaluation error: {e}")
        return False


# ============================================================
# SCANNER EXECUTOR
# ============================================================

def load_all_stocks() -> List[Dict]:
    """Load all stock data from JSON files."""
    stocks = []
    
    if not os.path.exists(STOCK_DETAILS_DIR):
        print(f"Warning: {STOCK_DETAILS_DIR} not found")
        return stocks
    
    for filename in os.listdir(STOCK_DETAILS_DIR):
        if filename.endswith('.json'):
            filepath = os.path.join(STOCK_DETAILS_DIR, filename)
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                    data['symbol'] = data.get('symbol', filename[:-5])
                    stocks.append(data)
            except Exception as e:
                print(f"Error loading {filename}: {e}")
    
    return stocks


def run_scan(query: str, stocks: Optional[List[Dict]] = None) -> List[Dict]:
    """Run a scan query and return matching stocks."""
    if stocks is None:
        stocks = load_all_stocks()
    
    print(f"Scanning {len(stocks)} stocks...")
    print(f"Query: {query}")
    
    matches = []
    
    for stock in stocks:
        try:
            if evaluate_query(stock, query):
                matches.append(stock)
        except Exception as e:
            pass  # Skip stocks that cause errors
    
    print(f"Found {len(matches)} matches")
    
    return matches


def format_results(matches: List[Dict]) -> List[Dict]:
    """Format matching stocks for output."""
    results = []
    
    for stock in matches:
        result = {
            'symbol': stock.get('symbol', ''),
            'name': stock.get('name', stock.get('symbol', '')),
            'sector': stock.get('sector', ''),
            'price': get_nested_value(stock, 'technical.close') or get_nested_value(stock, 'price') or 0,
            'change_pct': get_nested_value(stock, 'technical.change_pct') or get_nested_value(stock, 'change_pct') or 0,
            'market_cap': get_nested_value(stock, 'fundamentals.market_cap_cr') or 0,
            'pe_ratio': get_nested_value(stock, 'fundamentals.pe_ratio'),
            'pb_ratio': get_nested_value(stock, 'fundamentals.pb_ratio'),
            'roe': get_nested_value(stock, 'fundamentals.roe'),
            'roce': get_nested_value(stock, 'fundamentals.roce'),
            'debt_to_equity': get_nested_value(stock, 'fundamentals.debt_to_equity'),
            'promoter_pct': get_nested_value(stock, 'fund_holdings.promoter_pct'),
            'fii_pct': get_nested_value(stock, 'fund_holdings.fii_pct'),
            'volume_ratio': get_nested_value(stock, 'volume.ratio'),
        }
        results.append(result)
    
    # Sort by market cap descending
    results.sort(key=lambda x: x.get('market_cap', 0) or 0, reverse=True)
    
    return results


def main():
    parser = argparse.ArgumentParser(description='Execute scanner queries')
    parser.add_argument('--query', type=str, help='Scanner query to execute')
    parser.add_argument('--preset', type=str, help='Pre-built scanner preset name')
    parser.add_argument('--file', type=str, help='File containing query')
    parser.add_argument('--output', type=str, default=OUTPUT_FILE, help='Output file path')
    parser.add_argument('--list-presets', action='store_true', help='List available presets')
    args = parser.parse_args()
    
    if args.list_presets:
        print("\nAvailable Presets:")
        print("=" * 50)
        for key, preset in PRESETS.items():
            print(f"\n{key}:")
            print(f"  Name: {preset['name']}")
            print(f"  Query: {preset['query'][:80]}...")
        return
    
    # Get query
    query = None
    
    if args.preset:
        if args.preset in PRESETS:
            query = PRESETS[args.preset]['query']
            print(f"Using preset: {PRESETS[args.preset]['name']}")
        else:
            print(f"Unknown preset: {args.preset}")
            print(f"Available: {', '.join(PRESETS.keys())}")
            return
    elif args.query:
        query = args.query
    elif args.file:
        with open(args.file, 'r') as f:
            query = f.read().strip()
    else:
        print("Please provide --query, --preset, or --file")
        return
    
    # Run scan
    matches = run_scan(query)
    results = format_results(matches)
    
    # Output
    output = {
        'query': query,
        'timestamp': datetime.now().isoformat(),
        'count': len(results),
        'results': results
    }
    
    # Save to file
    os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else '.', exist_ok=True)
    with open(args.output, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\nResults saved to {args.output}")
    
    # Print top results
    if results:
        print(f"\nTop {min(10, len(results))} Results:")
        print("-" * 80)
        print(f"{'Symbol':<12} {'Name':<25} {'Price':>10} {'Change':>8} {'MCap':>10} {'P/E':>8}")
        print("-" * 80)
        
        for r in results[:10]:
            print(f"{r['symbol']:<12} {r['name'][:24]:<25} {r['price']:>10.2f} {r['change_pct']:>7.2f}% {r['market_cap']:>9.0f} {r['pe_ratio'] or 0:>8.1f}")


if __name__ == "__main__":
    main()
