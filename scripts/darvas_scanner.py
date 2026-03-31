#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════
 TradEdge — Darvas Box Breakout Scanner
 
 Logic:
   1. Find new 52-week high → start box formation
   2. After 3 consecutive days of no new high → box ceiling = highest high
   3. Box floor = lowest low during those 3 days after the high
   4. BREAKOUT = close > box ceiling on volume > 1.5x avg
   5. Filter: EMA sync (10 > 20 > 50 > 200)
   
 Output: data/darvas_breakouts.json
 Run: python3 scripts/darvas_scanner.py
 Schedule: GitHub Actions daily after market close
═══════════════════════════════════════════════════════════════
"""

import json
import os
import sys
from datetime import datetime, timedelta

try:
    import yfinance as yf
    import numpy as np
    import pandas as pd
except ImportError:
    print("Installing dependencies...")
    os.system("pip install yfinance numpy pandas --quiet")
    import yfinance as yf
    import numpy as np
    import pandas as pd


def load_symbols():
    """Load stock universe from nse_symbols.json"""
    paths = [
        'data/nse_symbols.json',
        '../data/nse_symbols.json',
        os.path.join(os.path.dirname(__file__), '..', 'data', 'nse_symbols.json')
    ]
    for p in paths:
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            symbols = [s.get('symbol', s.get('s', '')) for s in data if isinstance(s, dict)]
            print(f"✓ Loaded {len(symbols)} symbols from {p}")
            return symbols
    print("✗ nse_symbols.json not found")
    return []


def calc_emas(closes):
    """Calculate EMAs and check alignment"""
    if len(closes) < 200:
        return False, {}
    
    s = pd.Series(closes)
    ema10 = s.ewm(span=10, adjust=False).mean().iloc[-1]
    ema20 = s.ewm(span=20, adjust=False).mean().iloc[-1]
    ema50 = s.ewm(span=50, adjust=False).mean().iloc[-1]
    ema200 = s.ewm(span=200, adjust=False).mean().iloc[-1]
    
    aligned = ema10 > ema20 > ema50 > ema200
    return aligned, {
        'ema10': round(ema10, 2),
        'ema20': round(ema20, 2),
        'ema50': round(ema50, 2),
        'ema200': round(ema200, 2)
    }


def find_darvas_box(highs, lows, closes, volumes, lookback=252):
    """
    Darvas Box Detection:
    1. Find a new N-day high (box_period high, default 52-week = 252 days)
    2. After the high, wait for 3 consecutive days of no new high → ceiling confirmed
    3. Box floor = lowest low in the 3 confirmation days
    4. Breakout = close > ceiling with volume > 1.5x 20-day avg
    
    Returns: dict with box info or None
    """
    n = len(highs)
    if n < lookback + 10:
        return None
    
    # Use recent data
    h = highs[-lookback:]
    l = lows[-lookback:]
    c = closes[-lookback:]
    v = volumes[-lookback:]
    
    # Find the most recent new high
    box_ceiling = None
    box_floor = None
    ceiling_idx = None
    
    # Look for the latest Darvas Box formation
    for i in range(len(h) - 4, max(len(h) - 60, 3), -1):
        # Check if h[i] was a new high at that point
        prior_max = max(h[max(0, i-lookback):i])
        if h[i] >= prior_max:
            # Found a new high at index i
            # Check next 3 days: no new high
            if i + 3 >= len(h):
                continue
            
            ceiling_confirmed = True
            for j in range(1, 4):
                if i + j < len(h) and h[i + j] > h[i]:
                    ceiling_confirmed = False
                    break
            
            if ceiling_confirmed:
                box_ceiling = h[i]
                ceiling_idx = i
                # Box floor = lowest low in the 3 days after the high
                floor_lows = l[i+1:i+4]
                if len(floor_lows) > 0:
                    box_floor = min(floor_lows)
                break
    
    if box_ceiling is None or box_floor is None:
        return None
    
    # Check current status
    current_close = c[-1]
    current_high = h[-1]
    current_vol = v[-1]
    avg_vol = np.mean(v[-20:]) if len(v) >= 20 else np.mean(v)
    vol_ratio = current_vol / avg_vol if avg_vol > 0 else 0
    
    # Box age (days since ceiling was set)
    box_age = len(h) - 1 - ceiling_idx
    
    # Box range
    box_range_pct = ((box_ceiling - box_floor) / box_floor * 100) if box_floor > 0 else 0
    
    # Status
    if current_close > box_ceiling:
        if vol_ratio > 1.5:
            status = 'BREAKOUT'  # 🚀 Breakout with volume
        else:
            status = 'BREAKOUT_LOW_VOL'  # Breakout but low volume
    elif current_close >= box_floor:
        status = 'IN_BOX'  # Inside the box
    else:
        status = 'BELOW_BOX'  # Broke below floor
    
    return {
        'ceiling': round(box_ceiling, 2),
        'floor': round(box_floor, 2),
        'box_range_pct': round(box_range_pct, 1),
        'box_age_days': box_age,
        'current_close': round(current_close, 2),
        'distance_to_ceiling_pct': round((box_ceiling - current_close) / current_close * 100, 2),
        'vol_ratio': round(vol_ratio, 2),
        'status': status
    }


def scan_darvas(symbols, batch_size=50):
    """Scan all symbols for Darvas Box breakouts"""
    results = []
    errors = 0
    
    total = len(symbols)
    print(f"\n🔍 Scanning {total} stocks for Darvas Box patterns...")
    
    # Process in batches
    for batch_start in range(0, total, batch_size):
        batch = symbols[batch_start:batch_start + batch_size]
        tickers = [s + '.NS' for s in batch]
        
        try:
            # Download batch
            data = yf.download(tickers, period='1y', interval='1d', 
                             group_by='ticker', progress=False, threads=True)
            
            for sym in batch:
                ticker = sym + '.NS'
                try:
                    if len(tickers) == 1:
                        df = data
                    else:
                        df = data[ticker] if ticker in data.columns.get_level_values(0) else None
                    
                    if df is None or df.empty or len(df) < 60:
                        continue
                    
                    df = df.dropna(subset=['Close', 'High', 'Low', 'Volume'])
                    if len(df) < 60:
                        continue
                    
                    highs = df['High'].values
                    lows = df['Low'].values
                    closes = df['Close'].values
                    volumes = df['Volume'].values
                    
                    # Check EMA alignment first (fast filter)
                    ema_aligned, emas = calc_emas(closes)
                    if not ema_aligned:
                        continue
                    
                    # Find Darvas Box
                    box = find_darvas_box(highs, lows, closes, volumes)
                    if box is None:
                        continue
                    
                    # Only report breakouts and near-ceiling stocks
                    if box['status'] in ('BREAKOUT', 'BREAKOUT_LOW_VOL') or \
                       (box['status'] == 'IN_BOX' and box['distance_to_ceiling_pct'] <= 3):
                        
                        # Calculate additional metrics
                        chg_1d = ((closes[-1] - closes[-2]) / closes[-2] * 100) if len(closes) > 1 else 0
                        chg_1m = ((closes[-1] - closes[-22]) / closes[-22] * 100) if len(closes) > 22 else 0
                        
                        results.append({
                            'symbol': sym,
                            'cmp': box['current_close'],
                            'chg_pct': round(chg_1d, 2),
                            'chg_1m': round(chg_1m, 1),
                            'box_ceiling': box['ceiling'],
                            'box_floor': box['floor'],
                            'box_range_pct': box['box_range_pct'],
                            'box_age': box['box_age_days'],
                            'distance_pct': box['distance_to_ceiling_pct'],
                            'vol_ratio': box['vol_ratio'],
                            'status': box['status'],
                            'ema_sync': True,
                            **emas
                        })
                        
                        emoji = '🚀' if box['status'] == 'BREAKOUT' else '📦'
                        print(f"  {emoji} {sym}: {box['status']} | Ceiling ₹{box['ceiling']} | CMP ₹{box['current_close']} | Vol {box['vol_ratio']}x")
                
                except Exception as e:
                    errors += 1
                    continue
        
        except Exception as e:
            print(f"  ⚠ Batch error: {e}")
            errors += 1
        
        # Progress
        done = min(batch_start + batch_size, total)
        print(f"  Progress: {done}/{total} ({len(results)} found, {errors} errors)")
    
    # Sort: breakouts first, then by distance to ceiling
    results.sort(key=lambda x: (
        0 if x['status'] == 'BREAKOUT' else 1 if x['status'] == 'BREAKOUT_LOW_VOL' else 2,
        x.get('distance_pct', 99)
    ))
    
    return results


def main():
    print("═" * 60)
    print("  TradEdge Darvas Box Scanner")
    print("═" * 60)
    
    symbols = load_symbols()
    if not symbols:
        sys.exit(1)
    
    results = scan_darvas(symbols)
    
    # Summary
    breakouts = [r for r in results if r['status'] == 'BREAKOUT']
    near_ceiling = [r for r in results if r['status'] == 'IN_BOX']
    
    print(f"\n{'═' * 60}")
    print(f"  RESULTS: {len(results)} total")
    print(f"  🚀 Breakouts: {len(breakouts)}")
    print(f"  📦 Near ceiling: {len(near_ceiling)}")
    print(f"{'═' * 60}")
    
    # Save
    output = {
        'scanner': 'darvas_box',
        'generated_at': datetime.now().isoformat(),
        'total_scanned': len(symbols),
        'matches': len(results),
        'breakouts': len(breakouts),
        'near_ceiling': len(near_ceiling),
        'results': results
    }
    
    out_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'darvas_breakouts.json')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"\n✓ Saved to {out_path}")


if __name__ == '__main__':
    main()
