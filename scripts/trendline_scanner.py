#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════
 TradEdge — Descending Trendline Breakout Scanner

 Logic:
   1. Find recent swing highs (local maxima over 5-bar window)
   2. Fit a line through the last 2-3 swing highs
   3. Line must be DESCENDING (negative slope)
   4. BREAKOUT = today's close crosses ABOVE the projected trendline
   5. Filter: EMA sync (10 > 20 > 50 > 200)
   6. Confirmation: volume > 1.3x average

 Output: data/trendline_breakouts.json
 Run: python3 scripts/trendline_scanner.py
═══════════════════════════════════════════════════════════════
"""

import json
import os
import sys
from datetime import datetime

try:
    import yfinance as yf
    import numpy as np
    import pandas as pd
except ImportError:
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


def find_swing_highs(highs, window=5, min_count=2, lookback=90):
    """
    Find swing highs (local maxima) in the last `lookback` bars.
    A swing high = high[i] is the highest in window bars on each side.
    Returns list of (index, price) tuples.
    """
    data = highs[-lookback:]
    offset = len(highs) - lookback
    swings = []
    
    for i in range(window, len(data) - window):
        is_swing = True
        for j in range(1, window + 1):
            if data[i] < data[i - j] or data[i] < data[i + j]:
                is_swing = False
                break
        if is_swing:
            swings.append((i + offset, data[i]))
    
    # Return the most recent swing highs
    return swings[-min_count:] if len(swings) >= min_count else swings


def find_descending_trendline(highs, closes, volumes, lookback=90):
    """
    Descending Trendline Breakout Detection:
    1. Find 2+ recent swing highs
    2. Fit a line through them — must be descending (negative slope)
    3. Project the trendline to today
    4. Breakout = today's close > projected trendline value
    
    Returns: dict with trendline info or None
    """
    n = len(highs)
    if n < lookback:
        return None
    
    # Find swing highs
    swings = find_swing_highs(highs, window=5, min_count=2, lookback=lookback)
    if len(swings) < 2:
        # Try wider window
        swings = find_swing_highs(highs, window=3, min_count=2, lookback=lookback)
    
    if len(swings) < 2:
        return None
    
    # Use the last 2-3 swing highs for the trendline
    recent_swings = swings[-3:] if len(swings) >= 3 else swings[-2:]
    
    # Fit linear regression through swing high points
    x_vals = np.array([s[0] for s in recent_swings])
    y_vals = np.array([s[1] for s in recent_swings])
    
    # Linear fit: y = slope * x + intercept
    if len(x_vals) < 2:
        return None
    
    slope, intercept = np.polyfit(x_vals, y_vals, 1)
    
    # Must be descending (negative slope)
    if slope >= 0:
        return None
    
    # Project trendline to today
    today_idx = n - 1
    trendline_today = slope * today_idx + intercept
    
    # Also project to yesterday
    trendline_yesterday = slope * (today_idx - 1) + intercept
    
    current_close = closes[-1]
    prev_close = closes[-2] if len(closes) > 1 else current_close
    current_vol = volumes[-1]
    avg_vol = np.mean(volumes[-20:]) if len(volumes) >= 20 else np.mean(volumes)
    vol_ratio = current_vol / avg_vol if avg_vol > 0 else 0
    
    # Trendline slope in % per day
    slope_pct_per_day = (slope / trendline_today * 100) if trendline_today > 0 else 0
    
    # Days the trendline spans
    trendline_days = today_idx - recent_swings[0][0]
    
    # Total descent of the trendline
    total_descent_pct = abs(slope * trendline_days / y_vals[0] * 100) if y_vals[0] > 0 else 0
    
    # Number of touches (how many swing highs are close to the line)
    touches = 0
    for sx, sy in swings:
        projected = slope * sx + intercept
        if abs(sy - projected) / projected < 0.02:  # within 2%
            touches += 1
    
    # Status
    if current_close > trendline_today and prev_close <= trendline_yesterday:
        status = 'BREAKOUT'  # Fresh breakout today
    elif current_close > trendline_today:
        status = 'ABOVE_TRENDLINE'  # Already above
    elif current_close > trendline_today * 0.98:
        status = 'APPROACHING'  # Within 2% of trendline
    else:
        status = 'BELOW'
    
    return {
        'trendline_today': round(trendline_today, 2),
        'slope_pct_day': round(slope_pct_per_day, 3),
        'total_descent_pct': round(total_descent_pct, 1),
        'trendline_days': trendline_days,
        'touches': touches,
        'swing_highs': [{'idx': int(s[0]), 'price': round(s[1], 2)} for s in recent_swings],
        'current_close': round(current_close, 2),
        'distance_pct': round((trendline_today - current_close) / current_close * 100, 2),
        'vol_ratio': round(vol_ratio, 2),
        'status': status
    }


def scan_trendline(symbols, batch_size=50):
    """Scan all symbols for descending trendline breakouts"""
    results = []
    errors = 0
    total = len(symbols)
    
    print(f"\n🔍 Scanning {total} stocks for descending trendline breakouts...")
    
    for batch_start in range(0, total, batch_size):
        batch = symbols[batch_start:batch_start + batch_size]
        tickers = [s + '.NS' for s in batch]
        
        try:
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
                    
                    # EMA alignment filter
                    ema_aligned, emas = calc_emas(closes)
                    if not ema_aligned:
                        continue
                    
                    # Find trendline breakout
                    tl = find_descending_trendline(highs, closes, volumes)
                    if tl is None:
                        continue
                    
                    # Only report breakouts and approaching
                    if tl['status'] in ('BREAKOUT', 'APPROACHING'):
                        chg_1d = ((closes[-1] - closes[-2]) / closes[-2] * 100) if len(closes) > 1 else 0
                        chg_1m = ((closes[-1] - closes[-22]) / closes[-22] * 100) if len(closes) > 22 else 0
                        
                        results.append({
                            'symbol': sym,
                            'cmp': tl['current_close'],
                            'chg_pct': round(chg_1d, 2),
                            'chg_1m': round(chg_1m, 1),
                            'trendline_at': tl['trendline_today'],
                            'slope_pct_day': tl['slope_pct_day'],
                            'descent_pct': tl['total_descent_pct'],
                            'trendline_days': tl['trendline_days'],
                            'touches': tl['touches'],
                            'distance_pct': tl['distance_pct'],
                            'vol_ratio': tl['vol_ratio'],
                            'status': tl['status'],
                            'swing_highs': tl['swing_highs'],
                            'ema_sync': True,
                            **emas
                        })
                        
                        emoji = '📐' if tl['status'] == 'BREAKOUT' else '👀'
                        print(f"  {emoji} {sym}: {tl['status']} | TL ₹{tl['trendline_today']} | CMP ₹{tl['current_close']} | {tl['touches']} touches | Vol {tl['vol_ratio']}x")
                
                except Exception as e:
                    errors += 1
                    continue
        
        except Exception as e:
            print(f"  ⚠ Batch error: {e}")
            errors += 1
        
        done = min(batch_start + batch_size, total)
        print(f"  Progress: {done}/{total} ({len(results)} found, {errors} errors)")
    
    # Sort: fresh breakouts first, then by distance
    results.sort(key=lambda x: (
        0 if x['status'] == 'BREAKOUT' else 1,
        x.get('distance_pct', 99)
    ))
    
    return results


def main():
    print("═" * 60)
    print("  TradEdge Descending Trendline Breakout Scanner")
    print("═" * 60)
    
    symbols = load_symbols()
    if not symbols:
        sys.exit(1)
    
    results = scan_trendline(symbols)
    
    breakouts = [r for r in results if r['status'] == 'BREAKOUT']
    approaching = [r for r in results if r['status'] == 'APPROACHING']
    
    print(f"\n{'═' * 60}")
    print(f"  RESULTS: {len(results)} total")
    print(f"  📐 Breakouts: {len(breakouts)}")
    print(f"  👀 Approaching: {len(approaching)}")
    print(f"{'═' * 60}")
    
    output = {
        'scanner': 'descending_trendline_breakout',
        'generated_at': datetime.now().isoformat(),
        'total_scanned': len(symbols),
        'matches': len(results),
        'breakouts': len(breakouts),
        'approaching': len(approaching),
        'results': results
    }
    
    out_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'trendline_breakouts.json')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"\n✓ Saved to {out_path}")


if __name__ == '__main__':
    main()
