#!/usr/bin/env python3
"""
pnf_fetcher.py — TradEdge P&F OHLC Data Fetcher
Fetches daily OHLC for watchlist stocks from Yahoo Finance (NSE).
Outputs: data/ohlc_daily.json
Run: python scripts/pnf_fetcher.py
Schedule: Add to scanner-daily.yml or rrm-daily.yml GitHub Actions
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    import yfinance as yf
except ImportError:
    print("Installing yfinance...")
    os.system(f"{sys.executable} -m pip install yfinance -q")
    import yfinance as yf

# ── Config ──
DATA_DIR = Path("data")
OUTPUT_FILE = DATA_DIR / "ohlc_daily.json"
DAYS = 365  # 1 year of daily data
NSE_SUFFIX = ".NS"

# Default watchlist — can be overridden by nse_symbols.json
DEFAULT_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
    "SBIN", "BHARTIARTL", "BAJFINANCE", "KOTAKBANK", "LT", "TATAMOTORS",
    "MARUTI", "SUNPHARMA", "TITAN", "AXISBANK", "WIPRO", "DRREDDY",
    "ADANIENT", "BAJAJFINSV", "HCLTECH", "TECHM", "NESTLEIND",
    "ULTRACEMCO", "POWERGRID", "NTPC", "ONGC", "JSWSTEEL", "TATASTEEL",
    "COALINDIA", "HINDALCO", "GRASIM", "BPCL", "DIVISLAB", "CIPLA",
    "APOLLOHOSP", "EICHERMOT", "SHRIRAMFIN", "TATACONSUM", "M&M",
    "ASIANPAINT", "BRITANNIA", "HEROMOTOCO", "INDUSINDBK", "SBILIFE",
]

# Load extended symbols from nse_symbols.json if available
def load_symbols():
    sym_file = DATA_DIR / "nse_symbols.json"
    if sym_file.exists():
        try:
            with open(sym_file) as f:
                data = json.load(f)
            # nse_symbols.json can be a list of strings or list of objects with 'symbol' key
            if isinstance(data, list):
                if len(data) > 0 and isinstance(data[0], str):
                    symbols = data[:100]  # Cap at 100 for P&F (top liquid)
                elif len(data) > 0 and isinstance(data[0], dict):
                    symbols = [d.get("symbol", d.get("SYMBOL", "")) for d in data[:100]]
                else:
                    symbols = DEFAULT_SYMBOLS
            else:
                symbols = DEFAULT_SYMBOLS
            print(f"Loaded {len(symbols)} symbols from nse_symbols.json")
            return symbols
        except Exception as e:
            print(f"Error loading nse_symbols.json: {e}, using defaults")
    return DEFAULT_SYMBOLS


def fetch_ohlc(symbols, days=DAYS):
    """Fetch OHLC data for all symbols."""
    end = datetime.now()
    start = end - timedelta(days=days)
    result = {}
    failed = []

    print(f"Fetching {len(symbols)} symbols, {days} days...")

    # Batch download for speed
    tickers = [f"{s}{NSE_SUFFIX}" for s in symbols]
    batch_size = 20

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        batch_str = " ".join(batch)
        print(f"  Batch {i // batch_size + 1}: {len(batch)} tickers...")

        try:
            data = yf.download(
                batch_str,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                group_by="ticker",
                progress=False,
                threads=True,
            )

            for ticker in batch:
                sym = ticker.replace(NSE_SUFFIX, "")
                try:
                    if len(batch) == 1:
                        df = data
                    else:
                        df = data[ticker]

                    if df is None or df.empty:
                        failed.append(sym)
                        continue

                    # Handle MultiIndex columns from yfinance
                    if hasattr(df.columns, 'get_level_values'):
                        # Flatten multi-level columns
                        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

                    ohlc = []
                    for idx, row in df.iterrows():
                        try:
                            o = float(row.get("Open", 0))
                            h = float(row.get("High", 0))
                            l = float(row.get("Low", 0))
                            c = float(row.get("Close", 0))
                            v = int(row.get("Volume", 0))
                            if c > 0:
                                ohlc.append({
                                    "d": idx.strftime("%Y-%m-%d"),
                                    "o": round(o, 2),
                                    "h": round(h, 2),
                                    "l": round(l, 2),
                                    "c": round(c, 2),
                                    "v": v,
                                })
                        except (ValueError, TypeError):
                            continue

                    if ohlc:
                        result[sym] = {
                            "symbol": sym,
                            "name": sym,  # Can be enriched later
                            "exchange": "NSE",
                            "lastPrice": ohlc[-1]["c"],
                            "ohlc": ohlc,
                        }
                        print(f"    ✓ {sym}: {len(ohlc)} bars, last={ohlc[-1]['c']}")
                    else:
                        failed.append(sym)

                except Exception as e:
                    failed.append(sym)

        except Exception as e:
            print(f"  Batch error: {e}")
            failed.extend([t.replace(NSE_SUFFIX, "") for t in batch])

    return result, failed


def main():
    DATA_DIR.mkdir(exist_ok=True)
    symbols = load_symbols()

    ohlc_data, failed = fetch_ohlc(symbols)

    # Build output
    output = {
        "generated": datetime.now().isoformat(),
        "count": len(ohlc_data),
        "failed": failed,
        "stocks": ohlc_data,
    }

    # Write JSON
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = OUTPUT_FILE.stat().st_size / 1024
    print(f"\n✅ Written {OUTPUT_FILE} ({size_kb:.1f} KB)")
    print(f"   {len(ohlc_data)} stocks OK, {len(failed)} failed")
    if failed:
        print(f"   Failed: {', '.join(failed[:10])}{'...' if len(failed) > 10 else ''}")


if __name__ == "__main__":
    main()
