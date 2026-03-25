#!/usr/bin/env python3
"""
TradEdge RRM Scanner v1.0
Computes JdK RS-Ratio & RS-Momentum for entire NSE stock universe
across Daily / Weekly / Monthly timeframes vs Nifty 500 benchmark.

Output: data/rrm_scanner.json
  {
    meta: { generated_at, benchmark, stock_count, ... },
    stocks: [
      { symbol, name, sector, cmp, change_pct, d_ratio, d_mom, d_quad,
        w_ratio, w_mom, w_quad, m_ratio, m_mom, m_quad,
        signal, score, oneil_grade, composite_score, ... }
    ]
  }

Usage:
  python scripts/rrm_scanner.py                    # All stocks from nse_symbols.json
  python scripts/rrm_scanner.py --top 500          # Top 500 by market cap
  python scripts/rrm_scanner.py --symbols RELIANCE,TCS,INFY  # Specific stocks
"""

import json, os, sys, time, math
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
except ImportError:
    print("Installing dependencies...")
    os.system("pip install yfinance pandas numpy --break-system-packages -q")
    import yfinance as yf
    import pandas as pd
    import numpy as np

# ════════════════════════════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════════════════════════════

BENCHMARK = "^CRSLDX"  # Nifty 500 (better breadth than ^NSEI)
BENCHMARK_FALLBACK = "^NSEI"  # Nifty 50 fallback
DATA_DIR = Path("data")
SCANNER_FILE = DATA_DIR / "scanner_results.json"
SYMBOLS_FILE = DATA_DIR / "nse_symbols.json"
OUTPUT_FILE = DATA_DIR / "rrm_scanner.json"

WINDOW = 10  # JdK smoothing window
MAX_WORKERS = 8  # Parallel Yahoo fetch threads
BATCH_SIZE = 50  # Stocks per batch (pause between batches to avoid rate limits)
BATCH_PAUSE = 2  # Seconds pause between batches

# Timeframe configs: (Yahoo range, Yahoo interval, min_bars_needed)
TIMEFRAMES = {
    "d": {"range": "1y", "interval": "1d", "min_bars": 60},
    "w": {"range": "2y", "interval": "1wk", "min_bars": 30},
    "m": {"range": "5y", "interval": "1mo", "min_bars": 15},
}


# ════════════════════════════════════════════════════════════════
#  JdK RS-RATIO / RS-MOMENTUM CALCULATION
# ════════════════════════════════════════════════════════════════

def calc_jdk(stock_closes: list, bench_closes: list, window: int = WINDOW):
    """Calculate JdK RS-Ratio and RS-Momentum. Returns (ratio, momentum) or (None, None)."""
    n = min(len(stock_closes), len(bench_closes))
    if n < window * 3:
        return None, None

    sec = np.array(stock_closes[-n:], dtype=float)
    ben = np.array(bench_closes[-n:], dtype=float)

    # Relative strength raw
    rs_raw = np.where(ben > 0, sec / ben, 1.0)

    # Normalize: RS / SMA(RS, window) * 100
    rs_norm = np.full(n, np.nan)
    for i in range(window - 1, n):
        sma = np.mean(rs_raw[i - window + 1 : i + 1])
        if sma > 0:
            rs_norm[i] = (rs_raw[i] / sma) * 100

    # EMA smoothing → RS-Ratio
    alpha = 2.0 / (window + 1)
    rs_ratio = np.full(n, np.nan)
    rs_ratio[window - 1] = rs_norm[window - 1]
    for i in range(window, n):
        if not np.isnan(rs_norm[i]) and not np.isnan(rs_ratio[i - 1]):
            rs_ratio[i] = alpha * rs_norm[i] + (1 - alpha) * rs_ratio[i - 1]

    # RS-Momentum: RS-Ratio / RS-Ratio[window ago] * 100
    rs_mom = np.full(n, np.nan)
    for i in range(window, n):
        if not np.isnan(rs_ratio[i]) and not np.isnan(rs_ratio[i - window]):
            prev = rs_ratio[i - window]
            if prev > 0:
                rs_mom[i] = (rs_ratio[i] / prev) * 100

    # Get last valid values
    last_r = next((rs_ratio[i] for i in range(n - 1, -1, -1) if not np.isnan(rs_ratio[i])), None)
    last_m = next((rs_mom[i] for i in range(n - 1, -1, -1) if not np.isnan(rs_mom[i])), None)

    if last_r is None or last_m is None:
        return None, None

    return round(last_r, 2), round(last_m, 2)


def get_quadrant(ratio, mom):
    """Classify into RRG quadrant."""
    if ratio is None or mom is None:
        return "Unknown"
    if ratio >= 100 and mom >= 100:
        return "Leading"
    if ratio >= 100 and mom < 100:
        return "Weakening"
    if ratio < 100 and mom >= 100:
        return "Improving"
    return "Lagging"


# ════════════════════════════════════════════════════════════════
#  SIGNAL LOGIC (matches MATRIX)
# ════════════════════════════════════════════════════════════════

def compute_signal(d_quad, w_quad, m_quad):
    """
    Signal color logic:
      GREEN  — M+W+D all strong (Leading/Improving)
      YELLOW — M strong, W dip, D improving
      BLUE   — W+D strong, no M backing
      RED    — Structural weakness (Lagging/Weakening majority)
    """
    strong = {"Leading", "Improving"}
    weak = {"Lagging", "Weakening"}

    m_ok = m_quad in strong
    w_ok = w_quad in strong
    d_ok = d_quad in strong

    if m_ok and w_ok and d_ok:
        return "GREEN"
    if m_ok and w_quad == "Weakening" and d_ok:
        return "YELLOW"
    if w_ok and d_ok and not m_ok:
        return "BLUE"
    return "RED"


def compute_score(d_quad, w_quad, m_quad, signal):
    """Score 1-5 based on alignment."""
    pts = 0
    for q in [d_quad, w_quad, m_quad]:
        if q == "Leading":
            pts += 2
        elif q == "Improving":
            pts += 1
        elif q == "Weakening":
            pts -= 1
        elif q == "Lagging":
            pts -= 2
    # Map -6..+6 → 1..5
    score = max(1, min(5, round((pts + 6) / 12 * 4) + 1))
    # Boost for GREEN
    if signal == "GREEN" and score < 5:
        score += 1
    return min(5, score)


# ════════════════════════════════════════════════════════════════
#  DATA FETCHING
# ════════════════════════════════════════════════════════════════

def fetch_closes(ticker: str, tf_key: str) -> list:
    """Fetch closing prices from Yahoo Finance for given timeframe."""
    tf = TIMEFRAMES[tf_key]
    try:
        data = yf.download(
            ticker,
            period=tf["range"],
            interval=tf["interval"],
            progress=False,
            timeout=10,
        )
        if data is None or data.empty:
            return []
        closes = data["Close"].dropna().tolist()
        # Handle MultiIndex columns from yfinance
        if isinstance(closes[0], (list, tuple)):
            closes = [c[0] if isinstance(c, (list, tuple)) else c for c in closes]
        return [float(c) for c in closes if not math.isnan(c)]
    except Exception:
        return []


def fetch_benchmark(benchmark: str) -> dict:
    """Fetch benchmark closes for all timeframes."""
    print(f"  Fetching benchmark {benchmark}...")
    result = {}
    for tf_key in TIMEFRAMES:
        closes = fetch_closes(benchmark, tf_key)
        if len(closes) >= TIMEFRAMES[tf_key]["min_bars"]:
            result[tf_key] = closes
            print(f"    {tf_key}: {len(closes)} bars ✓")
        else:
            print(f"    {tf_key}: {len(closes)} bars ✗ (need {TIMEFRAMES[tf_key]['min_bars']})")
    return result


def process_stock(symbol: str, bench: dict) -> dict:
    """Process a single stock: fetch data for all timeframes, compute RRM."""
    ticker = symbol + ".NS"
    result = {"symbol": symbol}

    for tf_key, tf_label in [("d", "d"), ("w", "w"), ("m", "m")]:
        bench_closes = bench.get(tf_key, [])
        if not bench_closes:
            result[f"{tf_label}_ratio"] = None
            result[f"{tf_label}_mom"] = None
            result[f"{tf_label}_quad"] = "Unknown"
            continue

        closes = fetch_closes(ticker, tf_key)
        if len(closes) < TIMEFRAMES[tf_key]["min_bars"]:
            result[f"{tf_label}_ratio"] = None
            result[f"{tf_label}_mom"] = None
            result[f"{tf_label}_quad"] = "Unknown"
            continue

        ratio, mom = calc_jdk(closes, bench_closes)
        result[f"{tf_label}_ratio"] = ratio
        result[f"{tf_label}_mom"] = mom
        result[f"{tf_label}_quad"] = get_quadrant(ratio, mom)

    # Signal & Score
    result["signal"] = compute_signal(
        result.get("d_quad", "Unknown"),
        result.get("w_quad", "Unknown"),
        result.get("m_quad", "Unknown"),
    )
    result["score"] = compute_score(
        result.get("d_quad", "Unknown"),
        result.get("w_quad", "Unknown"),
        result.get("m_quad", "Unknown"),
        result["signal"],
    )

    return result


# ════════════════════════════════════════════════════════════════
#  LOAD STOCK UNIVERSE
# ════════════════════════════════════════════════════════════════

def load_symbols(top_n=None, specific=None):
    """Load stock symbols from scanner_results.json or nse_symbols.json."""
    stocks = []

    if specific:
        return [{"symbol": s.strip().upper(), "name": s.strip().upper()} for s in specific.split(",")]

    # Try scanner_results.json first (has all metadata)
    if SCANNER_FILE.exists():
        try:
            data = json.loads(SCANNER_FILE.read_text())
            all_stocks = data.get("all_stocks", [])
            if all_stocks:
                for s in all_stocks:
                    stocks.append({
                        "symbol": s.get("symbol", ""),
                        "name": s.get("name", s.get("symbol", "")),
                        "sector": s.get("sector", ""),
                        "cmp": s.get("close") or s.get("cmp", 0),
                        "change_pct": s.get("change_pct", 0),
                        "market_cap_cr": s.get("market_cap_cr", 0),
                        "oneil_grade": s.get("oneil_grade", ""),
                        "composite_score": s.get("composite_score", 0),
                        "eps_strength": s.get("eps_strength", 0),
                        "price_strength": s.get("price_strength", 0),
                        "rsi": s.get("rsi", 0),
                        "volume_ratio": s.get("volume_ratio", 0),
                    })
                print(f"  Loaded {len(stocks)} stocks from scanner_results.json")
        except Exception as e:
            print(f"  Warning: scanner_results.json parse error: {e}")

    # Fallback to nse_symbols.json
    if not stocks and SYMBOLS_FILE.exists():
        try:
            symbols = json.loads(SYMBOLS_FILE.read_text())
            stocks = [{"symbol": s, "name": s} for s in symbols if isinstance(s, str)]
            print(f"  Loaded {len(stocks)} symbols from nse_symbols.json")
        except Exception as e:
            print(f"  Warning: nse_symbols.json parse error: {e}")

    if not stocks:
        print("  ERROR: No stock data found!")
        sys.exit(1)

    # Sort by market cap and take top N
   if top_n:
        stocks.sort(key=lambda x: x.get("market_cap_cr") or 0, reverse=True)
        stocks = stocks[:top_n]
        print(f"  Filtered to top {top_n} by market cap")

    return stocks


# ════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════

def main():
    import argparse

    parser = argparse.ArgumentParser(description="TradEdge RRM Scanner")
    parser.add_argument("--top", type=int, default=None, help="Top N stocks by market cap")
    parser.add_argument("--symbols", type=str, default=None, help="Comma-separated symbols")
    parser.add_argument("--benchmark", type=str, default=BENCHMARK, help="Benchmark ticker")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help="Parallel threads")
    args = parser.parse_args()

    print("=" * 60)
    print("  TradEdge RRM Scanner v1.0")
    print("=" * 60)
    start_time = time.time()

    # 1. Load symbols
    print("\n[1/4] Loading stock universe...")
    stocks = load_symbols(top_n=args.top, specific=args.symbols)
    print(f"  Universe: {len(stocks)} stocks")

    # 2. Fetch benchmark
    print(f"\n[2/4] Fetching benchmark ({args.benchmark})...")
    bench = fetch_benchmark(args.benchmark)
    if not bench.get("d"):
        print(f"  Primary benchmark failed, trying fallback {BENCHMARK_FALLBACK}...")
        bench = fetch_benchmark(BENCHMARK_FALLBACK)
    if not bench.get("d"):
        print("  ERROR: Cannot fetch benchmark data!")
        sys.exit(1)

    # 3. Process all stocks
    print(f"\n[3/4] Computing RRM for {len(stocks)} stocks ({args.workers} threads)...")
    results = []
    failed = 0
    total = len(stocks)

    # Process in batches to avoid Yahoo rate limiting
    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch = stocks[batch_start:batch_end]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = math.ceil(total / BATCH_SIZE)

        print(f"  Batch {batch_num}/{total_batches} ({batch_start+1}-{batch_end}/{total})...")

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {}
            for stock in batch:
                sym = stock["symbol"]
                if not sym or len(sym) > 20:
                    continue
                future = executor.submit(process_stock, sym, bench)
                futures[future] = stock

            for future in as_completed(futures):
                stock = futures[future]
                try:
                    rrm = future.result()
                    # Merge scanner metadata with RRM data
                    merged = {**stock, **rrm}
                    results.append(merged)
                except Exception as e:
                    failed += 1

        # Pause between batches
        if batch_end < total:
            time.sleep(BATCH_PAUSE)

    # Sort by score (descending), then signal priority
    signal_order = {"GREEN": 0, "YELLOW": 1, "BLUE": 2, "RED": 3}
    results.sort(key=lambda x: (-x.get("score", 0), signal_order.get(x.get("signal", "RED"), 9)))

    print(f"  Done: {len(results)} processed, {failed} failed")

    # 4. Count stats
    quads = {"Leading": 0, "Weakening": 0, "Improving": 0, "Lagging": 0, "Unknown": 0}
    signals = {"GREEN": 0, "YELLOW": 0, "BLUE": 0, "RED": 0}
    for r in results:
        quads[r.get("d_quad", "Unknown")] = quads.get(r.get("d_quad", "Unknown"), 0) + 1
        signals[r.get("signal", "RED")] = signals.get(r.get("signal", "RED"), 0) + 1

    # 5. Save
    print(f"\n[4/4] Saving to {OUTPUT_FILE}...")
    output = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "benchmark": args.benchmark,
            "stock_count": len(results),
            "failed": failed,
            "timeframes": ["Daily", "Weekly", "Monthly"],
            "quadrant_distribution": quads,
            "signal_distribution": signals,
            "version": "1.0",
        },
        "stocks": results,
    }

    DATA_DIR.mkdir(exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=None, separators=(",", ":")))
    file_size = OUTPUT_FILE.stat().st_size / 1024
    elapsed = time.time() - start_time

    print(f"\n{'=' * 60}")
    print(f"  ✓ RRM Scanner complete!")
    print(f"  Stocks: {len(results)} | Failed: {failed}")
    print(f"  File: {OUTPUT_FILE} ({file_size:.0f} KB)")
    print(f"  Time: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  Signals: G={signals.get('GREEN',0)} Y={signals.get('YELLOW',0)} B={signals.get('BLUE',0)} R={signals.get('RED',0)}")
    print(f"  Daily Quads: L={quads.get('Leading',0)} W={quads.get('Weakening',0)} I={quads.get('Improving',0)} Lg={quads.get('Lagging',0)}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
