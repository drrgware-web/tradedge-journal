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
    os.system("pip install pandas numpy requests --break-system-packages -q")
    import pandas as pd
    import numpy as np

import requests

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
#  RSI CALCULATION
# ════════════════════════════════════════════════════════════════

def calc_rsi(closes: list, period: int = 14) -> float:
    """Calculate RSI from closing prices. Returns last RSI value or None."""
    if len(closes) < period + 1:
        return None
    prices = np.array(closes, dtype=float)
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    # Wilder's smoothing (EMA-style)
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


# ════════════════════════════════════════════════════════════════
#  SWEET SPOT SCORING
# ════════════════════════════════════════════════════════════════

def compute_sweet_spot(m_rsi, w_rsi, d_rsi, signal, score):
    """
    Sweet Spot Score (0-10):
      - Monthly RSI 55-75 (uptrend, not exhausted)  → +2
      - Weekly RSI 45-65 (momentum building)         → +2
      - Daily RSI 35-55 (pullback zone)              → +2
      - RRM signal GREEN/YELLOW                      → +2
      - RRM score 4-5                                → +2
    """
    pts = 0

    # Monthly RSI sweet zone
    if m_rsi is not None:
        if 55 <= m_rsi <= 75:
            pts += 2
        elif 45 <= m_rsi <= 80:
            pts += 1
        # Penalty for extreme
        if m_rsi > 85:
            pts -= 1

    # Weekly RSI sweet zone
    if w_rsi is not None:
        if 45 <= w_rsi <= 65:
            pts += 2
        elif 40 <= w_rsi <= 75:
            pts += 1
        if w_rsi > 80:
            pts -= 1

    # Daily RSI pullback zone
    if d_rsi is not None:
        if 35 <= d_rsi <= 55:
            pts += 2  # Ideal dip-buy zone
        elif 30 <= d_rsi <= 60:
            pts += 1
        if d_rsi > 75:
            pts -= 1  # Extended

    # RRM alignment bonus
    if signal in ("GREEN", "YELLOW"):
        pts += 2
    elif signal == "BLUE":
        pts += 1

    # Score bonus
    if score >= 4:
        pts += 2
    elif score >= 3:
        pts += 1

    return max(0, min(10, pts))


def get_sweet_label(sweet_score):
    """Label for sweet spot score."""
    if sweet_score >= 8:
        return "PERFECT"
    if sweet_score >= 6:
        return "SWEET"
    if sweet_score >= 4:
        return "OK"
    if sweet_score >= 2:
        return "WAIT"
    return "AVOID"


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

# Worker URL for Yahoo Finance proxy (avoids Yahoo blocking GitHub Actions IPs)
WORKER_URL = os.environ.get("WORKER_URL", "https://spring-fire-41a0.drrgware.workers.dev")

def fetch_closes(ticker: str, tf_key: str) -> list:
    """Fetch closing prices via Cloudflare worker."""
    tf = TIMEFRAMES[tf_key]
    try:
        # For index tickers (^NSEI etc) or tickers with dots, use yahoo-proxy with raw ticker
        # For stock symbols (RELIANCE), use yahoo-chart which auto-adds .NS
        is_index = ticker.startswith("^") or ticker.startswith("%5E")
        is_ns = ticker.endswith(".NS")

        if is_index or is_ns:
            # Use yahoo-proxy with raw ticker (no .NS manipulation)
            resp = requests.post(
                WORKER_URL,
                json={"ticker": ticker, "range": tf["range"], "interval": tf["interval"]},
                headers={"X-Kite-Action": "yahoo-proxy", "Content-Type": "application/json"},
                timeout=15,
            )
            if not resp.ok:
                return []
            data = resp.json()
            result = data.get("chart", {}).get("result", [{}])[0]
            closes_raw = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
            return [float(c) for c in closes_raw if c is not None and not math.isnan(float(c))]
        else:
            # Use yahoo-chart for stocks (auto-adds .NS, returns pre-parsed)
            resp = requests.post(
                WORKER_URL,
                json={"symbol": ticker.replace(".NS", ""), "range": tf["range"], "interval": tf["interval"]},
                headers={"X-Kite-Action": "yahoo-chart", "Content-Type": "application/json"},
                timeout=15,
            )
            if resp.ok:
                data = resp.json()
                prices = data.get("prices", [])
                if prices:
                    return [p["c"] for p in prices if p.get("c") is not None]
            return []
    except Exception as e:
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
    """Process a single stock: fetch data for all timeframes, compute RRM + RSI."""
    ticker = symbol + ".NS"
    result = {"symbol": symbol}

    for tf_key, tf_label in [("d", "d"), ("w", "w"), ("m", "m")]:
        bench_closes = bench.get(tf_key, [])
        if not bench_closes:
            result[f"{tf_label}_ratio"] = None
            result[f"{tf_label}_mom"] = None
            result[f"{tf_label}_quad"] = "Unknown"
            result[f"{tf_label}_rsi"] = None
            continue

        closes = fetch_closes(ticker, tf_key)
        if len(closes) < TIMEFRAMES[tf_key]["min_bars"]:
            result[f"{tf_label}_ratio"] = None
            result[f"{tf_label}_mom"] = None
            result[f"{tf_label}_quad"] = "Unknown"
            result[f"{tf_label}_rsi"] = None
            continue

        ratio, mom = calc_jdk(closes, bench_closes)
        result[f"{tf_label}_ratio"] = ratio
        result[f"{tf_label}_mom"] = mom
        result[f"{tf_label}_quad"] = get_quadrant(ratio, mom)

        # RSI for this timeframe
        result[f"{tf_label}_rsi"] = calc_rsi(closes, 14)

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

    # Sweet Spot
    result["sweet"] = compute_sweet_spot(
        result.get("m_rsi"),
        result.get("w_rsi"),
        result.get("d_rsi"),
        result["signal"],
        result["score"],
    )
    result["sweet_label"] = get_sweet_label(result["sweet"])

    return result


# ════════════════════════════════════════════════════════════════
#  LOAD STOCK UNIVERSE
# ════════════════════════════════════════════════════════════════

def load_symbols(top_n=None, specific=None):
    """Load stock symbols from scanner_results.json + nse_symbols.json (merged)."""
    stocks = []
    seen_symbols = set()

    if specific:
        return [{"symbol": s.strip().upper(), "name": s.strip().upper()} for s in specific.split(",")]

    # 1. Load scanner_results.json first (has rich metadata: sector, market cap, O'Neil grade)
    if SCANNER_FILE.exists():
        try:
            data = json.loads(SCANNER_FILE.read_text())
            all_stocks = data.get("all_stocks", [])
            if all_stocks:
                for s in all_stocks:
                    sym = s.get("symbol", "").strip().upper()
                    if not sym or sym in seen_symbols:
                        continue
                    seen_symbols.add(sym)
                    stocks.append({
                        "symbol": sym,
                        "name": s.get("name", sym),
                        "sector": s.get("sector", ""),
                        "cmp": s.get("close") or s.get("cmp", 0),
                        "change_pct": s.get("change_pct", 0),
                        "market_cap_cr": s.get("market_cap_cr") or 0,
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

    # 2. Merge nse_symbols.json — add any symbols NOT already in scanner_results
    if SYMBOLS_FILE.exists():
        try:
            symbols = json.loads(SYMBOLS_FILE.read_text())
            added = 0
            for s in symbols:
                if isinstance(s, str):
                    sym = s.strip().upper()
                    name = sym
                    sector = "Unknown"
                elif isinstance(s, dict):
                    sym = s.get("symbol", "").strip().upper()
                    name = s.get("name", sym)
                    sector = s.get("sector", "Unknown")
                else:
                    continue
                if not sym or sym in seen_symbols:
                    continue
                seen_symbols.add(sym)
                stocks.append({"symbol": sym, "name": name, "sector": sector})
                added += 1
            if added > 0:
                print(f"  Merged +{added} new symbols from nse_symbols.json (total: {len(stocks)})")
            else:
                print(f"  nse_symbols.json: no new symbols to add (all {len(symbols)} already in scanner_results)")
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

    # Sort by sweet spot (descending), then score, then signal priority
    signal_order = {"GREEN": 0, "YELLOW": 1, "BLUE": 2, "RED": 3}
    results.sort(key=lambda x: (-x.get("sweet", 0), -x.get("score", 0), signal_order.get(x.get("signal", "RED"), 9)))

    print(f"  Done: {len(results)} processed, {failed} failed")

    # 4. Count stats
    quads = {"Leading": 0, "Weakening": 0, "Improving": 0, "Lagging": 0, "Unknown": 0}
    signals = {"GREEN": 0, "YELLOW": 0, "BLUE": 0, "RED": 0}
    sweets = {"PERFECT": 0, "SWEET": 0, "OK": 0, "WAIT": 0, "AVOID": 0}
    for r in results:
        quads[r.get("d_quad", "Unknown")] = quads.get(r.get("d_quad", "Unknown"), 0) + 1
        signals[r.get("signal", "RED")] = signals.get(r.get("signal", "RED"), 0) + 1
        sweets[r.get("sweet_label", "AVOID")] = sweets.get(r.get("sweet_label", "AVOID"), 0) + 1

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
            "sweet_distribution": sweets,
            "version": "2.0",
        },
        "stocks": results,
    }

    DATA_DIR.mkdir(exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=None, separators=(",", ":")))
    file_size = OUTPUT_FILE.stat().st_size / 1024
    elapsed = time.time() - start_time

    print(f"\n{'=' * 60}")
    print(f"  ✓ RRM Scanner v2.0 complete!")
    print(f"  Stocks: {len(results)} | Failed: {failed}")
    print(f"  File: {OUTPUT_FILE} ({file_size:.0f} KB)")
    print(f"  Time: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  Signals: G={signals.get('GREEN',0)} Y={signals.get('YELLOW',0)} B={signals.get('BLUE',0)} R={signals.get('RED',0)}")
    print(f"  Sweet Spot: PERFECT={sweets.get('PERFECT',0)} SWEET={sweets.get('SWEET',0)} OK={sweets.get('OK',0)} WAIT={sweets.get('WAIT',0)} AVOID={sweets.get('AVOID',0)}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
