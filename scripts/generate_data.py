#!/usr/bin/env python3
"""
TradEdge Scanner — Unified Data Pipeline v1.0
================================================
Generates all JSON files needed by the frontend dashboard and stock pages.

Output files:
  data/scanner_results.json    — Full stock universe with indicators + scores
  data/scan_runs.json          — Scan results per preset/custom scan
  data/stock_details/          — Per-stock detail JSON (for stock.html)
  data/circuit_limits.json     — Circuit limit data
  data/earnings_calendar.json  — Upcoming earnings
  data/scan_config.json        — Saved scan configurations

Usage:
  python scripts/generate_data.py                    # Full run (all stocks, all scans)
  python scripts/generate_data.py --quick            # Quick mode (skip fundamentals)
  python scripts/generate_data.py --stock RELIANCE   # Generate detail for one stock
"""

import json
import os
import sys
import time
import argparse
import traceback
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
STOCK_DETAIL_DIR = DATA_DIR / "stock_details"
SYMBOL_FILE = SCRIPT_DIR / "nse_symbols.json"

sys.path.insert(0, str(SCRIPT_DIR))

from chartink_parser import ChartInkParser, PRESET_SCANS, IndicatorEngine
from catalyst_engine import CatalystDetector, SCAN_CATEGORIES, UniversalFilter
from oneil_scorer import ONeilScorer, GuruRatingEngine
from surveillance_checker import SurveillanceChecker
from circuit_earnings_tracker import CircuitLimitTracker, EarningsTracker, enrich_with_alerts

BATCH_SIZE = 50
BATCH_DELAY = 2.0
SKIP_FUNDAMENTALS = os.environ.get("SKIP_FUNDAMENTALS", "").lower() == "true"


def load_symbols() -> list[dict]:
    if not SYMBOL_FILE.exists():
        print("❌ nse_symbols.json not found")
        sys.exit(1)
    with open(SYMBOL_FILE) as f:
        return json.load(f)


def safe_float(val, default=None):
    try:
        if val is not None and not (isinstance(val, float) and np.isnan(val)):
            return round(float(val), 2)
    except (TypeError, ValueError):
        pass
    return default


def compute_technicals(df: pd.DataFrame) -> dict:
    """Compute all technical indicators for a stock."""
    if df is None or len(df) < 50:
        return {}

    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]
    ind = IndicatorEngine()

    latest = float(close.iloc[-1])
    prev = float(close.iloc[-2]) if len(close) > 1 else latest

    # RSI
    rsi_s = ind.rsi(close)
    rsi_val = safe_float(rsi_s.iloc[-1])

    # MACD
    macd_data = ind.macd(close)
    macd_hist = safe_float(macd_data["histogram"].iloc[-1])
    macd_hist_prev = safe_float(macd_data["histogram"].iloc[-2]) if len(macd_data["histogram"]) > 1 else 0
    macd_crossover = "none"
    if macd_hist and macd_hist_prev is not None:
        if macd_hist > 0 and macd_hist_prev <= 0: macd_crossover = "bullish_crossover"
        elif macd_hist < 0 and macd_hist_prev >= 0: macd_crossover = "bearish_crossover"
        elif macd_hist > 0: macd_crossover = "bullish"
        elif macd_hist < 0: macd_crossover = "bearish"

    # EMAs
    ema9 = ind.ema(close, 9)
    ema21 = ind.ema(close, 21)
    ema50 = ind.ema(close, 50)
    ema200 = ind.ema(close, 200)

    def cross_signal(fast, slow):
        if len(fast) < 2 or len(slow) < 2: return "none"
        curr = float(fast.iloc[-1] - slow.iloc[-1])
        prev = float(fast.iloc[-2] - slow.iloc[-2])
        if curr > 0 and prev <= 0: return "golden_cross"
        if curr < 0 and prev >= 0: return "death_cross"
        return "bullish" if curr > 0 else "bearish"

    # 52W
    high_52w = float(high.tail(252).max()) if len(high) >= 252 else float(high.max())
    low_52w = float(low.tail(252).min()) if len(low) >= 252 else float(low.min())
    pct_high = round((latest - high_52w) / high_52w * 100, 2)
    pct_low = round((latest - low_52w) / low_52w * 100, 2)

    breakout = "none"
    if pct_high >= 0: breakout = "new_52w_high"
    elif pct_high >= -2: breakout = "near_52w_high"
    if pct_low <= 0: breakout = "new_52w_low"
    elif pct_low <= 2: breakout = "near_52w_low"

    # Volume
    vol_avg = float(volume.tail(20).mean()) if len(volume) >= 20 else 1
    vol_ratio = round(float(volume.iloc[-1]) / vol_avg, 2) if vol_avg > 0 else 0
    vol_signal = "normal"
    if vol_ratio >= 3: vol_signal = "extreme_spike"
    elif vol_ratio >= 2: vol_signal = "spike"
    elif vol_ratio >= 1.5: vol_signal = "above_avg"
    elif vol_ratio <= 0.5: vol_signal = "dry"

    # Bollinger
    bb = ind.bollinger_bands(close)
    bb_upper = safe_float(bb["upper"].iloc[-1])
    bb_lower = safe_float(bb["lower"].iloc[-1])
    bb_signal = "neutral"
    if bb_upper and latest >= bb_upper: bb_signal = "upper_breakout"
    elif bb_lower and latest <= bb_lower: bb_signal = "lower_breakout"

    # Composite score
    score = 0
    if rsi_val:
        if rsi_val <= 30: score += 1
        elif rsi_val >= 70: score -= 1
        elif rsi_val >= 60: score += 1
        elif rsi_val <= 40: score -= 1
    if macd_crossover in ("bullish_crossover", "bullish"): score += 1
    elif macd_crossover in ("bearish_crossover", "bearish"): score -= 1
    ema_9_21 = cross_signal(ema9, ema21)
    ema_50_200 = cross_signal(ema50, ema200)
    if ema_9_21 in ("golden_cross", "bullish"): score += 1
    elif ema_9_21 in ("death_cross", "bearish"): score -= 1
    if ema_50_200 in ("golden_cross", "bullish"): score += 1
    elif ema_50_200 in ("death_cross", "bearish"): score -= 1
    if breakout in ("new_52w_high", "near_52w_high"): score += 1
    elif breakout in ("new_52w_low", "near_52w_low"): score -= 1
    if vol_signal in ("spike", "extreme_spike"): score += 1
    if bb_signal == "upper_breakout": score += 1
    elif bb_signal == "lower_breakout": score -= 1

    # Returns
    def ret(n):
        if len(close) >= n:
            p = float(close.iloc[-n])
            return round((latest - p) / p * 100, 2) if p > 0 else None
        return None

    return {
        "price": round(latest, 2),
        "change_pct": round((latest - prev) / prev * 100, 2) if prev > 0 else 0,
        "rsi": rsi_val,
        "composite_score": score,
        "macd": {"crossover": macd_crossover, "histogram": safe_float(macd_hist)},
        "ema": {
            "cross_9_21": ema_9_21,
            "cross_50_200": ema_50_200,
            "ema50": safe_float(ema50.iloc[-1]),
            "ema200": safe_float(ema200.iloc[-1]),
        },
        "breakout": {
            "signal": breakout,
            "high_52w": round(high_52w, 2),
            "low_52w": round(low_52w, 2),
            "pct_from_high": pct_high,
            "pct_from_low": pct_low,
        },
        "volume": {"ratio": vol_ratio, "signal": vol_signal, "latest": int(volume.iloc[-1]), "avg_20d": int(vol_avg)},
        "bollinger": {"signal": bb_signal},
        "returns": {"1d": ret(1), "1w": ret(5), "1m": ret(21), "3m": ret(63)},
    }


def compute_fundamentals(info: dict) -> dict:
    """Extract fundamentals from yfinance info."""
    mcap = info.get("marketCap")
    return {
        "market_cap_cr": round(mcap / 1e7, 2) if mcap else None,
        "mcap_category": (
            "mega_cap" if mcap and mcap/1e7 >= 100000 else
            "large_cap" if mcap and mcap/1e7 >= 20000 else
            "mid_cap" if mcap and mcap/1e7 >= 5000 else
            "small_cap" if mcap and mcap/1e7 >= 1000 else
            "micro_cap" if mcap else "unknown"
        ),
        "pe_ratio": safe_float(info.get("trailingPE") or info.get("forwardPE")),
        "pb_ratio": safe_float(info.get("priceToBook")),
        "eps": safe_float(info.get("trailingEps")),
        "book_value": safe_float(info.get("bookValue")),
        "roe": safe_float((info.get("returnOnEquity") or 0) * 100) if info.get("returnOnEquity") else None,
        "debt_to_equity": safe_float(info.get("debtToEquity")),
        "dividend_yield": safe_float((info.get("dividendYield") or 0) * 100) if info.get("dividendYield") else None,
        "revenue_growth": safe_float((info.get("revenueGrowth") or 0) * 100) if info.get("revenueGrowth") else None,
        "profit_margin": safe_float((info.get("profitMargins") or 0) * 100) if info.get("profitMargins") else None,
        "beta": safe_float(info.get("beta")),
        "sales_cr": round(info["totalRevenue"] / 1e7, 2) if info.get("totalRevenue") else None,
        "shares_float_cr": round(info["floatShares"] / 1e7, 2) if info.get("floatShares") else None,
    }


def compute_fund_holdings(info: dict) -> dict:
    inst = info.get("heldPercentInstitutions")
    insider = info.get("heldPercentInsiders")
    return {
        "no_of_funds": None,  # Not available from yfinance directly
        "fund_shares_change_pct": None,
        "institutional_pct": round(inst * 100, 2) if inst else None,
        "promoter_pct": round(insider * 100, 2) if insider else None,
        "shares_float_cr": round(info["floatShares"] / 1e7, 2) if info.get("floatShares") else None,
    }


# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════

def run_full_pipeline(quick=False, single_stock=None):
    print("=" * 60)
    print("  TradEdge Data Pipeline v1.0")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Mode: {'QUICK (no fundamentals)' if quick else 'FULL'}")
    print("=" * 60)

    symbols_list = load_symbols()

    # Filter to single stock if requested
    if single_stock:
        symbols_list = [s for s in symbols_list if s["symbol"].upper() == single_stock.upper()]
        if not symbols_list:
            print(f"❌ Stock {single_stock} not found in universe")
            return

    total = len(symbols_list)
    print(f"\n📊 Processing {total} stocks...")

    # Init engines
    parser = ChartInkParser()
    catalyst_detector = CatalystDetector()
    oneil_scorer = ONeilScorer()
    guru_engine = GuruRatingEngine()
    surveillance = SurveillanceChecker()
    circuit_tracker = CircuitLimitTracker()
    earnings_tracker = EarningsTracker()

    # Collect all scans to run
    all_scans = {}
    for scan_id, scan in SCAN_CATEGORIES.items():
        all_scans[scan_id] = {"clause": scan["clause"], "name": scan["name"], "matches": []}
    for preset_id, clause in PRESET_SCANS.items():
        if preset_id not in all_scans:
            all_scans[f"preset_{preset_id}"] = {"clause": clause, "name": preset_id.replace("_"," ").title(), "matches": []}

    # Results accumulators
    all_stocks = []
    stock_details = {}
    scan_start = time.time()

    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch = symbols_list[batch_start:batch_end]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        elapsed = time.time() - scan_start
        pct = batch_start / total * 100

        print(f"\n  [{pct:5.1f}%] Batch {batch_num}/{total_batches} ({len(all_stocks)} done, {elapsed:.0f}s)")

        yf_symbols = [f"{s['symbol']}.NS" for s in batch]

        # Download OHLCV
        try:
            if len(yf_symbols) == 1:
                # Single stock — use Ticker directly (more reliable)
                print(f"    Fetching {yf_symbols[0]}...")
                ticker = yf.Ticker(yf_symbols[0])
                data = ticker.history(period="1y")
                if data.empty:
                    print(f"    ✗ No data for {yf_symbols[0]}")
                    continue
            else:
                data = yf.download(yf_symbols, period="1y", group_by="ticker", auto_adjust=True, threads=True, progress=False)
        except Exception as e:
            print(f"    ✗ Batch failed: {e}")
            continue

        for sym_info in batch:
            symbol = sym_info["symbol"]
            yf_sym = f"{symbol}.NS"

            try:
                # Extract stock DF
                if len(yf_symbols) == 1:
                    stock_df = data.copy()
                else:
                    if yf_sym in data.columns.get_level_values(0):
                        stock_df = data[yf_sym].dropna(how="all")
                    else:
                        print(f"    ✗ {symbol}: not in download results")
                        continue

                if stock_df.empty or len(stock_df) < 20:
                    print(f"    ✗ {symbol}: insufficient data ({len(stock_df)} bars)")
                    continue
                
                print(f"    ✓ {symbol}: {len(stock_df)} bars, last close ₹{float(stock_df['Close'].iloc[-1]):.2f}")

                # Technicals
                tech = compute_technicals(stock_df)
                if not tech:
                    continue

                # Fundamentals (optional)
                info = {}
                fund_data = {}
                funda = {}
                if not quick and not SKIP_FUNDAMENTALS:
                    try:
                        ticker = yf.Ticker(yf_sym)
                        info = ticker.info or {}
                        funda = compute_fundamentals(info)
                        fund_data = compute_fund_holdings(info)
                    except Exception:
                        pass

                # Catalysts
                catalysts = catalyst_detector.detect_all(symbol, stock_df, info)

                # Run all scans
                matched_scans = []
                for scan_id, scan in all_scans.items():
                    try:
                        if parser.evaluate(scan["clause"], stock_df):
                            matched_scans.append(scan_id)
                            scan["matches"].append(symbol)
                    except Exception:
                        pass

                # Circuit info
                circuit_tracker.update_from_yfinance([symbol])
                circuit_info = circuit_tracker.get_circuit_info(symbol)

                # Build stock record
                stock_record = {
                    "symbol": symbol,
                    "name": sym_info.get("name", symbol),
                    "sector": info.get("sector") or sym_info.get("sector", "Unknown"),
                    "industry": info.get("industry", "Unknown"),
                    **tech,
                    "fundamentals": funda,
                    "fund_holdings": fund_data,
                    "catalysts": [c.to_dict() for c in catalysts[:5]],
                    "matched_scans": matched_scans,
                    "circuit": circuit_info.to_dict(),
                    "earnings_alert": earnings_tracker.get_earnings_alert(symbol),
                }

                all_stocks.append(stock_record)

                # Generate detailed stock JSON (for stock.html)
                # Generate for ALL modes (not just full) — price_history is essential for charts
                try:
                    # Price history for HLC chart + P&F/Renko
                    price_hist = []
                    for idx, row in stock_df.tail(250).iterrows():
                        dt = str(idx)[:10] if hasattr(idx, 'strftime') else str(idx)[:10]
                        price_hist.append({
                            "date": dt,
                            "o": safe_float(row.get("Open", row.get("open"))),
                            "h": safe_float(row.get("High", row.get("high"))),
                            "l": safe_float(row.get("Low", row.get("low"))),
                            "c": safe_float(row.get("Close", row.get("close"))),
                            "v": safe_float(row.get("Volume", row.get("volume"))),
                        })

                    # O'Neil, Guru, Surveillance — only in full mode
                    oneil_data = {}
                    guru_data = []
                    surv_data = {}
                    if not quick and info:
                        try:
                            oneil_result = oneil_scorer.score(stock_df, info)
                            oneil_data = oneil_result.to_dict()
                        except: pass
                        try:
                            guru_ratings = guru_engine.rate_all(stock_df, info)
                            guru_data = [r.to_dict() for r in guru_ratings]
                        except: pass
                        try:
                            surv_result = surveillance.check(symbol, stock_df, info)
                            surv_data = surv_result.to_dict()
                        except: pass

                    detail = {
                        **stock_record,
                        "oneil": oneil_data,
                        "guru_ratings": guru_data,
                        "surveillance": surv_data,
                        "price_history": price_hist,
                            "prev_fundamentals": {},  # Populated when historical data available
                            "yoy_fundamentals": {},   # Populated when historical data available
                            "management": [],  # Would need separate data source
                            "major_shareholders": [],  # Would need separate data source
                            "quarterly_results": [],  # Would need screener.in or moneycontrol
                            "ownership": {},  # Would need NSE corporate filings
                        }

                        # Save per-stock JSON
                        STOCK_DETAIL_DIR.mkdir(parents=True, exist_ok=True)
                        with open(STOCK_DETAIL_DIR / f"{symbol}.json", "w") as sf:
                            json.dump(detail, sf, indent=2, default=str)

                    except Exception as e:
                        pass

            except Exception as e:
                continue

        if batch_end < total:
            time.sleep(BATCH_DELAY)

    # ── Sort results ──
    all_stocks.sort(key=lambda x: x.get("composite_score", 0), reverse=True)

    # ── Save scanner_results.json ──
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    scanner_output = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "total_scanned": len(all_stocks),
            "version": "1.0",
            "elapsed_seconds": round(time.time() - scan_start, 1),
        },
        "stocks": all_stocks,
    }
    with open(DATA_DIR / "scanner_results.json", "w") as f:
        json.dump(scanner_output, f, indent=2, default=str)

    # ── Save scan_runs.json ──
    scan_output = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "total_scans": len(all_scans),
        },
        "scans": {},
    }
    for scan_id, scan in all_scans.items():
        if scan["matches"]:
            scan_output["scans"][scan_id] = {
                "name": scan["name"],
                "match_count": len(scan["matches"]),
                "symbols": scan["matches"],
            }
    with open(DATA_DIR / "scan_runs.json", "w") as f:
        json.dump(scan_output, f, indent=2, default=str)

    # ── Save circuit + earnings ──
    circuit_tracker.save()
    earnings_tracker.save()

    # ── Run Sector / Theme Analysis ──
    if not single_stock:
        try:
            from sector_theme_engine import run_sector_analysis
            print("\n  🌐 Running sector/theme analysis...")
            run_sector_analysis()
        except Exception as e:
            print(f"  ⚠ Sector analysis failed: {e}")

    elapsed = time.time() - scan_start
    print(f"\n{'=' * 60}")
    print(f"  ✅ Pipeline complete in {elapsed:.0f}s")
    print(f"  📁 data/scanner_results.json — {len(all_stocks)} stocks")
    print(f"  📁 data/scan_runs.json — {len(scan_output['scans'])} scans with matches")
    if not quick:
        detail_count = len(list(STOCK_DETAIL_DIR.glob("*.json"))) if STOCK_DETAIL_DIR.exists() else 0
        print(f"  📁 data/stock_details/ — {detail_count} stock detail files")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="TradEdge Data Pipeline")
    ap.add_argument("--quick", action="store_true", help="Skip fundamentals (faster)")
    ap.add_argument("--stock", type=str, help="Generate data for single stock only")
    args = ap.parse_args()
    run_full_pipeline(quick=args.quick, single_stock=args.stock)
