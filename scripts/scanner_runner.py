#!/usr/bin/env python3
"""
TradEdge Scanner Runner v1.0
==============================
Integrates the ChartInk clause parser with stock data to run scans
against the full 2803-stock NSE universe.

Manages:
  - Custom scan clauses (paste from ChartInk)
  - Pre-built preset scans
  - Scan configurations (saved scans with names/tags)
  - Fund holding data (No. of MFs, shares held, % change)
  - Output: data/scan_runs.json (results per scan)

Usage:
  python scripts/scanner_runner.py                          # Run all configured scans
  python scripts/scanner_runner.py --preset rsi_oversold    # Run a preset
  python scripts/scanner_runner.py --clause "( cash ( ... ) )"  # Run a custom clause
"""

import json
import os
import sys
import time
import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

# Add scripts dir to path
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from chartink_parser import ChartInkParser, PRESET_SCANS, IndicatorEngine

DATA_DIR = SCRIPT_DIR.parent / "data"
SCAN_CONFIG_FILE = DATA_DIR / "scan_config.json"
SCAN_RESULTS_FILE = DATA_DIR / "scan_runs.json"
SCAN_HISTORY_DIR = DATA_DIR / "scan_history"
SYMBOL_FILE = SCRIPT_DIR / "nse_symbols.json"

BATCH_SIZE = 50
BATCH_DELAY = 1.5


# ═══════════════════════════════════════════════════════════════════════════════
# SCAN CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

def load_scan_config() -> dict:
    """Load saved scan configurations."""
    if SCAN_CONFIG_FILE.exists():
        with open(SCAN_CONFIG_FILE) as f:
            return json.load(f)
    
    # Default config with some starter scans
    default_config = {
        "version": "1.0",
        "scans": [
            {
                "id": "preset_rsi_oversold",
                "name": "RSI Oversold (< 30)",
                "clause": PRESET_SCANS["rsi_oversold"],
                "type": "preset",
                "tags": ["momentum", "reversal"],
                "enabled": True,
            },
            {
                "id": "preset_rsi_overbought",
                "name": "RSI Overbought (> 70)",
                "clause": PRESET_SCANS["rsi_overbought"],
                "type": "preset",
                "tags": ["momentum", "reversal"],
                "enabled": True,
            },
            {
                "id": "preset_golden_cross",
                "name": "Golden Cross (50/200 EMA)",
                "clause": PRESET_SCANS["golden_cross_50_200"],
                "type": "preset",
                "tags": ["trend", "crossover"],
                "enabled": True,
            },
            {
                "id": "preset_macd_bullish",
                "name": "MACD Bullish Crossover",
                "clause": PRESET_SCANS["macd_bullish_crossover"],
                "type": "preset",
                "tags": ["momentum", "crossover"],
                "enabled": True,
            },
            {
                "id": "preset_volume_spike",
                "name": "Volume 3x Spike",
                "clause": PRESET_SCANS["volume_3x_spike"],
                "type": "preset",
                "tags": ["volume", "breakout"],
                "enabled": True,
            },
            {
                "id": "preset_minervini",
                "name": "Minervini Trend Template",
                "clause": PRESET_SCANS["trend_template_minervini"],
                "type": "preset",
                "tags": ["trend", "minervini"],
                "enabled": True,
            },
            {
                "id": "preset_52w_high",
                "name": "Near 52W High",
                "clause": PRESET_SCANS["52w_high_breakout"],
                "type": "preset",
                "tags": ["breakout", "momentum"],
                "enabled": True,
            },
            {
                "id": "preset_supertrend_pos",
                "name": "SuperTrend Positive",
                "clause": PRESET_SCANS["supertrend_positive"],
                "type": "preset",
                "tags": ["trend"],
                "enabled": True,
            },
        ],
    }
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(SCAN_CONFIG_FILE, "w") as f:
        json.dump(default_config, f, indent=2)
    
    return default_config


def add_custom_scan(name: str, clause: str, tags: list[str] = None) -> str:
    """Add a custom scan to the config. Returns the scan ID."""
    config = load_scan_config()
    scan_id = f"custom_{int(time.time())}"
    
    config["scans"].append({
        "id": scan_id,
        "name": name,
        "clause": clause,
        "type": "custom",
        "tags": tags or [],
        "enabled": True,
    })
    
    with open(SCAN_CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)
    
    return scan_id


# ═══════════════════════════════════════════════════════════════════════════════
# FUND HOLDING DATA
# ═══════════════════════════════════════════════════════════════════════════════

def get_fund_holdings(ticker_info: dict) -> dict:
    """
    Extract fund/institutional holding data from yfinance ticker info.
    
    Key metrics:
      - no_of_funds: Number of mutual funds holding the stock
      - fund_shares_pct: % of shares held by funds
      - fund_shares_change_pct: QoQ change in fund holdings
      - institutional_pct: Total institutional holding %
      - promoter_pct: Promoter holding %
    """
    holdings = {
        "no_of_funds": None,
        "fund_shares_cr": None,
        "fund_shares_change_pct": None,
        "institutional_pct": None,
        "promoter_pct": None,
        "public_pct": None,
        "shares_float_cr": None,
    }
    
    try:
        # yfinance provides some of these
        holders = ticker_info.get("heldPercentInstitutions")
        if holders:
            holdings["institutional_pct"] = round(holders * 100, 2)
        
        insider_pct = ticker_info.get("heldPercentInsiders")
        if insider_pct:
            holdings["promoter_pct"] = round(insider_pct * 100, 2)
        
        float_shares = ticker_info.get("floatShares")
        if float_shares:
            holdings["shares_float_cr"] = round(float_shares / 1e7, 2)
        
        # No. of fund holders
        fund_holders = ticker_info.get("fundHolders")
        if isinstance(fund_holders, (int, float)):
            holdings["no_of_funds"] = int(fund_holders)
    except Exception:
        pass
    
    return holdings


def get_fundamental_snapshot(ticker_info: dict) -> dict:
    """
    Extract fundamental data for the stock card.
    
    Includes: MCap, Sales, Book Value, P/E, Alpha, Beta, 
    Debt/Equity, Yield, U/D Volume Ratio
    """
    fundamentals = {
        "market_cap_cr": None,
        "sales_cr": None,
        "pe_ratio": None,
        "pb_ratio": None,
        "book_value": None,
        "eps": None,
        "dividend_yield": None,
        "roe": None,
        "roce": None,
        "debt_to_equity": None,
        "alpha": None,
        "beta": None,
        "revenue_growth": None,
        "profit_margin": None,
    }
    
    try:
        mcap = ticker_info.get("marketCap")
        if mcap:
            fundamentals["market_cap_cr"] = round(mcap / 1e7, 2)
        
        revenue = ticker_info.get("totalRevenue")
        if revenue:
            fundamentals["sales_cr"] = round(revenue / 1e7, 2)
        
        fundamentals["pe_ratio"] = _round(ticker_info.get("trailingPE") or ticker_info.get("forwardPE"))
        fundamentals["pb_ratio"] = _round(ticker_info.get("priceToBook"))
        fundamentals["book_value"] = _round(ticker_info.get("bookValue"))
        fundamentals["eps"] = _round(ticker_info.get("trailingEps"))
        
        dy = ticker_info.get("dividendYield")
        if dy:
            fundamentals["dividend_yield"] = round(dy * 100, 2)
        
        roe = ticker_info.get("returnOnEquity")
        if roe:
            fundamentals["roe"] = round(roe * 100, 2)
        
        fundamentals["debt_to_equity"] = _round(ticker_info.get("debtToEquity"))
        fundamentals["beta"] = _round(ticker_info.get("beta"))
        
        rg = ticker_info.get("revenueGrowth")
        if rg:
            fundamentals["revenue_growth"] = round(rg * 100, 2)
        
        pm = ticker_info.get("profitMargins")
        if pm:
            fundamentals["profit_margin"] = round(pm * 100, 2)
        
    except Exception:
        pass
    
    return fundamentals


def _round(val, decimals=2):
    if val is not None:
        try:
            return round(float(val), decimals)
        except (TypeError, ValueError):
            pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN SCANNER RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def run_scans(
    scan_ids: list[str] = None,
    custom_clause: str = None,
    preset_name: str = None,
    fetch_fundamentals: bool = True,
):
    """
    Run scans against the full stock universe.
    
    Args:
        scan_ids: List of scan IDs from config to run (None = all enabled)
        custom_clause: A one-off custom clause to evaluate
        preset_name: Name of a preset scan to run
        fetch_fundamentals: Whether to fetch fund holdings + fundamentals
    """
    print("=" * 60)
    print("  TradEdge Scanner Runner v1.0")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Load symbols
    if not SYMBOL_FILE.exists():
        print("❌ nse_symbols.json not found. Run build_symbol_list.py first.")
        return
    
    with open(SYMBOL_FILE) as f:
        symbols_list = json.load(f)
    
    total = len(symbols_list)
    print(f"\n📊 Universe: {total} NSE stocks")
    
    # Determine which scans to run
    scans_to_run = []
    
    if custom_clause:
        scans_to_run.append({
            "id": "custom_adhoc",
            "name": "Ad-hoc Scan",
            "clause": custom_clause,
            "type": "custom",
            "tags": [],
        })
    elif preset_name:
        clause = PRESET_SCANS.get(preset_name)
        if not clause:
            print(f"❌ Unknown preset: {preset_name}")
            print(f"   Available: {', '.join(PRESET_SCANS.keys())}")
            return
        scans_to_run.append({
            "id": f"preset_{preset_name}",
            "name": preset_name.replace("_", " ").title(),
            "clause": clause,
            "type": "preset",
            "tags": [],
        })
    else:
        config = load_scan_config()
        for scan in config["scans"]:
            if scan.get("enabled", True):
                if scan_ids is None or scan["id"] in scan_ids:
                    scans_to_run.append(scan)
    
    if not scans_to_run:
        print("⚠ No scans to run.")
        return
    
    print(f"🔍 Running {len(scans_to_run)} scan(s):")
    for s in scans_to_run:
        print(f"   • {s['name']}")
    
    # Initialize parser
    parser = ChartInkParser()
    
    # Initialize results
    scan_results = {s["id"]: {"scan": s, "matches": [], "count": 0} for s in scans_to_run}
    stock_fundamentals = {}
    
    # Process stocks in batches
    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    scan_start = time.time()
    processed = 0
    
    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch = symbols_list[batch_start:batch_end]
        batch_num = batch_start // BATCH_SIZE + 1
        elapsed = time.time() - scan_start
        pct = batch_start / total * 100
        
        print(f"\n  [{pct:5.1f}%] Batch {batch_num}/{total_batches} ({elapsed:.0f}s)")
        
        # Batch download OHLCV
        yf_symbols = [f"{s['symbol']}.NS" for s in batch]
        
        try:
            data = yf.download(
                yf_symbols,
                period="1y",
                group_by="ticker",
                auto_adjust=True,
                threads=True,
                progress=False,
            )
        except Exception as e:
            print(f"    ✗ Batch download failed: {e}")
            continue
        
        for sym_info in batch:
            symbol = sym_info["symbol"]
            yf_sym = f"{symbol}.NS"
            
            try:
                # Extract stock data
                if len(yf_symbols) == 1:
                    stock_df = data.copy()
                else:
                    if yf_sym in data.columns.get_level_values(0):
                        stock_df = data[yf_sym].dropna(how="all")
                    else:
                        continue
                
                if stock_df.empty or len(stock_df) < 50:
                    continue
                
                # Run each scan against this stock
                matched_scans = []
                for scan in scans_to_run:
                    try:
                        if parser.evaluate(scan["clause"], stock_df):
                            matched_scans.append(scan["id"])
                    except Exception:
                        pass
                
                if not matched_scans:
                    processed += 1
                    continue
                
                # Fetch fundamentals + fund data for matched stocks
                fund_data = {}
                funda_data = {}
                
                if fetch_fundamentals:
                    try:
                        ticker = yf.Ticker(yf_sym)
                        info = ticker.info or {}
                        fund_data = get_fund_holdings(info)
                        funda_data = get_fundamental_snapshot(info)
                    except Exception:
                        pass
                
                # Compute quick technicals for the result
                close = stock_df["Close"]
                latest_price = float(close.iloc[-1])
                change_pct = round((latest_price - float(close.iloc[-2])) / float(close.iloc[-2]) * 100, 2) if len(close) > 1 else 0
                
                rsi_val = round(float(IndicatorEngine.rsi(close).iloc[-1]), 2)
                
                stock_result = {
                    "symbol": symbol,
                    "name": sym_info.get("name", symbol),
                    "sector": sym_info.get("sector", "Unknown"),
                    "price": round(latest_price, 2),
                    "change_pct": change_pct,
                    "rsi": rsi_val,
                    "fundamentals": funda_data,
                    "fund_holdings": fund_data,
                }
                
                # Add to matching scan results
                for scan_id in matched_scans:
                    scan_results[scan_id]["matches"].append(stock_result)
                    scan_results[scan_id]["count"] += 1
                
                processed += 1
                
            except Exception as e:
                continue
        
        if batch_end < total:
            time.sleep(BATCH_DELAY)
    
    # Build output
    total_elapsed = time.time() - scan_start
    
    output = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "total_universe": total,
            "total_processed": processed,
            "total_scans": len(scans_to_run),
            "elapsed_seconds": round(total_elapsed, 1),
            "version": "1.0",
        },
        "scans": {},
    }
    
    for scan_id, result in scan_results.items():
        # Sort matches by change% descending
        result["matches"].sort(key=lambda x: x["change_pct"], reverse=True)
        
        output["scans"][scan_id] = {
            "name": result["scan"]["name"],
            "clause": result["scan"]["clause"],
            "type": result["scan"]["type"],
            "tags": result["scan"].get("tags", []),
            "match_count": result["count"],
            "matches": result["matches"],
        }
        
        print(f"\n  📋 {result['scan']['name']}: {result['count']} matches")
        for m in result["matches"][:5]:
            fund_str = ""
            if m["fund_holdings"].get("no_of_funds"):
                fund_str = f" | Funds: {m['fund_holdings']['no_of_funds']}"
            print(f"     {m['symbol']:15s} ₹{m['price']:>10.2f} ({m['change_pct']:+.2f}%) RSI:{m['rsi']:.1f}{fund_str}")
        if result["count"] > 5:
            print(f"     ... and {result['count'] - 5} more")
    
    # Save results
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(SCAN_RESULTS_FILE, "w") as f:
        json.dump(output, f, indent=2, default=str)
    
    # Save to history
    SCAN_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    history_file = SCAN_HISTORY_DIR / f"scan_{date_str}.json"
    with open(history_file, "w") as f:
        json.dump(output, f, indent=2, default=str)
    
    print(f"\n{'=' * 60}")
    print(f"  ✅ Done in {total_elapsed:.1f}s")
    print(f"  📁 Results: {SCAN_RESULTS_FILE}")
    print(f"  📁 History: {history_file}")
    print(f"{'=' * 60}")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="TradEdge Scanner Runner")
    parser.add_argument("--preset", type=str, help="Run a preset scan by name")
    parser.add_argument("--clause", type=str, help="Run a custom ChartInk clause")
    parser.add_argument("--scan-ids", type=str, nargs="+", help="Run specific scan IDs from config")
    parser.add_argument("--no-fundamentals", action="store_true", help="Skip fundamental data fetch (faster)")
    parser.add_argument("--list-presets", action="store_true", help="List all available presets")
    parser.add_argument("--add-scan", type=str, nargs=2, metavar=("NAME", "CLAUSE"), help="Add a custom scan to config")
    args = parser.parse_args()
    
    if args.list_presets:
        print("Available preset scans:")
        for name, clause in PRESET_SCANS.items():
            print(f"  • {name}")
            print(f"    {clause[:80]}...")
        return
    
    if args.add_scan:
        name, clause = args.add_scan
        scan_id = add_custom_scan(name, clause)
        print(f"✅ Added scan '{name}' with ID: {scan_id}")
        return
    
    run_scans(
        scan_ids=args.scan_ids,
        custom_clause=args.clause,
        preset_name=args.preset,
        fetch_fundamentals=not args.no_fundamentals,
    )


if __name__ == "__main__":
    main()
