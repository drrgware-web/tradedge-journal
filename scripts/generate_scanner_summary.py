#!/usr/bin/env python3
"""
Scanner Summary Generator
Aggregates all stock details into scanner_results.json for frontend consumption.
Creates category-based scan results for quick filtering.
"""

import json
import os
import time
from datetime import datetime
from typing import Dict, List, Any

import yfinance as yf

# Paths - Use repo root data directory (scripts/ is one level deep)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)  # Go up from scripts/ to repo root
DATA_DIR = os.path.join(REPO_ROOT, "data")
STOCK_DETAILS_DIR = os.path.join(DATA_DIR, "stock_details")
OUTPUT_PATH = os.path.join(DATA_DIR, "scanner_results.json")

def load_all_details() -> List[Dict]:
    """Load all stock detail JSON files."""
    details = []
    
    if not os.path.exists(STOCK_DETAILS_DIR):
        print(f"Stock details directory not found: {STOCK_DETAILS_DIR}")
        return details
    
    for filename in os.listdir(STOCK_DETAILS_DIR):
        if filename.endswith(".json"):
            filepath = os.path.join(STOCK_DETAILS_DIR, filename)
            try:
                with open(filepath, "r") as f:
                    detail = json.load(f)
                    details.append(detail)
            except Exception as e:
                print(f"Error loading {filename}: {e}")
    
    print(f"Loaded {len(details)} stock details")
    return details

def create_scan_categories(details: List[Dict]) -> Dict[str, List[Dict]]:
    """Create category-based scan results."""
    categories = {
        # O'Neil Grade Categories
        "oneil_a": [],      # A-grade stocks
        "oneil_ab": [],     # A and B grade
        
        # Guru Strategies
        "canslim_high": [],     # CANSLIM score >= 60
        "graham_value": [],     # Graham score >= 60
        "buffett_quality": [],  # Buffett score >= 60
        "lynch_growth": [],     # Lynch score >= 60
        
        # Surveillance
        "safe_stocks": [],      # No red flags
        "caution_stocks": [],   # Has red flags
        
        # Momentum
        "high_momentum": [],    # RS >= 80
        "breakout": [],         # Near 52w high
        
        # Fundamentals
        "high_roe": [],         # ROE >= 15
        "low_debt": [],         # D/E <= 50 (your data uses percentage)
        "high_growth": [],      # Revenue growth >= 15%
        
        # Volume
        "high_volume": [],      # Above avg volume
        "accumulation": [],     # Buyer demand A or B
        
        # Ownership
        "fii_buying": [],       # Institutional holding > 20%
        "high_promoter": [],    # Promoter >= 50%
    }
    
    for detail in details:
        symbol = detail.get("symbol", "")
        if not symbol:
            continue
            
        # Create summary entry
        entry = create_summary_entry(detail)
        
        # O'Neil categories
        oneil = detail.get("oneil", {})
        master_score = oneil.get("master_score", "")
        
        if master_score == "A":
            categories["oneil_a"].append(entry)
            categories["oneil_ab"].append(entry)
        elif master_score == "B":
            categories["oneil_ab"].append(entry)
            
        # Guru categories - check score_pct field
        guru_ratings = detail.get("guru_ratings", [])
        for rating in guru_ratings:
            strategy = rating.get("strategy", "").lower()
            score_pct = rating.get("score_pct", 0) or 0
            
            if "canslim" in strategy and score_pct >= 60:
                categories["canslim_high"].append(entry)
            elif "value" in strategy and score_pct >= 60:
                categories["graham_value"].append(entry)
            elif "quality" in strategy or "buffett" in strategy.lower():
                if score_pct >= 60:
                    categories["buffett_quality"].append(entry)
            elif "lynch" in strategy.lower() or "growth" in strategy.lower():
                if score_pct >= 60:
                    categories["lynch_growth"].append(entry)
                
        # Surveillance - check red_flag_count
        surveillance = detail.get("surveillance", {})
        red_flags = surveillance.get("red_flag_count", 0) or 0
        
        if red_flags == 0:
            categories["safe_stocks"].append(entry)
        else:
            categories["caution_stocks"].append(entry)
            
        # Momentum - from oneil.price_strength
        price_strength = oneil.get("price_strength", 0) or 0
        if price_strength >= 80:
            categories["high_momentum"].append(entry)
        
        # Breakout - calculate from breakout dict
        breakout = detail.get("breakout", {})
        pct_from_high = breakout.get("pct_from_high", -100) or -100
        if pct_from_high >= -5:  # Within 5% of 52W high
            categories["breakout"].append(entry)
            
        # Fundamentals - from fundamentals dict
        fund = detail.get("fundamentals", {})
        
        roe = fund.get("roe")
        if roe is not None and roe >= 15:
            categories["high_roe"].append(entry)
            
        de = fund.get("debt_to_equity") or fund.get("debt_equity")
        if de is not None and de <= 50:  # Your data uses percentage (35.65 = 35.65%)
            categories["low_debt"].append(entry)
            
        revenue_growth = fund.get("revenue_growth", 0) or 0
        if revenue_growth >= 15:
            categories["high_growth"].append(entry)
            
        # Volume - from volume dict
        vol_data = detail.get("volume", {})
        volume_ratio = vol_data.get("ratio", 1) if isinstance(vol_data, dict) else 1
        if volume_ratio >= 1.5:
            categories["high_volume"].append(entry)
            
        buyer_demand = oneil.get("buyer_demand", "C")
        if buyer_demand in ["A", "B"]:
            categories["accumulation"].append(entry)
            
        # Ownership - from fund_holdings dict
        fund_hold = detail.get("fund_holdings", {})
        institutional = fund_hold.get("institutional_pct", 0) or 0
        if institutional >= 20:
            categories["fii_buying"].append(entry)
            
        promoter = fund_hold.get("promoter_pct", 0) or 0
        if promoter >= 50:
            categories["high_promoter"].append(entry)
    
    # Sort each category by composite score
    for cat_name, stocks in categories.items():
        categories[cat_name] = sorted(
            stocks, 
            key=lambda x: x.get("composite_score", 0) or 0, 
            reverse=True
        )[:100]  # Top 100 per category
    
    return categories

def create_summary_entry(detail: Dict) -> Dict:
    """Create a summary entry for scanner results."""
    symbol = detail.get("symbol", "")
    fund = detail.get("fundamentals", {})
    tech = detail.get("technical", {})
    indicators = tech.get("indicators", {})
    returns = tech.get("returns", {})
    root_returns = detail.get("returns", {})
    oneil = detail.get("oneil", {})
    surveillance = detail.get("surveillance", {})
    vol_data = detail.get("volume", {})
    fund_hold = detail.get("fund_holdings", {})
    breakout = detail.get("breakout", {})
    
    # Get best guru rating
    guru_ratings = detail.get("guru_ratings", [])
    best_guru = max(guru_ratings, key=lambda x: x.get("score_pct", 0) or 0) if guru_ratings else {}
    
    # Get price - check multiple locations
    close_price = tech.get("close", 0) or detail.get("price", 0)
    
    # Get returns - check both locations
    return_1m = returns.get("1m", 0) or root_returns.get("1m", 0)
    return_3m = returns.get("3m", 0) or root_returns.get("3m", 0)
    return_6m = returns.get("6m", 0) or root_returns.get("6m", 0)
    return_1y = returns.get("1y", 0) or root_returns.get("1y", 0)
    
    # Get volume ratio from volume dict
    volume_ratio = vol_data.get("ratio", 1) if isinstance(vol_data, dict) else 1
    
    # Get change_pct
    change_pct = tech.get("change_pct", 0) or detail.get("change_pct", 0)
    
    # Get surveillance status from red_flag_count
    red_flags = surveillance.get("red_flag_count", 0) or 0
    surv_status = "SAFE" if red_flags == 0 else "CAUTION"
    
    return {
        "symbol": symbol,
        "name": detail.get("name", symbol),
        "sector": detail.get("sector", ""),
        
        # Price & Returns
        "close": close_price,
        "cmp": close_price,
        "change_pct": change_pct,
        "return_1w": returns.get("1w", 0) or root_returns.get("1w", 0),
        "return_1m": return_1m,
        "return_3m": return_3m,
        "return_6m": return_6m,
        "return_1y": return_1y,
        
        # Technicals
        "rsi": indicators.get("rsi", 0) or detail.get("rsi", 50),
        "sma_20": indicators.get("sma_20", 0),
        "sma_50": indicators.get("sma_50", 0),
        "sma_200": indicators.get("sma_200", 0),
        "high_52w": tech.get("high_52w", 0) or breakout.get("high_52w", 0),
        "low_52w": tech.get("low_52w", 0) or breakout.get("low_52w", 0),
        
        # Fundamentals
        "market_cap_cr": fund.get("market_cap_cr", 0),
        "pe": fund.get("pe_ratio", 0) or fund.get("pe", 0),
        "pb": fund.get("pb_ratio", 0) or fund.get("pb", 0),
        "roe": fund.get("roe") or 0,
        "roce": fund.get("roce") or 0,
        "debt_equity": fund.get("debt_to_equity", 0) or fund.get("debt_equity", 0),
        "revenue_growth": fund.get("revenue_growth", 0),
        "profit_margin": fund.get("profit_margin", 0),
        
        # O'Neil
        "oneil_grade": oneil.get("master_score", "-"),
        "composite_score": oneil.get("composite_score", 0),
        "eps_strength": oneil.get("eps_strength", 0),
        "price_strength": oneil.get("price_strength", 0),
        "buyer_demand": oneil.get("buyer_demand", "-"),
        "group_rank": oneil.get("group_rank", 0),
        "buyer_demand_score": oneil.get("buyer_demand_score", 50),

        # Best Guru
        "best_guru": best_guru.get("strategy", "-"),
        "best_guru_score": best_guru.get("score_pct", 0),
        
        # Surveillance
        "surveillance_status": surv_status,
        "red_flag_count": red_flags,
        
        # Volume
        "volume_ratio": volume_ratio,
        
        # Ownership
        "promoter_holding": fund_hold.get("promoter_pct", 0),
        "institutional_holding": fund_hold.get("institutional_pct", 0),
        
        # Update time
        "updated_at": detail.get("updated_at", "")
    }

def refresh_prices(details: List[Dict]) -> Dict[str, Dict]:
    """Batch-fetch latest close + change_pct from yfinance for all symbols."""
    symbols = [d.get("symbol", "") for d in details if d.get("symbol")]
    if not symbols:
        return {}

    fresh = {}  # {SYMBOL: {close, change_pct}}
    failed = []
    BATCH = 50
    total_batches = (len(symbols) + BATCH - 1) // BATCH

    print(f"\n  📡 Refreshing prices for {len(symbols)} symbols in {total_batches} batches of {BATCH}...")

    def download_batch(yf_syms):
        return yf.download(
            yf_syms,
            period="2d",
            group_by="ticker",
            auto_adjust=False,
            threads=True,
            progress=False,
        )

    def process_batch(data, batch, yf_syms, batch_num):
        if batch_num == 1:
            print(f"    [DEBUG] Batch 1 shape: {data.shape}")
            print(f"    [DEBUG] Batch 1 columns (first 5): {list(data.columns[:5])}")
            print(f"    [DEBUG] Batch 1 index (last 2): {list(data.index[-2:])}")

        for sym in batch:
            yf_sym = f"{sym}.NS"
            try:
                if len(yf_syms) == 1:
                    df = data
                else:
                    if yf_sym not in data.columns.get_level_values(0):
                        failed.append((sym, "not in download columns"))
                        continue
                    df = data[yf_sym].dropna(how="all")

                if len(df) < 2:
                    failed.append((sym, f"only {len(df)} rows"))
                    continue

                latest = float(df["Close"].iloc[-1])
                prev = float(df["Close"].iloc[-2])
                if prev > 0 and latest > 0:
                    fresh[sym] = {
                        "close": round(latest, 2),
                        "change_pct": round((latest - prev) / prev * 100, 2),
                    }
                else:
                    failed.append((sym, f"invalid prices: latest={latest}, prev={prev}"))
            except Exception as e:
                failed.append((sym, str(e)))

    for i in range(0, len(symbols), BATCH):
        batch = symbols[i:i + BATCH]
        batch_num = i // BATCH + 1
        yf_syms = [f"{s}.NS" for s in batch]

        try:
            data = download_batch(yf_syms)
            process_batch(data, batch, yf_syms, batch_num)
        except Exception as e:
            err_str = str(e).lower()
            if "rate" in err_str or "limit" in err_str or "429" in err_str or "too many" in err_str:
                print(f"    Batch {batch_num}/{total_batches} rate limited, retrying in 10s...")
                time.sleep(10)
                try:
                    data = download_batch(yf_syms)
                    process_batch(data, batch, yf_syms, batch_num)
                except Exception as e2:
                    print(f"    Batch {batch_num}/{total_batches} retry FAILED: {e2}")
                    for sym in batch:
                        failed.append((sym, f"batch retry error: {e2}"))
            else:
                print(f"    Batch {batch_num}/{total_batches} FAILED: {e}")
                for sym in batch:
                    failed.append((sym, f"batch error: {e}"))

        ok_count = len([s for s in batch if s in fresh])
        print(f"    Batch {batch_num}/{total_batches}: {ok_count}/{len(batch)} prices updated")

        if i + BATCH < len(symbols):
            time.sleep(3)

    print(f"  ✅ Refreshed {len(fresh)}/{len(symbols)} prices, {len(failed)} failed")
    if failed and len(failed) <= 20:
        for sym, reason in failed:
            print(f"    ✗ {sym}: {reason}")
    elif failed:
        print(f"    First 10 failures:")
        for sym, reason in failed[:10]:
            print(f"    ✗ {sym}: {reason}")
    return fresh


def generate_summary():
    """Generate complete scanner results."""
    details = load_all_details()

    if not details:
        print("No stock details found")
        return

    # Note: fresh prices are written to stock_details/*.json by scanner_runner.py
    # generate_scanner_summary.py just reads whatever is in those files

    # Create categories
    categories = create_scan_categories(details)
    
    # Create overall rankings
    all_stocks = [create_summary_entry(d) for d in details if d.get("symbol")]
    
    # Sort by composite score
    all_stocks_ranked = sorted(
        all_stocks, 
        key=lambda x: x.get("composite_score", 0), 
        reverse=True
    )
    
    # Generate final output
    output = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "total_stocks": len(details),
            "data_complete_count": sum(1 for d in details if d.get("data_complete")),
        },
        "categories": {
            name: {
                "count": len(stocks),
                "stocks": stocks
            }
            for name, stocks in categories.items()
        },
        "top_100": all_stocks_ranked[:100],
        "all_stocks": all_stocks_ranked
    }
    
    # Category descriptions
    output["category_info"] = {
        "oneil_a": {"name": "O'Neil A-Grade", "description": "Stocks with O'Neil Master Score A"},
        "oneil_ab": {"name": "O'Neil A/B Grade", "description": "Top tier O'Neil rated stocks"},
        "canslim_high": {"name": "CANSLIM Winners", "description": "High CANSLIM score (≥70)"},
        "graham_value": {"name": "Graham Value", "description": "Benjamin Graham value picks"},
        "buffett_quality": {"name": "Buffett Quality", "description": "Warren Buffett quality moat stocks"},
        "lynch_growth": {"name": "Lynch Growth", "description": "Peter Lynch growth at reasonable price"},
        "safe_stocks": {"name": "Safe Stocks", "description": "Passed all surveillance checks"},
        "caution_stocks": {"name": "Caution Required", "description": "Some surveillance flags raised"},
        "high_momentum": {"name": "High Momentum", "description": "Relative Strength ≥80"},
        "breakout": {"name": "Breakout Zone", "description": "Near 52-week high"},
        "high_roe": {"name": "High ROE", "description": "Return on Equity ≥20%"},
        "low_debt": {"name": "Low Debt", "description": "Debt/Equity ≤0.5"},
        "high_growth": {"name": "High Growth", "description": "EPS Growth ≥25%"},
        "high_volume": {"name": "Volume Surge", "description": "Volume 1.5x+ average"},
        "accumulation": {"name": "Accumulation", "description": "Institutional accumulation pattern"},
        "fii_buying": {"name": "FII Buying", "description": "FII holding increasing"},
        "high_promoter": {"name": "Promoter Confidence", "description": "Promoter holding ≥50%"},
    }
    
    # Save output
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)
    
    print(f"Scanner results saved to {OUTPUT_PATH}")
    print(f"Total stocks: {len(details)}")
    print(f"Categories generated: {len(categories)}")
    
    # Print category counts
    print("\nCategory Summary:")
    for name, stocks in categories.items():
        print(f"  {name}: {len(stocks)} stocks")

if __name__ == "__main__":
    generate_summary()
