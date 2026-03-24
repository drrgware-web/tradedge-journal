#!/usr/bin/env python3
"""
Scanner Summary Generator
Aggregates all stock details into scanner_results.json for frontend consumption.
Creates category-based scan results for quick filtering.
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
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
        "canslim_high": [],     # CANSLIM score >= 70
        "graham_value": [],     # Graham score >= 70
        "buffett_quality": [],  # Buffett score >= 70
        "lynch_growth": [],     # Lynch score >= 70
        
        # Surveillance
        "safe_stocks": [],      # Surveillance status = SAFE
        "caution_stocks": [],   # Surveillance status = CAUTION
        
        # Momentum
        "high_momentum": [],    # RS >= 80
        "breakout": [],         # Near 52w high
        
        # Fundamentals
        "high_roe": [],         # ROE >= 20
        "low_debt": [],         # D/E <= 0.5
        "high_growth": [],      # EPS growth >= 25%
        
        # Volume
        "high_volume": [],      # Above avg volume
        "accumulation": [],     # Buyer demand A or B
        
        # Ownership
        "fii_buying": [],       # FII holding increasing
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
            
        # Guru categories
        guru_ratings = detail.get("guru_ratings", [])
        for rating in guru_ratings:
            strategy = rating.get("strategy", "").lower()
            score = rating.get("score", 0)
            
            if strategy == "canslim" and score >= 70:
                categories["canslim_high"].append(entry)
            elif strategy == "graham" and score >= 70:
                categories["graham_value"].append(entry)
            elif strategy == "buffett" and score >= 70:
                categories["buffett_quality"].append(entry)
            elif strategy == "lynch" and score >= 70:
                categories["lynch_growth"].append(entry)
                
        # Surveillance
        surveillance = detail.get("surveillance", {})
        status = surveillance.get("status", "")
        
        if status == "SAFE":
            categories["safe_stocks"].append(entry)
        elif status == "CAUTION":
            categories["caution_stocks"].append(entry)
            
        # Momentum
        price_strength = oneil.get("price_strength", 0)
        if price_strength >= 80:
            categories["high_momentum"].append(entry)
            
        tech = detail.get("technical", {})
        high_proximity = tech.get("high_52w_proximity", 0)
        if high_proximity >= 95:
            categories["breakout"].append(entry)
            
        # Fundamentals
        fund = detail.get("fundamentals", {})
        
        roe = fund.get("roe", 0)
        if roe >= 20:
            categories["high_roe"].append(entry)
            
        de = fund.get("debt_equity", 999)
        if de <= 0.5:
            categories["low_debt"].append(entry)
            
        eps_growth = fund.get("eps_growth", 0) or detail.get("oneil", {}).get("breakdown", {}).get("eps_strength", 0)
        if eps_growth >= 25:
            categories["high_growth"].append(entry)
            
        # Volume
        volume_ratio = tech.get("volume_ratio", 1)
        if volume_ratio >= 1.5:
            categories["high_volume"].append(entry)
            
        buyer_demand = oneil.get("buyer_demand", "C")
        if buyer_demand in ["A", "B"]:
            categories["accumulation"].append(entry)
            
        # Ownership
        own = detail.get("ownership", {})
        fii_change = own.get("fii_change_qoq", 0)
        if fii_change > 0:
            categories["fii_buying"].append(entry)
            
        promoter = own.get("promoter", 0)
        if promoter >= 50:
            categories["high_promoter"].append(entry)
    
    # Sort each category by composite score
    for cat_name, stocks in categories.items():
        categories[cat_name] = sorted(
            stocks, 
            key=lambda x: x.get("composite_score", 0), 
            reverse=True
        )[:100]  # Top 100 per category
    
    return categories

def create_summary_entry(detail: Dict) -> Dict:
    """Create a summary entry for scanner results."""
    symbol = detail.get("symbol", "")
    fund = detail.get("fundamentals", {})
    tech = detail.get("technical", {})
    oneil = detail.get("oneil", {})
    surveillance = detail.get("surveillance", {})
    
    # Get best guru rating
    guru_ratings = detail.get("guru_ratings", [])
    best_guru = max(guru_ratings, key=lambda x: x.get("score", 0)) if guru_ratings else {}
    
    return {
        "symbol": symbol,
        "name": detail.get("name", symbol),
        "sector": detail.get("sector", ""),
        
        # Price & Returns
        "cmp": tech.get("close", 0),
        "change_pct": tech.get("change_pct", 0),
        "return_1m": tech.get("returns", {}).get("1m", 0),
        "return_3m": tech.get("returns", {}).get("3m", 0),
        "return_6m": tech.get("returns", {}).get("6m", 0),
        "return_1y": tech.get("returns", {}).get("1y", 0),
        
        # Fundamentals
        "market_cap": fund.get("market_cap", 0),
        "pe": fund.get("pe", 0),
        "roe": fund.get("roe", 0),
        "roce": fund.get("roce", 0),
        "debt_equity": fund.get("debt_equity", 0),
        
        # O'Neil
        "oneil_grade": oneil.get("master_score", "-"),
        "composite_score": oneil.get("composite_score", 0),
        "eps_strength": oneil.get("eps_strength", 0),
        "price_strength": oneil.get("price_strength", 0),
        "buyer_demand": oneil.get("buyer_demand", "-"),
        
        # Best Guru
        "best_guru": best_guru.get("strategy", "-"),
        "best_guru_score": best_guru.get("score", 0),
        
        # Surveillance
        "surveillance_status": surveillance.get("status", "-"),
        "risk_score": surveillance.get("risk_score", 0),
        "surveillance_flags": surveillance.get("flags", [])[:3],  # Top 3 flags
        
        # Volume
        "volume_ratio": tech.get("volume_ratio", 1),
        
        # Update time
        "updated_at": detail.get("updated_at", "")
    }

def generate_summary():
    """Generate complete scanner results."""
    details = load_all_details()
    
    if not details:
        print("No stock details found")
        return
    
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
