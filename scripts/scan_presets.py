#!/usr/bin/env python3
"""
Scan Presets Module v1.0
Pre-built scanner patterns for TradEdge - ChartInk style technical scans,
Techno-Funda, Funda-Tech, and pattern recognition scanners.

Integrates with stock_details JSON data to filter and rank stocks.
"""

import json
import os
import math
from datetime import datetime
from typing import Dict, List, Any, Callable, Optional, Tuple

# ============================================================================
# CONFIGURATION
# ============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")
STOCK_DETAILS_DIR = os.path.join(DATA_DIR, "stock_details")
SCAN_RESULTS_DIR = os.path.join(DATA_DIR, "scan_results")

# ============================================================================
# SCAN CATEGORIES
# ============================================================================

SCAN_CATEGORIES = {
    "momentum": {
        "name": "Momentum Scans",
        "icon": "⚡",
        "color": "#ffaa00"
    },
    "breakout": {
        "name": "Breakout Scans", 
        "icon": "🚀",
        "color": "#00ff88"
    },
    "pattern": {
        "name": "Pattern Scans",
        "icon": "📊",
        "color": "#00f0ff"
    },
    "consolidation": {
        "name": "Consolidation Scans",
        "icon": "🎯",
        "color": "#aa66ff"
    },
    "techno_funda": {
        "name": "Techno-Funda Scans",
        "icon": "💎",
        "color": "#ff00aa"
    },
    "volume": {
        "name": "Volume Scans",
        "icon": "📈",
        "color": "#ff8844"
    },
    "reversal": {
        "name": "Reversal Scans",
        "icon": "🔄",
        "color": "#44aaff"
    },
    "earnings": {
        "name": "Earnings Scans",
        "icon": "💰",
        "color": "#88ff44"
    }
}

# ============================================================================
# SCAN PRESETS DEFINITIONS
# ============================================================================

SCAN_PRESETS: Dict[str, Dict[str, Any]] = {
    
    # -------------------------------------------------------------------------
    # MOMENTUM SCANS
    # -------------------------------------------------------------------------
    
    "high_rs_momentum": {
        "id": "high_rs_momentum",
        "name": "High RS Momentum",
        "category": "momentum",
        "description": "Stocks with Relative Strength ≥80 and positive momentum",
        "conditions": [
            ("price_strength", ">=", 80),
            ("return_1m", ">", 0),
            ("return_3m", ">", 0),
            ("volume_ratio", ">=", 1.0)
        ],
        "sort_by": "price_strength",
        "sort_order": "desc"
    },
    
    "rs_new_high": {
        "id": "rs_new_high",
        "name": "RS New High",
        "category": "momentum",
        "description": "Relative Strength making new highs (RS ≥90)",
        "conditions": [
            ("price_strength", ">=", 90),
            ("high_52w_proximity", ">=", 90)
        ],
        "sort_by": "price_strength",
        "sort_order": "desc"
    },
    
    "momentum_surge": {
        "id": "momentum_surge",
        "name": "Momentum Surge",
        "category": "momentum",
        "description": "Strong 1-week and 1-month returns with volume",
        "conditions": [
            ("return_1w", ">=", 5),
            ("return_1m", ">=", 10),
            ("volume_ratio", ">=", 1.5)
        ],
        "sort_by": "return_1w",
        "sort_order": "desc"
    },
    
    "triple_momentum": {
        "id": "triple_momentum",
        "name": "Triple Momentum",
        "category": "momentum",
        "description": "Positive returns across all timeframes (1W, 1M, 3M)",
        "conditions": [
            ("return_1w", ">", 0),
            ("return_1m", ">", 5),
            ("return_3m", ">", 10),
            ("return_6m", ">", 15)
        ],
        "sort_by": "return_3m",
        "sort_order": "desc"
    },
    
    # -------------------------------------------------------------------------
    # BREAKOUT SCANS
    # -------------------------------------------------------------------------
    
    "52w_high_breakout": {
        "id": "52w_high_breakout",
        "name": "52W High Breakout",
        "category": "breakout",
        "description": "Stocks at or near 52-week high with volume confirmation",
        "conditions": [
            ("high_52w_proximity", ">=", 98),
            ("volume_ratio", ">=", 1.5),
            ("change_pct", ">", 0)
        ],
        "sort_by": "volume_ratio",
        "sort_order": "desc"
    },
    
    "breakout_with_volume": {
        "id": "breakout_with_volume",
        "name": "Breakout + Volume",
        "category": "breakout",
        "description": "Price breakout (>3%) with 2x average volume",
        "conditions": [
            ("change_pct", ">=", 3),
            ("volume_ratio", ">=", 2.0),
            ("close", ">", "sma_20")
        ],
        "sort_by": "volume_ratio",
        "sort_order": "desc"
    },
    
    "range_breakout": {
        "id": "range_breakout",
        "name": "Range Breakout",
        "category": "breakout",
        "description": "Breaking out of 20-day trading range",
        "conditions": [
            ("close", ">", "high_20d"),
            ("volume_ratio", ">=", 1.5)
        ],
        "sort_by": "change_pct",
        "sort_order": "desc"
    },
    
    "sma_breakout": {
        "id": "sma_breakout",
        "name": "SMA Breakout",
        "category": "breakout",
        "description": "Price crossing above all major SMAs (20, 50, 200)",
        "conditions": [
            ("close", ">", "sma_20"),
            ("close", ">", "sma_50"),
            ("close", ">", "sma_200"),
            ("sma_20", ">", "sma_50")
        ],
        "sort_by": "price_strength",
        "sort_order": "desc"
    },
    
    # -------------------------------------------------------------------------
    # PATTERN SCANS
    # -------------------------------------------------------------------------
    
    "vcp_pattern": {
        "id": "vcp_pattern",
        "name": "VCP (Volatility Contraction)",
        "category": "pattern",
        "description": "Mark Minervini's VCP - decreasing volatility with price support",
        "conditions": [
            ("volatility_20d", "<", "volatility_50d"),
            ("close", ">", "sma_50"),
            ("close", ">", "sma_200"),
            ("high_52w_proximity", ">=", 75),
            ("low_52w_proximity", ">=", 30)
        ],
        "sort_by": "price_strength",
        "sort_order": "desc"
    },
    
    "tight_flag": {
        "id": "tight_flag",
        "name": "Tight Flag Pattern",
        "category": "pattern",
        "description": "Tight consolidation after uptrend (flag pattern)",
        "conditions": [
            ("range_5d_pct", "<=", 5),
            ("return_1m", ">=", 10),
            ("close", ">", "sma_20"),
            ("volume_ratio", "<=", 1.2)
        ],
        "sort_by": "return_1m",
        "sort_order": "desc"
    },
    
    "cup_handle": {
        "id": "cup_handle",
        "name": "Cup & Handle Setup",
        "category": "pattern",
        "description": "Near highs after recovery (cup) with tight range (handle)",
        "conditions": [
            ("high_52w_proximity", ">=", 90),
            ("range_10d_pct", "<=", 8),
            ("close", ">", "sma_50"),
            ("return_3m", ">=", 15)
        ],
        "sort_by": "high_52w_proximity",
        "sort_order": "desc"
    },
    
    "inside_bar": {
        "id": "inside_bar",
        "name": "Inside Bar",
        "category": "pattern",
        "description": "Today's range within yesterday's range (consolidation)",
        "conditions": [
            ("is_inside_bar", "==", True),
            ("close", ">", "sma_20")
        ],
        "sort_by": "volume_ratio",
        "sort_order": "desc"
    },
    
    "narrow_range_7": {
        "id": "narrow_range_7",
        "name": "NR7 (Narrow Range 7)",
        "category": "pattern",
        "description": "Narrowest range of last 7 days - breakout imminent",
        "conditions": [
            ("is_nr7", "==", True),
            ("close", ">", "sma_50")
        ],
        "sort_by": "price_strength",
        "sort_order": "desc"
    },
    
    # -------------------------------------------------------------------------
    # CONSOLIDATION SCANS
    # -------------------------------------------------------------------------
    
    "tight_consolidation": {
        "id": "tight_consolidation",
        "name": "Tight Consolidation",
        "category": "consolidation",
        "description": "5-day price range less than 5%",
        "conditions": [
            ("range_5d_pct", "<=", 5),
            ("close", ">", "sma_50"),
            ("volume_ratio", "<=", 1.0)
        ],
        "sort_by": "range_5d_pct",
        "sort_order": "asc"
    },
    
    "low_volatility_squeeze": {
        "id": "low_volatility_squeeze",
        "name": "Low Volatility Squeeze",
        "category": "consolidation",
        "description": "Volatility at multi-week lows - expansion expected",
        "conditions": [
            ("volatility_10d", "<", "volatility_20d"),
            ("volatility_20d", "<", "volatility_50d"),
            ("close", ">", "sma_20")
        ],
        "sort_by": "volatility_10d",
        "sort_order": "asc"
    },
    
    "base_building": {
        "id": "base_building",
        "name": "Base Building",
        "category": "consolidation",
        "description": "Flat base formation with support",
        "conditions": [
            ("range_20d_pct", "<=", 15),
            ("close", ">", "sma_200"),
            ("return_1m", "between", (-5, 5))
        ],
        "sort_by": "range_20d_pct",
        "sort_order": "asc"
    },
    
    # -------------------------------------------------------------------------
    # TECHNO-FUNDA SCANS
    # -------------------------------------------------------------------------
    
    "techno_funda_quality": {
        "id": "techno_funda_quality",
        "name": "Techno-Funda Quality",
        "category": "techno_funda",
        "description": "High RS + Strong fundamentals (ROE>15, D/E<1, positive momentum)",
        "conditions": [
            ("price_strength", ">=", 70),
            ("roe", ">=", 15),
            ("debt_equity", "<=", 1),
            ("return_1m", ">", 0),
            ("surveillance_status", "==", "SAFE")
        ],
        "sort_by": "composite_score",
        "sort_order": "desc"
    },
    
    "funda_tech_breakout": {
        "id": "funda_tech_breakout",
        "name": "Funda-Tech Breakout",
        "category": "techno_funda",
        "description": "Strong fundamentals + Price breakout pattern",
        "conditions": [
            ("roe", ">=", 12),
            ("pe", "between", (5, 40)),
            ("high_52w_proximity", ">=", 90),
            ("volume_ratio", ">=", 1.3)
        ],
        "sort_by": "roe",
        "sort_order": "desc"
    },
    
    "growth_momentum": {
        "id": "growth_momentum",
        "name": "Growth + Momentum",
        "category": "techno_funda",
        "description": "High EPS growth with price momentum",
        "conditions": [
            ("eps_strength", ">=", 70),
            ("price_strength", ">=", 70),
            ("return_3m", ">=", 10)
        ],
        "sort_by": "composite_score",
        "sort_order": "desc"
    },
    
    "value_momentum": {
        "id": "value_momentum",
        "name": "Value + Momentum",
        "category": "techno_funda",
        "description": "Undervalued stocks gaining momentum (Low PE + High RS)",
        "conditions": [
            ("pe", "between", (5, 20)),
            ("price_strength", ">=", 60),
            ("roe", ">=", 12),
            ("return_1m", ">", 0)
        ],
        "sort_by": "price_strength",
        "sort_order": "desc"
    },
    
    "canslim_setup": {
        "id": "canslim_setup",
        "name": "CANSLIM Setup",
        "category": "techno_funda",
        "description": "O'Neil's CANSLIM criteria met with technical setup",
        "conditions": [
            ("oneil_grade", "in", ["A", "B"]),
            ("eps_strength", ">=", 70),
            ("price_strength", ">=", 70),
            ("high_52w_proximity", ">=", 85)
        ],
        "sort_by": "composite_score",
        "sort_order": "desc"
    },
    
    "buffett_momentum": {
        "id": "buffett_momentum",
        "name": "Buffett + Momentum",
        "category": "techno_funda",
        "description": "Quality moat stocks with price strength",
        "conditions": [
            ("roe", ">=", 15),
            ("debt_equity", "<=", 0.5),
            ("roce", ">=", 15),
            ("price_strength", ">=", 60)
        ],
        "sort_by": "roe",
        "sort_order": "desc"
    },
    
    # -------------------------------------------------------------------------
    # VOLUME SCANS
    # -------------------------------------------------------------------------
    
    "volume_explosion": {
        "id": "volume_explosion",
        "name": "Volume Explosion",
        "category": "volume",
        "description": "3x+ average volume with price gain",
        "conditions": [
            ("volume_ratio", ">=", 3.0),
            ("change_pct", ">", 0)
        ],
        "sort_by": "volume_ratio",
        "sort_order": "desc"
    },
    
    "accumulation_day": {
        "id": "accumulation_day",
        "name": "Accumulation Day",
        "category": "volume",
        "description": "Price up with above-average volume (institutional buying)",
        "conditions": [
            ("change_pct", ">=", 1),
            ("volume_ratio", ">=", 1.5),
            ("buyer_demand", "in", ["A", "B"])
        ],
        "sort_by": "volume_ratio",
        "sort_order": "desc"
    },
    
    "pocket_pivot": {
        "id": "pocket_pivot",
        "name": "Pocket Pivot",
        "category": "volume",
        "description": "Volume spike within base (Gil Morales pattern)",
        "conditions": [
            ("volume_ratio", ">=", 1.5),
            ("change_pct", ">", 0),
            ("close", ">", "sma_10"),
            ("range_10d_pct", "<=", 15)
        ],
        "sort_by": "volume_ratio",
        "sort_order": "desc"
    },
    
    "dry_up_volume": {
        "id": "dry_up_volume",
        "name": "Dry-Up Volume",
        "category": "volume",
        "description": "Very low volume - potential breakout setup",
        "conditions": [
            ("volume_ratio", "<=", 0.5),
            ("close", ">", "sma_50"),
            ("range_5d_pct", "<=", 5)
        ],
        "sort_by": "volume_ratio",
        "sort_order": "asc"
    },
    
    # -------------------------------------------------------------------------
    # REVERSAL SCANS
    # -------------------------------------------------------------------------
    
    "oversold_bounce": {
        "id": "oversold_bounce",
        "name": "Oversold Bounce",
        "category": "reversal",
        "description": "RSI recovering from oversold with price uptick",
        "conditions": [
            ("rsi", "between", (30, 45)),
            ("change_pct", ">", 0),
            ("return_1w", ">", 0)
        ],
        "sort_by": "change_pct",
        "sort_order": "desc"
    },
    
    "bullish_reversal": {
        "id": "bullish_reversal",
        "name": "Bullish Reversal",
        "category": "reversal",
        "description": "Downtrend reversing with volume",
        "conditions": [
            ("return_1m", "<", 0),
            ("return_1w", ">", 2),
            ("volume_ratio", ">=", 1.5),
            ("close", ">", "sma_10")
        ],
        "sort_by": "return_1w",
        "sort_order": "desc"
    },
    
    "52w_low_bounce": {
        "id": "52w_low_bounce",
        "name": "52W Low Bounce",
        "category": "reversal",
        "description": "Bouncing from 52-week lows with fundamentals intact",
        "conditions": [
            ("low_52w_proximity", "<=", 20),
            ("return_1w", ">", 3),
            ("roe", ">=", 10),
            ("debt_equity", "<=", 1.5)
        ],
        "sort_by": "return_1w",
        "sort_order": "desc"
    },
    
    "sma_200_reclaim": {
        "id": "sma_200_reclaim",
        "name": "200 SMA Reclaim",
        "category": "reversal",
        "description": "Price reclaiming 200-day SMA from below",
        "conditions": [
            ("close", ">", "sma_200"),
            ("prev_close", "<=", "sma_200"),
            ("volume_ratio", ">=", 1.2)
        ],
        "sort_by": "volume_ratio",
        "sort_order": "desc"
    },
    
    # -------------------------------------------------------------------------
    # EARNINGS SCANS
    # -------------------------------------------------------------------------
    
    "earnings_breakout": {
        "id": "earnings_breakout",
        "name": "Earnings Breakout",
        "category": "earnings",
        "description": "Strong post-earnings momentum",
        "conditions": [
            ("eps_strength", ">=", 80),
            ("return_1m", ">=", 10),
            ("volume_ratio", ">=", 1.5)
        ],
        "sort_by": "eps_strength",
        "sort_order": "desc"
    },
    
    "eps_acceleration": {
        "id": "eps_acceleration",
        "name": "EPS Acceleration",
        "category": "earnings",
        "description": "Accelerating quarterly EPS growth",
        "conditions": [
            ("eps_strength", ">=", 75),
            ("price_strength", ">=", 60)
        ],
        "sort_by": "eps_strength",
        "sort_order": "desc"
    },
    
    "positive_surprise": {
        "id": "positive_surprise",
        "name": "Positive Earnings Surprise",
        "category": "earnings",
        "description": "Beat estimates with price reaction",
        "conditions": [
            ("eps_strength", ">=", 70),
            ("return_1w", ">=", 3),
            ("volume_ratio", ">=", 1.3)
        ],
        "sort_by": "return_1w",
        "sort_order": "desc"
    },
    
    # -------------------------------------------------------------------------
    # GAP SCANS
    # -------------------------------------------------------------------------
    
    "gap_up_buy": {
        "id": "gap_up_buy",
        "name": "Gap Up Buy",
        "category": "breakout",
        "description": "Gap up >3% with volume - potential continuation",
        "conditions": [
            ("gap_pct", ">=", 3),
            ("volume_ratio", ">=", 2.0),
            ("close", ">", "sma_20")
        ],
        "sort_by": "gap_pct",
        "sort_order": "desc"
    },
    
    "gap_fill_long": {
        "id": "gap_fill_long",
        "name": "Gap Fill Long",
        "category": "reversal",
        "description": "Gap down filled and reclaimed",
        "conditions": [
            ("gap_pct", "<", -2),
            ("change_pct", ">", 0),
            ("close", ">", "open")
        ],
        "sort_by": "change_pct",
        "sort_order": "desc"
    }
}


# ============================================================================
# SCAN ENGINE
# ============================================================================

class ScanEngine:
    """Engine to run scans against stock details."""
    
    def __init__(self):
        self.stocks: List[Dict] = []
        self.load_stocks()
    
    def load_stocks(self):
        """Load all stock details from JSON files."""
        self.stocks = []
        
        if not os.path.exists(STOCK_DETAILS_DIR):
            print(f"Stock details directory not found: {STOCK_DETAILS_DIR}")
            return
        
        for filename in os.listdir(STOCK_DETAILS_DIR):
            if filename.endswith(".json"):
                filepath = os.path.join(STOCK_DETAILS_DIR, filename)
                try:
                    with open(filepath, "r") as f:
                        stock = json.load(f)
                        # Flatten nested data for easier access
                        flat = self._flatten_stock(stock)
                        self.stocks.append(flat)
                except Exception as e:
                    print(f"Error loading {filename}: {e}")
        
        print(f"Loaded {len(self.stocks)} stocks")
    
    def _flatten_stock(self, stock: Dict) -> Dict:
        """Flatten nested stock data for condition evaluation."""
        flat = {
            "symbol": stock.get("symbol", ""),
            "name": stock.get("name", ""),
            "sector": stock.get("sector", ""),
        }
        
        # Technical data
        tech = stock.get("technical", {})
        flat.update({
            "close": tech.get("close", 0),
            "open": tech.get("open", 0),
            "high": tech.get("high", 0),
            "low": tech.get("low", 0),
            "prev_close": tech.get("prev_close", 0),
            "volume": tech.get("volume", 0),
            "volume_ratio": tech.get("volume_ratio", 1),
            "change_pct": tech.get("change_pct", 0),
            "gap_pct": tech.get("gap_pct", 0),
            "rsi": tech.get("rsi", 50),
            "sma_10": tech.get("sma_10", 0),
            "sma_20": tech.get("sma_20", 0),
            "sma_50": tech.get("sma_50", 0),
            "sma_200": tech.get("sma_200", 0),
            "high_52w": tech.get("high_52w", 0),
            "low_52w": tech.get("low_52w", 0),
            "high_52w_proximity": tech.get("high_52w_proximity", 0),
            "low_52w_proximity": tech.get("low_52w_proximity", 0),
            "high_20d": tech.get("high_20d", 0),
            "low_20d": tech.get("low_20d", 0),
            "volatility_10d": tech.get("volatility_10d", 0),
            "volatility_20d": tech.get("volatility_20d", 0),
            "volatility_50d": tech.get("volatility_50d", 0),
            "range_5d_pct": tech.get("range_5d_pct", 0),
            "range_10d_pct": tech.get("range_10d_pct", 0),
            "range_20d_pct": tech.get("range_20d_pct", 0),
            "is_inside_bar": tech.get("is_inside_bar", False),
            "is_nr7": tech.get("is_nr7", False),
        })
        
        # Returns
        returns = tech.get("returns", {})
        flat.update({
            "return_1d": returns.get("1d", 0),
            "return_1w": returns.get("1w", 0),
            "return_1m": returns.get("1m", 0),
            "return_3m": returns.get("3m", 0),
            "return_6m": returns.get("6m", 0),
            "return_1y": returns.get("1y", 0),
        })
        
        # Fundamentals
        fund = stock.get("fundamentals", {})
        flat.update({
            "market_cap": fund.get("market_cap", 0),
            "pe": fund.get("pe", 0),
            "pb": fund.get("pb", 0),
            "roe": fund.get("roe", 0),
            "roce": fund.get("roce", 0),
            "debt_equity": fund.get("debt_equity", 0),
            "eps": fund.get("eps", 0),
            "book_value": fund.get("book_value", 0),
            "dividend_yield": fund.get("dividend_yield", 0),
        })
        
        # O'Neil scores
        oneil = stock.get("oneil", {})
        flat.update({
            "oneil_grade": oneil.get("master_score", "-"),
            "composite_score": oneil.get("composite_score", 0),
            "eps_strength": oneil.get("eps_strength", 0),
            "price_strength": oneil.get("price_strength", 0),
            "buyer_demand": oneil.get("buyer_demand", "-"),
            "group_rank": oneil.get("group_rank", 0),
        })
        
        # Surveillance
        surv = stock.get("surveillance", {})
        flat.update({
            "surveillance_status": surv.get("status", "-"),
            "risk_score": surv.get("risk_score", 0),
        })
        
        # Ownership
        own = stock.get("ownership", {})
        flat.update({
            "promoter_holding": own.get("promoter", 0),
            "fii_holding": own.get("fii", 0),
            "dii_holding": own.get("dii", 0),
        })
        
        return flat
    
    def evaluate_condition(self, stock: Dict, condition: Tuple) -> bool:
        """Evaluate a single condition against a stock."""
        field, operator, value = condition
        
        # Get stock value (handle field references like "sma_20")
        stock_value = stock.get(field)
        
        # Handle comparison with another field
        if isinstance(value, str) and value in stock:
            compare_value = stock.get(value, 0)
        else:
            compare_value = value
        
        # Handle None/missing values
        if stock_value is None:
            return False
        
        try:
            if operator == ">=":
                return stock_value >= compare_value
            elif operator == "<=":
                return stock_value <= compare_value
            elif operator == ">":
                return stock_value > compare_value
            elif operator == "<":
                return stock_value < compare_value
            elif operator == "==":
                return stock_value == compare_value
            elif operator == "!=":
                return stock_value != compare_value
            elif operator == "in":
                return stock_value in compare_value
            elif operator == "between":
                low, high = compare_value
                return low <= stock_value <= high
            else:
                return False
        except (TypeError, ValueError):
            return False
    
    def run_scan(self, scan_id: str) -> List[Dict]:
        """Run a scan preset and return matching stocks."""
        if scan_id not in SCAN_PRESETS:
            print(f"Unknown scan: {scan_id}")
            return []
        
        preset = SCAN_PRESETS[scan_id]
        conditions = preset.get("conditions", [])
        sort_by = preset.get("sort_by", "composite_score")
        sort_order = preset.get("sort_order", "desc")
        
        # Filter stocks
        matches = []
        for stock in self.stocks:
            if all(self.evaluate_condition(stock, cond) for cond in conditions):
                matches.append(stock)
        
        # Sort results
        reverse = sort_order == "desc"
        matches.sort(key=lambda x: x.get(sort_by, 0) or 0, reverse=reverse)
        
        return matches[:100]  # Top 100
    
    def run_all_scans(self) -> Dict[str, Dict]:
        """Run all scans and return results."""
        results = {}
        
        for scan_id, preset in SCAN_PRESETS.items():
            matches = self.run_scan(scan_id)
            results[scan_id] = {
                "id": scan_id,
                "name": preset["name"],
                "category": preset["category"],
                "description": preset["description"],
                "count": len(matches),
                "stocks": matches
            }
            print(f"  {preset['name']}: {len(matches)} stocks")
        
        return results
    
    def run_custom_scan(self, conditions: List[Tuple], sort_by: str = "composite_score", 
                        sort_order: str = "desc") -> List[Dict]:
        """Run a custom scan with arbitrary conditions."""
        matches = []
        for stock in self.stocks:
            if all(self.evaluate_condition(stock, cond) for cond in conditions):
                matches.append(stock)
        
        reverse = sort_order == "desc"
        matches.sort(key=lambda x: x.get(sort_by, 0) or 0, reverse=reverse)
        
        return matches[:100]


# ============================================================================
# MAIN - Generate scan results
# ============================================================================

def generate_scan_results():
    """Generate all scan results and save to JSON."""
    print("=" * 60)
    print("SCAN PRESETS ENGINE")
    print("=" * 60)
    
    # Create output directory
    os.makedirs(SCAN_RESULTS_DIR, exist_ok=True)
    
    # Initialize engine
    engine = ScanEngine()
    
    if not engine.stocks:
        print("No stocks loaded - run scanner workflow first")
        return
    
    print(f"\nRunning {len(SCAN_PRESETS)} scan presets...")
    print("-" * 40)
    
    # Run all scans
    results = engine.run_all_scans()
    
    # Group by category
    by_category = {}
    for scan_id, result in results.items():
        cat = result["category"]
        if cat not in by_category:
            by_category[cat] = {
                "info": SCAN_CATEGORIES.get(cat, {}),
                "scans": []
            }
        by_category[cat]["scans"].append({
            "id": result["id"],
            "name": result["name"],
            "description": result["description"],
            "count": result["count"]
        })
    
    # Build output
    output = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "total_stocks": len(engine.stocks),
            "total_scans": len(SCAN_PRESETS)
        },
        "categories": SCAN_CATEGORIES,
        "scans_by_category": by_category,
        "scan_results": results
    }
    
    # Save main results
    output_path = os.path.join(DATA_DIR, "scan_presets_results.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    
    print("-" * 40)
    print(f"Results saved to {output_path}")
    
    # Print summary
    print("\n" + "=" * 60)
    print("SCAN SUMMARY BY CATEGORY")
    print("=" * 60)
    
    for cat_id, cat_data in by_category.items():
        cat_info = cat_data["info"]
        print(f"\n{cat_info.get('icon', '📊')} {cat_info.get('name', cat_id)}")
        for scan in cat_data["scans"]:
            print(f"   • {scan['name']}: {scan['count']} stocks")


if __name__ == "__main__":
    generate_scan_results()
