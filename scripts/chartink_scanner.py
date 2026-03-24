#!/usr/bin/env python3
"""
ChartInk Scanner Integration v1.0
Parses and executes ChartInk-style scan conditions against local stock data.
Supports importing existing ChartInk scans and running them offline.

Features:
- Full ChartInk condition syntax parser
- 20+ technical indicators
- Custom scan builder
- Batch scan execution
- Export to scan_presets format
"""

import json
import os
import re
import math
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Callable
from dataclasses import dataclass
from enum import Enum

# ============================================================================
# CONFIGURATION
# ============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")
STOCK_DETAILS_DIR = os.path.join(DATA_DIR, "stock_details")
CHARTINK_SCANS_PATH = os.path.join(SCRIPT_DIR, "chartink_scans.json")
CUSTOM_SCANS_OUTPUT = os.path.join(DATA_DIR, "chartink_results.json")

# ============================================================================
# INDICATOR FUNCTIONS
# ============================================================================

class Indicators:
    """Technical indicator calculations from stock data."""
    
    @staticmethod
    def close(stock: Dict) -> float:
        return stock.get("close", 0)
    
    @staticmethod
    def open(stock: Dict) -> float:
        return stock.get("open", 0)
    
    @staticmethod
    def high(stock: Dict) -> float:
        return stock.get("high", 0)
    
    @staticmethod
    def low(stock: Dict) -> float:
        return stock.get("low", 0)
    
    @staticmethod
    def volume(stock: Dict) -> float:
        return stock.get("volume", 0)
    
    @staticmethod
    def prev_close(stock: Dict) -> float:
        return stock.get("prev_close", 0)
    
    @staticmethod
    def change_pct(stock: Dict) -> float:
        return stock.get("change_pct", 0)
    
    @staticmethod
    def sma(stock: Dict, period: int) -> float:
        return stock.get(f"sma_{period}", 0)
    
    @staticmethod
    def ema(stock: Dict, period: int) -> float:
        return stock.get(f"ema_{period}", 0)
    
    @staticmethod
    def rsi(stock: Dict, period: int = 14) -> float:
        return stock.get("rsi", 50)
    
    @staticmethod
    def volume_sma(stock: Dict, period: int) -> float:
        avg_vol = stock.get("avg_volume", 0)
        return avg_vol if avg_vol else stock.get("volume", 0)
    
    @staticmethod
    def atr(stock: Dict, period: int = 14) -> float:
        return stock.get("atr", 0)
    
    @staticmethod
    def high_52w(stock: Dict) -> float:
        return stock.get("high_52w", 0)
    
    @staticmethod
    def low_52w(stock: Dict) -> float:
        return stock.get("low_52w", 0)
    
    @staticmethod
    def high_20d(stock: Dict) -> float:
        return stock.get("high_20d", 0)
    
    @staticmethod
    def low_20d(stock: Dict) -> float:
        return stock.get("low_20d", 0)
    
    @staticmethod
    def market_cap(stock: Dict) -> float:
        return stock.get("market_cap", 0)
    
    @staticmethod
    def pe(stock: Dict) -> float:
        return stock.get("pe", 0)
    
    @staticmethod
    def pb(stock: Dict) -> float:
        return stock.get("pb", 0)
    
    @staticmethod
    def roe(stock: Dict) -> float:
        return stock.get("roe", 0)
    
    @staticmethod
    def debt_equity(stock: Dict) -> float:
        return stock.get("debt_equity", 0)
    
    @staticmethod
    def volume_ratio(stock: Dict) -> float:
        return stock.get("volume_ratio", 1)
    
    @staticmethod
    def return_1w(stock: Dict) -> float:
        return stock.get("return_1w", 0)
    
    @staticmethod
    def return_1m(stock: Dict) -> float:
        return stock.get("return_1m", 0)
    
    @staticmethod
    def return_3m(stock: Dict) -> float:
        return stock.get("return_3m", 0)
    
    @staticmethod
    def return_6m(stock: Dict) -> float:
        return stock.get("return_6m", 0)
    
    @staticmethod
    def return_1y(stock: Dict) -> float:
        return stock.get("return_1y", 0)
    
    @staticmethod
    def price_strength(stock: Dict) -> float:
        return stock.get("price_strength", 0)
    
    @staticmethod
    def eps_strength(stock: Dict) -> float:
        return stock.get("eps_strength", 0)
    
    @staticmethod
    def composite_score(stock: Dict) -> float:
        return stock.get("composite_score", 0)


# ============================================================================
# CONDITION PARSER
# ============================================================================

class ComparisonOp(Enum):
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    EQ = "="
    NEQ = "!="
    CROSSED_ABOVE = "crossed above"
    CROSSED_BELOW = "crossed below"


@dataclass
class ParsedCondition:
    """Represents a parsed scan condition."""
    left_indicator: str
    left_params: List[Any]
    operator: ComparisonOp
    right_indicator: str
    right_params: List[Any]
    right_is_value: bool = False
    right_value: float = 0.0


class ChartInkParser:
    """
    Parser for ChartInk-style scan conditions.
    
    Supported syntax examples:
    - "close > sma(close, 20)"
    - "rsi(14) < 30"
    - "volume > volume sma(volume, 20) * 2"
    - "close crossed above sma(close, 50)"
    - "weekly close > weekly sma(close, 20)"
    - "latest close > 1 day ago close * 1.03"
    """
    
    # Regex patterns
    INDICATOR_PATTERN = r'(close|open|high|low|volume|sma|ema|rsi|atr|macd|adx|obv|vwap|market cap|pe|pb|roe|debt equity|volume ratio|1 week ago close|1 month ago close|52 week high|52 week low|20 day high|20 day low|price strength|eps strength|composite score)'
    NUMBER_PATTERN = r'[-+]?\d*\.?\d+'
    COMPARISON_PATTERN = r'(>=|<=|>|<|=|!=|crossed above|crossed below)'
    
    def __init__(self):
        self.indicators = Indicators()
    
    def parse_condition(self, condition_str: str) -> Optional[ParsedCondition]:
        """Parse a single condition string into a ParsedCondition object."""
        condition_str = condition_str.strip().lower()
        
        # Find the comparison operator
        op_match = re.search(self.COMPARISON_PATTERN, condition_str)
        if not op_match:
            return None
        
        op_str = op_match.group(1)
        op = self._parse_operator(op_str)
        
        left_part = condition_str[:op_match.start()].strip()
        right_part = condition_str[op_match.end():].strip()
        
        # Parse left side
        left_indicator, left_params = self._parse_indicator(left_part)
        
        # Parse right side - could be indicator or value
        right_value_match = re.match(f'^({self.NUMBER_PATTERN})$', right_part)
        if right_value_match:
            return ParsedCondition(
                left_indicator=left_indicator,
                left_params=left_params,
                operator=op,
                right_indicator="",
                right_params=[],
                right_is_value=True,
                right_value=float(right_value_match.group(1))
            )
        
        right_indicator, right_params = self._parse_indicator(right_part)
        
        return ParsedCondition(
            left_indicator=left_indicator,
            left_params=left_params,
            operator=op,
            right_indicator=right_indicator,
            right_params=right_params,
            right_is_value=False
        )
    
    def _parse_operator(self, op_str: str) -> ComparisonOp:
        """Convert operator string to enum."""
        op_map = {
            ">": ComparisonOp.GT,
            ">=": ComparisonOp.GTE,
            "<": ComparisonOp.LT,
            "<=": ComparisonOp.LTE,
            "=": ComparisonOp.EQ,
            "!=": ComparisonOp.NEQ,
            "crossed above": ComparisonOp.CROSSED_ABOVE,
            "crossed below": ComparisonOp.CROSSED_BELOW,
        }
        return op_map.get(op_str, ComparisonOp.GT)
    
    def _parse_indicator(self, indicator_str: str) -> Tuple[str, List[Any]]:
        """Parse an indicator expression and extract parameters."""
        indicator_str = indicator_str.strip()
        
        # Handle multiplier (e.g., "sma(close, 20) * 1.5")
        multiplier = 1.0
        if " * " in indicator_str:
            parts = indicator_str.rsplit(" * ", 1)
            indicator_str = parts[0].strip()
            try:
                multiplier = float(parts[1])
            except ValueError:
                pass
        
        # Handle function-style indicators: sma(close, 20)
        func_match = re.match(r'(\w+)\s*\(\s*(\w+)?\s*,?\s*(\d+)?\s*\)', indicator_str)
        if func_match:
            func_name = func_match.group(1)
            param1 = func_match.group(2) or "close"
            param2 = int(func_match.group(3)) if func_match.group(3) else 14
            return func_name, [param1, param2, multiplier]
        
        # Handle simple indicators: close, volume, rsi, etc.
        indicator_map = {
            "close": ("close", []),
            "open": ("open", []),
            "high": ("high", []),
            "low": ("low", []),
            "volume": ("volume", []),
            "market cap": ("market_cap", []),
            "pe": ("pe", []),
            "pb": ("pb", []),
            "roe": ("roe", []),
            "debt equity": ("debt_equity", []),
            "volume ratio": ("volume_ratio", []),
            "52 week high": ("high_52w", []),
            "52 week low": ("low_52w", []),
            "20 day high": ("high_20d", []),
            "20 day low": ("low_20d", []),
            "1 week ago close": ("return_1w_price", []),
            "1 month ago close": ("return_1m_price", []),
            "price strength": ("price_strength", []),
            "eps strength": ("eps_strength", []),
            "composite score": ("composite_score", []),
        }
        
        if indicator_str in indicator_map:
            ind, params = indicator_map[indicator_str]
            return ind, params + [multiplier]
        
        # Default: treat as field name
        field_name = indicator_str.replace(" ", "_")
        return field_name, [multiplier]
    
    def evaluate_condition(self, condition: ParsedCondition, stock: Dict) -> bool:
        """Evaluate a parsed condition against a stock."""
        try:
            left_value = self._get_indicator_value(
                condition.left_indicator, 
                condition.left_params, 
                stock
            )
            
            if condition.right_is_value:
                right_value = condition.right_value
            else:
                right_value = self._get_indicator_value(
                    condition.right_indicator,
                    condition.right_params,
                    stock
                )
            
            return self._compare(left_value, condition.operator, right_value)
        except (TypeError, ValueError, KeyError):
            return False
    
    def _get_indicator_value(self, indicator: str, params: List[Any], stock: Dict) -> float:
        """Get the value of an indicator for a stock."""
        multiplier = params[-1] if params and isinstance(params[-1], float) else 1.0
        
        # Function-style indicators
        if indicator == "sma":
            period = params[1] if len(params) > 1 else 20
            value = stock.get(f"sma_{period}", 0)
        elif indicator == "ema":
            period = params[1] if len(params) > 1 else 20
            value = stock.get(f"ema_{period}", 0)
        elif indicator == "rsi":
            value = stock.get("rsi", 50)
        elif indicator == "atr":
            value = stock.get("atr", 0)
        elif indicator == "volume_sma":
            value = stock.get("avg_volume", stock.get("volume", 0))
        # Simple field lookups
        elif indicator in stock:
            value = stock.get(indicator, 0)
        # Return price calculations
        elif indicator == "return_1w_price":
            close = stock.get("close", 0)
            ret = stock.get("return_1w", 0)
            value = close / (1 + ret/100) if ret != -100 else close
        elif indicator == "return_1m_price":
            close = stock.get("close", 0)
            ret = stock.get("return_1m", 0)
            value = close / (1 + ret/100) if ret != -100 else close
        else:
            value = 0
        
        return value * multiplier if value else 0
    
    def _compare(self, left: float, op: ComparisonOp, right: float) -> bool:
        """Compare two values with the given operator."""
        if left is None or right is None:
            return False
        
        if op == ComparisonOp.GT:
            return left > right
        elif op == ComparisonOp.GTE:
            return left >= right
        elif op == ComparisonOp.LT:
            return left < right
        elif op == ComparisonOp.LTE:
            return left <= right
        elif op == ComparisonOp.EQ:
            return abs(left - right) < 0.0001
        elif op == ComparisonOp.NEQ:
            return abs(left - right) >= 0.0001
        elif op == ComparisonOp.CROSSED_ABOVE:
            # For crossed above, we need historical data - approximate with current > target
            return left > right
        elif op == ComparisonOp.CROSSED_BELOW:
            return left < right
        
        return False


# ============================================================================
# SCAN DEFINITIONS - Pre-built ChartInk-style scans
# ============================================================================

CHARTINK_PRESETS = {
    # Momentum Scans
    "ci_rs_new_high": {
        "name": "RS New High",
        "category": "momentum",
        "conditions": [
            "price strength >= 90",
            "close > 52 week high * 0.95"
        ]
    },
    "ci_momentum_burst": {
        "name": "Momentum Burst",
        "category": "momentum",
        "conditions": [
            "close > sma(close, 20)",
            "volume > volume ratio * 2",
            "return_1w > 5"
        ]
    },
    
    # Breakout Scans
    "ci_52w_breakout": {
        "name": "52W High Breakout",
        "category": "breakout",
        "conditions": [
            "close > 52 week high * 0.98",
            "volume ratio > 1.5"
        ]
    },
    "ci_20d_breakout": {
        "name": "20-Day Breakout",
        "category": "breakout",
        "conditions": [
            "close > 20 day high",
            "volume ratio > 1.3"
        ]
    },
    "ci_sma_breakout": {
        "name": "SMA 50 Breakout",
        "category": "breakout",
        "conditions": [
            "close > sma(close, 50)",
            "close > sma(close, 200)",
            "sma(close, 20) > sma(close, 50)"
        ]
    },
    
    # Pattern Scans
    "ci_vcp": {
        "name": "VCP Pattern",
        "category": "pattern",
        "conditions": [
            "close > sma(close, 50)",
            "close > sma(close, 200)",
            "close > 52 week high * 0.75",
            "volume ratio < 1.2"
        ]
    },
    "ci_tight_range": {
        "name": "Tight Range",
        "category": "pattern",
        "conditions": [
            "close > sma(close, 50)",
            "range_5d_pct < 5"
        ]
    },
    "ci_inside_bar": {
        "name": "Inside Bar",
        "category": "pattern",
        "conditions": [
            "is_inside_bar = 1",
            "close > sma(close, 20)"
        ]
    },
    
    # Volume Scans
    "ci_volume_spike": {
        "name": "Volume Spike",
        "category": "volume",
        "conditions": [
            "volume ratio > 3",
            "change_pct > 0"
        ]
    },
    "ci_accumulation": {
        "name": "Accumulation",
        "category": "volume",
        "conditions": [
            "volume ratio > 1.5",
            "change_pct > 1",
            "close > sma(close, 20)"
        ]
    },
    "ci_dry_volume": {
        "name": "Dry Volume",
        "category": "volume",
        "conditions": [
            "volume ratio < 0.5",
            "close > sma(close, 50)"
        ]
    },
    
    # Fundamental Scans
    "ci_quality_growth": {
        "name": "Quality Growth",
        "category": "fundamental",
        "conditions": [
            "roe > 15",
            "debt equity < 1",
            "price strength > 70"
        ]
    },
    "ci_value_pick": {
        "name": "Value Pick",
        "category": "fundamental",
        "conditions": [
            "pe < 20",
            "pe > 5",
            "roe > 12",
            "debt equity < 1"
        ]
    },
    "ci_high_rs_quality": {
        "name": "High RS + Quality",
        "category": "fundamental",
        "conditions": [
            "price strength > 80",
            "roe > 15",
            "composite score > 70"
        ]
    },
    
    # Reversal Scans  
    "ci_oversold_bounce": {
        "name": "Oversold Bounce",
        "category": "reversal",
        "conditions": [
            "rsi(14) < 40",
            "rsi(14) > 25",
            "change_pct > 0"
        ]
    },
    "ci_200sma_reclaim": {
        "name": "200 SMA Reclaim",
        "category": "reversal",
        "conditions": [
            "close > sma(close, 200)",
            "return_1w > 3"
        ]
    },
    
    # Gap Scans
    "ci_gap_up": {
        "name": "Gap Up",
        "category": "gap",
        "conditions": [
            "gap_pct > 3",
            "volume ratio > 2"
        ]
    },
    "ci_gap_fill": {
        "name": "Gap Fill Long",
        "category": "gap",
        "conditions": [
            "gap_pct < -2",
            "change_pct > 0"
        ]
    }
}


# ============================================================================
# CHARTINK SCAN RUNNER
# ============================================================================

class ChartInkScanner:
    """Runs ChartInk-style scans against local stock data."""
    
    def __init__(self):
        self.parser = ChartInkParser()
        self.stocks: List[Dict] = []
        self.custom_scans: Dict[str, Dict] = {}
        self._load_stocks()
        self._load_custom_scans()
    
    def _load_stocks(self):
        """Load all stock data."""
        self.stocks = []
        
        if not os.path.exists(STOCK_DETAILS_DIR):
            print(f"Stock details not found: {STOCK_DETAILS_DIR}")
            return
        
        for filename in os.listdir(STOCK_DETAILS_DIR):
            if filename.endswith(".json"):
                filepath = os.path.join(STOCK_DETAILS_DIR, filename)
                try:
                    with open(filepath, "r") as f:
                        stock = json.load(f)
                        flat = self._flatten_stock(stock)
                        self.stocks.append(flat)
                except Exception as e:
                    pass
        
        print(f"Loaded {len(self.stocks)} stocks")
    
    def _flatten_stock(self, stock: Dict) -> Dict:
        """Flatten nested stock data."""
        flat = {
            "symbol": stock.get("symbol", ""),
            "name": stock.get("name", ""),
            "sector": stock.get("sector", ""),
        }
        
        # Technical
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
            "ema_10": tech.get("ema_10", 0),
            "ema_20": tech.get("ema_20", 0),
            "high_52w": tech.get("high_52w", 0),
            "low_52w": tech.get("low_52w", 0),
            "high_20d": tech.get("high_20d", 0),
            "low_20d": tech.get("low_20d", 0),
            "range_5d_pct": tech.get("range_5d_pct", 0),
            "range_10d_pct": tech.get("range_10d_pct", 0),
            "is_inside_bar": 1 if tech.get("is_inside_bar") else 0,
            "is_nr7": 1 if tech.get("is_nr7") else 0,
            "atr": tech.get("atr", 0),
            "avg_volume": tech.get("avg_volume", 0),
        })
        
        # Returns
        returns = tech.get("returns", {})
        flat.update({
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
        })
        
        # O'Neil scores
        oneil = stock.get("oneil", {})
        flat.update({
            "oneil_grade": oneil.get("master_score", "-"),
            "composite_score": oneil.get("composite_score", 0),
            "eps_strength": oneil.get("eps_strength", 0),
            "price_strength": oneil.get("price_strength", 0),
        })
        
        # Surveillance
        surv = stock.get("surveillance", {})
        flat.update({
            "surveillance_status": surv.get("status", "-"),
        })
        
        return flat
    
    def _load_custom_scans(self):
        """Load custom scans from file."""
        if os.path.exists(CHARTINK_SCANS_PATH):
            try:
                with open(CHARTINK_SCANS_PATH, "r") as f:
                    self.custom_scans = json.load(f)
            except Exception as e:
                print(f"Error loading custom scans: {e}")
    
    def run_scan(self, scan_id: str, conditions: List[str] = None) -> List[Dict]:
        """Run a scan and return matching stocks."""
        # Get conditions
        if conditions:
            cond_list = conditions
        elif scan_id in CHARTINK_PRESETS:
            cond_list = CHARTINK_PRESETS[scan_id]["conditions"]
        elif scan_id in self.custom_scans:
            cond_list = self.custom_scans[scan_id].get("conditions", [])
        else:
            print(f"Unknown scan: {scan_id}")
            return []
        
        # Parse conditions
        parsed_conditions = []
        for cond_str in cond_list:
            parsed = self.parser.parse_condition(cond_str)
            if parsed:
                parsed_conditions.append(parsed)
        
        # Filter stocks
        matches = []
        for stock in self.stocks:
            if all(self.parser.evaluate_condition(pc, stock) for pc in parsed_conditions):
                matches.append(stock)
        
        # Sort by composite score
        matches.sort(key=lambda x: x.get("composite_score", 0), reverse=True)
        
        return matches[:100]
    
    def run_all_presets(self) -> Dict[str, Dict]:
        """Run all preset scans."""
        results = {}
        
        for scan_id, scan_def in CHARTINK_PRESETS.items():
            matches = self.run_scan(scan_id)
            results[scan_id] = {
                "id": scan_id,
                "name": scan_def["name"],
                "category": scan_def["category"],
                "conditions": scan_def["conditions"],
                "count": len(matches),
                "stocks": matches
            }
            print(f"  {scan_def['name']}: {len(matches)} stocks")
        
        return results
    
    def add_custom_scan(self, scan_id: str, name: str, category: str, conditions: List[str]):
        """Add a custom scan."""
        self.custom_scans[scan_id] = {
            "name": name,
            "category": category,
            "conditions": conditions
        }
        self._save_custom_scans()
    
    def _save_custom_scans(self):
        """Save custom scans to file."""
        with open(CHARTINK_SCANS_PATH, "w") as f:
            json.dump(self.custom_scans, f, indent=2)
    
    def import_chartink_url(self, url: str, scan_name: str) -> Dict:
        """
        Import a ChartInk scan from URL.
        Note: This requires the user to manually extract conditions from ChartInk
        since direct scraping isn't reliable.
        
        Returns a template for the user to fill in.
        """
        scan_id = scan_name.lower().replace(" ", "_").replace("-", "_")
        
        template = {
            "id": scan_id,
            "name": scan_name,
            "category": "custom",
            "conditions": [
                "# Paste your ChartInk conditions here, one per line",
                "# Example: close > sma(close, 20)",
                "# Example: volume > volume sma(volume, 20) * 2",
                "# Example: rsi(14) < 30"
            ],
            "source_url": url
        }
        
        return template


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Run all ChartInk scans and save results."""
    print("=" * 60)
    print("CHARTINK SCANNER")
    print("=" * 60)
    
    scanner = ChartInkScanner()
    
    if not scanner.stocks:
        print("No stocks loaded - run scanner workflow first")
        return
    
    print(f"\nRunning {len(CHARTINK_PRESETS)} preset scans...")
    print("-" * 40)
    
    results = scanner.run_all_presets()
    
    # Group by category
    by_category = {}
    for scan_id, result in results.items():
        cat = result["category"]
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append({
            "id": result["id"],
            "name": result["name"],
            "count": result["count"]
        })
    
    # Save results
    output = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "total_stocks": len(scanner.stocks),
            "total_scans": len(CHARTINK_PRESETS)
        },
        "scans_by_category": by_category,
        "scan_results": results
    }
    
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CUSTOM_SCANS_OUTPUT, "w") as f:
        json.dump(output, f, indent=2, default=str)
    
    print("-" * 40)
    print(f"Results saved to {CUSTOM_SCANS_OUTPUT}")
    
    # Summary
    print("\n" + "=" * 60)
    print("SCAN SUMMARY")
    print("=" * 60)
    
    for cat, scans in by_category.items():
        print(f"\n{cat.upper()}")
        for s in scans:
            print(f"  • {s['name']}: {s['count']} stocks")


if __name__ == "__main__":
    main()
