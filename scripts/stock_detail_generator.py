#!/usr/bin/env python3
"""
Stock Detail Generator v1.0
Master orchestrator for TradEdge Scanner Phase 2
Populates complete stock data: fundamentals, O'Neil scores, guru ratings, 
surveillance flags, ownership, and quarterly results.

Usage:
    python stock_detail_generator.py                    # Process all stocks
    python stock_detail_generator.py --top 500          # Top 500 by volume
    python stock_detail_generator.py --symbol RELIANCE  # Single stock
    python stock_detail_generator.py --batch 1 --total 5  # Batch processing
"""

import json
import os
import sys
import time
import re
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
import yfinance as yf

# Try importing requests - will be installed in workflow
try:
    import requests
except ImportError:
    print("requests not installed - run: pip install requests")
    sys.exit(1)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Cloudflare Worker endpoints
YAHOO_WORKER = "https://spring-fire-41a0.drrgware.workers.dev"
SCREENER_PROXY = "https://spring-fire-41a0.drrgware.workers.dev"  # With X-Kite-Action: screener

# Paths - Use repo root data directory (scripts/ is one level deep)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)  # Go up from scripts/ to repo root
DATA_DIR = os.path.join(REPO_ROOT, "data")
STOCK_DETAILS_DIR = os.path.join(DATA_DIR, "stock_details")
SCANNER_RESULTS_PATH = os.path.join(DATA_DIR, "scanner_results.json")
NSE_SYMBOLS_PATH = os.path.join(SCRIPT_DIR, "nse_symbols.json")

# Rate limiting
SCREENER_DELAY = 0.5  # 500ms between Screener.in requests
YAHOO_DELAY = 0.1     # 100ms between Yahoo requests
MAX_RETRIES = 3
BATCH_SIZE = 50       # Process in batches to manage memory

# Headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/html",
}

# ============================================================================
# O'NEIL SCORER (Integrated from oneil_scorer.py)
# ============================================================================

class ONeilScorer:
    """
    Calculates O'Neil Master Score (A-E) based on:
    - EPS Strength (0-99)
    - Price Strength / Relative Strength (0-99)
    - Buyer Demand (Accumulation/Distribution)
    - Group Rank (Sector performance)
    """
    
    # EPS Growth scoring thresholds
    EPS_THRESHOLDS = [
        (50, 99), (40, 90), (30, 80), (25, 70), (20, 60),
        (15, 50), (10, 40), (5, 30), (0, 20), (-999, 10)
    ]
    
    # RS scoring thresholds (percentile rank)
    RS_THRESHOLDS = [
        (90, 99), (80, 90), (70, 80), (60, 70), (50, 60),
        (40, 50), (30, 40), (20, 30), (10, 20), (0, 10)
    ]
    
    def __init__(self):
        self.sector_performance = {}
        
    def calculate_eps_strength(self, quarterly_eps: List[float], annual_eps: List[float],
                               trailing_eps: float = None, forward_eps: float = None) -> int:
        """
        Calculate EPS Strength (0-99) based on:
        - Current quarter EPS growth vs year-ago quarter
        - Last 3 quarters EPS growth acceleration
        - Annual EPS growth trend
        - Fallback: trailing vs forward EPS growth from yfinance
        """
        score = 50  # Base score
        used_quarterly = False

        if len(quarterly_eps) >= 5:
            # Current quarter vs year-ago quarter
            current_q = quarterly_eps[0]
            year_ago_q = quarterly_eps[4] if len(quarterly_eps) > 4 else quarterly_eps[-1]

            if year_ago_q > 0:
                q_growth = ((current_q - year_ago_q) / year_ago_q) * 100
                for threshold, points in self.EPS_THRESHOLDS:
                    if q_growth >= threshold:
                        score = points
                        break
                used_quarterly = True

            # Check for acceleration in recent quarters
            if len(quarterly_eps) >= 3:
                recent_growth = [
                    (quarterly_eps[i] - quarterly_eps[i+4]) / quarterly_eps[i+4] * 100
                    if len(quarterly_eps) > i+4 and quarterly_eps[i+4] > 0 else 0
                    for i in range(3)
                ]
                if all(g > 0 for g in recent_growth):
                    # Accelerating EPS - bonus
                    if recent_growth[0] > recent_growth[1] > recent_growth[2]:
                        score = min(99, score + 10)

        # Fallback: use trailing/forward EPS from yfinance
        if not used_quarterly and trailing_eps and trailing_eps > 0:
            if forward_eps and forward_eps > 0:
                eps_growth = ((forward_eps - trailing_eps) / trailing_eps) * 100
                for threshold, points in self.EPS_THRESHOLDS:
                    if eps_growth >= threshold:
                        score = points
                        break
            else:
                # Have trailing EPS but no forward — score based on absolute level
                score = 55 if trailing_eps > 0 else 40

        # Annual EPS trend
        if len(annual_eps) >= 3:
            if all(annual_eps[i] > annual_eps[i+1] for i in range(len(annual_eps)-1)):
                score = min(99, score + 5)  # Consistent growth bonus

        return max(1, min(99, score))
    
    def calculate_price_strength(self, returns: Dict[str, float], market_returns: Dict[str, float] = None) -> int:
        """
        Calculate Relative Strength (0-99) based on:
        - 12-month price performance relative to market
        - 6-month and 3-month momentum
        """
        if not returns:
            return 50
            
        # Default market returns if not provided
        if not market_returns:
            market_returns = {"1y": 12, "6m": 6, "3m": 3, "1m": 1}
        
        score = 50
        
        # 12-month relative strength (weighted 40%)
        stock_1y = returns.get("1y", 0) or returns.get("12m", 0) or 0
        market_1y = market_returns.get("1y", 12)
        rs_1y = stock_1y - market_1y
        
        # 6-month relative strength (weighted 30%)
        stock_6m = returns.get("6m", 0) or 0
        market_6m = market_returns.get("6m", 6)
        rs_6m = stock_6m - market_6m
        
        # 3-month relative strength (weighted 30%)
        stock_3m = returns.get("3m", 0) or 0
        market_3m = market_returns.get("3m", 3)
        rs_3m = stock_3m - market_3m
        
        # Combined RS score
        combined_rs = (rs_1y * 0.4) + (rs_6m * 0.3) + (rs_3m * 0.3)
        
        # Convert to 0-99 scale
        if combined_rs >= 50:
            score = 99
        elif combined_rs >= 30:
            score = 90
        elif combined_rs >= 20:
            score = 80
        elif combined_rs >= 10:
            score = 70
        elif combined_rs >= 0:
            score = 60
        elif combined_rs >= -10:
            score = 50
        elif combined_rs >= -20:
            score = 40
        elif combined_rs >= -30:
            score = 30
        else:
            score = 20
            
        return max(1, min(99, score))
    
    def calculate_buyer_demand(self, volume_data: Dict, price_data: Dict) -> Tuple[str, int]:
        """
        Calculate Accumulation/Distribution rating (A-E) and score.
        A = Heavy accumulation
        B = Moderate accumulation
        C = Neutral
        D = Moderate distribution
        E = Heavy distribution
        """
        score = 50
        
        avg_volume = volume_data.get("avg_volume", 0)
        recent_volume = volume_data.get("recent_volume", 0)
        up_days_volume = volume_data.get("up_days_volume", 0)
        down_days_volume = volume_data.get("down_days_volume", 0)
        
        if avg_volume > 0:
            volume_ratio = recent_volume / avg_volume
            
            # Volume trend analysis
            if up_days_volume > 0 and down_days_volume > 0:
                acc_dist_ratio = up_days_volume / down_days_volume
                
                if acc_dist_ratio >= 1.5 and volume_ratio >= 1.2:
                    return ("A", 90)
                elif acc_dist_ratio >= 1.2 and volume_ratio >= 1.0:
                    return ("B", 70)
                elif acc_dist_ratio >= 0.8:
                    return ("C", 50)
                elif acc_dist_ratio >= 0.5:
                    return ("D", 30)
                else:
                    return ("E", 10)
        
        return ("C", 50)
    
    def calculate_group_rank(self, sector: str, sector_performance: Dict[str, float]) -> int:
        """
        Calculate Group/Industry Rank (1-197 style, converted to percentile).
        Based on sector's 6-month relative performance.
        """
        if not sector or not sector_performance:
            return 50
            
        sector_perf = sector_performance.get(sector, 0)
        all_perfs = sorted(sector_performance.values(), reverse=True)
        
        if sector_perf in all_perfs:
            rank = all_perfs.index(sector_perf) + 1
            percentile = ((len(all_perfs) - rank + 1) / len(all_perfs)) * 100
            return int(percentile)
        
        return 50
    
    def calculate_master_score(
        self,
        eps_strength: int,
        price_strength: int,
        buyer_demand: int,
        group_rank: int
    ) -> Tuple[str, int, Dict]:
        """
        Calculate O'Neil Master Score (A-E) with detailed breakdown.
        Weighting: EPS 25%, RS 35%, Buyer Demand 25%, Group Rank 15%
        """
        composite = (
            eps_strength * 0.25 +
            price_strength * 0.35 +
            buyer_demand * 0.25 +
            group_rank * 0.15
        )
        
        if composite >= 85:
            grade = "A"
        elif composite >= 70:
            grade = "B"
        elif composite >= 55:
            grade = "C"
        elif composite >= 40:
            grade = "D"
        else:
            grade = "E"
            
        breakdown = {
            "eps_strength": eps_strength,
            "price_strength": price_strength,
            "buyer_demand": buyer_demand,
            "group_rank": group_rank,
            "composite_score": round(composite, 1)
        }
        
        return (grade, int(composite), breakdown)


# ============================================================================
# GURU RATINGS (Integrated from oneil_scorer.py)
# ============================================================================

class GuruRatings:
    """
    Calculates strategy scores for:
    - CANSLIM (O'Neil)
    - Graham (Value)
    - Buffett (Quality + Moat)
    - Lynch (PEG + Growth)
    - O'Shaughnessy (Quantitative Value)
    """
    
    def calculate_canslim(self, data: Dict) -> Tuple[int, List[str]]:
        """
        CANSLIM Score (0-100):
        C - Current quarterly EPS growth >= 25%
        A - Annual EPS growth >= 25%
        N - New products/highs/management
        S - Supply/Demand (low float, high demand)
        L - Leader in industry
        I - Institutional sponsorship
        M - Market direction (uptrend)
        """
        score = 0
        flags = []
        
        # C - Current quarterly EPS
        q_eps_growth = data.get("quarterly_eps_growth", 0)
        if q_eps_growth >= 25:
            score += 15
            flags.append("C✓")
        elif q_eps_growth >= 15:
            score += 10
            
        # A - Annual EPS growth
        annual_eps_growth = data.get("annual_eps_growth", 0)
        if annual_eps_growth >= 25:
            score += 15
            flags.append("A✓")
        elif annual_eps_growth >= 15:
            score += 10
            
        # N - New highs (52-week high proximity)
        high_proximity = data.get("high_52w_proximity", 0)
        if high_proximity >= 90:
            score += 15
            flags.append("N✓")
        elif high_proximity >= 80:
            score += 10
            
        # S - Supply/Demand
        avg_volume = data.get("avg_volume", 0)
        recent_volume = data.get("recent_volume", 0)
        if avg_volume > 0 and recent_volume / avg_volume >= 1.5:
            score += 15
            flags.append("S✓")
        elif avg_volume > 0 and recent_volume / avg_volume >= 1.2:
            score += 10
            
        # L - Leader (RS >= 80)
        rs = data.get("rs_rating", 50)
        if rs >= 80:
            score += 15
            flags.append("L✓")
        elif rs >= 70:
            score += 10
            
        # I - Institutional sponsorship
        inst_holding = data.get("institutional_holding", 0)
        if inst_holding >= 20:
            score += 15
            flags.append("I✓")
        elif inst_holding >= 10:
            score += 10
            
        # M - Market direction (assumed uptrend for now)
        market_trend = data.get("market_trend", "up")
        if market_trend == "up":
            score += 10
            flags.append("M✓")
            
        return (min(100, score), flags)
    
    def calculate_graham(self, data: Dict) -> Tuple[int, List[str]]:
        """
        Benjamin Graham Value Score (0-100):
        - P/E <= 15
        - P/B <= 1.5
        - Debt/Equity <= 0.5
        - Current ratio >= 2
        - Dividend paying
        - Consistent earnings (5+ years)
        """
        score = 0
        flags = []
        
        pe = data.get("pe", 0)
        if pe > 0:
            if pe <= 10:
                score += 20
                flags.append("PE✓")
            elif pe <= 15:
                score += 15
            elif pe <= 20:
                score += 5
                
        pb = data.get("pb", 0)
        if pb > 0:
            if pb <= 1.0:
                score += 20
                flags.append("PB✓")
            elif pb <= 1.5:
                score += 15
            elif pb <= 2.0:
                score += 5
                
        de = data.get("debt_equity", 0)
        if de <= 0.3:
            score += 20
            flags.append("DE✓")
        elif de <= 0.5:
            score += 15
        elif de <= 1.0:
            score += 5
            
        current_ratio = data.get("current_ratio", 0)
        if current_ratio >= 2.0:
            score += 15
            flags.append("CR✓")
        elif current_ratio >= 1.5:
            score += 10
            
        div_yield = data.get("dividend_yield", 0)
        if div_yield >= 2.0:
            score += 15
            flags.append("DY✓")
        elif div_yield >= 1.0:
            score += 10
            
        # P/E * P/B <= 22.5 (Graham Number)
        if pe > 0 and pb > 0 and (pe * pb) <= 22.5:
            score += 10
            flags.append("GN✓")
            
        return (min(100, score), flags)
    
    def calculate_buffett(self, data: Dict) -> Tuple[int, List[str]]:
        """
        Warren Buffett Quality Score (0-100):
        - ROE >= 15% consistently
        - Low debt
        - Consistent earnings growth
        - Strong brand/moat indicators
        - Reasonable valuation
        """
        score = 0
        flags = []
        
        roe = data.get("roe", 0)
        if roe >= 20:
            score += 25
            flags.append("ROE✓")
        elif roe >= 15:
            score += 20
        elif roe >= 10:
            score += 10
            
        roce = data.get("roce", 0)
        if roce >= 20:
            score += 20
            flags.append("ROCE✓")
        elif roce >= 15:
            score += 15
            
        de = data.get("debt_equity", 999)
        if de <= 0.3:
            score += 20
            flags.append("DE✓")
        elif de <= 0.5:
            score += 15
        elif de <= 1.0:
            score += 5
            
        # Consistent earnings (5 years of positive EPS growth)
        eps_growth_yrs = data.get("consecutive_eps_growth_years", 0)
        if eps_growth_yrs >= 5:
            score += 20
            flags.append("EPS5Y✓")
        elif eps_growth_yrs >= 3:
            score += 10
            
        # Free cash flow positive
        fcf = data.get("free_cash_flow", 0)
        if fcf > 0:
            score += 15
            flags.append("FCF✓")
            
        return (min(100, score), flags)
    
    def calculate_lynch(self, data: Dict) -> Tuple[int, List[str]]:
        """
        Peter Lynch PEG Score (0-100):
        - PEG <= 1.0 (P/E / EPS growth rate)
        - Insider ownership
        - Institutional underownership
        - Strong cash position
        - Growth with value
        """
        score = 0
        flags = []
        
        peg = data.get("peg", 0)
        if peg > 0:
            if peg <= 0.5:
                score += 30
                flags.append("PEG✓✓")
            elif peg <= 1.0:
                score += 25
                flags.append("PEG✓")
            elif peg <= 1.5:
                score += 15
            elif peg <= 2.0:
                score += 5
                
        # EPS growth rate
        eps_growth = data.get("annual_eps_growth", 0)
        if eps_growth >= 25:
            score += 25
            flags.append("EG✓")
        elif eps_growth >= 15:
            score += 15
            
        # Promoter holding (insider proxy)
        promoter = data.get("promoter_holding", 0)
        if promoter >= 50:
            score += 15
            flags.append("PR✓")
        elif promoter >= 30:
            score += 10
            
        # Low institutional (undiscovered)
        inst = data.get("institutional_holding", 0)
        if 10 <= inst <= 40:
            score += 15
            flags.append("UI✓")
            
        # Cash ratio
        cash_ratio = data.get("cash_ratio", 0)
        if cash_ratio >= 0.5:
            score += 15
            flags.append("CA✓")
            
        return (min(100, score), flags)
    
    def calculate_oshaughnessy(self, data: Dict) -> Tuple[int, List[str]]:
        """
        James O'Shaughnessy Quantitative Value (0-100):
        - Market cap > 150M
        - P/S <= 1.5
        - Strong 12-month momentum
        - High dividend yield
        - Value + momentum combination
        """
        score = 0
        flags = []
        
        mcap = data.get("market_cap", 0)
        if mcap >= 10000:  # 10000 Cr = large cap
            score += 15
            flags.append("MC✓")
        elif mcap >= 2000:
            score += 10
            
        ps = data.get("price_to_sales", 0)
        if ps > 0:
            if ps <= 1.0:
                score += 20
                flags.append("PS✓")
            elif ps <= 1.5:
                score += 15
            elif ps <= 2.0:
                score += 5
                
        # 12-month momentum
        ret_1y = data.get("return_1y", 0)
        if ret_1y >= 30:
            score += 20
            flags.append("M12✓")
        elif ret_1y >= 15:
            score += 15
        elif ret_1y >= 0:
            score += 5
            
        # 6-month momentum
        ret_6m = data.get("return_6m", 0)
        if ret_6m >= 20:
            score += 15
            flags.append("M6✓")
        elif ret_6m >= 10:
            score += 10
            
        # Shareholder yield (div + buyback)
        div_yield = data.get("dividend_yield", 0)
        if div_yield >= 3:
            score += 15
            flags.append("SY✓")
        elif div_yield >= 1.5:
            score += 10
            
        # Earnings stability
        eps_stability = data.get("eps_stability", 0)
        if eps_stability >= 80:
            score += 15
            flags.append("ES✓")
            
        return (min(100, score), flags)
    
    def get_all_ratings(self, data: Dict) -> List[Dict]:
        """Get all guru ratings for a stock."""
        ratings = []
        
        canslim_score, canslim_flags = self.calculate_canslim(data)
        ratings.append({
            "strategy": "CANSLIM",
            "guru": "William O'Neil",
            "score": canslim_score,
            "flags": canslim_flags,
            "grade": self._score_to_grade(canslim_score)
        })
        
        graham_score, graham_flags = self.calculate_graham(data)
        ratings.append({
            "strategy": "Graham",
            "guru": "Benjamin Graham",
            "score": graham_score,
            "flags": graham_flags,
            "grade": self._score_to_grade(graham_score)
        })
        
        buffett_score, buffett_flags = self.calculate_buffett(data)
        ratings.append({
            "strategy": "Buffett",
            "guru": "Warren Buffett",
            "score": buffett_score,
            "flags": buffett_flags,
            "grade": self._score_to_grade(buffett_score)
        })
        
        lynch_score, lynch_flags = self.calculate_lynch(data)
        ratings.append({
            "strategy": "Lynch",
            "guru": "Peter Lynch",
            "score": lynch_score,
            "flags": lynch_flags,
            "grade": self._score_to_grade(lynch_score)
        })
        
        oshaughnessy_score, oshaughnessy_flags = self.calculate_oshaughnessy(data)
        ratings.append({
            "strategy": "O'Shaughnessy",
            "guru": "James O'Shaughnessy",
            "score": oshaughnessy_score,
            "flags": oshaughnessy_flags,
            "grade": self._score_to_grade(oshaughnessy_score)
        })
        
        return ratings
    
    def _score_to_grade(self, score: int) -> str:
        if score >= 80:
            return "A"
        elif score >= 60:
            return "B"
        elif score >= 40:
            return "C"
        elif score >= 20:
            return "D"
        return "F"


# ============================================================================
# SURVEILLANCE CHECKER (Integrated from surveillance_checker.py)
# ============================================================================

class SurveillanceChecker:
    """
    9-Point Surveillance Checklist:
    1. ASM/GSM stage
    2. High debt (D/E > 2)
    3. Low institutional holding (< 5%)
    4. Penny stock (< ₹20)
    5. High promoter pledging (> 20%)
    6. Low liquidity (avg vol < 10k)
    7. Institutional decline (QoQ drop)
    8. Operator-driven patterns
    9. Recent bad news/litigation
    """
    
    def __init__(self):
        # ASM/GSM stocks (to be loaded from NSE)
        self.asm_stocks = set()
        self.gsm_stocks = set()
        
    def check_all(self, data: Dict) -> Dict:
        """Run all surveillance checks and return results."""
        checks = {}
        flags = []
        risk_score = 0
        
        symbol = data.get("symbol", "")
        
        # 1. ASM/GSM Check
        asm_stage = self._check_asm_gsm(symbol)
        checks["asm_gsm"] = {
            "status": asm_stage,
            "passed": asm_stage == "None",
            "description": f"ASM/GSM Stage: {asm_stage}"
        }
        if asm_stage != "None":
            flags.append(f"ASM: {asm_stage}")
            risk_score += 25
            
        # 2. High Debt Check
        de = data.get("debt_equity", 0)
        high_debt = de > 2.0
        checks["high_debt"] = {
            "status": f"D/E: {de:.2f}" if de else "N/A",
            "passed": not high_debt,
            "description": "Debt/Equity ratio check (threshold: 2.0)"
        }
        if high_debt:
            flags.append(f"High Debt: {de:.2f}")
            risk_score += 15
            
        # 3. Low Institutional Holding
        inst = data.get("institutional_holding", 0)
        low_inst = inst < 5
        checks["low_institutional"] = {
            "status": f"{inst:.1f}%",
            "passed": not low_inst,
            "description": "Institutional holding check (threshold: 5%)"
        }
        if low_inst:
            flags.append(f"Low Inst: {inst:.1f}%")
            risk_score += 10
            
        # 4. Penny Stock Check
        cmp = data.get("cmp", 0)
        penny = cmp < 20
        checks["penny_stock"] = {
            "status": f"₹{cmp:.2f}",
            "passed": not penny,
            "description": "Penny stock check (threshold: ₹20)"
        }
        if penny:
            flags.append(f"Penny: ₹{cmp:.2f}")
            risk_score += 20
            
        # 5. Promoter Pledging
        pledge = data.get("promoter_pledging", 0)
        high_pledge = pledge > 20
        checks["promoter_pledging"] = {
            "status": f"{pledge:.1f}%",
            "passed": not high_pledge,
            "description": "Promoter pledging check (threshold: 20%)"
        }
        if high_pledge:
            flags.append(f"Pledging: {pledge:.1f}%")
            risk_score += 15
            
        # 6. Low Liquidity
        avg_vol = data.get("avg_volume", 0)
        low_liq = avg_vol < 10000
        checks["low_liquidity"] = {
            "status": f"{avg_vol:,.0f}",
            "passed": not low_liq,
            "description": "Liquidity check (threshold: 10,000 shares)"
        }
        if low_liq:
            flags.append(f"Low Vol: {avg_vol:,.0f}")
            risk_score += 10
            
        # 7. Institutional Decline
        inst_change = data.get("institutional_change_qoq", 0)
        inst_decline = inst_change < -2  # 2% decline
        checks["institutional_decline"] = {
            "status": f"{inst_change:+.1f}%",
            "passed": not inst_decline,
            "description": "QoQ institutional change check"
        }
        if inst_decline:
            flags.append(f"Inst Decline: {inst_change:+.1f}%")
            risk_score += 10
            
        # 8. Operator Pattern (high volatility + unusual volume)
        volatility = data.get("volatility_30d", 0)
        vol_ratio = data.get("volume_ratio", 1)
        operator = volatility > 50 and vol_ratio > 3
        checks["operator_pattern"] = {
            "status": f"Vol: {volatility:.1f}%, VR: {vol_ratio:.1f}x",
            "passed": not operator,
            "description": "Unusual price/volume pattern check"
        }
        if operator:
            flags.append("Operator Pattern")
            risk_score += 15
            
        # 9. Circuit Band Limited (2% or 5%)
        circuit = data.get("circuit_band", "20")
        circuit_limited = circuit in ["2", "5"]
        checks["circuit_limited"] = {
            "status": f"{circuit}%",
            "passed": not circuit_limited,
            "description": "Circuit band restriction check"
        }
        if circuit_limited:
            flags.append(f"Circuit: {circuit}%")
            risk_score += 20
            
        # Calculate overall status
        passed_count = sum(1 for c in checks.values() if c["passed"])
        total_count = len(checks)
        
        return {
            "checks": checks,
            "flags": flags,
            "risk_score": min(100, risk_score),
            "passed_count": passed_count,
            "total_count": total_count,
            "status": "SAFE" if risk_score < 20 else "CAUTION" if risk_score < 50 else "DANGER",
            "updated_at": datetime.now().isoformat()
        }
    
    def _check_asm_gsm(self, symbol: str) -> str:
        """Check if stock is in ASM/GSM list."""
        if symbol in self.gsm_stocks:
            return "GSM"
        if symbol in self.asm_stocks:
            return "ASM"
        return "None"


# ============================================================================
# SCREENER.IN DATA FETCHER
# ============================================================================

class ScreenerFetcher:
    """
    Fetches fundamental data from Screener.in:
    - Company overview (Market Cap, P/E, ROE, etc.)
    - Quarterly results
    - Shareholding pattern
    - Key ratios
    """
    
    def __init__(self, worker_url: str = SCREENER_PROXY):
        self.worker_url = worker_url
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
    def fetch_company_data(self, symbol: str) -> Optional[Dict]:
        """
        Fetch all company data from Screener.in.
        Returns fundamentals, quarterly results, ownership.
        """
        try:
            # Screener URL format: screener.in/company/{SYMBOL}/consolidated/
            # or screener.in/company/{SYMBOL}/ for standalone
            
            screener_url = f"https://www.screener.in/company/{symbol}/consolidated/"
            
            response = self.session.get(
                self.worker_url,
                params={"url": screener_url},
                headers={"X-Kite-Action": "screener"},
                timeout=30
            )
            
            if response.status_code == 404:
                # Try standalone
                screener_url = f"https://www.screener.in/company/{symbol}/"
                response = self.session.get(
                    self.worker_url,
                    params={"url": screener_url},
                    headers={"X-Kite-Action": "screener"},
                    timeout=30
                )
            
            if response.status_code != 200:
                return None
                
            html = response.text
            return self._parse_screener_html(html, symbol)
            
        except Exception as e:
            print(f"Error fetching Screener data for {symbol}: {e}")
            return None
    
    def _parse_screener_html(self, html: str, symbol: str) -> Dict:
        """Parse Screener.in HTML to extract data."""
        data = {
            "symbol": symbol,
            "fundamentals": {},
            "quarterly_results": [],
            "ownership": {},
            "key_ratios": {}
        }
        
        try:
            # Extract key metrics using regex patterns
            patterns = {
                "market_cap": r'Market Cap[^\d]*([\d,\.]+)\s*Cr',
                "pe": r'Stock P/E[^\d]*([\d\.]+)',
                "book_value": r'Book Value[^\d]*([\d,\.]+)',
                "dividend_yield": r'Dividend Yield[^\d]*([\d\.]+)\s*%',
                "roce": r'ROCE[^\d]*([\d\.]+)\s*%',
                "roe": r'ROE[^\d]*([\d\.]+)\s*%',
                "face_value": r'Face Value[^\d]*([\d\.]+)',
                "debt_equity": r'Debt to equity[^\d]*([\d\.]+)',
                "current_ratio": r'Current ratio[^\d]*([\d\.]+)',
                "promoter_holding": r'Promoters[^\d]*([\d\.]+)\s*%',
                "fii_holding": r'FIIs[^\d]*([\d\.]+)\s*%',
                "dii_holding": r'DIIs[^\d]*([\d\.]+)\s*%',
                "public_holding": r'Public[^\d]*([\d\.]+)\s*%',
                "eps": r'EPS[^\d]*([\d\.]+)',
                "peg": r'PEG Ratio[^\d]*([\d\.]+)',
                "price_to_sales": r'Price to Sales[^\d]*([\d\.]+)',
            }
            
            for key, pattern in patterns.items():
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    value = match.group(1).replace(",", "")
                    try:
                        data["fundamentals"][key] = float(value)
                    except ValueError:
                        data["fundamentals"][key] = value
            
            # Extract quarterly results table
            quarters = self._extract_quarterly_results(html)
            if quarters:
                data["quarterly_results"] = quarters
                
            # Extract shareholding
            data["ownership"] = {
                "promoter": data["fundamentals"].get("promoter_holding", 0),
                "fii": data["fundamentals"].get("fii_holding", 0),
                "dii": data["fundamentals"].get("dii_holding", 0),
                "public": data["fundamentals"].get("public_holding", 0),
            }
            
        except Exception as e:
            print(f"Error parsing Screener HTML for {symbol}: {e}")
            
        return data
    
    def _extract_quarterly_results(self, html: str) -> List[Dict]:
        """Extract quarterly results from Screener HTML."""
        quarters = []
        
        try:
            # Look for quarterly results table
            # Pattern: Quarter ending, Sales, Expenses, Operating Profit, OPM, Net Profit, EPS
            quarter_pattern = r'(Mar|Jun|Sep|Dec)\s*(\d{4})[^\d]*([\d,\.]+)[^\d]*([\d,\.]+)[^\d]*([\d,\.]+)[^\d]*([\d\.]+)%[^\d]*([\d,\.]+)[^\d]*([\d\.]+)'
            
            matches = re.findall(quarter_pattern, html)
            for match in matches[:8]:  # Last 8 quarters
                try:
                    quarters.append({
                        "quarter": f"{match[0]} {match[1]}",
                        "sales": float(match[2].replace(",", "")),
                        "expenses": float(match[3].replace(",", "")),
                        "operating_profit": float(match[4].replace(",", "")),
                        "opm": float(match[5]),
                        "net_profit": float(match[6].replace(",", "")),
                        "eps": float(match[7])
                    })
                except (ValueError, IndexError):
                    continue
                    
        except Exception as e:
            print(f"Error extracting quarterly results: {e}")
            
        return quarters


# ============================================================================
# YAHOO FINANCE FETCHER (Enhanced)
# ============================================================================

class YahooFetcher:
    """
    Enhanced Yahoo Finance data fetcher via Cloudflare Worker.
    Gets: Price, Volume, Returns, Technical indicators
    """
    
    def __init__(self, worker_url: str = YAHOO_WORKER):
        self.worker_url = worker_url
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
    def fetch_stock_data(self, symbol: str, period: str = "1y") -> Optional[Dict]:
        """Fetch stock data from Yahoo via Worker."""
        try:
            yahoo_symbol = f"{symbol}.NS"
            
            response = self.session.get(
                f"{self.worker_url}/quote/{yahoo_symbol}",
                params={"period": period},
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            return None
            
        except Exception as e:
            print(f"Error fetching Yahoo data for {symbol}: {e}")
            return None
    
    def calculate_returns(self, prices: List[float]) -> Dict[str, float]:
        """Calculate returns for various periods."""
        if not prices or len(prices) < 2:
            return {}
            
        returns = {}
        current = prices[-1]
        
        periods = [
            ("1d", 1), ("1w", 5), ("1m", 21), 
            ("3m", 63), ("6m", 126), ("1y", 252)
        ]
        
        for name, days in periods:
            if len(prices) > days:
                past = prices[-(days+1)]
                if past > 0:
                    returns[name] = ((current - past) / past) * 100
                    
        return returns


# ============================================================================
# MASTER STOCK DETAIL GENERATOR
# ============================================================================

class StockDetailGenerator:
    """
    Master orchestrator that generates complete stock details.
    Combines: Yahoo data, Screener fundamentals, O'Neil scores,
    Guru ratings, Surveillance checks.
    """
    
    def __init__(self):
        self.yahoo = YahooFetcher()
        self.screener = ScreenerFetcher()
        self.oneil = ONeilScorer()
        self.guru = GuruRatings()
        self.surveillance = SurveillanceChecker()
        
        # Load existing stock universe
        self.symbols = self._load_symbols()
        
        # Market returns for RS calculation
        self.market_returns = {"1y": 15, "6m": 8, "3m": 4, "1m": 1.5}
        
        # Sector performance cache
        self.sector_performance = {}
        
    def _load_symbols(self) -> List[str]:
        """Load NSE symbols list."""
        if os.path.exists(NSE_SYMBOLS_PATH):
            with open(NSE_SYMBOLS_PATH, "r") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return [s["symbol"] if isinstance(s, dict) else s for s in data]
                return list(data.keys()) if isinstance(data, dict) else []
        return []
    
    def generate_stock_detail(self, symbol: str, existing_data: Dict = None) -> Dict:
        """
        Generate complete stock detail for a single symbol.
        Merges with existing technical data if available.
        """
        detail = existing_data or {
            "symbol": symbol,
            "name": symbol,
            "updated_at": datetime.now().isoformat()
        }
        
        try:
            # 1. Fetch Screener.in fundamentals
            screener_data = self.screener.fetch_company_data(symbol)
            if screener_data:
                detail["fundamentals"] = screener_data.get("fundamentals", {})
                detail["quarterly_results"] = screener_data.get("quarterly_results", [])
                detail["ownership"] = screener_data.get("ownership", {})
                
            # 2. Build data dict for scoring
            scoring_data = self._build_scoring_data(detail)
            
            # 3. Calculate O'Neil scores
            eps_strength = self.oneil.calculate_eps_strength(
                scoring_data.get("quarterly_eps", []),
                scoring_data.get("annual_eps", []),
                trailing_eps=scoring_data.get("trailing_eps"),
                forward_eps=scoring_data.get("forward_eps"),
            )
            
            price_strength = self.oneil.calculate_price_strength(
                scoring_data.get("returns", {}),
                self.market_returns
            )
            
            buyer_demand_rating, buyer_demand_score = self.oneil.calculate_buyer_demand(
                scoring_data.get("volume_data", {}),
                scoring_data.get("price_data", {})
            )
            
            group_rank = self.oneil.calculate_group_rank(
                scoring_data.get("sector", ""),
                self.sector_performance
            )
            
            grade, composite, breakdown = self.oneil.calculate_master_score(
                eps_strength, price_strength, buyer_demand_score, group_rank
            )
            
            detail["oneil"] = {
                "master_score": grade,
                "composite_score": composite,
                "eps_strength": eps_strength,
                "price_strength": price_strength,
                "buyer_demand": buyer_demand_rating,
                "buyer_demand_score": buyer_demand_score,
                "group_rank": group_rank,
                "breakdown": breakdown
            }
            
            # 4. Calculate Guru ratings
            detail["guru_ratings"] = self.guru.get_all_ratings(scoring_data)
            
            # 5. Run Surveillance checks
            surveillance_data = {
                "symbol": symbol,
                "cmp": scoring_data.get("cmp", 0),
                "debt_equity": detail.get("fundamentals", {}).get("debt_equity", 0),
                "institutional_holding": scoring_data.get("institutional_holding", 0),
                "promoter_pledging": scoring_data.get("promoter_pledging", 0),
                "avg_volume": scoring_data.get("avg_volume", 0),
                "institutional_change_qoq": scoring_data.get("institutional_change_qoq", 0),
                "volatility_30d": scoring_data.get("volatility_30d", 0),
                "volume_ratio": scoring_data.get("volume_ratio", 1),
                "circuit_band": scoring_data.get("circuit_band", "20")
            }
            detail["surveillance"] = self.surveillance.check_all(surveillance_data)
            
            # 6. Update timestamp
            detail["updated_at"] = datetime.now().isoformat()
            detail["data_complete"] = True
            
        except Exception as e:
            print(f"Error generating detail for {symbol}: {e}")
            traceback.print_exc()
            detail["error"] = str(e)
            detail["data_complete"] = False
            
        return detail
    
    def _build_scoring_data(self, detail: Dict) -> Dict:
        """Build unified data dict for all scorers."""
        fund = detail.get("fundamentals", {})
        tech = detail.get("technical", {})
        own = detail.get("ownership", {})
        quarters = detail.get("quarterly_results", [])
        
        # Extract EPS from quarterly results
        quarterly_eps = [q.get("eps", 0) for q in quarters[:8]]
        
        # Calculate returns from price history
        returns = tech.get("returns", {})
        
        # Volume data — fetch OHLCV from yfinance for up/down day volume
        avg_volume = tech.get("avg_volume", 0)
        recent_volume = tech.get("volume", 0)
        up_days_volume = 0
        down_days_volume = 0
        symbol = detail.get("symbol", "")
        try:
            ticker = yf.Ticker(symbol + ".NS")
            hist = ticker.history(period="3mo", interval="1d")
            if not hist.empty:
                up_days_volume = int(hist[hist['Close'] > hist['Open']]['Volume'].sum())
                down_days_volume = int(hist[hist['Close'] < hist['Open']]['Volume'].sum())
        except Exception:
            pass

        return {
            "symbol": detail.get("symbol", ""),
            "cmp": tech.get("close", 0) or fund.get("current_price", 0),
            "pe": fund.get("pe", 0),
            "pb": fund.get("price_to_book", 0) or (fund.get("market_cap", 0) / fund.get("book_value", 1) if fund.get("book_value") else 0),
            "roe": fund.get("roe", 0),
            "roce": fund.get("roce", 0),
            "debt_equity": fund.get("debt_equity", 0),
            "current_ratio": fund.get("current_ratio", 0),
            "dividend_yield": fund.get("dividend_yield", 0),
            "eps": fund.get("eps", 0),
            "peg": fund.get("peg", 0),
            "price_to_sales": fund.get("price_to_sales", 0),
            "market_cap": fund.get("market_cap", 0),
            
            # Ownership
            "promoter_holding": own.get("promoter", 0),
            "fii_holding": own.get("fii", 0),
            "dii_holding": own.get("dii", 0),
            "institutional_holding": own.get("fii", 0) + own.get("dii", 0),
            "promoter_pledging": own.get("promoter_pledging", 0),
            "institutional_change_qoq": own.get("institutional_change_qoq", 0),
            
            # EPS data
            "quarterly_eps": quarterly_eps,
            "annual_eps": [],  # Need annual data
            "quarterly_eps_growth": self._calc_eps_growth(quarterly_eps),
            "annual_eps_growth": fund.get("eps_growth", 0),
            "consecutive_eps_growth_years": 0,
            "trailing_eps": fund.get("trailing_eps", 0) or fund.get("eps", 0),
            "forward_eps": fund.get("forward_eps", 0),
            
            # Price data
            "returns": returns,
            "return_1y": returns.get("1y", 0),
            "return_6m": returns.get("6m", 0),
            "return_3m": returns.get("3m", 0),
            "high_52w_proximity": tech.get("high_52w_proximity", 0),
            
            # Volume data
            "avg_volume": avg_volume,
            "recent_volume": recent_volume,
            "volume_ratio": recent_volume / avg_volume if avg_volume > 0 else 1,
            "up_days_volume": up_days_volume,
            "down_days_volume": down_days_volume,
            "volume_data": {
                "avg_volume": avg_volume,
                "recent_volume": recent_volume,
                "up_days_volume": up_days_volume,
                "down_days_volume": down_days_volume,
            },
            
            # Technical
            "rs_rating": tech.get("rs_rating", 50),
            "volatility_30d": tech.get("volatility_30d", 0),
            "circuit_band": tech.get("circuit_band", "20"),
            
            # Other
            "sector": detail.get("sector", ""),
            "free_cash_flow": fund.get("free_cash_flow", 0),
            "cash_ratio": fund.get("cash_ratio", 0),
            "eps_stability": 50,  # Default
            "market_trend": "up"  # Assume uptrend
        }
    
    def _calc_eps_growth(self, quarterly_eps: List[float]) -> float:
        """Calculate YoY EPS growth from quarterly data."""
        if len(quarterly_eps) >= 5 and quarterly_eps[4] > 0:
            return ((quarterly_eps[0] - quarterly_eps[4]) / quarterly_eps[4]) * 100
        return 0
    
    def process_all_stocks(
        self,
        symbols: List[str] = None,
        top_n: int = None,
        batch_num: int = None,
        total_batches: int = None
    ) -> Dict[str, Any]:
        """
        Process multiple stocks and generate details.
        
        Args:
            symbols: List of symbols to process (default: all)
            top_n: Process only top N by volume
            batch_num: Current batch number (1-indexed)
            total_batches: Total number of batches
        """
        target_symbols = symbols or self.symbols
        
        # Apply batch filtering if specified
        if batch_num and total_batches:
            batch_size = len(target_symbols) // total_batches
            start_idx = (batch_num - 1) * batch_size
            end_idx = start_idx + batch_size if batch_num < total_batches else len(target_symbols)
            target_symbols = target_symbols[start_idx:end_idx]
            print(f"Processing batch {batch_num}/{total_batches}: {len(target_symbols)} stocks")
        
        # Apply top N filter
        if top_n and top_n < len(target_symbols):
            # Sort by existing volume data if available
            target_symbols = target_symbols[:top_n]
            print(f"Processing top {top_n} stocks")
        
        results = {
            "processed": 0,
            "success": 0,
            "failed": 0,
            "errors": [],
            "start_time": datetime.now().isoformat()
        }
        
        os.makedirs(STOCK_DETAILS_DIR, exist_ok=True)
        
        for i, symbol in enumerate(target_symbols):
            try:
                print(f"[{i+1}/{len(target_symbols)}] Processing {symbol}...")
                
                # Load existing data if available
                detail_path = os.path.join(STOCK_DETAILS_DIR, f"{symbol}.json")
                existing_data = None
                if os.path.exists(detail_path):
                    with open(detail_path, "r") as f:
                        existing_data = json.load(f)
                
                # Generate complete detail
                detail = self.generate_stock_detail(symbol, existing_data)
                
                # Save to file
                with open(detail_path, "w") as f:
                    json.dump(detail, f, indent=2, default=str)
                
                results["success"] += 1
                
                # Rate limiting
                time.sleep(SCREENER_DELAY)
                
            except Exception as e:
                results["failed"] += 1
                results["errors"].append({"symbol": symbol, "error": str(e)})
                print(f"  Error: {e}")
                
            results["processed"] += 1
            
            # Progress update every 50 stocks
            if (i + 1) % 50 == 0:
                print(f"Progress: {i+1}/{len(target_symbols)} ({results['success']} success, {results['failed']} failed)")
        
        results["end_time"] = datetime.now().isoformat()

        # Second pass: build sector_performance and re-score group_rank + composite
        sector_returns = {}  # {sector: [list of 1y returns]}
        detail_cache = {}
        for symbol in target_symbols:
            detail_path = os.path.join(STOCK_DETAILS_DIR, f"{symbol}.json")
            if not os.path.exists(detail_path):
                continue
            try:
                with open(detail_path, "r") as f:
                    detail = json.load(f)
                detail_cache[symbol] = detail
                sector = detail.get("sector", "")
                ret_1y = detail.get("technical", {}).get("returns", {}).get("1y", 0) or 0
                if sector:
                    sector_returns.setdefault(sector, []).append(ret_1y)
            except Exception:
                continue

        if sector_returns:
            self.sector_performance = {
                s: sum(rets) / len(rets) for s, rets in sector_returns.items()
            }
            print(f"\nRe-scoring {len(detail_cache)} stocks with sector performance ({len(self.sector_performance)} sectors)...")
            for symbol, detail in detail_cache.items():
                try:
                    scoring_data = self._build_scoring_data(detail)
                    eps_strength = self.oneil.calculate_eps_strength(
                        scoring_data.get("quarterly_eps", []),
                        scoring_data.get("annual_eps", []),
                        trailing_eps=scoring_data.get("trailing_eps"),
                        forward_eps=scoring_data.get("forward_eps"),
                    )
                    price_strength = self.oneil.calculate_price_strength(
                        scoring_data.get("returns", {}),
                        self.market_returns
                    )
                    _, buyer_demand_score = self.oneil.calculate_buyer_demand(
                        scoring_data.get("volume_data", {}),
                        scoring_data.get("price_data", {})
                    )
                    group_rank = self.oneil.calculate_group_rank(
                        scoring_data.get("sector", ""),
                        self.sector_performance
                    )
                    grade, composite, breakdown = self.oneil.calculate_master_score(
                        eps_strength, price_strength, buyer_demand_score, group_rank
                    )
                    detail["oneil"]["master_score"] = grade
                    detail["oneil"]["composite_score"] = composite
                    detail["oneil"]["group_rank"] = group_rank
                    detail["oneil"]["breakdown"] = breakdown
                    detail_path = os.path.join(STOCK_DETAILS_DIR, f"{symbol}.json")
                    with open(detail_path, "w") as f:
                        json.dump(detail, f, indent=2, default=str)
                except Exception:
                    continue

        # Save summary
        summary_path = os.path.join(DATA_DIR, "generation_summary.json")
        with open(summary_path, "w") as f:
            json.dump(results, f, indent=2)
        
        print(f"\nCompleted: {results['success']} success, {results['failed']} failed")
        return results


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Stock Detail Generator for TradEdge")
    parser.add_argument("--symbol", type=str, help="Process single symbol")
    parser.add_argument("--top", type=int, help="Process top N stocks by volume")
    parser.add_argument("--batch", type=int, help="Batch number (1-indexed)")
    parser.add_argument("--total", type=int, help="Total number of batches")
    parser.add_argument("--test", action="store_true", help="Test mode - process first 5 stocks")
    
    args = parser.parse_args()
    
    generator = StockDetailGenerator()
    
    if args.symbol:
        # Single symbol mode
        print(f"Generating detail for {args.symbol}...")
        detail = generator.generate_stock_detail(args.symbol)
        
        # Save to file
        os.makedirs(STOCK_DETAILS_DIR, exist_ok=True)
        detail_path = os.path.join(STOCK_DETAILS_DIR, f"{args.symbol}.json")
        with open(detail_path, "w") as f:
            json.dump(detail, f, indent=2, default=str)
        
        print(f"Saved to {detail_path}")
        print(json.dumps(detail, indent=2, default=str)[:2000])  # Preview
        
    elif args.test:
        # Test mode
        print("Test mode: Processing first 5 stocks...")
        results = generator.process_all_stocks(symbols=generator.symbols[:5])
        print(json.dumps(results, indent=2))
        
    else:
        # Full processing
        results = generator.process_all_stocks(
            top_n=args.top,
            batch_num=args.batch,
            total_batches=args.total
        )
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
