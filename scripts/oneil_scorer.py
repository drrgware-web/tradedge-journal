#!/usr/bin/env python3
"""
TradEdge O'Neil / CANSLIM Scoring Engine v1.0
===============================================
Computes stock quality scores based on William O'Neil's methodology.

Scores:
  - EPS Strength (0-99): Earnings Per Share growth ranking
  - Price Strength (0-99): Relative price strength vs market
  - Buyer Demand (A-E): Accumulation/Distribution rating
  - Group Rank (0-99): Industry group relative ranking
  - Master Score (A-E): Composite of all above

Guru Strategy Ratings:
  - William O'Neil (CANSLIM)
  - Benjamin Graham (Value)
  - Warren Buffett (Quality + Moat)
  - Peter Lynch (PEG + Growth)
  - James O'Shaughnessy (What Works on Wall Street)

Data sources: yfinance fundamentals + OHLCV
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Optional


@dataclass
class ONeilScore:
    """Complete O'Neil analysis for a stock."""
    # Core scores
    eps_strength: int = 0          # 0-99
    price_strength: int = 0        # 0-99 (RS Rating)
    buyer_demand: str = "D"        # A-E
    buyer_demand_score: int = 0    # 0-99 numeric
    group_rank: int = 0            # 0-99
    master_score: str = "D"        # A-E
    
    # Detailed EPS metrics
    eps_current_qtr_growth: float = 0    # Current quarter EPS growth %
    eps_prior_qtr_growth: float = 0      # Prior quarter EPS growth %
    eps_annual_growth: float = 0         # Annual EPS growth %
    eps_consistency: float = 0           # EPS growth consistency (0-1)
    
    # Price metrics
    rs_rating: int = 0                   # Relative Strength rating 0-99
    price_vs_52w_high_pct: float = 0     # % from 52-week high
    above_200dma: bool = False
    above_50dma: bool = False
    
    # Volume/Demand metrics
    up_down_volume_ratio: float = 0      # U/D volume ratio
    accumulation_days: int = 0           # Days of institutional accumulation
    
    # AI-generated analysis text
    analysis_text: str = ""
    
    def to_dict(self) -> dict:
        return {
            "eps_strength": self.eps_strength,
            "price_strength": self.price_strength,
            "buyer_demand": self.buyer_demand,
            "buyer_demand_score": self.buyer_demand_score,
            "group_rank": self.group_rank,
            "master_score": self.master_score,
            "eps_current_qtr_growth": self.eps_current_qtr_growth,
            "eps_prior_qtr_growth": self.eps_prior_qtr_growth,
            "eps_annual_growth": self.eps_annual_growth,
            "rs_rating": self.rs_rating,
            "price_vs_52w_high_pct": self.price_vs_52w_high_pct,
            "above_200dma": self.above_200dma,
            "above_50dma": self.above_50dma,
            "up_down_volume_ratio": self.up_down_volume_ratio,
            "accumulation_days": self.accumulation_days,
            "analysis_text": self.analysis_text,
        }


@dataclass
class GuruRating:
    """Guru strategy pass/fail rating."""
    guru_name: str
    strategy_name: str
    score: str          # "PASS", "FAIR", "FAIL"
    score_pct: int      # 0-100
    criteria_met: int
    criteria_total: int
    details: list       # list of (criterion, passed: bool, detail: str)
    
    def to_dict(self) -> dict:
        return {
            "guru": self.guru_name,
            "strategy": self.strategy_name,
            "score": self.score,
            "score_pct": self.score_pct,
            "criteria_met": self.criteria_met,
            "criteria_total": self.criteria_total,
            "details": [{"criterion": d[0], "passed": d[1], "detail": d[2]} for d in self.details],
        }


# ═══════════════════════════════════════════════════════════════════════════════
# O'NEIL SCORING ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class ONeilScorer:
    """Computes O'Neil CANSLIM scores for a stock."""
    
    def score(self, df: pd.DataFrame, info: dict,
              market_df: pd.DataFrame = None) -> ONeilScore:
        """
        Compute full O'Neil analysis.
        
        Args:
            df: Stock OHLCV DataFrame (1 year minimum)
            info: yfinance ticker.info dict
            market_df: Nifty 50 / benchmark OHLCV (for RS calculation)
        """
        result = ONeilScore()
        
        if df is None or len(df) < 50:
            return result
        
        close = df["Close"]
        high = df["High"]
        low = df["Low"]
        volume = df["Volume"]
        
        # ── EPS Strength ──
        result.eps_strength = self._calc_eps_strength(info)
        result.eps_current_qtr_growth = self._get_eps_growth(info, "current")
        result.eps_prior_qtr_growth = self._get_eps_growth(info, "prior")
        result.eps_annual_growth = self._get_annual_eps_growth(info)
        
        # ── Price Strength (RS Rating) ──
        result.price_strength = self._calc_price_strength(close, market_df)
        result.rs_rating = result.price_strength
        
        high_52w = float(high.tail(252).max()) if len(high) >= 252 else float(high.max())
        result.price_vs_52w_high_pct = round((float(close.iloc[-1]) - high_52w) / high_52w * 100, 2)
        
        ema200 = close.ewm(span=200, adjust=False).mean()
        ema50 = close.ewm(span=50, adjust=False).mean()
        result.above_200dma = float(close.iloc[-1]) > float(ema200.iloc[-1])
        result.above_50dma = float(close.iloc[-1]) > float(ema50.iloc[-1])
        
        # ── Buyer Demand (Accumulation/Distribution) ──
        bd_score = self._calc_buyer_demand(close, volume)
        result.buyer_demand_score = bd_score
        result.buyer_demand = self._score_to_grade(bd_score)
        result.up_down_volume_ratio = self._calc_ud_volume_ratio(close, volume)
        result.accumulation_days = self._count_accumulation_days(close, volume, 50)
        
        # ── Group Rank ──
        result.group_rank = self._calc_group_rank(info)
        
        # ── Master Score ──
        avg = (result.eps_strength + result.price_strength + bd_score + result.group_rank) / 4
        result.master_score = self._score_to_grade(int(avg))
        
        # ── Analysis Text ──
        result.analysis_text = self._generate_analysis(result, info)
        
        return result
    
    def _calc_eps_strength(self, info: dict) -> int:
        """
        EPS Strength 0-99.
        Based on: trailing EPS growth, revenue growth, profit margins.
        """
        score = 50  # Start at median
        
        # EPS growth
        eps = info.get("trailingEps", 0) or 0
        if eps > 0:
            score += 10
        
        # Revenue growth
        rg = info.get("revenueGrowth", 0) or 0
        if rg > 0.25:
            score += 20
        elif rg > 0.15:
            score += 15
        elif rg > 0.10:
            score += 10
        elif rg > 0:
            score += 5
        elif rg < -0.10:
            score -= 15
        
        # Profit margins
        pm = info.get("profitMargins", 0) or 0
        if pm > 0.20:
            score += 15
        elif pm > 0.10:
            score += 10
        elif pm > 0.05:
            score += 5
        elif pm < 0:
            score -= 20
        
        # ROE
        roe = info.get("returnOnEquity", 0) or 0
        if roe > 0.25:
            score += 10
        elif roe > 0.15:
            score += 5
        elif roe < 0:
            score -= 10
        
        # Earnings growth
        eg = info.get("earningsGrowth", 0) or 0
        if eg > 0.25:
            score += 15
        elif eg > 0.10:
            score += 10
        elif eg < -0.10:
            score -= 15
        
        return max(1, min(99, score))
    
    def _calc_price_strength(self, close: pd.Series, market_df: pd.DataFrame = None) -> int:
        """
        Price Strength (RS Rating) 0-99.
        Measures stock's price performance relative to the market.
        """
        if len(close) < 50:
            return 50
        
        # Calculate weighted returns (recent performance weighted more)
        latest = float(close.iloc[-1])
        
        # 3-month, 6-month, 9-month, 12-month returns
        returns = []
        for period in [63, 126, 189, 252]:
            if len(close) >= period:
                past = float(close.iloc[-period])
                if past > 0:
                    returns.append((latest - past) / past * 100)
        
        if not returns:
            return 50
        
        # Weight: 40% recent quarter, 20% each for older quarters
        if len(returns) >= 4:
            weighted = returns[0] * 0.4 + returns[1] * 0.2 + returns[2] * 0.2 + returns[3] * 0.2
        elif len(returns) >= 2:
            weighted = returns[0] * 0.6 + returns[1] * 0.4
        else:
            weighted = returns[0]
        
        # Map to 0-99 scale (roughly: -50% = 1, 0% = 50, +100% = 99)
        score = int(50 + weighted * 0.5)
        return max(1, min(99, score))
    
    def _calc_buyer_demand(self, close: pd.Series, volume: pd.Series) -> int:
        """
        Buyer Demand score 0-99.
        Based on accumulation/distribution analysis.
        """
        if len(close) < 50:
            return 50
        
        # Calculate daily A/D
        changes = close.diff()
        up_vol = volume[changes > 0].tail(50).sum()
        down_vol = volume[changes < 0].tail(50).sum()
        
        if down_vol == 0:
            ratio = 2.0
        else:
            ratio = float(up_vol) / float(down_vol)
        
        # Map ratio to score
        # ratio 2.0+ = 90+, 1.5 = 75, 1.0 = 50, 0.5 = 25, < 0.5 = 10
        if ratio >= 2.0:
            score = 90
        elif ratio >= 1.5:
            score = 75 + int((ratio - 1.5) / 0.5 * 15)
        elif ratio >= 1.0:
            score = 50 + int((ratio - 1.0) / 0.5 * 25)
        elif ratio >= 0.5:
            score = 25 + int((ratio - 0.5) / 0.5 * 25)
        else:
            score = max(5, int(ratio / 0.5 * 25))
        
        return max(1, min(99, score))
    
    def _calc_ud_volume_ratio(self, close: pd.Series, volume: pd.Series, period: int = 50) -> float:
        """Calculate Up/Down Volume Ratio."""
        changes = close.diff().tail(period)
        vol = volume.tail(period)
        
        up_vol = float(vol[changes > 0].sum())
        down_vol = float(vol[changes <= 0].sum())
        
        return round(up_vol / down_vol, 2) if down_vol > 0 else 9.99
    
    def _count_accumulation_days(self, close: pd.Series, volume: pd.Series, period: int) -> int:
        """Count days with above-average volume on up days (institutional buying)."""
        avg_vol = volume.tail(period).mean()
        changes = close.diff().tail(period)
        vol = volume.tail(period)
        
        return int(((changes > 0) & (vol > avg_vol * 1.5)).sum())
    
    def _calc_group_rank(self, info: dict) -> int:
        """
        Group Rank 0-99.
        Based on industry/sector relative strength.
        Simplified: uses sector + fundamentals as proxy.
        """
        score = 50
        
        # Strong sectors get a boost
        sector = info.get("sector", "")
        strong_sectors = ["Technology", "Healthcare", "Financial Services"]
        weak_sectors = ["Real Estate", "Utilities"]
        
        if sector in strong_sectors:
            score += 15
        elif sector in weak_sectors:
            score -= 10
        
        # Revenue growth as industry proxy
        rg = info.get("revenueGrowth", 0) or 0
        score += int(rg * 50)
        
        return max(1, min(99, score))
    
    def _get_eps_growth(self, info: dict, period: str = "current") -> float:
        """Get EPS growth percentage."""
        eg = info.get("earningsGrowth", 0) or 0
        return round(eg * 100, 1)
    
    def _get_annual_eps_growth(self, info: dict) -> float:
        rg = info.get("revenueGrowth", 0) or 0
        return round(rg * 100, 1)
    
    def _score_to_grade(self, score: int) -> str:
        """Convert numeric score to A-E grade."""
        if score >= 80:
            return "A"
        elif score >= 60:
            return "B"
        elif score >= 40:
            return "C"
        elif score >= 20:
            return "D"
        return "E"
    
    def _generate_analysis(self, result: ONeilScore, info: dict) -> str:
        """Generate human-readable analysis text."""
        name = info.get("longName", info.get("shortName", "This stock"))
        sector = info.get("sector", "its sector")
        
        # EPS assessment
        eps_word = "STRONG" if result.eps_strength >= 70 else "FAIR" if result.eps_strength >= 40 else "POOR"
        
        # Price assessment
        rs_word = "excellent" if result.rs_rating >= 80 else "fair" if result.rs_rating >= 50 else "weak"
        
        # Revenue
        rg = info.get("revenueGrowth", 0) or 0
        rev_cr = info.get("totalRevenue", 0)
        rev_str = f"Rs. {rev_cr/1e7:,.2f} Cr." if rev_cr else "N/A"
        
        pm = info.get("profitMargins", 0) or 0
        roe = info.get("returnOnEquity", 0) or 0
        
        text = f"Master Score {result.master_score} :\n"
        text += f"{name} operates in the {sector} sector. "
        text += f"It has operating revenue of {rev_str} on a trailing 12-month basis. "
        
        if rg:
            text += f"Annual revenue growth of {rg*100:.0f}% is {'outstanding' if rg > 0.20 else 'fair' if rg > 0 else 'declining'}. "
        if pm:
            text += f"Pre-tax margin of {pm*100:.0f}% {'is strong' if pm > 0.15 else 'needs improvement'}. "
        if roe:
            text += f"ROE of {roe*100:.0f}% {'is strong' if roe > 0.15 else 'is fair but needs improvement'}. "
        
        text += f"\n\nFrom an O'Neil Methodology perspective, the stock has an EPS Rank of {result.eps_strength} "
        text += f"which is {eps_word}, a RS Rating of {result.rs_rating} which is {rs_word}, "
        text += f"Buyer Demand at {result.buyer_demand}, "
        text += f"and Group Rank of {result.group_rank}. "
        text += f"Master Score of {result.master_score}."
        
        return text


# ═══════════════════════════════════════════════════════════════════════════════
# GURU STRATEGY RATINGS
# ═══════════════════════════════════════════════════════════════════════════════

class GuruRatingEngine:
    """Evaluate stocks against famous investor strategies."""
    
    def rate_all(self, df: pd.DataFrame, info: dict) -> list[GuruRating]:
        """Run all guru strategies and return ratings."""
        return [
            self.oneil_canslim(df, info),
            self.graham_value(info),
            self.buffett_quality(info),
            self.lynch_growth(info),
            self.oshaughnessy_value(df, info),
        ]
    
    def oneil_canslim(self, df: pd.DataFrame, info: dict) -> GuruRating:
        """William O'Neil CANSLIM criteria."""
        criteria = []
        
        # C - Current quarterly EPS growth > 25%
        eg = (info.get("earningsGrowth", 0) or 0) * 100
        criteria.append(("Current EPS growth > 25%", eg > 25, f"{eg:.1f}%"))
        
        # A - Annual EPS growth > 25%
        rg = (info.get("revenueGrowth", 0) or 0) * 100
        criteria.append(("Annual revenue growth > 25%", rg > 25, f"{rg:.1f}%"))
        
        # N - New high / new product
        if df is not None and len(df) >= 252:
            high_52w = float(df["High"].tail(252).max())
            near_high = float(df["Close"].iloc[-1]) >= high_52w * 0.85
            criteria.append(("Within 15% of 52W high", near_high, f"{(float(df['Close'].iloc[-1])/high_52w*100):.0f}%"))
        
        # S - Supply and demand (shares outstanding)
        shares = info.get("sharesOutstanding", 0)
        criteria.append(("Shares outstanding < 50Cr", shares and shares < 5e8, f"{shares/1e7:.1f}Cr" if shares else "N/A"))
        
        # L - Leader (RS > 80)
        if df is not None and len(df) >= 252:
            ret = (float(df["Close"].iloc[-1]) - float(df["Close"].iloc[-252])) / float(df["Close"].iloc[-252]) * 100
            criteria.append(("RS Rating > 80 proxy (1Y ret > 20%)", ret > 20, f"{ret:.1f}%"))
        
        # I - Institutional sponsorship
        inst = info.get("heldPercentInstitutions", 0) or 0
        criteria.append(("Institutional holding > 10%", inst > 0.10, f"{inst*100:.1f}%"))
        
        # M - Market direction (simplified: above 200 DMA)
        if df is not None and len(df) >= 200:
            above_200 = float(df["Close"].iloc[-1]) > float(df["Close"].ewm(span=200).mean().iloc[-1])
            criteria.append(("Above 200 DMA", above_200, "Yes" if above_200 else "No"))
        
        return self._build_rating("William J. O'Neil", "CANSLIM", criteria)
    
    def graham_value(self, info: dict) -> GuruRating:
        """Benjamin Graham value investing criteria."""
        criteria = []
        
        pe = info.get("trailingPE", 0) or 0
        criteria.append(("P/E < 15", 0 < pe < 15, f"{pe:.1f}" if pe else "N/A"))
        
        pb = info.get("priceToBook", 0) or 0
        criteria.append(("P/B < 1.5", 0 < pb < 1.5, f"{pb:.2f}" if pb else "N/A"))
        
        # P/E × P/B < 22.5 (Graham Number)
        pe_pb = pe * pb if pe and pb else 0
        criteria.append(("P/E × P/B < 22.5", 0 < pe_pb < 22.5, f"{pe_pb:.1f}" if pe_pb else "N/A"))
        
        de = info.get("debtToEquity", 0) or 0
        criteria.append(("Debt/Equity < 100%", 0 <= de < 100, f"{de:.0f}%" if de else "N/A"))
        
        dy = (info.get("dividendYield", 0) or 0) * 100
        criteria.append(("Dividend yield > 0%", dy > 0, f"{dy:.2f}%"))
        
        eps = info.get("trailingEps", 0) or 0
        criteria.append(("Positive EPS", eps > 0, f"₹{eps:.2f}" if eps else "N/A"))
        
        mcap = info.get("marketCap", 0) or 0
        criteria.append(("Market cap > ₹500 Cr", mcap > 5e9, f"₹{mcap/1e7:.0f}Cr" if mcap else "N/A"))
        
        return self._build_rating("Benjamin Graham", "Value Investing", criteria)
    
    def buffett_quality(self, info: dict) -> GuruRating:
        """Warren Buffett quality + moat criteria."""
        criteria = []
        
        roe = (info.get("returnOnEquity", 0) or 0) * 100
        criteria.append(("ROE > 15%", roe > 15, f"{roe:.1f}%"))
        
        pm = (info.get("profitMargins", 0) or 0) * 100
        criteria.append(("Profit margin > 10%", pm > 10, f"{pm:.1f}%"))
        
        de = info.get("debtToEquity", 0) or 0
        criteria.append(("Low debt (D/E < 50%)", 0 <= de < 50, f"{de:.0f}%" if de else "N/A"))
        
        rg = (info.get("revenueGrowth", 0) or 0) * 100
        criteria.append(("Revenue growth > 5%", rg > 5, f"{rg:.1f}%"))
        
        eg = (info.get("earningsGrowth", 0) or 0) * 100
        criteria.append(("Earnings growth > 10%", eg > 10, f"{eg:.1f}%"))
        
        pe = info.get("trailingPE", 0) or 0
        criteria.append(("Reasonable P/E (< 25)", 0 < pe < 25, f"{pe:.1f}" if pe else "N/A"))
        
        return self._build_rating("Warren Buffett", "Quality + Moat", criteria)
    
    def lynch_growth(self, info: dict) -> GuruRating:
        """Peter Lynch PEG + growth criteria."""
        criteria = []
        
        pe = info.get("trailingPE", 0) or 0
        eg = (info.get("earningsGrowth", 0) or 0) * 100
        
        # PEG ratio
        peg = pe / eg if eg > 0 else 99
        criteria.append(("PEG ratio < 1.0", 0 < peg < 1.0, f"{peg:.2f}" if eg > 0 else "N/A"))
        
        criteria.append(("P/E < 40", 0 < pe < 40, f"{pe:.1f}" if pe else "N/A"))
        criteria.append(("Earnings growth > 15%", eg > 15, f"{eg:.1f}%"))
        
        rg = (info.get("revenueGrowth", 0) or 0) * 100
        criteria.append(("Revenue growth > 10%", rg > 10, f"{rg:.1f}%"))
        
        de = info.get("debtToEquity", 0) or 0
        criteria.append(("Debt/Equity < 80%", 0 <= de < 80, f"{de:.0f}%" if de else "N/A"))
        
        inst = (info.get("heldPercentInstitutions", 0) or 0) * 100
        criteria.append(("Institutional holding < 60%", inst < 60, f"{inst:.1f}%"))
        
        return self._build_rating("Peter Lynch", "Growth at Reasonable Price", criteria)
    
    def oshaughnessy_value(self, df: pd.DataFrame, info: dict) -> GuruRating:
        """James O'Shaughnessy — What Works on Wall Street."""
        criteria = []
        
        mcap = info.get("marketCap", 0) or 0
        criteria.append(("Market cap > ₹1000 Cr", mcap > 1e10, f"₹{mcap/1e7:.0f}Cr" if mcap else "N/A"))
        
        pe = info.get("trailingPE", 0) or 0
        criteria.append(("P/E < 20", 0 < pe < 20, f"{pe:.1f}" if pe else "N/A"))
        
        ps = info.get("priceToSalesTrailing12Months", 0) or 0
        criteria.append(("P/S < 1.5", 0 < ps < 1.5, f"{ps:.2f}" if ps else "N/A"))
        
        # Price momentum (6-month return positive)
        if df is not None and len(df) >= 126:
            ret_6m = (float(df["Close"].iloc[-1]) - float(df["Close"].iloc[-126])) / float(df["Close"].iloc[-126]) * 100
            criteria.append(("6-month return > 0%", ret_6m > 0, f"{ret_6m:.1f}%"))
        
        dy = (info.get("dividendYield", 0) or 0) * 100
        criteria.append(("Pays dividends", dy > 0, f"{dy:.2f}%"))
        
        rg = (info.get("revenueGrowth", 0) or 0) * 100
        criteria.append(("Revenue growth positive", rg > 0, f"{rg:.1f}%"))
        
        return self._build_rating("James O'Shaughnessy", "What Works on Wall Street", criteria)
    
    def _build_rating(self, guru: str, strategy: str, criteria: list) -> GuruRating:
        met = sum(1 for _, passed, _ in criteria if passed)
        total = len(criteria)
        pct = int(met / total * 100) if total > 0 else 0
        
        if pct >= 80:
            score = "PASS"
        elif pct >= 50:
            score = "FAIR"
        else:
            score = "FAIL"
        
        return GuruRating(
            guru_name=guru,
            strategy_name=strategy,
            score=score,
            score_pct=pct,
            criteria_met=met,
            criteria_total=total,
            details=criteria,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("  O'Neil Scoring Engine — Self Test")
    print("=" * 60)
    
    # Synthetic data
    np.random.seed(42)
    dates = pd.date_range("2025-01-01", periods=300, freq="B")
    price = 500 + np.cumsum(np.random.randn(300) * 5)
    
    df = pd.DataFrame({
        "Open": price + np.random.randn(300) * 2,
        "High": price + abs(np.random.randn(300) * 3),
        "Low": price - abs(np.random.randn(300) * 3),
        "Close": price,
        "Volume": np.random.randint(500000, 10000000, 300).astype(float),
    }, index=dates)
    
    # Mock info
    info = {
        "longName": "Test Company Ltd",
        "sector": "Technology",
        "trailingPE": 22.5,
        "priceToBook": 3.2,
        "bookValue": 156.0,
        "trailingEps": 22.2,
        "dividendYield": 0.012,
        "returnOnEquity": 0.18,
        "debtToEquity": 45.0,
        "profitMargins": 0.12,
        "revenueGrowth": 0.22,
        "earningsGrowth": 0.28,
        "marketCap": 50000000000,
        "totalRevenue": 30000000000,
        "sharesOutstanding": 200000000,
        "heldPercentInstitutions": 0.35,
        "heldPercentInsiders": 0.45,
    }
    
    # Test O'Neil scoring
    scorer = ONeilScorer()
    result = scorer.score(df, info)
    
    print(f"\n  Master Score: {result.master_score}")
    print(f"  EPS Strength: {result.eps_strength}")
    print(f"  Price Strength (RS): {result.price_strength}")
    print(f"  Buyer Demand: {result.buyer_demand} ({result.buyer_demand_score})")
    print(f"  Group Rank: {result.group_rank}")
    print(f"  U/D Volume: {result.up_down_volume_ratio}")
    print(f"  Above 200 DMA: {result.above_200dma}")
    print(f"  52W High %: {result.price_vs_52w_high_pct}%")
    
    # Test Guru Ratings
    print(f"\n{'=' * 60}")
    print("  Guru Ratings")
    print(f"{'=' * 60}")
    
    guru_engine = GuruRatingEngine()
    ratings = guru_engine.rate_all(df, info)
    
    for r in ratings:
        icon = "✅" if r.score == "PASS" else "🟡" if r.score == "FAIR" else "❌"
        print(f"\n  {icon} {r.guru_name} ({r.strategy_name})")
        print(f"     Score: {r.score} ({r.score_pct}%) — {r.criteria_met}/{r.criteria_total} criteria met")
        for criterion, passed, detail in r.details:
            mark = "✓" if passed else "✗"
            print(f"       {mark} {criterion}: {detail}")
    
    print(f"\n{'=' * 60}")
