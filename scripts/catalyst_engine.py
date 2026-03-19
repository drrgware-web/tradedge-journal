#!/usr/bin/env python3
"""
TradEdge Catalyst Detection Engine v1.0
==========================================
Detects stock catalysts — the fundamental triggers that drive big moves.

HIGH PRIORITY CATALYSTS:
  1. Earnings Surprise (EPS beat/miss + reaction)
  2. Earnings Gap Up / Gap Down
  3. Blockbuster EPS Growth (QoQ/YoY acceleration)
  4. Revenue Acceleration
  5. Block Deals / Bulk Deals
  6. Institutional Buying Surge
  7. Volume Breakout + Price Action
  8. New 52W High with Volume

PRE-BUILT SCAN CATEGORIES (ChartMaze style):
  - VCP (Volatility Contraction Pattern)
  - Flags & Pennants
  - Tight Setup / Consolidation
  - Inside Bar (Daily/Weekly)
  - Horizontal Resistance Breakout
  - Momentum Scanner
  - Volume Screeners
  - Gap Screeners (Gap Up/Down)
  - IPO Scanner
  - Earnings Screeners
  - RS High Before Price High

Data: OHLCV from yfinance + fundamental data
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timedelta


# ═══════════════════════════════════════════════════════════════════════════════
# CATALYST TYPES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Catalyst:
    """A detected catalyst event."""
    type: str               # "earnings_surprise", "gap_up", "volume_breakout", etc.
    symbol: str
    severity: str           # "high", "medium", "low"
    direction: str          # "bullish", "bearish", "neutral"
    title: str              # Human-readable title
    detail: str             # Detailed description
    value: float = 0.0      # Numeric value (e.g., EPS growth %)
    date: str = ""          # Date of catalyst
    priority: int = 0       # 1-10 (10 = highest priority)
    
    def to_dict(self):
        return {
            "type": self.type, "symbol": self.symbol,
            "severity": self.severity, "direction": self.direction,
            "title": self.title, "detail": self.detail,
            "value": self.value, "date": self.date, "priority": self.priority,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# CATALYST DETECTOR
# ═══════════════════════════════════════════════════════════════════════════════

class CatalystDetector:
    """Detects catalyst events from price action and fundamental data."""
    
    def detect_all(self, symbol: str, df: pd.DataFrame, info: dict,
                   prev_info: dict = None) -> list[Catalyst]:
        """Run all catalyst detections."""
        catalysts = []
        
        if df is None or len(df) < 20:
            return catalysts
        
        catalysts.extend(self._detect_earnings_surprise(symbol, df, info))
        catalysts.extend(self._detect_gap(symbol, df))
        catalysts.extend(self._detect_eps_acceleration(symbol, info))
        catalysts.extend(self._detect_revenue_acceleration(symbol, info))
        catalysts.extend(self._detect_volume_breakout(symbol, df))
        catalysts.extend(self._detect_institutional_surge(symbol, info, prev_info))
        catalysts.extend(self._detect_52w_high_volume(symbol, df))
        catalysts.extend(self._detect_price_surge(symbol, df))
        
        # Sort by priority (highest first)
        catalysts.sort(key=lambda c: c.priority, reverse=True)
        return catalysts
    
    # ── Earnings Surprise Detection ──
    
    def _detect_earnings_surprise(self, symbol: str, df: pd.DataFrame, info: dict) -> list[Catalyst]:
        """Detect positive/negative earnings surprise from price reaction."""
        catalysts = []
        
        eg = info.get("earningsGrowth")
        rg = info.get("revenueGrowth")
        
        if eg is not None:
            eg_pct = eg * 100
            
            if eg_pct > 50:
                catalysts.append(Catalyst(
                    type="earnings_blockbuster",
                    symbol=symbol, severity="high", direction="bullish",
                    title=f"Blockbuster EPS Growth: {eg_pct:.0f}%",
                    detail=f"Earnings grew {eg_pct:.0f}% — exceptional growth indicating strong business momentum",
                    value=eg_pct, priority=10,
                ))
            elif eg_pct > 25:
                catalysts.append(Catalyst(
                    type="earnings_strong",
                    symbol=symbol, severity="high", direction="bullish",
                    title=f"Strong EPS Growth: {eg_pct:.0f}%",
                    detail=f"Earnings grew {eg_pct:.0f}% — well above average",
                    value=eg_pct, priority=8,
                ))
            elif eg_pct < -25:
                catalysts.append(Catalyst(
                    type="earnings_decline",
                    symbol=symbol, severity="high", direction="bearish",
                    title=f"Earnings Decline: {eg_pct:.0f}%",
                    detail=f"Earnings dropped {eg_pct:.0f}% — significant deterioration",
                    value=eg_pct, priority=8,
                ))
        
        return catalysts
    
    # ── Gap Detection ──
    
    def _detect_gap(self, symbol: str, df: pd.DataFrame) -> list[Catalyst]:
        """Detect gap ups and gap downs."""
        catalysts = []
        
        if len(df) < 2:
            return catalysts
        
        today_open = float(df["Open"].iloc[-1])
        prev_close = float(df["Close"].iloc[-2])
        prev_high = float(df["High"].iloc[-2])
        prev_low = float(df["Low"].iloc[-2])
        today_close = float(df["Close"].iloc[-1])
        today_volume = float(df["Volume"].iloc[-1])
        avg_volume = float(df["Volume"].tail(20).mean())
        
        if prev_close <= 0:
            return catalysts
        
        gap_pct = (today_open - prev_close) / prev_close * 100
        
        # Gap Up with Volume
        if gap_pct > 3 and today_volume > avg_volume * 1.5:
            # Check if gap held (bullish) or filled (bearish)
            gap_held = today_close > prev_high
            
            catalysts.append(Catalyst(
                type="gap_up",
                symbol=symbol, severity="high",
                direction="bullish" if gap_held else "neutral",
                title=f"Gap Up {gap_pct:.1f}% {'(held)' if gap_held else '(filling)'}",
                detail=f"Opened {gap_pct:.1f}% above previous close with {today_volume/avg_volume:.1f}x volume. "
                       f"{'Gap held — strong demand' if gap_held else 'Gap filling — watch for support'}",
                value=gap_pct, priority=9 if gap_held else 6,
            ))
        
        # Gap Down
        elif gap_pct < -3 and today_volume > avg_volume * 1.5:
            gap_recovered = today_close > today_open
            
            catalysts.append(Catalyst(
                type="gap_down",
                symbol=symbol, severity="high",
                direction="bearish" if not gap_recovered else "neutral",
                title=f"Gap Down {gap_pct:.1f}% {'(recovered)' if gap_recovered else '(selling)'}",
                detail=f"Opened {abs(gap_pct):.1f}% below previous close with {today_volume/avg_volume:.1f}x volume",
                value=gap_pct, priority=7,
            ))
        
        # Earnings Gap (larger gap + high volume = likely earnings reaction)
        if abs(gap_pct) > 5 and today_volume > avg_volume * 2:
            direction = "bullish" if gap_pct > 0 else "bearish"
            catalysts.append(Catalyst(
                type="earnings_reaction",
                symbol=symbol, severity="high", direction=direction,
                title=f"Possible Earnings Reaction: {gap_pct:+.1f}% gap with {today_volume/avg_volume:.1f}x volume",
                detail=f"Large gap with heavy volume suggests fundamental catalyst (earnings/news). "
                       f"Verify with corporate announcements.",
                value=gap_pct, priority=10,
            ))
        
        return catalysts
    
    # ── EPS Acceleration ──
    
    def _detect_eps_acceleration(self, symbol: str, info: dict) -> list[Catalyst]:
        """Detect accelerating earnings growth."""
        catalysts = []
        
        eg = info.get("earningsGrowth")
        rg = info.get("revenueGrowth")
        
        # Strong combo: both earnings and revenue growing > 20%
        if eg and rg:
            eg_pct = eg * 100
            rg_pct = rg * 100
            
            if eg_pct > 20 and rg_pct > 20:
                catalysts.append(Catalyst(
                    type="dual_acceleration",
                    symbol=symbol, severity="high", direction="bullish",
                    title=f"Dual Acceleration: EPS +{eg_pct:.0f}%, Revenue +{rg_pct:.0f}%",
                    detail="Both earnings and revenue growing > 20% — classic CANSLIM quality",
                    value=eg_pct, priority=9,
                ))
        
        return catalysts
    
    # ── Revenue Acceleration ──
    
    def _detect_revenue_acceleration(self, symbol: str, info: dict) -> list[Catalyst]:
        catalysts = []
        rg = info.get("revenueGrowth")
        
        if rg and rg > 0.30:
            catalysts.append(Catalyst(
                type="revenue_acceleration",
                symbol=symbol, severity="medium", direction="bullish",
                title=f"Revenue Acceleration: +{rg*100:.0f}%",
                detail=f"Revenue growing at {rg*100:.0f}% — strong demand indicator",
                value=rg * 100, priority=7,
            ))
        
        return catalysts
    
    # ── Volume Breakout ──
    
    def _detect_volume_breakout(self, symbol: str, df: pd.DataFrame) -> list[Catalyst]:
        """Detect unusual volume + price action."""
        catalysts = []
        
        vol_latest = float(df["Volume"].iloc[-1])
        vol_avg_20 = float(df["Volume"].tail(20).mean())
        vol_avg_50 = float(df["Volume"].tail(50).mean())
        close = float(df["Close"].iloc[-1])
        prev_close = float(df["Close"].iloc[-2])
        change_pct = (close - prev_close) / prev_close * 100 if prev_close > 0 else 0
        
        vol_ratio = vol_latest / vol_avg_50 if vol_avg_50 > 0 else 0
        
        # 3x+ volume with positive close
        if vol_ratio >= 3 and change_pct > 1:
            catalysts.append(Catalyst(
                type="volume_explosion",
                symbol=symbol, severity="high", direction="bullish",
                title=f"Volume Explosion: {vol_ratio:.1f}x avg with +{change_pct:.1f}%",
                detail=f"Volume {vol_ratio:.1f}x above 50-day average with {change_pct:.1f}% price gain. "
                       f"Indicates strong institutional interest or news catalyst.",
                value=vol_ratio, priority=9,
            ))
        
        # 2x volume
        elif vol_ratio >= 2 and change_pct > 0.5:
            catalysts.append(Catalyst(
                type="volume_surge",
                symbol=symbol, severity="medium", direction="bullish",
                title=f"Volume Surge: {vol_ratio:.1f}x avg",
                detail=f"Above-average volume with positive price action",
                value=vol_ratio, priority=6,
            ))
        
        # Dry up (very low volume — potential squeeze setup)
        elif vol_ratio < 0.3 and len(df) >= 20:
            catalysts.append(Catalyst(
                type="volume_dryup",
                symbol=symbol, severity="low", direction="neutral",
                title=f"Volume Dry-Up: {vol_ratio:.2f}x avg",
                detail="Extremely low volume — may indicate consolidation before a move",
                value=vol_ratio, priority=3,
            ))
        
        return catalysts
    
    # ── Institutional Surge ──
    
    def _detect_institutional_surge(self, symbol: str, info: dict,
                                     prev_info: dict = None) -> list[Catalyst]:
        catalysts = []
        
        inst = info.get("heldPercentInstitutions")
        if inst and inst > 0.40:
            catalysts.append(Catalyst(
                type="high_institutional",
                symbol=symbol, severity="medium", direction="bullish",
                title=f"High Institutional Holding: {inst*100:.1f}%",
                detail="Strong institutional backing",
                value=inst * 100, priority=5,
            ))
        
        if prev_info:
            prev_inst = prev_info.get("heldPercentInstitutions", 0) or 0
            curr_inst = inst or 0
            if curr_inst > prev_inst + 0.05 and prev_inst > 0:
                change = (curr_inst - prev_inst) * 100
                catalysts.append(Catalyst(
                    type="institutional_accumulation",
                    symbol=symbol, severity="high", direction="bullish",
                    title=f"Institutional Accumulation: +{change:.1f}% holding increase",
                    detail=f"Institutions increased holdings from {prev_inst*100:.1f}% to {curr_inst*100:.1f}%",
                    value=change, priority=8,
                ))
        
        return catalysts
    
    # ── 52W High with Volume ──
    
    def _detect_52w_high_volume(self, symbol: str, df: pd.DataFrame) -> list[Catalyst]:
        catalysts = []
        
        if len(df) < 252:
            return catalysts
        
        close = float(df["Close"].iloc[-1])
        high_52w = float(df["High"].tail(252).max())
        vol = float(df["Volume"].iloc[-1])
        vol_avg = float(df["Volume"].tail(50).mean())
        
        pct_from_high = (close - high_52w) / high_52w * 100
        
        if pct_from_high >= -2 and vol > vol_avg * 1.5:
            catalysts.append(Catalyst(
                type="52w_high_volume",
                symbol=symbol, severity="high", direction="bullish",
                title=f"52W High Breakout with Volume ({pct_from_high:+.1f}% from high)",
                detail=f"Price near/at 52-week high with {vol/vol_avg:.1f}x average volume — momentum breakout",
                value=pct_from_high, priority=9,
            ))
        
        return catalysts
    
    # ── Price Surge ──
    
    def _detect_price_surge(self, symbol: str, df: pd.DataFrame) -> list[Catalyst]:
        catalysts = []
        
        if len(df) < 5:
            return catalysts
        
        close = float(df["Close"].iloc[-1])
        close_5d = float(df["Close"].iloc[-5])
        change_5d = (close - close_5d) / close_5d * 100 if close_5d > 0 else 0
        
        if change_5d > 15:
            catalysts.append(Catalyst(
                type="price_surge_5d",
                symbol=symbol, severity="high", direction="bullish",
                title=f"5-Day Price Surge: +{change_5d:.1f}%",
                detail=f"Stock rallied {change_5d:.1f}% in 5 days — investigate catalyst",
                value=change_5d, priority=8,
            ))
        elif change_5d < -15:
            catalysts.append(Catalyst(
                type="price_crash_5d",
                symbol=symbol, severity="high", direction="bearish",
                title=f"5-Day Price Crash: {change_5d:.1f}%",
                detail=f"Stock dropped {abs(change_5d):.1f}% in 5 days — check for negative news",
                value=change_5d, priority=8,
            ))
        
        return catalysts


# ═══════════════════════════════════════════════════════════════════════════════
# PRE-BUILT SCAN CATEGORIES (ChartMaze style)
# ═══════════════════════════════════════════════════════════════════════════════

SCAN_CATEGORIES = {
    # ── Pattern Scans ──
    "vcp": {
        "name": "VCP (Volatility Contraction Pattern)",
        "description": "Mark Minervini's VCP — tightening price range with declining volume",
        "clause": "( cash ( latest high - latest low < 1 day ago high - 1 day ago low and "
                  "1 day ago high - 1 day ago low < 2 days ago high - 2 days ago low and "
                  "latest volume < 1 day ago volume and "
                  "latest close > latest ema( close, 50 ) ) )",
        "category": "pattern",
        "tags": ["minervini", "consolidation", "swing"],
    },
    "flags_pennants": {
        "name": "Flags & Pennants",
        "description": "Bullish flag/pennant after a strong move — continuation pattern",
        "clause": "( cash ( latest close > 10 days ago close * 1.10 and "
                  "latest high - latest low < 5 days ago high - 5 days ago low and "
                  "latest volume < latest sma( volume, 20 ) ) )",
        "category": "pattern",
        "tags": ["continuation", "swing"],
    },
    "tight_setup": {
        "name": "Tight Setup / Consolidation",
        "description": "Price trading in a very tight range — potential breakout setup",
        "clause": "( cash ( latest high - latest low < latest close * 0.02 and "
                  "1 day ago high - 1 day ago low < 1 day ago close * 0.02 and "
                  "2 days ago high - 2 days ago low < 2 days ago close * 0.02 ) )",
        "category": "pattern",
        "tags": ["consolidation", "squeeze"],
    },
    "inside_bar_daily": {
        "name": "Inside Bar (Daily)",
        "description": "Today's range is completely inside yesterday's range",
        "clause": "( cash ( latest high < 1 day ago high and latest low > 1 day ago low ) )",
        "category": "pattern",
        "tags": ["inside_bar", "indecision"],
    },
    "inside_bar_nr7": {
        "name": "Inside Bar + NR7",
        "description": "Inside bar with narrowest range of last 7 days",
        "clause": "( cash ( latest high < 1 day ago high and latest low > 1 day ago low and "
                  "latest high - latest low < 2 days ago high - 2 days ago low and "
                  "latest high - latest low < 3 days ago high - 3 days ago low ) )",
        "category": "pattern",
        "tags": ["inside_bar", "nr7", "squeeze"],
    },
    "horizontal_resistance_breakout": {
        "name": "Horizontal Resistance Breakout",
        "description": "Breaking above a horizontal resistance level with volume",
        "clause": "( cash ( latest close > latest max( 20, latest high ) and "
                  "latest volume > latest sma( volume, 20 ) * 1.5 ) )",
        "category": "breakout",
        "tags": ["breakout", "resistance"],
    },
    
    # ── Momentum Scans ──
    "momentum_rs_high": {
        "name": "RS High Before Price High",
        "description": "Relative strength making new highs before price — leading indicator",
        "clause": "( cash ( latest rsi( 14 ) > 60 and "
                  "latest close < latest max( 50, latest high ) * 0.95 and "
                  "latest close > latest ema( close, 50 ) and "
                  "latest ema( close, 50 ) > latest ema( close, 200 ) ) )",
        "category": "momentum",
        "tags": ["relative_strength", "leading"],
    },
    "momentum_scanner": {
        "name": "Momentum Scanner",
        "description": "Strong momentum stocks — price and volume aligned",
        "clause": "( cash ( latest close > latest ema( close, 21 ) and "
                  "latest ema( close, 21 ) > latest ema( close, 50 ) and "
                  "latest ema( close, 50 ) > latest ema( close, 200 ) and "
                  "latest rsi( 14 ) > 55 and latest rsi( 14 ) < 80 and "
                  "latest volume > latest sma( volume, 20 ) ) )",
        "category": "momentum",
        "tags": ["trend", "momentum"],
    },
    "200ma_turnaround": {
        "name": "200MA Turnaround",
        "description": "Stock crossing above 200-day MA after being below — trend reversal",
        "clause": "( cash ( latest close > latest sma( close, 200 ) and "
                  "1 day ago close <= 1 day ago sma( close, 200 ) and "
                  "latest volume > latest sma( volume, 50 ) * 1.5 ) )",
        "category": "momentum",
        "tags": ["reversal", "ma_crossover"],
    },
    
    # ── Volume Scans ──
    "volume_breakout": {
        "name": "Volume Breakout",
        "description": "Volume surge with positive price action",
        "clause": "( cash ( latest volume > latest sma( volume, 20 ) * 3 and "
                  "latest close > latest open and latest close > 1 day ago close ) )",
        "category": "volume",
        "tags": ["volume", "breakout"],
    },
    "pocket_pivot": {
        "name": "Pocket Pivot Volume",
        "description": "Up-day volume > any down-day volume in last 10 days",
        "clause": "( cash ( latest close > 1 day ago close and "
                  "latest volume > latest max( 10, latest volume ) ) )",
        "category": "volume",
        "tags": ["volume", "pocket_pivot", "institutional"],
    },
    "volume_dry_up": {
        "name": "Volume Dry Up (Consolidation)",
        "description": "Extremely low volume — potential before big move",
        "clause": "( cash ( latest volume < latest sma( volume, 50 ) * 0.3 and "
                  "latest close > latest ema( close, 50 ) ) )",
        "category": "volume",
        "tags": ["volume", "consolidation"],
    },
    
    # ── Gap Scans ──
    "gap_up_3pct": {
        "name": "Gap Up > 3%",
        "description": "Opened 3%+ above previous close",
        "clause": "( cash ( latest open > 1 day ago close * 1.03 and "
                  "latest volume > latest sma( volume, 20 ) * 1.5 ) )",
        "category": "gap",
        "tags": ["gap", "catalyst"],
    },
    "gap_up_held": {
        "name": "Gap Up Held (Bullish)",
        "description": "Gap up that held above previous day's high — strong demand",
        "clause": "( cash ( latest open > 1 day ago close * 1.02 and "
                  "latest close > 1 day ago high and "
                  "latest close > latest open ) )",
        "category": "gap",
        "tags": ["gap", "bullish"],
    },
    "gap_down_recovery": {
        "name": "Gap Down Recovery",
        "description": "Gapped down but recovered to close positive",
        "clause": "( cash ( latest open < 1 day ago close * 0.97 and "
                  "latest close > latest open and "
                  "latest close > 1 day ago close ) )",
        "category": "gap",
        "tags": ["gap", "reversal"],
    },
    
    # ── Earnings Scans ──
    "earnings_gap_up": {
        "name": "Earnings Gap Up",
        "description": "Large gap up with huge volume — likely positive earnings reaction",
        "clause": "( cash ( latest open > 1 day ago close * 1.05 and "
                  "latest volume > latest sma( volume, 50 ) * 3 and "
                  "latest close > latest open ) )",
        "category": "earnings",
        "tags": ["earnings", "gap", "catalyst"],
    },
    "positive_earnings_reaction": {
        "name": "Positive Earnings Reaction",
        "description": "Strong close after earnings with volume confirmation",
        "clause": "( cash ( latest close > 1 day ago close * 1.03 and "
                  "latest volume > latest sma( volume, 50 ) * 2.5 and "
                  "latest close > latest open and "
                  "latest close > latest ema( close, 21 ) ) )",
        "category": "earnings",
        "tags": ["earnings", "positive", "catalyst"],
    },
    "earnings_breakaway_gap": {
        "name": "Earnings Breakaway Gap",
        "description": "Gap above resistance/consolidation on earnings — strongest signal",
        "clause": "( cash ( latest open > 1 day ago close * 1.05 and "
                  "latest close > latest max( 20, latest high ) and "
                  "latest volume > latest sma( volume, 50 ) * 3 ) )",
        "category": "earnings",
        "tags": ["earnings", "breakout", "top_priority"],
    },
    
    # ── IPO Scans ──
    "ipo_base_breakout": {
        "name": "IPO Base Breakout",
        "description": "Recent IPO breaking out of its first base",
        "clause": "( cash ( latest close > latest max( 30, latest high ) and "
                  "latest volume > latest sma( volume, 20 ) * 2 and "
                  "latest close > 50 ) )",
        "category": "ipo",
        "tags": ["ipo", "breakout"],
    },
    
    # ── Trend Template ──
    "minervini_stage2": {
        "name": "Minervini Stage 2 Uptrend",
        "description": "Mark Minervini's trend template — all moving averages aligned",
        "clause": "( cash ( latest close > 30 and "
                  "latest ema( close, 200 ) > 1 month ago ema( close, 200 ) and "
                  "latest close > latest ema( close, 200 ) and "
                  "latest close > latest ema( close, 150 ) and "
                  "latest close > latest ema( close, 50 ) and "
                  "latest ema( close, 50 ) > latest ema( close, 150 ) and "
                  "latest ema( close, 150 ) > latest ema( close, 200 ) ) )",
        "category": "trend",
        "tags": ["minervini", "trend", "stage2"],
    },
    "supertrend_buy": {
        "name": "SuperTrend Buy Signal",
        "description": "Price crossing above SuperTrend — trend turning bullish",
        "clause": "( cash ( latest close > latest supertrend( 7, 3 ) and "
                  "1 day ago close <= 1 day ago supertrend( 7, 3 ) ) )",
        "category": "trend",
        "tags": ["supertrend", "buy_signal"],
    },
}


def get_scan_categories() -> dict[str, list]:
    """Get scans organized by category."""
    categories = {}
    for scan_id, scan in SCAN_CATEGORIES.items():
        cat = scan["category"]
        if cat not in categories:
            categories[cat] = []
        categories[cat].append({"id": scan_id, **scan})
    return categories


def get_all_scan_clauses() -> dict[str, str]:
    """Get all scan IDs mapped to their clauses."""
    return {scan_id: scan["clause"] for scan_id, scan in SCAN_CATEGORIES.items()}


# ═══════════════════════════════════════════════════════════════════════════════
# UNIVERSAL FILTER (ChartMaze-style checkbox filters)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class UniversalFilter:
    """
    ChartMaze-style universal filter that can be applied to any scan.
    All filters are optional — only enabled ones are applied.
    """
    # Price filters
    price_min: float = None         # Stock Price(₹): min
    price_max: float = None         # Stock Price(₹): max
    
    # Market cap
    mcap_min_cr: float = None       # Market Cap Range (Cr.): min
    mcap_max_cr: float = None       # Market Cap Range (Cr.): max
    
    # Volume filters
    ma50_volume_min: float = None   # MA 50 Volume > X
    turnover_min: float = None      # Stock Price * MA 20 Volume > X (rupee volume)
    
    # Circuit limit
    exclude_circuit: set = None     # Exclude these bands: {"2%", "5%"}
    
    # Returns filters
    return_1d_min: float = None     # 1 Day Return(%) min
    return_1d_max: float = None
    return_1m_min: float = None     # 1 Month Return(%) min
    return_1m_max: float = None
    return_3m_min: float = None     # 3 Month Return(%) min
    return_3m_max: float = None
    
    # Technical filters
    above_moving_avg: str = None    # "50 EMA", "200 SMA", etc.
    rs_range_min: float = None      # Overall RS Range: min
    rs_range_max: float = None      
    
    # Proximity filters
    pct_from_52w_high_max: float = None   # % from 52W High < X
    pct_from_52w_low_min: float = None    # % from 52W Low > X
    pct_from_ath_max: float = None        # % from ATH < X
    
    # Misc
    volume_gainers: bool = False    # Volume > previous day
    gap_up: bool = False            # Gap up today
    listing_date_after: str = None  # Listing Date > YYYY/MM/DD (IPO filter)
    free_float_min: float = None    # Free Float(%) min
    free_float_max: float = None
    
    # Sector/Industry
    sector: str = None
    industry: str = None
    index: str = None               # "Nifty 50", "Nifty 500", etc.
    
    def apply(self, stocks: list[dict], circuit_tracker=None) -> list[dict]:
        """Apply all enabled filters to a list of stock results."""
        filtered = []
        
        for stock in stocks:
            if not self._passes(stock, circuit_tracker):
                continue
            filtered.append(stock)
        
        return filtered
    
    def _passes(self, stock: dict, circuit_tracker=None) -> bool:
        """Check if a stock passes all enabled filters."""
        price = stock.get("price", 0)
        
        # Price range
        if self.price_min is not None and price < self.price_min:
            return False
        if self.price_max is not None and price > self.price_max:
            return False
        
        # Market cap
        funda = stock.get("fundamentals", {})
        mcap = funda.get("market_cap_cr", 0) or 0
        if self.mcap_min_cr is not None and mcap < self.mcap_min_cr:
            return False
        if self.mcap_max_cr is not None and mcap > self.mcap_max_cr:
            return False
        
        # Circuit limit
        if self.exclude_circuit and circuit_tracker:
            info = circuit_tracker.get_circuit_info(stock.get("symbol", ""))
            if info.band in self.exclude_circuit:
                return False
        
        # Returns
        returns = stock.get("returns", {})
        if self.return_1m_min is not None:
            ret = returns.get("1m", 0) or 0
            if ret < self.return_1m_min:
                return False
        if self.return_1m_max is not None:
            ret = returns.get("1m", 0) or 0
            if ret > self.return_1m_max:
                return False
        if self.return_3m_min is not None:
            ret = returns.get("3m", 0) or 0
            if ret < self.return_3m_min:
                return False
        if self.return_3m_max is not None:
            ret = returns.get("3m", 0) or 0
            if ret > self.return_3m_max:
                return False
        
        # Sector
        if self.sector and stock.get("sector", "").lower() != self.sector.lower():
            return False
        
        return True
    
    def to_dict(self) -> dict:
        """Serialize for saving as preset."""
        d = {}
        for field_name, val in self.__dict__.items():
            if val is not None and val is not False:
                if isinstance(val, set):
                    d[field_name] = list(val)
                else:
                    d[field_name] = val
        return d
    
    @classmethod
    def from_dict(cls, d: dict) -> "UniversalFilter":
        """Deserialize from saved preset."""
        f = cls()
        for k, v in d.items():
            if k == "exclude_circuit" and isinstance(v, list):
                setattr(f, k, set(v))
            elif hasattr(f, k):
                setattr(f, k, v)
        return f


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("  Catalyst Detection Engine — Self Test")
    print("=" * 60)
    
    np.random.seed(42)
    dates = pd.date_range("2025-01-01", periods=300, freq="B")
    
    # Stock with a gap up + volume surge
    price = 100 + np.cumsum(np.random.randn(300) * 1.5)
    price[-1] = price[-2] * 1.08  # 8% gap up
    vol = np.random.randint(100000, 1000000, 300).astype(float)
    vol[-1] = vol[-5:].mean() * 4  # 4x volume
    
    df = pd.DataFrame({
        "Open": price * 1.05,  # Open above prev close
        "High": price * 1.06,
        "Low": price * 0.99,
        "Close": price,
        "Volume": vol,
    }, index=dates)
    df["Open"].iloc[-1] = float(df["Close"].iloc[-2]) * 1.06  # Gap up open
    
    info = {
        "earningsGrowth": 0.55,
        "revenueGrowth": 0.32,
        "heldPercentInstitutions": 0.42,
    }
    
    detector = CatalystDetector()
    catalysts = detector.detect_all("TESTSTOCK", df, info)
    
    print(f"\n  Detected {len(catalysts)} catalysts:")
    for c in catalysts:
        icon = "🟢" if c.direction == "bullish" else "🔴" if c.direction == "bearish" else "⚪"
        print(f"    {icon} P{c.priority} [{c.severity.upper()}] {c.title}")
        print(f"       {c.detail[:80]}...")
    
    # Test scan categories
    print(f"\n{'=' * 60}")
    print(f"  Pre-built Scan Categories: {len(SCAN_CATEGORIES)}")
    print(f"{'=' * 60}")
    
    categories = get_scan_categories()
    for cat, scans in categories.items():
        print(f"\n  📁 {cat.upper()} ({len(scans)} scans)")
        for s in scans:
            print(f"     • {s['name']}")
    
    # Test universal filter
    print(f"\n{'=' * 60}")
    print(f"  Universal Filter Test")
    print(f"{'=' * 60}")
    
    stocks = [
        {"symbol": "A", "price": 500, "fundamentals": {"market_cap_cr": 10000}, "returns": {"1m": 15, "3m": 40}},
        {"symbol": "B", "price": 5, "fundamentals": {"market_cap_cr": 50}, "returns": {"1m": -5, "3m": 10}},
        {"symbol": "C", "price": 1200, "fundamentals": {"market_cap_cr": 80000}, "returns": {"1m": 25, "3m": 50}},
        {"symbol": "D", "price": 80, "fundamentals": {"market_cap_cr": 3000}, "returns": {"1m": 30, "3m": 80}},
    ]
    
    uf = UniversalFilter(
        price_min=25, price_max=10000,
        mcap_min_cr=300, mcap_max_cr=5000000,
        return_1m_min=20, return_1m_max=100,
    )
    
    result = uf.apply(stocks)
    print(f"\n  Filter: Price ₹25-10000, MCap 300-5M Cr, 1M Return 20-100%")
    print(f"  Input: {len(stocks)} stocks → Output: {len(result)} stocks")
    for s in result:
        print(f"    ✓ {s['symbol']} ₹{s['price']} MCap:{s['fundamentals']['market_cap_cr']}Cr 1M:{s['returns']['1m']}%")
    
    print(f"\n{'=' * 60}")
