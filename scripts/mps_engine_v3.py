"""
================================================================================
    ╔╦╗╦═╗╔═╗╔╦╗╔═╗╔╦╗╔═╗╔═╗
     ║ ╠╦╝╠═╣ ║║║╣  ║║║ ╦║╣
     ╩ ╩╚═╩ ╩═╩╝╚═╝═╩╝╚═╝╚═╝
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    ✦  T R A D E   T H E   P U L S E  ✦
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    MPS v3.1 — Market Pulse Score Engine
    By Dr. Rahul Ware
================================================================================

MPS (Market Pulse Score) Calculation Engine v3.1
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TradEdge Command Center — Unified Market Health Scoring System

Universe: Nifty 500
Score Range: 0–100
Update: Daily EOD (~4:15 PM IST via GitHub Actions)

7 Pillars:
  1. Structural  (18%) — % stocks above 200 SMA
  2. Breadth     (18%) — Composite: 60% SMA participation + 40% A/D flow
  3. Spark       (13%) — Stockbee 4% breakout count + Burst Ratio (4.5%)
  4. Quality     (13%) — Net new 52-week highs
  5. Sentiment   (13%) — VIX + PCR composite
  6. Momentum    (13%) — RSI Breadth (% stocks with RSI > 50)
  7. Volatility  (12%) — ATR Breadth (% stocks with ATR% > 4%) [INVERSE]

9 Smart Modifiers:
  1. Exhaustion Penalty    (RSI > 70 overheating)
  2. Persistence Boost     (21-day structural bull streak)
  3. Divergence Warning    (Nifty new high + falling breadth)
  4. FII Flow Warning      (5+ day consecutive FII selling)
  5. Warning Day           (3+ pillars scoring below 40% of max)
  6. Volatility Regime     (ATR Breadth calm/danger/panic)
  7. Crude Oil Stress      (Brent crude price impact)         ★ NEW v3.1
  8. Global Yield Pressure (US 10Y yield EM impact)           ★ NEW v3.1
  9. Rupee Stress          (USD/INR 20-day rate of change)    ★ NEW v3.1

4 Daily States:
  - NORMAL        — Trend is healthy, trade your plan
  - WARNING       — 3+ pillars weak, stop new entries
  - OVEREXTENDED  — Sell into strength, book partial profits
  - EXHAUSTED     — Bottom fishing zone, watch for reversal candles

6 Volatility Regime Modifier:
  - ATR Breadth < 20%   → +5 pts (calm = high confidence)
  - ATR Breadth > 30%   → -10 pts (erratic = danger)
  - ATR Breadth > 50%   → -15 pts (panic/capitulation)

Changelog v3.1 (from v3.0):
  - Added Modifier 7: Crude Oil Stress (Brent price impact on India)
  - Added Modifier 8: Global Yield Pressure (US 10Y yield EM impact)
  - Added Modifier 9: Rupee Stress (USD/INR 20-day rate of change)
  - RawMarketData: added brent_crude, us10y_yield, usd_inr fields
  - calculate_mps: added usd_inr_20d_ago parameter
  - MPSResult: added macro_summary field
  - 21 daily data inputs (was 18), 9 modifiers (was 6)
  - Version bumped to 3.1

Changelog v3.0 (from v2.0):
  - Structural/Breadth weights reduced 25% → 18% each
  - Added Pillar 6: Momentum (RSI Breadth, 13%)
  - Added Pillar 7: Volatility (ATR Breadth inverse, 12%)
  - Spark pillar now composite: 60% Stockbee 4% + 40% Burst Ratio (4.5%)
  - Added Modifier 5: Warning Day (-10 when 3+ pillars < 40% of max)
  - Added Modifier 6: Volatility Regime (+5 calm / -10 danger / -15 panic)
  - Added Daily State field (NORMAL/WARNING/OVEREXTENDED/EXHAUSTED)
  - Hard Money Zone risk updated: 0.25% → 0.5% (backtested)
  - Burst Ratio and ATR Regime fields added to JSON output
  - 18 daily data inputs (was 14), 7 Chartink scanners (was 4)
================================================================================
"""

import json
import sys
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional, List


# =============================================================================
# 0. ANIMATED BANNER
# =============================================================================

def print_banner(animate=True):
    """Print the animated TradEdge MPS v3.1 banner."""
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    MAGENTA = "\033[95m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"
    
    logo = f"""{CYAN}{BOLD}
    ╔╦╗╦═╗╔═╗╔╦╗╔═╗╔╦╗╔═╗╔═╗
     ║ ╠╦╝╠═╣ ║║║╣  ║║║ ╦║╣
     ╩ ╩╚═╩ ╩═╩╝╚═╝═╩╝╚═╝╚═╝{RESET}"""

    border = f"    {CYAN}{'━' * 38}{RESET}"
    
    slogan_text = "✦  T R A D E   T H E   P U L S E  ✦"
    credit_text = "By Dr. Rahul Ware"
    version_text = "MPS v3.1 — Market Pulse Score Engine"
    
    print(logo)
    print(border)
    
    if animate and sys.stdout.isatty():
        # Animated slogan: character-by-character with color sweep
        sys.stdout.write("    ")
        colors = [RED, YELLOW, GREEN, CYAN, MAGENTA]
        for i, ch in enumerate(slogan_text):
            color = colors[i % len(colors)]
            sys.stdout.write(f"{BOLD}{color}{ch}{RESET}")
            sys.stdout.flush()
            time.sleep(0.03)
        print()
        
        # Animated credit: fade in with dots
        sys.stdout.write(f"    {DIM}")
        for ch in credit_text:
            sys.stdout.write(f"{GREEN}{BOLD}{ch}{RESET}")
            sys.stdout.flush()
            time.sleep(0.04)
        print()
    else:
        # Non-animated fallback (GitHub Actions, piped output)
        print(f"    {BOLD}{GREEN}{slogan_text}{RESET}")
        print(f"    {BOLD}{MAGENTA}{credit_text}{RESET}")
    
    print(border)
    print(f"    {DIM}{version_text}{RESET}")
    print()


# =============================================================================
# 1. DATA STRUCTURES
# =============================================================================

@dataclass
class RawMarketData:
    """Raw inputs fetched from Chartink / NSE / Yahoo Finance daily — 21 data points."""
    date: str                          # YYYY-MM-DD

    # Structural (from Chartink)
    stocks_above_200sma: int = 0       # count of Nifty 500 stocks > 200 SMA
    total_universe: int = 500          # Nifty 500

    # Breadth — positional component (from Chartink)
    stocks_above_50sma: int = 0        # count of Nifty 500 stocks > 50 SMA

    # Breadth — flow component (from NSE)
    advances: int = 0                  # stocks that closed up today
    declines: int = 0                  # stocks that closed down today
    unchanged: int = 0                 # stocks flat today

    # Spark — Stockbee breakout (from Chartink)
    stocks_up_4pct: int = 0            # stocks with >= 4% daily gain + 1.5x vol

    # Spark — Burst Ratio (from Chartink)       ★ NEW v3
    burst_gainers_4_5pct: int = 0      # stocks with >= 4.5% daily gain
    burst_losers_4_5pct: int = 0       # stocks with <= -4.5% daily loss

    # Quality (from NSE)
    new_52w_highs: int = 0
    new_52w_lows: int = 0

    # Sentiment (from NSE)
    india_vix: float = 0.0
    pcr: float = 0.0                   # Put-Call Ratio (Nifty options)

    # Momentum — RSI Breadth (from Chartink)     ★ NEW v3
    stocks_rsi_above_50: int = 0       # stocks with RSI(14) > 50

    # Volatility — ATR Breadth (from Chartink)   ★ NEW v3
    stocks_atr_pct_above_4: int = 0    # stocks with ATR(14)/Close > 4%

    # For modifiers
    stocks_rsi_above_70: int = 0       # stocks with RSI(14) > 70
    nifty_at_52w_high: bool = False

    # FII flow (from NSE / moneycontrol)
    fii_net_buy_crores: float = 0.0    # negative = selling

    # Macro — Global (from Yahoo Finance via yfinance)    ★ NEW v3.1
    brent_crude: float = 0.0               # Brent crude oil price in USD
    us10y_yield: float = 0.0               # US 10-Year Treasury yield %
    usd_inr: float = 0.0                   # USD/INR exchange rate


@dataclass
class PillarScore:
    """Score for a single pillar (0–100 raw, then weighted)."""
    name: str
    raw_value: float
    raw_score: float         # normalized 0–100
    weight: float            # pillar weight (decimal)
    weighted_score: float    # raw_score × weight
    description: str = ""
    sub_components: dict = field(default_factory=dict)


@dataclass
class ModifierResult:
    """Result of a smart modifier check."""
    name: str
    triggered: bool
    adjustment: float
    reason: str = ""


@dataclass
class MPSResult:
    """Final MPS v3.1 output for the day."""
    date: str
    version: str
    pillar_scores: list
    base_score: float
    modifiers: list
    total_modifier: float
    final_score: float
    zone: str
    zone_emoji: str
    zone_action: str
    risk_per_trade: str
    state: str
    state_message: str
    burst_ratio: float
    burst_label: str
    rsi_breadth_pct: float
    atr_breadth_pct: float
    atr_regime: str
    macro_summary: str = ""                  # ★ NEW v3.1


# =============================================================================
# 2. PILLAR SCORING FUNCTIONS (each returns 0–100)
# =============================================================================

def _interpolate(value: float, breakpoints: list) -> float:
    """Linear interpolation between breakpoints."""
    if value <= breakpoints[0][0]:
        return float(breakpoints[0][1])
    if value >= breakpoints[-1][0]:
        return float(breakpoints[-1][1])
    for i in range(len(breakpoints) - 1):
        x0, y0 = breakpoints[i]
        x1, y1 = breakpoints[i + 1]
        if x0 <= value <= x1:
            ratio = (value - x0) / (x1 - x0)
            return y0 + ratio * (y1 - y0)
    return float(breakpoints[-1][1])


def score_structural(pct_above_200sma: float) -> float:
    """Pillar 1: Structural Health (18% weight)."""
    breakpoints = [(0, 0), (30, 25), (50, 50), (70, 75), (85, 100)]
    return _interpolate(pct_above_200sma, breakpoints)


def score_breadth_composite(pct_above_50sma: float, ad_ratio: float) -> tuple:
    """
    Pillar 2: Breadth (18% weight) — Composite.
    60% positional (>50 SMA) + 40% flow (A/D ratio).
    Returns: (composite_score, positional_score, ad_score)
    """
    sma_bps = [(0, 0), (25, 20), (50, 50), (65, 75), (80, 100)]
    positional_score = _interpolate(pct_above_50sma, sma_bps)

    ad_bps = [(0.4, 0), (0.7, 25), (1.0, 50), (1.5, 75), (2.0, 90), (3.0, 100)]
    ad_score = _interpolate(ad_ratio, ad_bps)

    composite = 0.60 * positional_score + 0.40 * ad_score
    return composite, positional_score, ad_score


def score_spark_composite(stocks_up_4pct: int, burst_gainers: int, burst_losers: int) -> tuple:
    """
    Pillar 3: Spark (13% weight) — Composite.
    60% Stockbee 4% count + 40% Burst Ratio (4.5% gainers/losers).
    Returns: (composite_score, stockbee_score, burst_score, burst_ratio)
    """
    # Stockbee component (60%)
    spark_bps = [(0, 0), (5, 15), (15, 40), (25, 65), (40, 85), (60, 100)]
    stockbee_score = _interpolate(float(stocks_up_4pct), spark_bps)

    # Burst Ratio component (40%)
    ratio = (burst_gainers / max(burst_losers, 1)) * 100
    if ratio > 400:
        burst_score = 100
    elif ratio > 200:
        burst_score = 75
    elif ratio > 100:
        burst_score = 50
    elif ratio > 50:
        burst_score = 25
    else:
        burst_score = 0

    composite = stockbee_score * 0.6 + burst_score * 0.4
    return composite, stockbee_score, burst_score, ratio


def score_quality(new_highs: int, new_lows: int) -> float:
    """Pillar 4: Quality (13% weight)."""
    net = new_highs - new_lows
    breakpoints = [(-50, 0), (-20, 20), (0, 45), (20, 65), (50, 80), (100, 100)]
    return _interpolate(float(net), breakpoints)


def score_sentiment(vix: float, pcr: float) -> tuple:
    """
    Pillar 5: Sentiment (13% weight).
    50% VIX score + 50% PCR score.
    Returns: (composite_score, vix_score, pcr_score)
    """
    vix_bps = [(12, 100), (14, 85), (18, 60), (22, 35), (28, 15), (35, 0)]
    vix_score = _interpolate(vix, vix_bps)

    if pcr <= 1.0:
        pcr_bps = [(0.5, 20), (0.7, 60), (0.8, 80), (1.0, 100)]
    else:
        pcr_bps = [(1.0, 100), (1.2, 80), (1.5, 50), (2.0, 15)]
    pcr_score = _interpolate(pcr, pcr_bps)

    return 0.5 * vix_score + 0.5 * pcr_score, vix_score, pcr_score


def score_momentum(pct_rsi_above_50: float) -> float:
    """
    Pillar 6: Momentum / RSI Breadth (13% weight).
    % of Nifty 500 with RSI(14) > 50.
    Linear 0–100 mapping.
    """
    return min(max(pct_rsi_above_50, 0), 100)


def score_volatility(pct_atr_above_4: float) -> tuple:
    """
    Pillar 7: Volatility / ATR Breadth (12% weight).
    INVERSE scoring — lower ATR breadth = calmer = better for swing trading.
    Returns: (score, regime_label)
    """
    if pct_atr_above_4 < 10:
        return 100, "Squeeze"       # Tight ranges, breakout imminent
    elif pct_atr_above_4 < 25:
        return 75, "Healthy"        # Smooth trends, institutional buying
    elif pct_atr_above_4 < 35:
        return 40, "Danger"         # Market becoming erratic
    else:
        return 10, "Panic"          # Extreme volatility / capitulation


def get_burst_label(ratio: float) -> str:
    """Human-readable Burst Ratio label."""
    if ratio > 400:
        return "Super Trend"
    elif ratio > 200:
        return "Strong Trend"
    elif ratio > 100:
        return "Moderate"
    elif ratio > 50:
        return "Weak"
    else:
        return "Distribution"


# =============================================================================
# 3. SMART MODIFIERS
# =============================================================================

def check_exhaustion_penalty(stocks_rsi_above_70: int, total: int = 500) -> ModifierResult:
    """
    Modifier 1: Exhaustion Penalty.
    When too many stocks have RSI > 70, market is vertically stretched.
    """
    pct = (stocks_rsi_above_70 / total) * 100 if total > 0 else 0

    if pct >= 95:
        return ModifierResult("Exhaustion Penalty", True, -15.0,
                              f"{pct:.1f}% stocks with RSI>70 — extreme overheating")
    elif pct >= 90:
        return ModifierResult("Exhaustion Penalty", True, -10.0,
                              f"{pct:.1f}% stocks with RSI>70 — significant overheating")
    elif pct >= 80:
        return ModifierResult("Exhaustion Penalty", True, -5.0,
                              f"{pct:.1f}% stocks with RSI>70 — market stretched")
    else:
        return ModifierResult("Exhaustion Penalty", False, 0.0,
                              f"{pct:.1f}% stocks with RSI>70 — within normal range")


def check_persistence_boost(structural_bull_streak_days: int) -> ModifierResult:
    """
    Modifier 2: Persistence Boost.
    Stability bonus for sustained structural bull (>50% above 200 SMA).
    """
    if structural_bull_streak_days >= 63:
        return ModifierResult("Persistence Boost", True, +7.0,
                              f"{structural_bull_streak_days} days of structural bull — very stable foundation")
    elif structural_bull_streak_days >= 42:
        return ModifierResult("Persistence Boost", True, +5.0,
                              f"{structural_bull_streak_days} days of structural bull — strong stability")
    elif structural_bull_streak_days >= 21:
        return ModifierResult("Persistence Boost", True, +3.0,
                              f"{structural_bull_streak_days} days of structural bull — stability bonus active")
    else:
        return ModifierResult("Persistence Boost", False, 0.0,
                              f"{structural_bull_streak_days} days — streak too short for bonus (need 21+)")


def check_divergence_warning(nifty_at_52w_high: bool, pct_above_50sma: float,
                              prev_pct_above_50sma: float) -> ModifierResult:
    """
    Modifier 3: Divergence Warning (Zombie Rally Detector).
    Nifty hits 52W high but breadth is falling → -8 points.
    """
    breadth_falling = pct_above_50sma < prev_pct_above_50sma

    if nifty_at_52w_high and breadth_falling:
        return ModifierResult("Divergence Warning", True, -8.0,
                              f"⚠️ Zombie Rally: Nifty at 52W high but breadth falling "
                              f"({prev_pct_above_50sma:.1f}% → {pct_above_50sma:.1f}%)")
    else:
        return ModifierResult("Divergence Warning", False, 0.0,
                              "No divergence detected")


def check_fii_flow_warning(fii_net_consecutive_sell_days: int,
                            fii_5day_net_crores: float) -> ModifierResult:
    """
    Modifier 4: FII Flow Warning.
    Persistent FII selling pressure → penalty scales with streak + magnitude.
    """
    if fii_net_consecutive_sell_days < 5:
        return ModifierResult("FII Flow Warning", False, 0.0,
                              f"FII sell streak: {fii_net_consecutive_sell_days} days — below threshold (need 5+)")

    if fii_net_consecutive_sell_days >= 11:
        base_penalty = -8.0
        severity = "heavy institutional exit"
    elif fii_net_consecutive_sell_days >= 8:
        base_penalty = -5.0
        severity = "significant FII pressure"
    else:
        base_penalty = -3.0
        severity = "FII caution zone"

    magnitude_penalty = 0.0
    magnitude_note = ""
    if fii_5day_net_crores < -10000:
        magnitude_penalty = -2.0
        magnitude_note = f" + heavy outflow (₹{abs(fii_5day_net_crores):,.0f} Cr in 5 days)"

    return ModifierResult(
        "FII Flow Warning", True, base_penalty + magnitude_penalty,
        f"FII net sellers for {fii_net_consecutive_sell_days} consecutive days — "
        f"{severity}{magnitude_note}"
    )


def check_warning_day(pillar_scores: list) -> ModifierResult:
    """
    Modifier 5: Warning Day.
    When 3+ pillars score below 40% of their maximum → -10 points.
    """
    pillar_maxes = {
        "Structural": 18, "Breadth": 18, "Spark": 13, "Quality": 13,
        "Sentiment": 13, "Momentum": 13, "Volatility": 12,
    }

    weak_pillars = []
    for p in pillar_scores:
        max_val = pillar_maxes.get(p.name, 10)
        if p.weighted_score < max_val * 0.4:
            weak_pillars.append(p.name)

    if len(weak_pillars) >= 3:
        return ModifierResult("Warning Day", True, -10.0,
                              f"{len(weak_pillars)} pillars weak (<40%): {', '.join(weak_pillars)}")
    else:
        return ModifierResult("Warning Day", False, 0.0,
                              f"{len(weak_pillars)} pillars weak — below threshold (need 3+)")


def check_volatility_regime(atr_breadth_pct: float) -> ModifierResult:
    """
    Modifier 6: Volatility Regime.
    ATR Breadth-based market calm/stress assessment.
    """
    if atr_breadth_pct > 50:
        return ModifierResult("Volatility Regime", True, -15.0,
                              f"ATR Breadth {atr_breadth_pct:.1f}% — panic/capitulation territory")
    elif atr_breadth_pct > 30:
        return ModifierResult("Volatility Regime", True, -10.0,
                              f"ATR Breadth {atr_breadth_pct:.1f}% — erratic, reduce position sizes")
    elif atr_breadth_pct < 20:
        return ModifierResult("Volatility Regime", True, +5.0,
                              f"ATR Breadth {atr_breadth_pct:.1f}% — calm market, high confidence")
    else:
        return ModifierResult("Volatility Regime", False, 0.0,
                              f"ATR Breadth {atr_breadth_pct:.1f}% — normal volatility")


# =============================================================================
# 3b. MACRO MODIFIERS  ★ NEW v3.1
# =============================================================================

def check_crude_oil_stress(brent_price: float) -> ModifierResult:
    """
    Modifier 7: Crude Oil Stress.  ★ NEW v3.1
    India imports ~85% crude. High oil = imported inflation + margin compression.
    """
    if brent_price <= 0:
        return ModifierResult("Crude Oil Stress", False, 0.0,
                              "Crude data unavailable")
    
    if brent_price > 110:
        return ModifierResult("Crude Oil Stress", True, -8.0,
                              f"Brent ${brent_price:.1f} — emergency level, exit cyclicals")
    elif brent_price > 95:
        return ModifierResult("Crude Oil Stress", True, -5.0,
                              f"Brent ${brent_price:.1f} — high stress, imported inflation risk")
    elif brent_price < 65:
        return ModifierResult("Crude Oil Stress", True, +3.0,
                              f"Brent ${brent_price:.1f} — tailwind for India, lower input costs")
    elif brent_price < 75:
        return ModifierResult("Crude Oil Stress", True, +2.0,
                              f"Brent ${brent_price:.1f} — comfortable zone, margin expansion")
    else:
        return ModifierResult("Crude Oil Stress", False, 0.0,
                              f"Brent ${brent_price:.1f} — neutral range ($75-$95)")


def check_global_yield_pressure(us10y: float) -> ModifierResult:
    """
    Modifier 8: Global Yield Pressure.  ★ NEW v3.1
    Rising US yields pull FII money out of emerging markets.
    """
    if us10y <= 0:
        return ModifierResult("Global Yield Pressure", False, 0.0,
                              "US 10Y data unavailable")
    
    if us10y > 5.0:
        return ModifierResult("Global Yield Pressure", True, -5.0,
                              f"US 10Y {us10y:.2f}% — extreme EM pressure, FII exodus likely")
    elif us10y > 4.25:
        return ModifierResult("Global Yield Pressure", True, -3.0,
                              f"US 10Y {us10y:.2f}% — gravity pulling capital to US")
    elif us10y < 3.5:
        return ModifierResult("Global Yield Pressure", True, +3.0,
                              f"US 10Y {us10y:.2f}% — risk-on, FII inflows to EM")
    elif us10y < 3.8:
        return ModifierResult("Global Yield Pressure", True, +1.0,
                              f"US 10Y {us10y:.2f}% — mild EM tailwind")
    else:
        return ModifierResult("Global Yield Pressure", False, 0.0,
                              f"US 10Y {us10y:.2f}% — neutral range (3.8-4.25%)")


def check_rupee_stress(usd_inr: float, usd_inr_20d_ago: float = 0.0) -> ModifierResult:
    """
    Modifier 9: Rupee Stress.  ★ NEW v3.1
    Weak INR erodes FII returns and increases import costs.
    Uses 20-day rate of change to detect acceleration.
    """
    if usd_inr <= 0:
        return ModifierResult("Rupee Stress", False, 0.0,
                              "USD/INR data unavailable")
    
    # Calculate 20-day depreciation rate
    if usd_inr_20d_ago > 0:
        depreciation_pct = ((usd_inr - usd_inr_20d_ago) / usd_inr_20d_ago) * 100
    else:
        depreciation_pct = 0.0
    
    if depreciation_pct > 3.0:
        return ModifierResult("Rupee Stress", True, -5.0,
                              f"INR ₹{usd_inr:.2f} — vertical spike ({depreciation_pct:+.1f}% in 20d), panic selling likely")
    elif depreciation_pct > 2.0:
        return ModifierResult("Rupee Stress", True, -3.0,
                              f"INR ₹{usd_inr:.2f} — rapid weakening ({depreciation_pct:+.1f}% in 20d), FII headwind")
    elif depreciation_pct < -1.0:
        return ModifierResult("Rupee Stress", True, +2.0,
                              f"INR ₹{usd_inr:.2f} — appreciating ({depreciation_pct:+.1f}% in 20d), FII confidence boost")
    elif depreciation_pct < 0:
        return ModifierResult("Rupee Stress", True, +1.0,
                              f"INR ₹{usd_inr:.2f} — stable/strengthening ({depreciation_pct:+.1f}% in 20d)")
    else:
        return ModifierResult("Rupee Stress", False, 0.0,
                              f"INR ₹{usd_inr:.2f} — stable ({depreciation_pct:+.1f}% in 20d)")


# =============================================================================
# 4. STATE DETERMINATION
# =============================================================================

def determine_state(modifiers: list, rsi_breadth_pct: float, structural_pct: float) -> tuple:
    """
    Determine the daily market state based on modifiers and breadth.
    Returns: (state_name, state_message)
    """
    # Check Warning Day first (highest priority)
    for m in modifiers:
        if m.name == "Warning Day" and m.triggered:
            return "WARNING", "3+ pillars weak. Stop new entries. Tighten existing stops."

    # Overextended: both RSI breadth and structural very high
    if rsi_breadth_pct > 80 and structural_pct > 70:
        return "OVEREXTENDED", "Sell into strength. Book partial profits."

    # Exhausted: RSI breadth or structural extremely low
    if rsi_breadth_pct < 20 or structural_pct < 15:
        return "EXHAUSTED", "Bottom fishing zone. Watch for reversal candles."

    return "NORMAL", "Trend is healthy — trade your plan."


# =============================================================================
# 5. ZONE CLASSIFICATION
# =============================================================================

def classify_zone(score: float) -> tuple:
    """Returns (zone_name, emoji, action, risk_per_trade)."""
    if score >= 75:
        return (
            "Easy Money Zone", "🟢",
            "Full position sizing. Breakouts have high success rate. "
            "Be aggressive on quality setups.",
            "1.5–2% risk per trade"
        )
    elif score >= 60:
        return (
            "Smart Money Zone", "🔵",
            "Standard sizing. Market is selective — focus on sector leaders. "
            "Don't chase laggards.",
            "1% risk per trade"
        )
    elif score >= 40:
        return (
            "Hard Money Zone", "🟡",
            "Selective mean-reversion setups. Backtested edge exists at 0.5% risk. "
            "Tight stops, quick exits.",
            "0.5% risk per trade"
        )
    else:
        return (
            "No Money Zone", "🔴",
            "Cash is King. Even good setups will fail due to market drag. "
            "Protect capital, wait for conditions to improve.",
            "0% exposure"
        )


# =============================================================================
# 6. MACRO SUMMARY BUILDER  ★ NEW v3.1
# =============================================================================

def build_macro_summary(modifiers: list) -> str:
    """Build a human-readable macro summary from the 3 macro modifiers."""
    macro_mods = [m for m in modifiers if m.name in (
        "Crude Oil Stress", "Global Yield Pressure", "Rupee Stress"
    )]
    active = [m for m in macro_mods if m.triggered]
    if not active:
        return "Macro: All neutral — no global headwinds or tailwinds."
    
    total = sum(m.adjustment for m in active)
    parts = []
    for m in active:
        sign = "+" if m.adjustment > 0 else ""
        parts.append(f"{m.name} ({sign}{m.adjustment:.0f})")
    
    direction = "tailwind" if total > 0 else "headwind"
    sign = "+" if total > 0 else ""
    return f"Macro {direction} ({sign}{total:.0f}): {', '.join(parts)}"


# =============================================================================
# 7. MAIN CALCULATION ENGINE
# =============================================================================

def calculate_mps(
    data: RawMarketData,
    structural_bull_streak_days: int = 0,
    prev_pct_above_50sma: float = 0.0,
    fii_net_consecutive_sell_days: int = 0,
    fii_5day_net_crores: float = 0.0,
    usd_inr_20d_ago: float = 0.0,          # ★ NEW v3.1
) -> MPSResult:
    """
    Master function: takes raw market data and returns the full MPS v3.1 result.
    """

    # --- Calculate percentages ---
    pct_above_200sma = (data.stocks_above_200sma / data.total_universe) * 100
    pct_above_50sma = (data.stocks_above_50sma / data.total_universe) * 100
    rsi_breadth_pct = (data.stocks_rsi_above_50 / data.total_universe) * 100
    atr_breadth_pct = (data.stocks_atr_pct_above_4 / data.total_universe) * 100

    # --- A/D Ratio ---
    total_traded = data.advances + data.declines
    if total_traded > 0:
        ad_ratio = data.advances / data.declines if data.declines > 0 else 3.0
    else:
        ad_ratio = 1.0

    # --- Score each pillar ---
    pillars = []

    # P1: Structural (18%)
    s1_raw = score_structural(pct_above_200sma)
    pillars.append(PillarScore(
        "Structural", pct_above_200sma, s1_raw, 0.18, s1_raw * 0.18,
        f"{pct_above_200sma:.1f}% of Nifty 500 above 200 SMA"
    ))

    # P2: Breadth — Composite (18%)
    breadth_comp, sma_sub, ad_sub = score_breadth_composite(pct_above_50sma, ad_ratio)
    pillars.append(PillarScore(
        "Breadth", pct_above_50sma, breadth_comp, 0.18, breadth_comp * 0.18,
        f"Composite: {pct_above_50sma:.1f}% > 50 SMA (score {sma_sub:.1f}) "
        f"+ A/D {ad_ratio:.2f} (score {ad_sub:.1f})",
        sub_components={
            "positional_pct": round(pct_above_50sma, 1),
            "positional_score": round(sma_sub, 1),
            "ad_ratio": round(ad_ratio, 2),
            "ad_score": round(ad_sub, 1),
            "advances": data.advances,
            "declines": data.declines,
        }
    ))

    # P3: Spark — Composite (13%)
    spark_comp, stockbee_sub, burst_sub, burst_ratio = score_spark_composite(
        data.stocks_up_4pct, data.burst_gainers_4_5pct, data.burst_losers_4_5pct
    )
    burst_label = get_burst_label(burst_ratio)
    pillars.append(PillarScore(
        "Spark", float(data.stocks_up_4pct), spark_comp, 0.13, spark_comp * 0.13,
        f"Stockbee 4%+: {data.stocks_up_4pct} (score {stockbee_sub:.1f}) "
        f"+ Burst {burst_ratio:.0f} [{burst_label}] (score {burst_sub:.1f})",
        sub_components={
            "stockbee_count": data.stocks_up_4pct,
            "stockbee_score": round(stockbee_sub, 1),
            "burst_gainers": data.burst_gainers_4_5pct,
            "burst_losers": data.burst_losers_4_5pct,
            "burst_ratio": round(burst_ratio, 1),
            "burst_label": burst_label,
            "burst_score": round(burst_sub, 1),
        }
    ))

    # P4: Quality (13%)
    net_new_highs = data.new_52w_highs - data.new_52w_lows
    s4_raw = score_quality(data.new_52w_highs, data.new_52w_lows)
    pillars.append(PillarScore(
        "Quality", float(net_new_highs), s4_raw, 0.13, s4_raw * 0.13,
        f"Net New Highs: {net_new_highs} ({data.new_52w_highs}H - {data.new_52w_lows}L)"
    ))

    # P5: Sentiment (13%)
    s5_raw, vix_sub, pcr_sub = score_sentiment(data.india_vix, data.pcr)
    pillars.append(PillarScore(
        "Sentiment", data.india_vix, s5_raw, 0.13, s5_raw * 0.13,
        f"VIX: {data.india_vix:.2f} (score {vix_sub:.1f}), PCR: {data.pcr:.2f} (score {pcr_sub:.1f})",
        sub_components={"vix_score": round(vix_sub, 1), "pcr_score": round(pcr_sub, 1)}
    ))

    # P6: Momentum / RSI Breadth (13%)
    s6_raw = score_momentum(rsi_breadth_pct)
    pillars.append(PillarScore(
        "Momentum", rsi_breadth_pct, s6_raw, 0.13, s6_raw * 0.13,
        f"{rsi_breadth_pct:.1f}% of Nifty 500 with RSI(14) > 50"
    ))

    # P7: Volatility / ATR Breadth (12%)
    s7_raw, atr_regime = score_volatility(atr_breadth_pct)
    pillars.append(PillarScore(
        "Volatility", atr_breadth_pct, s7_raw, 0.12, s7_raw * 0.12,
        f"ATR Breadth: {atr_breadth_pct:.1f}% of stocks with ATR% > 4% — [{atr_regime}]"
    ))

    # --- Base score ---
    base_score = sum(p.weighted_score for p in pillars)

    # --- Apply modifiers ---
    modifiers = []

    mod1 = check_exhaustion_penalty(data.stocks_rsi_above_70, data.total_universe)
    modifiers.append(mod1)

    mod2 = check_persistence_boost(structural_bull_streak_days)
    modifiers.append(mod2)

    mod3 = check_divergence_warning(
        data.nifty_at_52w_high, pct_above_50sma, prev_pct_above_50sma
    )
    modifiers.append(mod3)

    mod4 = check_fii_flow_warning(fii_net_consecutive_sell_days, fii_5day_net_crores)
    modifiers.append(mod4)

    mod5 = check_warning_day(pillars)
    modifiers.append(mod5)

    mod6 = check_volatility_regime(atr_breadth_pct)
    modifiers.append(mod6)

    # ★ NEW v3.1 — Macro modifiers
    mod7 = check_crude_oil_stress(data.brent_crude)
    modifiers.append(mod7)

    mod8 = check_global_yield_pressure(data.us10y_yield)
    modifiers.append(mod8)

    mod9 = check_rupee_stress(data.usd_inr, usd_inr_20d_ago)
    modifiers.append(mod9)

    total_modifier = sum(m.adjustment for m in modifiers)

    # --- Final score (clamped 0–100) ---
    final_score = max(0.0, min(100.0, base_score + total_modifier))

    # --- Zone classification ---
    zone_name, zone_emoji, zone_action, risk = classify_zone(final_score)

    # --- Daily State ---
    state, state_message = determine_state(modifiers, rsi_breadth_pct, pct_above_200sma)

    # --- Macro Summary ---  ★ NEW v3.1
    macro_summary = build_macro_summary(modifiers)

    return MPSResult(
        date=data.date,
        version="3.1",
        pillar_scores=[asdict(p) for p in pillars],
        base_score=round(base_score, 2),
        modifiers=[asdict(m) for m in modifiers],
        total_modifier=round(total_modifier, 2),
        final_score=round(final_score, 2),
        zone=zone_name,
        zone_emoji=zone_emoji,
        zone_action=zone_action,
        risk_per_trade=risk,
        state=state,
        state_message=state_message,
        burst_ratio=round(burst_ratio, 1),
        burst_label=burst_label,
        rsi_breadth_pct=round(rsi_breadth_pct, 1),
        atr_breadth_pct=round(atr_breadth_pct, 1),
        atr_regime=atr_regime,
        macro_summary=macro_summary,
    )


# =============================================================================
# 8. OUTPUT FORMATTERS
# =============================================================================

def format_mps_report(result: MPSResult) -> str:
    """Pretty-print the MPS v3.1 result for console/log output."""
    lines = []
    lines.append("=" * 75)
    lines.append(f"  MPS COMMAND CENTER v{result.version} — {result.date}")
    lines.append(f"  ✦ Trade the Pulse ✦  |  By Dr. Rahul Ware")
    lines.append("=" * 75)
    lines.append("")

    # Pillar breakdown
    lines.append("  PILLAR SCORES (7 Pillars)")
    lines.append("  " + "─" * 65)
    for p in result.pillar_scores:
        bar = "█" * int(p['raw_score'] / 5) + "░" * (20 - int(p['raw_score'] / 5))
        pct_label = f"{p['weight']:.0%}" if p['weight'] >= 0.10 else f"{p['weight']*100:.1f}%"
        lines.append(f"  {p['name']:<12} {bar} {p['raw_score']:5.1f} × {pct_label} = {p['weighted_score']:5.2f}")
        lines.append(f"               {p['description']}")
    lines.append("  " + "─" * 65)
    lines.append(f"  {'Base Score':<12} {'':20s} {result.base_score:>22.2f}")
    lines.append("")

    # Modifiers
    lines.append("  SMART MODIFIERS (9 Modifiers)")
    lines.append("  " + "─" * 65)
    for m in result.modifiers:
        status = "✅ ACTIVE" if m['triggered'] else "⬜ inactive"
        adj = f"{m['adjustment']:+.1f}" if m['triggered'] else "  0.0"
        lines.append(f"  {m['name']:<22} {status}  {adj:>6}")
        lines.append(f"               {m['reason']}")
    lines.append("  " + "─" * 65)
    lines.append(f"  {'Modifier Total':<22} {result.total_modifier:>36.2f}")
    lines.append("")

    # Macro summary
    lines.append(f"  {result.macro_summary}")
    lines.append("")

    # State banner
    state_icons = {"NORMAL": "✦", "WARNING": "⚠️", "OVEREXTENDED": "🔥", "EXHAUSTED": "💀"}
    lines.append(f"  ┌{'─'*63}┐")
    lines.append(f"  │  STATE: {state_icons.get(result.state, '?')} {result.state:<15} {result.state_message:<32}│")
    lines.append(f"  ├{'─'*63}┤")
    lines.append(f"  │  FINAL MPS: {result.final_score:6.2f}  {result.zone_emoji} {result.zone:<28s}│")
    lines.append(f"  │  Risk: {result.risk_per_trade:<51s}│")
    burst_line = f"  Burst: {result.burst_ratio:.0f} [{result.burst_label}]  RSI: {result.rsi_breadth_pct:.1f}%  ATR: {result.atr_breadth_pct:.1f}% [{result.atr_regime}]"
    pad = max(0, 63 - len(burst_line))
    lines.append(f"  │{burst_line}{' ' * pad}│")
    lines.append(f"  └{'─'*63}┘")
    lines.append(f"  Action: {result.zone_action}")
    lines.append("")

    return "\n".join(lines)


def to_json(result: MPSResult) -> str:
    """Export MPS v3.1 result as JSON (for GitHub Pages / mps_latest.json)."""
    return json.dumps(asdict(result), indent=2, ensure_ascii=False)


# =============================================================================
# 9. DEMO / TEST SCENARIOS
# =============================================================================

if __name__ == "__main__":

    # Show animated banner
    print_banner(animate=True)

    # =========================================================================
    # SCENARIO 1: Strong Bull Market Day (Easy Money) + Macro Tailwind
    # =========================================================================
    print("\n📈 SCENARIO 1: Strong Bull + Macro Tailwind")
    bull_data = RawMarketData(
        date="2026-03-15",
        stocks_above_200sma=340,
        stocks_above_50sma=310,
        advances=320, declines=150, unchanged=30,
        stocks_up_4pct=32,
        burst_gainers_4_5pct=22, burst_losers_4_5pct=3,
        new_52w_highs=85, new_52w_lows=12,
        india_vix=13.5, pcr=0.95,
        stocks_rsi_above_50=342,
        stocks_atr_pct_above_4=65,
        stocks_rsi_above_70=45,
        nifty_at_52w_high=False,
        fii_net_buy_crores=1250.0,
        # ★ Macro v3.1
        brent_crude=72.5,       # Low crude = tailwind
        us10y_yield=3.6,        # Low yield = mild tailwind
        usd_inr=85.20,          # Stable INR
    )
    result1 = calculate_mps(
        bull_data,
        structural_bull_streak_days=35,
        fii_net_consecutive_sell_days=0,
        fii_5day_net_crores=3200.0,
        usd_inr_20d_ago=85.50,  # INR appreciated slightly
    )
    print(format_mps_report(result1))

    # =========================================================================
    # SCENARIO 2: Warning Day + Macro Headwind
    # =========================================================================
    print("\n⚠️ SCENARIO 2: Warning Day + Macro Headwind")
    warning_data = RawMarketData(
        date="2026-03-15",
        stocks_above_200sma=218,
        stocks_above_50sma=165,
        advances=680, declines=1320, unchanged=0,
        stocks_up_4pct=5,
        burst_gainers_4_5pct=4, burst_losers_4_5pct=11,
        new_52w_highs=8, new_52w_lows=38,
        india_vix=22.8, pcr=0.72,
        stocks_rsi_above_50=191,
        stocks_atr_pct_above_4=160,
        stocks_rsi_above_70=12,
        nifty_at_52w_high=False,
        fii_net_buy_crores=-3200.0,
        # ★ Macro v3.1 — all headwinds
        brent_crude=102.0,      # High crude = stress
        us10y_yield=4.8,        # High yield = EM pressure
        usd_inr=88.50,          # Weak rupee
    )
    result2 = calculate_mps(
        warning_data,
        structural_bull_streak_days=0,
        fii_net_consecutive_sell_days=8,
        fii_5day_net_crores=-12000.0,
        usd_inr_20d_ago=86.20,  # INR depreciated ~2.7%
    )
    print(format_mps_report(result2))

    # =========================================================================
    # SCENARIO 3: Bear Market / Exhausted + Macro Neutral
    # =========================================================================
    print("\n🔴 SCENARIO 3: Bear Market + Macro Neutral")
    bear_data = RawMarketData(
        date="2026-03-15",
        stocks_above_200sma=100,
        stocks_above_50sma=75,
        advances=80, declines=390, unchanged=30,
        stocks_up_4pct=2,
        burst_gainers_4_5pct=1, burst_losers_4_5pct=19,
        new_52w_highs=5, new_52w_lows=120,
        india_vix=32.0, pcr=1.8,
        stocks_rsi_above_50=74,
        stocks_atr_pct_above_4=220,
        stocks_rsi_above_70=3,
        nifty_at_52w_high=False,
        fii_net_buy_crores=-5800.0,
        # ★ Macro v3.1
        brent_crude=85.0,       # Neutral
        us10y_yield=4.0,        # Neutral
        usd_inr=86.50,          # Neutral
    )
    result3 = calculate_mps(
        bear_data,
        structural_bull_streak_days=0,
        fii_net_consecutive_sell_days=12,
        fii_5day_net_crores=-25000.0,
        usd_inr_20d_ago=86.30,
    )
    print(format_mps_report(result3))

    # --- Print JSON for Scenario 1 ---
    print("\n📋 JSON OUTPUT (Scenario 1 — for GitHub Pages):")
    print(to_json(result1))
