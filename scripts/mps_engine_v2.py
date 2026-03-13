"""
================================================================================
MPS (Market Pulse Score) Calculation Engine v2.0
================================================================================
TradEdge Command Center — Unified Market Health Scoring System

Universe: Nifty 500
Score Range: 0–100
Update: Daily EOD (~4:00 PM IST)

Pillars:
  1. Structural (25%) — % stocks above 200 SMA
  2. Breadth    (25%) — Composite: 60% SMA participation + 40% A/D flow
  3. Spark      (20%) — Stockbee 4% breakout count
  4. Quality    (15%) — Net new 52-week highs
  5. Sentiment  (15%) — VIX + PCR composite

Modifiers:
  - Exhaustion Penalty    (RSI > 70 overheating)
  - Persistence Boost     (21-day structural bull streak)
  - Divergence Warning    (Nifty new high + falling breadth)
  - FII Flow Warning      (5+ day consecutive FII selling)

Output: MPS score (0–100) + Money Zone classification

Changelog v2.0:
  - Breadth pillar now composite: 60% positional (>50 SMA) + 40% flow (A/D)
  - Added FII 5-day flow direction as 4th smart modifier
  - Updated test scenarios to cover new inputs
================================================================================
"""

import json
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional, List


# =============================================================================
# 1. DATA STRUCTURES
# =============================================================================

@dataclass
class RawMarketData:
    """Raw inputs fetched from Chartink / NSE daily."""
    date: str                          # YYYY-MM-DD

    # Structural (from Chartink)
    stocks_above_200sma: int = 0       # count of Nifty 500 stocks > 200 SMA
    total_universe: int = 500          # Nifty 500

    # Breadth — positional component (from Chartink)
    stocks_above_50sma: int = 0        # count of Nifty 500 stocks > 50 SMA

    # Breadth — flow component (from NSE / Chartink)
    advances: int = 0                  # stocks that closed up today
    declines: int = 0                  # stocks that closed down today
    unchanged: int = 0                 # stocks flat today

    # Spark (from Chartink)
    stocks_up_4pct: int = 0            # stocks with >= 4% daily gain

    # Quality (from NSE)
    new_52w_highs: int = 0             # 52-week new highs today
    new_52w_lows: int = 0              # 52-week new lows today

    # Sentiment (from NSE)
    india_vix: float = 0.0             # India VIX value
    pcr: float = 0.0                   # Put-Call Ratio (Nifty options)

    # For modifiers
    stocks_rsi_above_70: int = 0       # stocks with RSI(14) > 70
    nifty_at_52w_high: bool = False    # did Nifty hit a new 52W high today?

    # FII flow (from NSE / moneycontrol)
    fii_net_buy_crores: float = 0.0    # FII net buy/sell in crores (negative = selling)


@dataclass
class PillarScore:
    """Score for a single pillar (0–100 raw, then weighted)."""
    name: str
    raw_value: float          # the actual metric value
    raw_score: float          # normalized 0–100
    weight: float             # pillar weight
    weighted_score: float     # raw_score × weight
    description: str = ""
    sub_components: dict = field(default_factory=dict)  # for composite pillars


@dataclass
class ModifierResult:
    """Result of a smart modifier check."""
    name: str
    triggered: bool
    adjustment: float         # points added/subtracted to final score
    reason: str = ""


@dataclass
class MPSResult:
    """Final MPS output for the day."""
    date: str
    pillar_scores: list       # list of PillarScore dicts
    base_score: float         # sum of weighted scores before modifiers
    modifiers: list           # list of ModifierResult dicts
    total_modifier: float     # net modifier adjustment
    final_score: float        # clamped 0–100
    zone: str                 # Money Zone name
    zone_emoji: str           # Zone emoji
    zone_action: str          # Recommended action
    risk_per_trade: str       # Position sizing guidance


# =============================================================================
# 2. PILLAR SCORING FUNCTIONS (each returns 0–100)
# =============================================================================

def score_structural(pct_above_200sma: float) -> float:
    """
    Structural Health: % of Nifty 500 above 200 SMA.

    Scoring (linear interpolation):
      0%   → 0
      30%  → 25   (weak floor)
      50%  → 50   (neutral — Nitin's bull/bear line)
      70%  → 75   (strong floor)
      85%+ → 100  (rock solid)
    """
    breakpoints = [(0, 0), (30, 25), (50, 50), (70, 75), (85, 100)]
    return _interpolate(pct_above_200sma, breakpoints)


def score_breadth_composite(pct_above_50sma: float, ad_ratio: float) -> tuple:
    """
    Breadth Participation — COMPOSITE PILLAR (v2.0)

    Two sub-components:
      A) Positional (60% weight): % stocks above 50 SMA
         Tells you WHERE stocks are relative to trend.
      B) Flow (40% weight): Advance/Decline ratio
         Tells you what stocks DID today — are they moving up or down?

    Why both matter:
      - 60% above 50 SMA + strong A/D = healthy bull (army is positioned AND marching)
      - 60% above 50 SMA + weak A/D   = positioned but stalling (early warning)
      - 40% above 50 SMA + strong A/D = recovery attempt (watch for follow-through)
      - 40% above 50 SMA + weak A/D   = confirmed weakness

    Positional scoring (same as v1):
      0%   → 0
      25%  → 20
      50%  → 50
      65%  → 75
      80%+ → 100

    A/D Ratio scoring:
      0.4  → 0    (extreme decline dominance: ~70% declining)
      0.7  → 25   (bearish: more declines than advances)
      1.0  → 50   (neutral: equal advances and declines)
      1.5  → 75   (bullish: 60% advancing)
      2.0  → 90   (strong advance day)
      3.0+ → 100  (blowout breadth thrust — very rare, very bullish)

    Returns: (composite_score, positional_score, ad_score)
    """
    # A) Positional sub-score
    sma_breakpoints = [(0, 0), (25, 20), (50, 50), (65, 75), (80, 100)]
    positional_score = _interpolate(pct_above_50sma, sma_breakpoints)

    # B) A/D flow sub-score
    ad_breakpoints = [(0.4, 0), (0.7, 25), (1.0, 50), (1.5, 75), (2.0, 90), (3.0, 100)]
    ad_score = _interpolate(ad_ratio, ad_breakpoints)

    # Composite: 60% positional + 40% flow
    composite = 0.60 * positional_score + 0.40 * ad_score

    return composite, positional_score, ad_score


def score_spark(stocks_up_4pct: int) -> float:
    """
    Spark / Momentum Heat: Count of stocks with >= 4% daily gain.
    (Stockbee methodology adapted for Indian markets)

    Scoring:
      0      → 0
      5      → 15   (barely warm)
      15     → 40   (warming up)
      25     → 65   (Stockbee's "hot" threshold)
      40     → 85   (on fire)
      60+    → 100  (euphoric breakout day)
    """
    breakpoints = [(0, 0), (5, 15), (15, 40), (25, 65), (40, 85), (60, 100)]
    return _interpolate(float(stocks_up_4pct), breakpoints)


def score_quality(new_highs: int, new_lows: int) -> float:
    """
    Quality: Net New Highs (52W Highs minus 52W Lows).

    Scoring based on net value:
      -50 or worse → 0    (deep bear)
      -20          → 20
       0           → 45   (neutral)
      +20          → 65
      +50          → 80
      +100+        → 100  (strong bull rotation)
    """
    net = new_highs - new_lows
    breakpoints = [(-50, 0), (-20, 20), (0, 45), (20, 65), (50, 80), (100, 100)]
    return _interpolate(float(net), breakpoints)


def score_sentiment(vix: float, pcr: float) -> float:
    """
    Sentiment: Composite of India VIX and Nifty PCR.

    VIX scoring (lower = more bullish):
      < 12  → 100  (extreme complacency)
      14    → 85
      18    → 60   (neutral)
      22    → 35
      28    → 15
      35+   → 0    (panic)

    PCR scoring (sweet spot is 0.8–1.2):
      < 0.5 → 20   (extreme call buying — speculative)
      0.7   → 60
      0.8   → 80
      1.0   → 100  (ideal balance)
      1.2   → 80
      1.5   → 50   (heavy put buying — fear)
      2.0+  → 15   (extreme fear)

    Final sentiment = 50% VIX score + 50% PCR score
    """
    # VIX component
    vix_breakpoints = [(12, 100), (14, 85), (18, 60), (22, 35), (28, 15), (35, 0)]
    vix_score = _interpolate(vix, vix_breakpoints)

    # PCR component (two-sided)
    if pcr <= 1.0:
        pcr_breakpoints = [(0.5, 20), (0.7, 60), (0.8, 80), (1.0, 100)]
    else:
        pcr_breakpoints = [(1.0, 100), (1.2, 80), (1.5, 50), (2.0, 15)]
    pcr_score = _interpolate(pcr, pcr_breakpoints)

    return 0.5 * vix_score + 0.5 * pcr_score


# =============================================================================
# 3. SMART MODIFIERS
# =============================================================================

def check_exhaustion_penalty(stocks_rsi_above_70: int, total: int = 500) -> ModifierResult:
    """
    Exhaustion Penalty: If > 80% of stocks have RSI(14) > 70,
    the market is vertically stretched.

    Penalty scale:
      80% RSI>70 → -5 points
      90% RSI>70 → -10 points
      95%+       → -15 points
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
    Persistence Boost: If Structural Bull (>50% above 200 SMA) holds
    for 21+ consecutive trading days, award a stability bonus.

    Bonus scale:
      21 days → +3 points
      42 days → +5 points
      63 days → +7 points (quarter of sustained bull)
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
    Divergence Warning (Zombie Rally Detector):
    If Nifty hits a new 52W high BUT breadth (% above 50 SMA) is FALLING,
    it signals a narrow rally that could reverse.

    Penalty: -8 points when triggered.
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
    FII Flow Warning (v2.0): FIIs are the marginal price-setter in Indian
    markets. When they sell persistently, even "healthy" markets can crack.

    Logic:
      - Track consecutive days of net FII selling
      - Trigger warning at 5+ consecutive sell days
      - Penalty scales with streak length AND cumulative outflow size

    Penalty scale:
      5–7 consecutive sell days   → -3 points  (caution)
      8–10 consecutive sell days  → -5 points  (significant pressure)
      11+ consecutive sell days   → -8 points  (heavy institutional exit)

    Additional severity: If 5-day cumulative outflow exceeds ₹10,000 Cr,
    add an extra -2 points (heavy magnitude selling).

    Why direction > amount:
      - ₹500 Cr selling on one day is noise
      - ₹500 Cr selling for 10 straight days is ₹5,000 Cr of persistent pressure
      - The STREAK matters more than any single day's number
    """
    if fii_net_consecutive_sell_days < 5:
        return ModifierResult("FII Flow Warning", False, 0.0,
                              f"FII sell streak: {fii_net_consecutive_sell_days} days — below threshold (need 5+)")

    # Base penalty by streak length
    if fii_net_consecutive_sell_days >= 11:
        base_penalty = -8.0
        severity = "heavy institutional exit"
    elif fii_net_consecutive_sell_days >= 8:
        base_penalty = -5.0
        severity = "significant FII pressure"
    else:  # 5-7 days
        base_penalty = -3.0
        severity = "FII caution zone"

    # Magnitude bonus penalty: if 5-day outflow > ₹10,000 Cr
    magnitude_penalty = 0.0
    magnitude_note = ""
    if fii_5day_net_crores < -10000:
        magnitude_penalty = -2.0
        magnitude_note = f" + heavy outflow (₹{abs(fii_5day_net_crores):,.0f} Cr in 5 days)"

    total_penalty = base_penalty + magnitude_penalty

    return ModifierResult(
        "FII Flow Warning", True, total_penalty,
        f"FII net sellers for {fii_net_consecutive_sell_days} consecutive days — "
        f"{severity}{magnitude_note}"
    )


# =============================================================================
# 4. ZONE CLASSIFICATION
# =============================================================================

def classify_zone(score: float) -> tuple:
    """
    Returns (zone_name, emoji, action, risk_per_trade) based on MPS score.
    """
    if score >= 75:
        return (
            "Easy Money Zone",
            "🟢",
            "Full position sizing. Breakouts have high success rate. "
            "Be aggressive on quality setups.",
            "1.5–2% risk per trade"
        )
    elif score >= 60:
        return (
            "Smart Money Zone",
            "🔵",
            "Standard sizing. Market is selective — focus on sector leaders. "
            "Don't chase laggards.",
            "1% risk per trade"
        )
    elif score >= 40:
        return (
            "Hard Money Zone",
            "🟡",
            "Defensive sizing. Choppy/rangebound conditions. "
            "Quick scalps only, tight stops.",
            "0.25% risk per trade"
        )
    else:
        return (
            "No Money Zone",
            "🔴",
            "Cash is King. Even good setups will fail due to market drag. "
            "Protect capital, wait for conditions to improve.",
            "0% exposure"
        )


# =============================================================================
# 5. MAIN CALCULATION ENGINE
# =============================================================================

def calculate_mps(
    data: RawMarketData,
    structural_bull_streak_days: int = 0,
    prev_pct_above_50sma: float = 0.0,
    fii_net_consecutive_sell_days: int = 0,
    fii_5day_net_crores: float = 0.0,
) -> MPSResult:
    """
    Master function: takes raw market data and returns the full MPS result.

    Parameters:
      data: Today's RawMarketData
      structural_bull_streak_days: consecutive days with >50% above 200 SMA
      prev_pct_above_50sma: yesterday's breadth % (for divergence check)
      fii_net_consecutive_sell_days: how many straight days FIIs were net sellers
      fii_5day_net_crores: cumulative FII net flow over last 5 trading days (₹Cr)
    """

    # --- Calculate percentages ---
    pct_above_200sma = (data.stocks_above_200sma / data.total_universe) * 100
    pct_above_50sma = (data.stocks_above_50sma / data.total_universe) * 100

    # --- A/D Ratio ---
    total_traded = data.advances + data.declines
    if total_traded > 0:
        ad_ratio = data.advances / data.declines if data.declines > 0 else 3.0
    else:
        ad_ratio = 1.0  # default neutral if no data

    # --- Score each pillar ---
    pillars = []

    # Structural (25%)
    s1_raw = score_structural(pct_above_200sma)
    pillars.append(PillarScore(
        name="Structural",
        raw_value=pct_above_200sma,
        raw_score=s1_raw,
        weight=0.25,
        weighted_score=s1_raw * 0.25,
        description=f"{pct_above_200sma:.1f}% of Nifty 500 above 200 SMA"
    ))

    # Breadth — COMPOSITE (25%)
    breadth_composite, sma_sub, ad_sub = score_breadth_composite(pct_above_50sma, ad_ratio)
    pillars.append(PillarScore(
        name="Breadth",
        raw_value=pct_above_50sma,
        raw_score=breadth_composite,
        weight=0.25,
        weighted_score=breadth_composite * 0.25,
        description=(
            f"Composite: {pct_above_50sma:.1f}% > 50 SMA (score {sma_sub:.1f}) "
            f"+ A/D {ad_ratio:.2f} (score {ad_sub:.1f})"
        ),
        sub_components={
            "positional_pct": round(pct_above_50sma, 1),
            "positional_score": round(sma_sub, 1),
            "positional_weight": 0.60,
            "ad_ratio": round(ad_ratio, 2),
            "ad_score": round(ad_sub, 1),
            "ad_weight": 0.40,
            "advances": data.advances,
            "declines": data.declines,
            "unchanged": data.unchanged,
        }
    ))

    # Spark (20%)
    s3_raw = score_spark(data.stocks_up_4pct)
    pillars.append(PillarScore(
        name="Spark",
        raw_value=float(data.stocks_up_4pct),
        raw_score=s3_raw,
        weight=0.20,
        weighted_score=s3_raw * 0.20,
        description=f"{data.stocks_up_4pct} stocks with ≥4% daily gain"
    ))

    # Quality (15%)
    net_new_highs = data.new_52w_highs - data.new_52w_lows
    s4_raw = score_quality(data.new_52w_highs, data.new_52w_lows)
    pillars.append(PillarScore(
        name="Quality",
        raw_value=float(net_new_highs),
        raw_score=s4_raw,
        weight=0.15,
        weighted_score=s4_raw * 0.15,
        description=f"Net New Highs: {net_new_highs} ({data.new_52w_highs}H - {data.new_52w_lows}L)"
    ))

    # Sentiment (15%)
    s5_raw = score_sentiment(data.india_vix, data.pcr)
    pillars.append(PillarScore(
        name="Sentiment",
        raw_value=data.india_vix,
        raw_score=s5_raw,
        weight=0.15,
        weighted_score=s5_raw * 0.15,
        description=f"VIX: {data.india_vix:.2f}, PCR: {data.pcr:.2f}"
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

    total_modifier = sum(m.adjustment for m in modifiers)

    # --- Final score (clamped 0–100) ---
    final_score = max(0.0, min(100.0, base_score + total_modifier))

    # --- Zone classification ---
    zone_name, zone_emoji, zone_action, risk = classify_zone(final_score)

    return MPSResult(
        date=data.date,
        pillar_scores=[asdict(p) for p in pillars],
        base_score=round(base_score, 2),
        modifiers=[asdict(m) for m in modifiers],
        total_modifier=round(total_modifier, 2),
        final_score=round(final_score, 2),
        zone=zone_name,
        zone_emoji=zone_emoji,
        zone_action=zone_action,
        risk_per_trade=risk,
    )


# =============================================================================
# 6. HELPER: Linear Interpolation
# =============================================================================

def _interpolate(value: float, breakpoints: list) -> float:
    """
    Linear interpolation between breakpoints.
    breakpoints: list of (input_value, output_score) tuples, sorted by input.
    Values below/above clamp to first/last score.
    """
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


# =============================================================================
# 7. OUTPUT FORMATTER
# =============================================================================

def format_mps_report(result: MPSResult) -> str:
    """Pretty-print the MPS result for console/log output."""
    lines = []
    lines.append("=" * 70)
    lines.append(f"  MPS COMMAND CENTER v2.0 — {result.date}")
    lines.append("=" * 70)
    lines.append("")

    # Pillar breakdown
    lines.append("  PILLAR SCORES")
    lines.append("  " + "-" * 60)
    for p in result.pillar_scores:
        bar = "█" * int(p['raw_score'] / 5) + "░" * (20 - int(p['raw_score'] / 5))
        lines.append(f"  {p['name']:<12} {bar} {p['raw_score']:5.1f} × {p['weight']:.0%} = {p['weighted_score']:5.2f}")
        lines.append(f"               {p['description']}")
        # Show sub-components for composite pillars
        if p.get('sub_components'):
            sc = p['sub_components']
            lines.append(f"               ├─ Positional (60%): {sc.get('positional_pct', 0)}% > 50 SMA → score {sc.get('positional_score', 0)}")
            lines.append(f"               └─ A/D Flow   (40%): {sc.get('advances', 0)}A / {sc.get('declines', 0)}D = {sc.get('ad_ratio', 0)} → score {sc.get('ad_score', 0)}")
    lines.append("  " + "-" * 60)
    lines.append(f"  {'Base Score':<12} {'':20s} {result.base_score:>18.2f}")
    lines.append("")

    # Modifiers
    lines.append("  SMART MODIFIERS")
    lines.append("  " + "-" * 60)
    for m in result.modifiers:
        status = "✅ ACTIVE" if m['triggered'] else "⬜ inactive"
        adj = f"{m['adjustment']:+.1f}" if m['triggered'] else "  0.0"
        lines.append(f"  {m['name']:<22} {status}  {adj:>6}")
        lines.append(f"               {m['reason']}")
    lines.append("  " + "-" * 60)
    lines.append(f"  {'Modifier Total':<22} {result.total_modifier:>31.2f}")
    lines.append("")

    # Final Score & Zone
    lines.append("  ┌──────────────────────────────────────────────────────────┐")
    lines.append(f"  │  FINAL MPS: {result.final_score:6.2f}  {result.zone_emoji} {result.zone:<28s}│")
    lines.append(f"  │  Risk: {result.risk_per_trade:<49s}│")
    lines.append("  └──────────────────────────────────────────────────────────┘")
    lines.append(f"  Action: {result.zone_action}")
    lines.append("")

    return "\n".join(lines)


def to_json(result: MPSResult) -> str:
    """Export MPS result as JSON (for GitHub Pages consumption)."""
    return json.dumps(asdict(result), indent=2, ensure_ascii=False)


# =============================================================================
# 8. DEMO / TEST SCENARIOS
# =============================================================================

if __name__ == "__main__":

    # =========================================================================
    # SCENARIO 1: Strong Bull Market Day
    # Everything firing — strong floor, broad participation, hot spark
    # =========================================================================
    print("\n📈 SCENARIO 1: Strong Bull Market Day")
    bull_data = RawMarketData(
        date="2026-03-13",
        stocks_above_200sma=340,       # 68% — strong floor
        stocks_above_50sma=310,        # 62% — healthy positional
        advances=320,                  # strong A/D day
        declines=150,
        unchanged=30,
        stocks_up_4pct=32,             # above Stockbee's 25 threshold
        new_52w_highs=85,
        new_52w_lows=12,
        india_vix=13.5,
        pcr=0.95,
        stocks_rsi_above_70=45,        # 9% — not overheated
        nifty_at_52w_high=False,
        fii_net_buy_crores=1250.0,     # FII buying today
    )
    result1 = calculate_mps(
        bull_data,
        structural_bull_streak_days=35,
        fii_net_consecutive_sell_days=0,
        fii_5day_net_crores=3200.0,    # net buyer over 5 days
    )
    print(format_mps_report(result1))

    # =========================================================================
    # SCENARIO 2: Choppy Market with FII Selling
    # Weak breadth + FIIs dumping for 6 days straight
    # =========================================================================
    print("\n📊 SCENARIO 2: Choppy Market + FII Selling Pressure")
    choppy_data = RawMarketData(
        date="2026-03-13",
        stocks_above_200sma=240,       # 48% — below the line
        stocks_above_50sma=200,        # 40% — weak positional
        advances=180,                  # slightly negative A/D
        declines=280,
        unchanged=40,
        stocks_up_4pct=8,              # barely any spark
        new_52w_highs=20,
        new_52w_lows=35,
        india_vix=21.0,
        pcr=1.4,
        stocks_rsi_above_70=15,
        nifty_at_52w_high=False,
        fii_net_buy_crores=-850.0,
    )
    result2 = calculate_mps(
        choppy_data,
        structural_bull_streak_days=0,
        fii_net_consecutive_sell_days=6,   # 6 straight days of selling
        fii_5day_net_crores=-4200.0,
    )
    print(format_mps_report(result2))

    # =========================================================================
    # SCENARIO 3: Zombie Rally with A/D Divergence
    # Nifty at high, but breadth falling AND A/D weak
    # =========================================================================
    print("\n🧟 SCENARIO 3: Zombie Rally — Index high, breadth crumbling")
    zombie_data = RawMarketData(
        date="2026-03-13",
        stocks_above_200sma=280,       # 56% — barely structural bull
        stocks_above_50sma=220,        # 44% — falling from 52% yesterday
        advances=190,                  # A/D just barely negative
        declines=270,
        unchanged=40,
        stocks_up_4pct=18,
        new_52w_highs=40,
        new_52w_lows=25,
        india_vix=15.5,
        pcr=0.85,
        stocks_rsi_above_70=30,
        nifty_at_52w_high=True,
        fii_net_buy_crores=200.0,
    )
    result3 = calculate_mps(
        zombie_data,
        structural_bull_streak_days=28,
        prev_pct_above_50sma=52.0,
        fii_net_consecutive_sell_days=0,
        fii_5day_net_crores=800.0,
    )
    print(format_mps_report(result3))

    # =========================================================================
    # SCENARIO 4: Bear Market / Panic + Heavy FII Exodus
    # Everything broken + FIIs selling for 12 straight days with huge outflow
    # =========================================================================
    print("\n🔴 SCENARIO 4: Bear Market + FII Exodus")
    bear_data = RawMarketData(
        date="2026-03-13",
        stocks_above_200sma=100,       # 20% — structural bear
        stocks_above_50sma=75,         # 15% — army collapsed
        advances=80,                   # extreme decline dominance
        declines=390,
        unchanged=30,
        stocks_up_4pct=2,              # dead
        new_52w_highs=5,
        new_52w_lows=120,
        india_vix=32.0,
        pcr=1.8,
        stocks_rsi_above_70=3,
        nifty_at_52w_high=False,
        fii_net_buy_crores=-3200.0,
    )
    result4 = calculate_mps(
        bear_data,
        structural_bull_streak_days=0,
        fii_net_consecutive_sell_days=12,
        fii_5day_net_crores=-15000.0,   # massive outflow
    )
    print(format_mps_report(result4))

    # =========================================================================
    # SCENARIO 5 (NEW): Recovery Attempt
    # Positional breadth still low, but A/D ratio surging (early recovery signal)
    # =========================================================================
    print("\n🔄 SCENARIO 5: Recovery Attempt — A/D surging, but stocks still below SMAs")
    recovery_data = RawMarketData(
        date="2026-03-13",
        stocks_above_200sma=230,       # 46% — still below structural line
        stocks_above_50sma=190,        # 38% — positional still weak
        advances=380,                  # BIG A/D surge! Recovery flow
        declines=100,
        unchanged=20,
        stocks_up_4pct=28,             # spark is hot
        new_52w_highs=30,
        new_52w_lows=18,
        india_vix=19.0,
        pcr=1.05,
        stocks_rsi_above_70=20,
        nifty_at_52w_high=False,
        fii_net_buy_crores=2100.0,     # FIIs buying the dip
    )
    result5 = calculate_mps(
        recovery_data,
        structural_bull_streak_days=0,
        fii_net_consecutive_sell_days=0,
        fii_5day_net_crores=1500.0,
    )
    print(format_mps_report(result5))

    # --- Print JSON for Scenario 1 ---
    print("\n📋 JSON OUTPUT (Scenario 1 — for GitHub Pages):")
    print(to_json(result1))
