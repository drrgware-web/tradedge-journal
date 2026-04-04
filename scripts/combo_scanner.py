#!/usr/bin/env python3
"""
EdgeCloud Combo Scanner v1.0
================================
Scans NSE stock universe for two high-probability combo signals:

  1. BO + PPV  →  Donchian Channel Breakout with Pocket Pivot Volume
  2. PB + PPV  →  Cloud Pullback with Pocket Pivot Volume

These combos correspond to the blue price bars in TEChart.

Logic ported 1:1 from techart.html EdgeCloud JS:
  - SuperTrend(10,3) → Walking Line
  - EMA(21) → Running Line
  - Cloud = zone between WL and RL
  - Donchian Channel(20) breakout detection
  - Pullback into cloud detection
  - Pocket Pivot Volume detection (vol > max down-day vol in last 10 bars)

Usage:
  python combo_scanner.py                       # Scan top 500
  python combo_scanner.py --mode full           # Scan all ~3000 stocks
  python combo_scanner.py --mode nifty100       # Scan NIFTY 100 only
  python combo_scanner.py --mode test           # First 20 stocks
  python combo_scanner.py --symbol BLISSGVS     # Single stock debug
  python combo_scanner.py --notify              # Send Telegram alerts
"""

import json
import os
import sys
import time
import argparse
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

# ════════════════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════════════════

YAHOO_WORKER = "https://spring-fire-41a0.drrgware.workers.dev"
TELEGRAM_BOT = "8659936599:AAFKV6MKfHOSJKKTVqISJI-SwQ_cerTaAbQ"
TELEGRAM_CHAT = "183752078"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")
NSE_SYMBOLS_PATH = os.path.join(SCRIPT_DIR, "nse_symbols.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "combo_scanner.json")

YAHOO_DELAY = 0.12  # 120ms between requests
MAX_RETRIES = 2
BATCH_SIZE = 50
MIN_BARS = 40  # Need at least 40 bars for indicators
LOW_RISK_THRESHOLD = 5.0  # Default SL threshold for low/high risk split

# EdgeCloud defaults (match techart.html EC_DEFAULTS)
EC = {
    "st_period": 10, "st_mult": 3,
    "ema_period": 21,
    "dc_period": 20,
    "pp_lookback": 10,
}

NIFTY_100 = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
    "SBIN", "BHARTIARTL", "ITC", "KOTAKBANK", "LT", "AXISBANK",
    "HCLTECH", "ASIANPAINT", "MARUTI", "SUNPHARMA", "TITAN", "WIPRO",
    "ULTRACEMCO", "BAJFINANCE", "NESTLEIND", "TECHM", "DMART", "NTPC",
    "TATAMOTORS", "POWERGRID", "ONGC", "TATASTEEL", "M&M", "JSWSTEEL",
    "BAJAJFINSV", "ADANIENT", "ADANIPORTS", "COALINDIA", "HINDALCO",
    "GRASIM", "CIPLA", "DRREDDY", "EICHERMOT", "BRITANNIA", "DIVISLAB",
    "APOLLOHOSP", "SBILIFE", "BAJAJ-AUTO", "HDFCLIFE", "INDUSINDBK",
    "HEROMOTOCO", "DABUR", "SHREECEM", "TATACONSUM", "ADANIGREEN",
]


# ════════════════════════════════════════════════════════════════════════════
# INDICATORS — ported 1:1 from techart.html JS
# ════════════════════════════════════════════════════════════════════════════

def calc_ema(data: List[float], period: int) -> List[Optional[float]]:
    k = 2.0 / (period + 1)
    out = []
    prev = None
    for v in data:
        if v is None:
            out.append(prev)
            continue
        prev = v if prev is None else v * k + prev * (1 - k)
        out.append(prev)
    return out


def calc_atr(ohlc: List[dict], period: int) -> List[Optional[float]]:
    tr = []
    for i, d in enumerate(ohlc):
        h, l = d["high"], d["low"]
        pc = ohlc[i - 1]["close"] if i > 0 else l
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))
    out = []
    s = 0.0
    for i in range(len(tr)):
        s += tr[i]
        if i >= period:
            s -= tr[i - period]
            out.append(s / period)
        elif i == period - 1:
            out.append(s / period)
        else:
            out.append(None)
    return out


def calc_supertrend(ohlc: List[dict], period: int, mult: float):
    n = len(ohlc)
    st = [None] * n
    dirn = [1] * n
    atr = calc_atr(ohlc, period)
    prev_up = 0.0
    prev_dn = 0.0
    prev_dir = 1
    for i in range(n):
        if atr[i] is None:
            continue
        mid = (ohlc[i]["high"] + ohlc[i]["low"]) / 2
        up = mid - mult * atr[i]
        dn = mid + mult * atr[i]
        if prev_up > 0:
            up = max(up, prev_up)
        if prev_dn > 0:
            dn = min(dn, prev_dn)
        d2 = 1 if ohlc[i]["close"] > prev_dn else (-1 if ohlc[i]["close"] < prev_up else prev_dir)
        st[i] = up if d2 == 1 else dn
        dirn[i] = d2
        prev_up, prev_dn, prev_dir = up, dn, d2
    return st, dirn


def calc_rsi(closes: List[float], period: int) -> List[Optional[float]]:
    out = [None] * len(closes)
    avg_g = 0.0
    avg_l = 0.0
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        g = d if d > 0 else 0
        l = -d if d < 0 else 0
        if i <= period:
            avg_g += g / period
            avg_l += l / period
            if i == period:
                out[i] = 100.0 if avg_l == 0 else 100.0 - 100.0 / (1 + avg_g / avg_l)
        else:
            avg_g = (avg_g * (period - 1) + g) / period
            avg_l = (avg_l * (period - 1) + l) / period
            out[i] = 100.0 if avg_l == 0 else 100.0 - 100.0 / (1 + avg_g / avg_l)
    return out


def calc_sma(data: List[float], period: int) -> List[Optional[float]]:
    out = []
    s = 0.0
    for i in range(len(data)):
        s += data[i] if data[i] else 0
        if i >= period:
            s -= data[i - period] if data[i - period] else 0
        out.append(s / period if i >= period - 1 else None)
    return out


def calc_bb_width(closes: List[float], period: int) -> List[Optional[float]]:
    sma = calc_sma(closes, period)
    out = []
    for i in range(len(closes)):
        if sma[i] is None or i < period - 1:
            out.append(None)
            continue
        s2 = sum((closes[j] - sma[i]) ** 2 for j in range(i - period + 1, i + 1))
        std = (s2 / period) ** 0.5
        upper = sma[i] + 2 * std
        lower = sma[i] - 2 * std
        out.append((upper - lower) / sma[i] * 100 if sma[i] > 0 else 0)
    return out


def calc_adx(ohlc: List[dict], period: int):
    n = len(ohlc)
    dm_p = [0.0]
    dm_m = [0.0]
    for i in range(1, n):
        up_m = ohlc[i]["high"] - ohlc[i - 1]["high"]
        dn_m = ohlc[i - 1]["low"] - ohlc[i]["low"]
        dm_p.append(up_m if up_m > dn_m and up_m > 0 else 0)
        dm_m.append(dn_m if dn_m > up_m and dn_m > 0 else 0)
    atr = calc_atr(ohlc, period)
    sm_dm_p = calc_ema(dm_p, period)
    sm_dm_m = calc_ema(dm_m, period)
    sm_atr = calc_ema([v or 0 for v in atr], period)
    dx = []
    di_p = [None] * n
    di_m = [None] * n
    for i in range(n):
        if sm_atr[i] is None or sm_atr[i] == 0:
            dx.append(None)
            continue
        dp = sm_dm_p[i] / sm_atr[i] * 100
        dm = sm_dm_m[i] / sm_atr[i] * 100
        di_p[i] = dp
        di_m[i] = dm
        s = dp + dm
        dx.append(0 if s == 0 else abs(dp - dm) / s * 100)
    adx_line = calc_ema([v or 0 for v in dx], period)
    return adx_line, di_p, di_m


def calc_fusion_state(bbw, rsi, adx, di_p, di_m, adx_rising):
    if bbw is None or rsi is None or adx is None:
        return "DRIFT"
    if bbw < 10 and rsi > 55 and adx_rising:
        return "IGNITION"
    if bbw >= 10 and rsi > 55 and adx > 25 and (di_p or 0) > (di_m or 0):
        return "THRUST"
    if bbw >= 10 and rsi < 45 and adx > 25 and (di_m or 0) > (di_p or 0):
        return "FADE"
    if bbw < 10 and adx < 20:
        return "COIL"
    return "DRIFT"


# ════════════════════════════════════════════════════════════════════════════
# COMBO SIGNAL DETECTION
# ════════════════════════════════════════════════════════════════════════════

def detect_combos(ohlc: List[dict]) -> dict:
    """
    Run full EdgeCloud analysis on OHLCV data.
    Returns dict with combo signals for the LAST bar (today).
    """
    n = len(ohlc)
    if n < MIN_BARS:
        return {"signals": [], "error": f"Only {n} bars"}

    closes = [d["close"] for d in ohlc]

    # ── EdgeCloud ──
    walking, walk_dir = calc_supertrend(ohlc, EC["st_period"], EC["st_mult"])
    running = calc_ema(closes, EC["ema_period"])

    # ── Trend Structure (Minervini Stage 2 filters) ──
    ema50 = calc_ema(closes, 50)
    ema200 = calc_ema(closes, 200)

    # ── Fusion state (for filtering) ──
    bbw = calc_bb_width(closes, 20)
    rsi = calc_rsi(closes, 14)
    adx_line, di_p, di_m = calc_adx(ohlc, 14)
    fusion_states = []
    for i in range(n):
        adx_rising = i >= 3 and adx_line[i] is not None and adx_line[i - 3] is not None and adx_line[i] > adx_line[i - 3]
        fusion_states.append(calc_fusion_state(bbw[i], rsi[i], adx_line[i], di_p[i], di_m[i], adx_rising))

    # ── Pocket Pivot Volume ──
    pp_flags = set()
    for i in range(2, n):
        prev_c = ohlc[i - 1]["close"]
        if ohlc[i]["close"] < prev_c:
            continue  # Must be an up-close bar
        max_dv = 0
        for j in range(max(0, i - EC["pp_lookback"]), i):
            pc = ohlc[j - 1]["close"] if j > 0 else ohlc[j]["close"]
            if ohlc[j]["close"] < pc and ohlc[j]["volume"] > max_dv:
                max_dv = ohlc[j]["volume"]
        if max_dv > 0 and ohlc[i]["volume"] > max_dv:
            pp_flags.add(i)

    # ── Donchian Channel(20) breakout ──
    dc_period = EC["dc_period"]
    dc_upper = [None] * n
    dc_lower = [None] * n
    for i in range(n):
        if i < dc_period - 1:
            continue
        hh = max(ohlc[j]["high"] for j in range(i - dc_period + 1, i + 1))
        ll = min(ohlc[j]["low"] for j in range(i - dc_period + 1, i + 1))
        dc_upper[i] = hh
        dc_lower[i] = ll

    bo_flags = {}  # idx -> 'bull' or 'bear'
    in_bull_bo = False
    in_bear_bo = False
    for i in range(dc_period, n):
        prev_upper = dc_upper[i - 1]
        prev_lower = dc_lower[i - 1]
        if prev_upper is not None and ohlc[i]["close"] > prev_upper and not in_bull_bo:
            bo_flags[i] = "bull"
            in_bull_bo = True
            in_bear_bo = False
        elif prev_lower is not None and ohlc[i]["close"] < prev_lower and not in_bear_bo:
            bo_flags[i] = "bear"
            in_bear_bo = True
            in_bull_bo = False
        else:
            if prev_upper is not None and ohlc[i]["close"] <= prev_upper:
                in_bull_bo = False
            if prev_lower is not None and ohlc[i]["close"] >= prev_lower:
                in_bear_bo = False

    # ── Pullback detection ──
    pb_flags = set()
    for i in range(3, n):
        if walking[i] is None or running[i] is None:
            continue
        cloud_top = max(walking[i], running[i])
        cloud_bot = min(walking[i], running[i])
        bull_cloud = running[i] > walking[i]  # RL > WL = bullish
        fs = fusion_states[i]
        if fs == "FADE":
            continue  # skip counter-trend

        if bull_cloud:
            was_above = any(
                ohlc[j]["close"] > max(walking[j] or 0, running[j] or 0)
                for j in range(max(0, i - 5), i)
            )
            if (was_above and ohlc[i]["low"] <= cloud_top
                    and ohlc[i]["close"] >= cloud_bot
                    and ohlc[i]["close"] > ohlc[i]["open"]):
                pb_flags.add(i)
        else:
            was_below = any(
                ohlc[j]["close"] < min(
                    walking[j] if walking[j] is not None else float("inf"),
                    running[j] if running[j] is not None else float("inf"),
                )
                for j in range(max(0, i - 5), i)
            )
            if (was_below and ohlc[i]["high"] >= cloud_bot
                    and ohlc[i]["close"] <= cloud_top
                    and ohlc[i]["close"] < ohlc[i]["open"]):
                pb_flags.add(i)

    # ── ATR for risk calculation ──
    atr = calc_atr(ohlc, 14)

    # ── Check LAST bar for combo signals ──
    last = n - 1
    signals = []

    # ═══ MANDATORY TREND FILTERS (Minervini Stage 2) ═══
    # These MUST pass for any signal to be generated:
    #   1. EMA(50) > EMA(200)  — long-term trend is UP
    #   2. Price > EMA(50)     — price is above the trend
    #   3. Fusion is NOT FADE  — not in bearish pressure
    e50 = ema50[last]
    e200 = ema200[last]
    close_price = ohlc[last]["close"]
    fusion_last = fusion_states[last]

    trend_ok = (
        e50 is not None and e200 is not None
        and e50 > e200                      # EMA 50 above EMA 200
        and close_price > e50               # Price above EMA 50
        and fusion_last != "FADE"           # Not in bearish FADE
    )

    # ═══ EXTENSION / CLIMAX FILTERS ═══
    # Reject stocks that are over-extended (chasing territory):
    #
    #   1. Price > 25% above EMA(50) → too stretched, pullback likely
    #   2. Price > 50% above EMA(200) → climax territory
    #   3. RSI > 80 → overbought exhaustion
    #   4. ATR sell ratio > 3× → today's range is 3× normal = climax day
    #   5. 20-day return > 40% → parabolic move, not a base breakout

    extension_pct_50 = ((close_price - e50) / e50 * 100) if e50 and e50 > 0 else 0
    extension_pct_200 = ((close_price - e200) / e200 * 100) if e200 and e200 > 0 else 0
    rsi_last = rsi[last] or 50

    # 20-day return
    lookback_20 = min(20, last)
    ret_20d = ((close_price - ohlc[last - lookback_20]["close"]) / ohlc[last - lookback_20]["close"] * 100) if lookback_20 > 0 else 0

    # ATR sell ratio (today's range vs ATR)
    atr_val_check = atr[last] if atr[last] else close_price * 0.02
    today_range = ohlc[last]["high"] - ohlc[last]["low"]
    atr_sell_ratio = today_range / atr_val_check if atr_val_check > 0 else 1

    not_extended = (
        extension_pct_50 <= 25              # Not > 25% above EMA50
        and extension_pct_200 <= 50         # Not > 50% above EMA200
        and rsi_last <= 80                  # Not overbought exhaustion
        and atr_sell_ratio <= 3.0           # Not a climax range day
        and ret_20d <= 40                   # Not parabolic in last 20 days
    )

    # Extension warning flags (for display even if not filtered)
    ext_warnings = []
    if extension_pct_50 > 25:
        ext_warnings.append(f">{extension_pct_50:.0f}% above EMA50")
    if extension_pct_200 > 50:
        ext_warnings.append(f">{extension_pct_200:.0f}% above EMA200")
    if rsi_last > 80:
        ext_warnings.append(f"RSI {rsi_last:.0f} overbought")
    if atr_sell_ratio > 3.0:
        ext_warnings.append(f"ATR ratio {atr_sell_ratio:.1f}× climax")
    if ret_20d > 40:
        ext_warnings.append(f"+{ret_20d:.0f}% in 20 days")

    is_pp = last in pp_flags
    is_bo = last in bo_flags and bo_flags[last] == "bull"
    is_bd = last in bo_flags and bo_flags[last] == "bear"
    is_pb = last in pb_flags

    # Only generate signals if BOTH trend and extension filters pass
    if trend_ok and not_extended:
        if is_pp and is_bo:
            signals.append("BO+PPV")
        if is_pp and is_pb:
            signals.append("PB+PPV")

    # Cloud direction
    wl = walking[last]
    rl = running[last]
    bull_cloud = (rl or 0) > (wl or 0)
    cloud_width = abs((wl or 0) - (rl or 0))

    bar = ohlc[last]
    close = bar["close"]
    atr_val = atr[last] if atr[last] else close * 0.02

    # ═══ MULTI-STRATEGY SL & RISK CALCULATION ═══
    #
    # Strategy 1: WL SL — Stop below Walking Line (SuperTrend)
    #   SL = WL - 0.5 × ATR
    #   Best for: Pullback entries near the cloud
    #
    # Strategy 2: Cloud Bottom SL — Stop below the cloud zone
    #   SL = min(WL, RL) - 0.5 × ATR
    #   Best for: Breakout entries, wider stop
    #
    # Strategy 3: Swing Low SL — Stop below recent 5-bar low
    #   SL = min(low of last 5 bars) - 0.25 × ATR
    #   Best for: Tight mechanical stop
    #
    # Strategy 4: ATR SL — Fixed 2× ATR below close
    #   SL = Close - 2 × ATR
    #   Best for: Volatility-adjusted universal stop

    sl_strategies = {}

    # Strategy 1: Walking Line SL
    if wl and wl > 0:
        sl_wl = wl - 0.5 * atr_val
        risk_wl = (close - sl_wl) / close * 100 if close > sl_wl else 99
        sl_strategies["wl"] = {
            "sl": round(sl_wl, 2),
            "risk_pct": round(risk_wl, 2),
            "label": "Walking Line",
        }

    # Strategy 2: Cloud Bottom SL
    cloud_bot = min(wl or close, rl or close)
    sl_cloud = cloud_bot - 0.5 * atr_val
    risk_cloud = (close - sl_cloud) / close * 100 if close > sl_cloud else 99
    sl_strategies["cloud"] = {
        "sl": round(sl_cloud, 2),
        "risk_pct": round(risk_cloud, 2),
        "label": "Cloud Bottom",
    }

    # Strategy 3: Swing Low SL (5-bar low)
    lookback = min(5, last)
    swing_low = min(ohlc[j]["low"] for j in range(last - lookback, last + 1))
    sl_swing = swing_low - 0.25 * atr_val
    risk_swing = (close - sl_swing) / close * 100 if close > sl_swing else 99
    sl_strategies["swing"] = {
        "sl": round(sl_swing, 2),
        "risk_pct": round(risk_swing, 2),
        "label": "Swing Low (5)",
    }

    # Strategy 4: ATR SL (2× ATR)
    sl_atr = close - 2 * atr_val
    risk_atr = (close - sl_atr) / close * 100 if close > sl_atr else 99
    sl_strategies["atr2x"] = {
        "sl": round(sl_atr, 2),
        "risk_pct": round(risk_atr, 2),
        "label": "2× ATR",
    }

    # ── Best SL: lowest risk that's still valid (> 0.5%) ──
    valid_strategies = {k: v for k, v in sl_strategies.items() if 0.5 <= v["risk_pct"] <= 15}
    if valid_strategies:
        best_key = min(valid_strategies, key=lambda k: valid_strategies[k]["risk_pct"])
        best_sl = valid_strategies[best_key]
    else:
        best_key = "atr2x"
        best_sl = sl_strategies["atr2x"]

    # ── Is this a LOW RISK entry? ──
    is_low_risk = best_sl["risk_pct"] <= LOW_RISK_THRESHOLD

    # ── R:R Ratio (reward to risk) ──
    # T1 = 2× ATR above close, T2 = 4× ATR
    t1 = close + 2 * atr_val
    t2 = close + 4 * atr_val
    t3 = close + 6 * atr_val
    risk_amt = close - best_sl["sl"]
    rr_t1 = (t1 - close) / risk_amt if risk_amt > 0 else 0
    rr_t2 = (t2 - close) / risk_amt if risk_amt > 0 else 0

    # ── Entry Quality Grade ──
    # A+ = Low risk + IGNITION/THRUST + Bull cloud + RSI > 50
    # A  = Low risk + Bull cloud + RSI > 45
    # B  = Low risk but neutral fusion/cloud
    # C  = Higher risk or bearish
    grade = "C"
    fs = fusion_states[last]
    rsi_val = rsi[last] or 50
    if is_low_risk and bull_cloud and fs in ("IGNITION", "THRUST") and rsi_val > 50:
        grade = "A+"
    elif is_low_risk and bull_cloud and rsi_val > 45:
        grade = "A"
    elif is_low_risk:
        grade = "B"

    # Build result
    result = {
        "signals": signals,
        "fusion": fs,
        "cloud": "BULL" if bull_cloud else "BEAR",
        "close": round(close, 2),
        "volume": bar["volume"],
        "change_pct": round((close - ohlc[last - 1]["close"]) / ohlc[last - 1]["close"] * 100, 2) if last > 0 else 0,
        "walking": round(wl, 2) if wl else None,
        "running": round(rl, 2) if rl else None,
        "cloud_width": round(cloud_width, 2),
        "rsi": round(rsi_val, 1),
        "adx": round(adx_line[last], 1) if adx_line[last] else None,
        "bbw": round(bbw[last], 1) if bbw[last] else None,
        "atr": round(atr_val, 2),
        "is_pp": is_pp,
        "is_bo": is_bo,
        "is_bd": is_bd,
        "is_pb": is_pb,
        "dc_upper": round(dc_upper[last], 2) if dc_upper[last] else None,
        "dc_lower": round(dc_lower[last], 2) if dc_lower[last] else None,
        # ── Trend filters ──
        "ema50": round(e50, 2) if e50 else None,
        "ema200": round(e200, 2) if e200 else None,
        "ema50_above_200": bool(e50 and e200 and e50 > e200),
        "price_above_ema50": bool(e50 and close_price > e50),
        "trend_ok": trend_ok,
        # ── Extension filters ──
        "ext_pct_50": round(extension_pct_50, 1),
        "ext_pct_200": round(extension_pct_200, 1),
        "ret_20d": round(ret_20d, 1),
        "atr_sell_ratio": round(atr_sell_ratio, 1),
        "not_extended": not_extended,
        "ext_warnings": ext_warnings,
        # ── Risk fields ──
        "sl_strategies": sl_strategies,
        "best_sl": best_sl["sl"],
        "best_sl_strategy": best_sl["label"],
        "risk_pct": best_sl["risk_pct"],
        "is_low_risk": is_low_risk,
        "t1": round(t1, 2),
        "t2": round(t2, 2),
        "t3": round(t3, 2),
        "rr_t1": round(rr_t1, 1),
        "rr_t2": round(rr_t2, 1),
        "grade": grade,
    }
    return result


# ════════════════════════════════════════════════════════════════════════════
# DATA FETCHING
# ════════════════════════════════════════════════════════════════════════════

def fetch_ohlcv(symbol: str, session: requests.Session) -> Optional[List[dict]]:
    """Fetch 6 months daily OHLCV via Cloudflare Worker."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.post(
                YAHOO_WORKER,
                headers={
                    "Content-Type": "application/json",
                    "X-Kite-Action": "yahoo-proxy",
                },
                json={
                    "ticker": f"{symbol}.NS",
                    "range": "6mo",
                    "interval": "1d",
                    "_t": int(time.time() * 1000),
                },
                timeout=15,
            )
            if resp.status_code != 200:
                continue

            data = resp.json()
            result = data.get("chart", {}).get("result", [None])[0]
            if not result:
                continue

            ts = result.get("timestamp", [])
            q = (result.get("indicators", {}).get("quote", [{}]))[0]
            if not ts or not q:
                continue

            ohlc = []
            seen = set()
            for i in range(len(ts)):
                if q.get("close", [None])[i] is None:
                    continue
                from datetime import date as dt_date
                d = datetime.utcfromtimestamp(ts[i])
                key = d.strftime("%Y-%m-%d")
                if key in seen:
                    continue
                seen.add(key)
                ohlc.append({
                    "date": key,
                    "open": q["open"][i] or q["close"][i],
                    "high": q["high"][i] or q["close"][i],
                    "low": q["low"][i] or q["close"][i],
                    "close": q["close"][i],
                    "volume": (q.get("volume") or [0])[i] or 0,
                })
            return ohlc if len(ohlc) >= MIN_BARS else None

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(0.5)
            continue
    return None


# ════════════════════════════════════════════════════════════════════════════
# SYMBOL LOADING
# ════════════════════════════════════════════════════════════════════════════

def load_symbols() -> List[str]:
    if os.path.exists(NSE_SYMBOLS_PATH):
        with open(NSE_SYMBOLS_PATH) as f:
            data = json.load(f)
            if isinstance(data, list):
                return [s["symbol"] if isinstance(s, dict) else s for s in data]
            elif isinstance(data, dict):
                return list(data.keys())
    return []


def get_scan_universe(mode: str, all_symbols: List[str]) -> List[str]:
    if mode == "nifty100":
        return [s for s in NIFTY_100 if s in all_symbols]
    elif mode == "test":
        return all_symbols[:20]
    elif mode == "full":
        return all_symbols
    else:  # top500
        # Prioritize NIFTY 100 + fill to 500
        priority = [s for s in NIFTY_100 if s in all_symbols]
        remaining = [s for s in all_symbols if s not in priority][:500 - len(priority)]
        return priority + remaining


# ════════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ════════════════════════════════════════════════════════════════════════════

def send_telegram(message: str):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT,
                "text": message,
                "parse_mode": "HTML",
            },
            timeout=10,
        )
    except Exception as e:
        print(f"Telegram error: {e}")


def format_telegram_alert(hits: List[dict]) -> str:
    now = datetime.now().strftime("%H:%M IST")
    lines = [f"🔵 <b>EdgeCloud Combo Scanner</b> — {now}\n"]

    # Split into risk groups
    low_risk = [h for h in hits if h.get("risk_pct", 99) <= LOW_RISK_THRESHOLD]
    high_risk = [h for h in hits if h.get("risk_pct", 99) > LOW_RISK_THRESHOLD]

    # ── Section 1: LOW RISK ──
    if low_risk:
        lines.append(f"🟢 <b>LOW RISK (SL ≤ {LOW_RISK_THRESHOLD}%) — {len(low_risk)} stocks</b>")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        for h in sorted(low_risk, key=lambda x: x.get("risk_pct", 99)):
            sigs = " + ".join(h["signals"])
            lines.append(
                f"⭐ <b>{h['symbol']}</b> ₹{h['close']} ({h['change_pct']:+.1f}%)"
                f" · {sigs} · {h['fusion']} · {h['cloud']} · <b>{h.get('grade','—')}</b>"
            )
            lines.append(
                f"   🟢 SL ₹{h.get('best_sl','—')} ({h.get('best_sl_strategy','—')})"
                f" · Risk <b>{h.get('risk_pct','—')}%</b> · R:R 1:{h.get('rr_t1','—')}"
            )
            lines.append(
                f"   T1 ₹{h.get('t1','—')} · T2 ₹{h.get('t2','—')}"
                f" · RSI {h.get('rsi','—')} · ADX {h.get('adx','—')}"
            )
        lines.append("")

    # ── Section 2: HIGHER RISK ──
    if high_risk:
        lines.append(f"🟡 <b>HIGHER RISK (SL > {LOW_RISK_THRESHOLD}%) — {len(high_risk)} stocks</b>")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        for h in sorted(high_risk, key=lambda x: x.get("risk_pct", 99)):
            sigs = " + ".join(h["signals"])
            risk_emoji = "🟡" if h.get("risk_pct", 99) <= 8 else "🔴"
            lines.append(
                f"● <b>{h['symbol']}</b> ₹{h['close']} ({h['change_pct']:+.1f}%)"
                f" · {sigs} · {h['fusion']} · {h['cloud']} · {h.get('grade','—')}"
            )
            lines.append(
                f"   {risk_emoji} SL ₹{h.get('best_sl','—')} ({h.get('best_sl_strategy','—')})"
                f" · Risk <b>{h.get('risk_pct','—')}%</b> · R:R 1:{h.get('rr_t1','—')}"
            )
        lines.append("")

    if not hits:
        lines.append("No combo signals found in this scan.")

    lines.append(f"✅ Total: {len(hits)} · 🟢 Low risk: {len(low_risk)} · 🟡 Higher risk: {len(high_risk)}")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════════════════
# MAIN SCANNER
# ════════════════════════════════════════════════════════════════════════════

def run_scanner(mode: str = "top500", symbol: str = None, notify: bool = False, low_risk_only: bool = False):
    start = time.time()
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
    })

    # Single stock mode
    if symbol:
        print(f"\n═══ Single Stock: {symbol} ═══")
        ohlc = fetch_ohlcv(symbol, session)
        if not ohlc:
            print(f"❌ No data for {symbol}")
            return
        result = detect_combos(ohlc)
        print(f"\nBars: {len(ohlc)} | Last: {ohlc[-1]['date']}")
        print(f"Close: ₹{result['close']} | Change: {result['change_pct']:+.1f}%")
        print(f"Fusion: {result['fusion']} | Cloud: {result['cloud']}")
        print(f"WL: ₹{result['walking']} | RL: ₹{result['running']} | Cloud Width: ₹{result['cloud_width']}")
        print(f"RSI: {result['rsi']} | ADX: {result['adx']} | BBW: {result['bbw']}%")
        print(f"ATR(14): ₹{result.get('atr','—')} | DC Upper: ₹{result['dc_upper']} | DC Lower: ₹{result['dc_lower']}")
        print(f"\n📈 TREND FILTERS (Minervini Stage 2):")
        print(f"  EMA(50): ₹{result.get('ema50','—')} | EMA(200): ₹{result.get('ema200','—')}")
        print(f"  EMA50 > EMA200: {'✅' if result.get('ema50_above_200') else '❌'}")
        print(f"  Price > EMA50:  {'✅' if result.get('price_above_ema50') else '❌'}")
        print(f"  Fusion ≠ FADE:  {'✅' if result['fusion'] != 'FADE' else '❌'}")
        print(f"  TREND OK:       {'✅ PASS' if result.get('trend_ok') else '❌ FAIL — no signals generated'}")
        print(f"\n🔥 EXTENSION / CLIMAX CHECK:")
        print(f"  Price vs EMA50:  {result.get('ext_pct_50',0):+.1f}% {'⚠ EXTENDED' if result.get('ext_pct_50',0) > 25 else '✅ OK'}")
        print(f"  Price vs EMA200: {result.get('ext_pct_200',0):+.1f}% {'⚠ CLIMAX' if result.get('ext_pct_200',0) > 50 else '✅ OK'}")
        print(f"  RSI:             {result.get('rsi',0):.0f} {'⚠ OVERBOUGHT' if result.get('rsi',0) > 80 else '✅ OK'}")
        print(f"  ATR Sell Ratio:  {result.get('atr_sell_ratio',0):.1f}× {'⚠ CLIMAX DAY' if result.get('atr_sell_ratio',0) > 3 else '✅ OK'}")
        print(f"  20-Day Return:   {result.get('ret_20d',0):+.1f}% {'⚠ PARABOLIC' if result.get('ret_20d',0) > 40 else '✅ OK'}")
        print(f"  NOT EXTENDED:    {'✅ PASS' if result.get('not_extended') else '❌ FAIL — ' + ', '.join(result.get('ext_warnings', []))}")
        print(f"\nPP: {'✅' if result['is_pp'] else '—'} | BO: {'✅' if result['is_bo'] else '—'} | "
              f"BD: {'✅' if result['is_bd'] else '—'} | PB: {'✅' if result['is_pb'] else '—'}")
        print(f"\n🔵 COMBO SIGNALS: {result['signals'] if result['signals'] else 'None'}")
        
        # Risk analysis
        print(f"\n{'═' * 50}")
        print(f"📊 RISK ANALYSIS")
        print(f"{'─' * 50}")
        for key, strat in result.get('sl_strategies', {}).items():
            risk = strat['risk_pct']
            emoji = '🟢' if risk <= 5 else '🟡' if risk <= 8 else '🔴'
            best = ' ← BEST' if strat['sl'] == result.get('best_sl') else ''
            print(f"  {emoji} {strat['label']:15s} SL ₹{strat['sl']:>8.2f}  Risk {risk:>5.2f}%{best}")
        
        print(f"\n  Best SL: ₹{result.get('best_sl','—')} ({result.get('best_sl_strategy','—')})")
        print(f"  Risk: {result.get('risk_pct','—')}%")
        print(f"  Low Risk Entry: {'✅ YES' if result.get('is_low_risk') else '❌ NO'}")
        print(f"\n  T1: ₹{result.get('t1','—')} (R:R 1:{result.get('rr_t1','—')})")
        print(f"  T2: ₹{result.get('t2','—')} (R:R 1:{result.get('rr_t2','—')})")
        print(f"  T3: ₹{result.get('t3','—')}")
        print(f"\n  Entry Grade: {result.get('grade','—')}")
        return

    # Batch scan mode
    all_symbols = load_symbols()
    if not all_symbols:
        print("❌ No symbols loaded")
        return

    universe = get_scan_universe(mode, all_symbols)
    total = len(universe)
    print(f"\n═══ EdgeCloud Combo Scanner v1.0 ═══")
    print(f"Mode: {mode} | Universe: {total} stocks")
    print(f"Signals: BO+PPV, PB+PPV")
    print(f"Risk Filter: {'≤5% ONLY' if low_risk_only else 'All (showing risk %)'}")
    print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}\n")

    hits = []  # Stocks with combo signals
    all_results = []  # All scanned stocks (for JSON output)
    scanned = 0
    errors = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch = universe[batch_start:batch_start + BATCH_SIZE]
        for sym in batch:
            try:
                ohlc = fetch_ohlcv(sym, session)
                if not ohlc:
                    errors += 1
                    continue

                result = detect_combos(ohlc)
                result["symbol"] = sym
                result["date"] = ohlc[-1]["date"]
                all_results.append(result)

                if result["signals"]:
                    # Apply low-risk filter if enabled
                    if low_risk_only and not result.get("is_low_risk"):
                        scanned += 1
                        time.sleep(YAHOO_DELAY)
                        continue
                    
                    hits.append(result)
                    sig_str = " + ".join(result["signals"])
                    risk_emoji = "🟢" if result.get("risk_pct", 99) <= 5 else "🟡" if result.get("risk_pct", 99) <= 8 else "🔴"
                    print(f"  🔵 {sym:>15s} │ {sig_str:12s} │ ₹{result['close']:>8.2f} │ "
                          f"{result['change_pct']:+5.1f}% │ {result['fusion']:8s} │ {result['cloud']:4s} │ "
                          f"{risk_emoji} {result.get('risk_pct',99):>4.1f}% │ {result.get('grade','—')}")

                scanned += 1
                time.sleep(YAHOO_DELAY)

            except Exception as e:
                errors += 1
                traceback.print_exc()

        # Progress
        done = min(batch_start + BATCH_SIZE, total)
        pct = done / total * 100
        print(f"  [{done}/{total}] {pct:.0f}% — {len(hits)} hits, {errors} errors", flush=True)

    elapsed = time.time() - start

    # ── Summary ──
    print(f"\n{'═' * 60}")
    print(f"SCAN COMPLETE — {elapsed:.1f}s")
    print(f"Scanned: {scanned} | Errors: {errors} | Hits: {len(hits)}")

    bo_ppv = [h for h in hits if "BO+PPV" in h["signals"]]
    pb_ppv = [h for h in hits if "PB+PPV" in h["signals"]]
    low_risk_hits = [h for h in hits if h.get("risk_pct", 99) <= LOW_RISK_THRESHOLD]
    high_risk_hits = [h for h in hits if h.get("risk_pct", 99) > LOW_RISK_THRESHOLD]

    # ── Group 1: LOW RISK ──
    if low_risk_hits:
        print(f"\n🟢 LOW RISK (SL ≤ {LOW_RISK_THRESHOLD}%) — {len(low_risk_hits)} stocks:")
        print(f"   {'Symbol':>15s} │ {'Signal':12s} │ {'Close':>8s} │ {'Chg':>6s} │ {'Fusion':8s} │ {'SL':>8s} │ {'Risk':>5s} │ {'R:R':>5s} │ Grd")
        print(f"   {'─'*15} │ {'─'*12} │ {'─'*8} │ {'─'*6} │ {'─'*8} │ {'─'*8} │ {'─'*5} │ {'─'*5} │ {'─'*3}")
        for h in sorted(low_risk_hits, key=lambda x: x.get("risk_pct", 99)):
            sigs = "+".join(h["signals"])
            print(f"   {h['symbol']:>15s} │ {sigs:12s} │ ₹{h['close']:>6.0f} │ {h['change_pct']:+5.1f}% │ {h['fusion']:8s} │ "
                  f"₹{h.get('best_sl',0):>6.0f} │ {h.get('risk_pct',0):>4.1f}% │ 1:{h.get('rr_t1',0):>3.1f} │ {h.get('grade','—')}")

    # ── Group 2: HIGHER RISK ──
    if high_risk_hits:
        print(f"\n🟡 HIGHER RISK (SL > {LOW_RISK_THRESHOLD}%) — {len(high_risk_hits)} stocks:")
        print(f"   {'Symbol':>15s} │ {'Signal':12s} │ {'Close':>8s} │ {'Chg':>6s} │ {'Fusion':8s} │ {'SL':>8s} │ {'Risk':>5s} │ {'R:R':>5s} │ Grd")
        print(f"   {'─'*15} │ {'─'*12} │ {'─'*8} │ {'─'*6} │ {'─'*8} │ {'─'*8} │ {'─'*5} │ {'─'*5} │ {'─'*3}")
        for h in sorted(high_risk_hits, key=lambda x: x.get("risk_pct", 99)):
            sigs = "+".join(h["signals"])
            print(f"   {h['symbol']:>15s} │ {sigs:12s} │ ₹{h['close']:>6.0f} │ {h['change_pct']:+5.1f}% │ {h['fusion']:8s} │ "
                  f"₹{h.get('best_sl',0):>6.0f} │ {h.get('risk_pct',0):>4.1f}% │ 1:{h.get('rr_t1',0):>3.1f} │ {h.get('grade','—')}")

    if not hits:
        print("\n   No combo signals found today.")

    print(f"\n   Total: {len(hits)} │ 🟢 Low risk ≤{LOW_RISK_THRESHOLD}%: {len(low_risk_hits)} │ 🟡 Higher risk >{LOW_RISK_THRESHOLD}%: {len(high_risk_hits)}")

    # ── Save JSON ──
    output = {
        "generated_at": datetime.now().isoformat(),
        "mode": mode,
        "low_risk_only": low_risk_only,
        "scanned": scanned,
        "errors": errors,
        "total_hits": len(hits),
        "bo_ppv_count": len(bo_ppv),
        "pb_ppv_count": len(pb_ppv),
        "low_risk_count": len(low_risk_hits),
        "high_risk_count": len(high_risk_hits),
        "low_risk_hits": sorted(low_risk_hits, key=lambda x: (x.get("grade", "Z"), x.get("risk_pct", 99))),
        "high_risk_hits": sorted(high_risk_hits, key=lambda x: x.get("risk_pct", 99)),
        "hits": sorted(hits, key=lambda x: (x.get("grade", "Z"), x.get("risk_pct", 99))),
        "config": EC,
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n📄 Results saved to {OUTPUT_FILE}")

    # ── Telegram ──
    if notify and hits:
        msg = format_telegram_alert(hits)
        send_telegram(msg)
        print("📱 Telegram alert sent")

    return output


# ════════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EdgeCloud Combo Scanner")
    parser.add_argument("--mode", default="top500", choices=["full", "top500", "nifty100", "test"])
    parser.add_argument("--symbol", help="Single stock debug mode")
    parser.add_argument("--notify", action="store_true", help="Send Telegram alerts")
    parser.add_argument("--low-risk", action="store_true", dest="low_risk", help="Only show entries with ≤5%% SL risk")
    parser.add_argument("--sl", type=float, default=5.0, help="SL threshold %% for low/high risk split (default: 5.0)")
    args = parser.parse_args()

    # Override the low-risk threshold globally if --sl is set
    global LOW_RISK_THRESHOLD
    LOW_RISK_THRESHOLD = args.sl

    run_scanner(mode=args.mode, symbol=args.symbol, notify=args.notify, low_risk_only=args.low_risk)
