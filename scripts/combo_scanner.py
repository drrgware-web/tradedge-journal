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

    # ── Check LAST bar for combo signals ──
    last = n - 1
    signals = []

    is_pp = last in pp_flags
    is_bo = last in bo_flags and bo_flags[last] == "bull"
    is_bd = last in bo_flags and bo_flags[last] == "bear"
    is_pb = last in pb_flags

    if is_pp and is_bo:
        signals.append("BO+PPV")
    if is_pp and is_pb:
        signals.append("PB+PPV")

    # Cloud direction
    wl = walking[last]
    rl = running[last]
    bull_cloud = (rl or 0) > (wl or 0)
    cloud_width = abs((wl or 0) - (rl or 0))

    # Build result
    bar = ohlc[last]
    result = {
        "signals": signals,
        "fusion": fusion_states[last],
        "cloud": "BULL" if bull_cloud else "BEAR",
        "close": round(bar["close"], 2),
        "volume": bar["volume"],
        "change_pct": round((bar["close"] - ohlc[last - 1]["close"]) / ohlc[last - 1]["close"] * 100, 2) if last > 0 else 0,
        "walking": round(wl, 2) if wl else None,
        "running": round(rl, 2) if rl else None,
        "cloud_width": round(cloud_width, 2),
        "rsi": round(rsi[last], 1) if rsi[last] else None,
        "adx": round(adx_line[last], 1) if adx_line[last] else None,
        "bbw": round(bbw[last], 1) if bbw[last] else None,
        "is_pp": is_pp,
        "is_bo": is_bo,
        "is_bd": is_bd,
        "is_pb": is_pb,
        "dc_upper": round(dc_upper[last], 2) if dc_upper[last] else None,
        "dc_lower": round(dc_lower[last], 2) if dc_lower[last] else None,
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

    bo_ppv = [h for h in hits if "BO+PPV" in h["signals"]]
    pb_ppv = [h for h in hits if "PB+PPV" in h["signals"]]

    if bo_ppv:
        lines.append("━━ <b>BO + PPV (Breakout + Pocket Pivot)</b> ━━")
        for h in bo_ppv:
            emoji = "🟢" if h["cloud"] == "BULL" else "🔴"
            lines.append(
                f"{emoji} <b>{h['symbol']}</b> ₹{h['close']} "
                f"({h['change_pct']:+.1f}%) · {h['fusion']} · {h['cloud']}"
            )
            lines.append(
                f"   RSI {h.get('rsi','—')} · ADX {h.get('adx','—')} · "
                f"BBW {h.get('bbw','—')}% · DC↑ ₹{h.get('dc_upper','—')}"
            )
        lines.append("")

    if pb_ppv:
        lines.append("━━ <b>PB + PPV (Pullback + Pocket Pivot)</b> ━━")
        for h in pb_ppv:
            emoji = "🟢" if h["cloud"] == "BULL" else "🔴"
            lines.append(
                f"{emoji} <b>{h['symbol']}</b> ₹{h['close']} "
                f"({h['change_pct']:+.1f}%) · {h['fusion']} · {h['cloud']}"
            )
            lines.append(
                f"   RSI {h.get('rsi','—')} · ADX {h.get('adx','—')} · "
                f"WL ₹{h.get('walking','—')} · RL ₹{h.get('running','—')}"
            )
        lines.append("")

    if not bo_ppv and not pb_ppv:
        lines.append("No combo signals found in this scan.")

    lines.append(f"Scanned {len(hits)} stocks" if not bo_ppv and not pb_ppv
                 else f"✅ {len(bo_ppv)} BO+PPV · {len(pb_ppv)} PB+PPV")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════════════════
# MAIN SCANNER
# ════════════════════════════════════════════════════════════════════════════

def run_scanner(mode: str = "top500", symbol: str = None, notify: bool = False):
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
        print(f"DC Upper: ₹{result['dc_upper']} | DC Lower: ₹{result['dc_lower']}")
        print(f"\nPP: {'✅' if result['is_pp'] else '—'} | BO: {'✅' if result['is_bo'] else '—'} | "
              f"BD: {'✅' if result['is_bd'] else '—'} | PB: {'✅' if result['is_pb'] else '—'}")
        print(f"\n🔵 COMBO SIGNALS: {result['signals'] if result['signals'] else 'None'}")
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
                    hits.append(result)
                    sig_str = " + ".join(result["signals"])
                    print(f"  🔵 {sym:>15s} │ {sig_str:12s} │ ₹{result['close']:>8.2f} │ "
                          f"{result['change_pct']:+5.1f}% │ {result['fusion']:8s} │ {result['cloud']:4s}")

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

    if bo_ppv:
        print(f"\n🔵 BO + PPV ({len(bo_ppv)}):")
        for h in bo_ppv:
            print(f"   {h['symbol']:>15s} ₹{h['close']:>8.2f} {h['change_pct']:+5.1f}% "
                  f"│ {h['fusion']:8s} {h['cloud']:4s} │ RSI {h.get('rsi','—')} ADX {h.get('adx','—')}")

    if pb_ppv:
        print(f"\n🔵 PB + PPV ({len(pb_ppv)}):")
        for h in pb_ppv:
            print(f"   {h['symbol']:>15s} ₹{h['close']:>8.2f} {h['change_pct']:+5.1f}% "
                  f"│ {h['fusion']:8s} {h['cloud']:4s} │ WL ₹{h.get('walking','—')} RL ₹{h.get('running','—')}")

    if not hits:
        print("\n   No combo signals found today.")

    # ── Save JSON ──
    output = {
        "generated_at": datetime.now().isoformat(),
        "mode": mode,
        "scanned": scanned,
        "errors": errors,
        "total_hits": len(hits),
        "bo_ppv_count": len(bo_ppv),
        "pb_ppv_count": len(pb_ppv),
        "hits": hits,
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
    args = parser.parse_args()

    run_scanner(mode=args.mode, symbol=args.symbol, notify=args.notify)
