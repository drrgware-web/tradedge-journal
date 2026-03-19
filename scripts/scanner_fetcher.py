#!/usr/bin/env python3
"""
TradEdge Stock Scanner v1.0
===========================
Fetches all NSE stocks via yfinance, computes technical & fundamental indicators,
and outputs scan results as JSON for the TradEdge dashboard.

Scanners:
  1. RSI (Overbought/Oversold)
  2. MACD Crossover Signals
  3. EMA Crossovers (9/21, 50/200)
  4. 52-Week High/Low Breakout
  5. Volume Spike (2x+ average)
  6. Bollinger Band Squeeze/Breakout
  7. P/E Ratio Filter
  8. Market Cap Filter

Data Source: yfinance
Output: data/scanner_results.json
"""

import json
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf


# ─── Configuration ───────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
OUTPUT_FILE = DATA_DIR / "scanner_results.json"
SYMBOL_LIST_FILE = SCRIPT_DIR / "nse_symbols.json"

# yfinance settings
HISTORY_PERIOD = "1y"       # 1 year of daily data
BATCH_SIZE = 50             # stocks per batch download (yfinance handles 50 well)
BATCH_DELAY = 2.0           # seconds between batches (rate limit friendly)
MAX_RETRIES = 2             # retries per failed batch
SKIP_FUNDAMENTALS = os.environ.get("SKIP_FUNDAMENTALS", "").lower() == "true"
# Set SKIP_FUNDAMENTALS=true to skip yfinance .info calls (much faster, ~3x speed)
# Fundamentals (P/E, market cap) will be null but technicals will still work

# Technical indicator parameters
RSI_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
BB_PERIOD = 20
BB_STD = 2
VOL_AVG_PERIOD = 20
VOL_SPIKE_MULTIPLIER = 2.0
EMA_SHORT_1 = 9
EMA_LONG_1 = 21
EMA_SHORT_2 = 50
EMA_LONG_2 = 200


# ─── NSE Symbol List ────────────────────────────────────────────────────────

def get_nse_symbols() -> list[dict]:
    """
    Load NSE symbols from the JSON config file.
    The file should contain a list of dicts: [{"symbol": "RELIANCE", "name": "Reliance Industries", "sector": "Energy"}, ...]
    If the file doesn't exist, generate a starter list from Nifty 500 + common stocks.
    """
    if SYMBOL_LIST_FILE.exists():
        with open(SYMBOL_LIST_FILE) as f:
            return json.load(f)
    
    print("⚠ nse_symbols.json not found. Using built-in Nifty 500 starter list.")
    print("  Run 'python scripts/build_symbol_list.py' to generate the full list.")
    
    # Fallback: well-known NSE symbols (Nifty 50 + key mid/small caps)
    # Full list should be generated separately via build_symbol_list.py
    starter_symbols = [
        # Nifty 50
        {"symbol": "RELIANCE", "name": "Reliance Industries", "sector": "Energy"},
        {"symbol": "HDFCBANK", "name": "HDFC Bank", "sector": "Banking"},
        {"symbol": "BHARTIARTL", "name": "Bharti Airtel", "sector": "Telecom"},
        {"symbol": "SBIN", "name": "State Bank of India", "sector": "Banking"},
        {"symbol": "ICICIBANK", "name": "ICICI Bank", "sector": "Banking"},
        {"symbol": "TCS", "name": "TCS", "sector": "IT"},
        {"symbol": "BAJFINANCE", "name": "Bajaj Finance", "sector": "NBFC"},
        {"symbol": "HINDUNILVR", "name": "Hindustan Unilever", "sector": "FMCG"},
        {"symbol": "INFY", "name": "Infosys", "sector": "IT"},
        {"symbol": "LICI", "name": "LIC of India", "sector": "Insurance"},
        {"symbol": "LT", "name": "Larsen & Toubro", "sector": "Infrastructure"},
        {"symbol": "SUNPHARMA", "name": "Sun Pharma", "sector": "Pharma"},
        {"symbol": "MARUTI", "name": "Maruti Suzuki", "sector": "Auto"},
        {"symbol": "ITC", "name": "ITC", "sector": "FMCG"},
        {"symbol": "M&M", "name": "Mahindra & Mahindra", "sector": "Auto"},
        {"symbol": "AXISBANK", "name": "Axis Bank", "sector": "Banking"},
        {"symbol": "NTPC", "name": "NTPC", "sector": "Power"},
        {"symbol": "KOTAKBANK", "name": "Kotak Mahindra Bank", "sector": "Banking"},
        {"symbol": "TITAN", "name": "Titan Company", "sector": "Consumer"},
        {"symbol": "TATAMOTORS", "name": "Tata Motors", "sector": "Auto"},
        {"symbol": "TVSMOTOR", "name": "TVS Motor", "sector": "Auto"},
        {"symbol": "GRASIM", "name": "Grasim Industries", "sector": "Cement"},
        {"symbol": "INDIGO", "name": "InterGlobe Aviation", "sector": "Aviation"},
        {"symbol": "DIVISLAB", "name": "Divi's Laboratories", "sector": "Pharma"},
        {"symbol": "WIPRO", "name": "Wipro", "sector": "IT"},
        {"symbol": "HCLTECH", "name": "HCL Technologies", "sector": "IT"},
        {"symbol": "ADANIENT", "name": "Adani Enterprises", "sector": "Conglomerate"},
        {"symbol": "ADANIPORTS", "name": "Adani Ports", "sector": "Infrastructure"},
        {"symbol": "POWERGRID", "name": "Power Grid Corp", "sector": "Power"},
        {"symbol": "ASIANPAINT", "name": "Asian Paints", "sector": "Paints"},
        {"symbol": "BAJAJFINSV", "name": "Bajaj Finserv", "sector": "NBFC"},
        {"symbol": "NESTLEIND", "name": "Nestle India", "sector": "FMCG"},
        {"symbol": "ULTRACEMCO", "name": "UltraTech Cement", "sector": "Cement"},
        {"symbol": "JSWSTEEL", "name": "JSW Steel", "sector": "Metals"},
        {"symbol": "TATASTEEL", "name": "Tata Steel", "sector": "Metals"},
        {"symbol": "ONGC", "name": "ONGC", "sector": "Energy"},
        {"symbol": "COALINDIA", "name": "Coal India", "sector": "Mining"},
        {"symbol": "TECHM", "name": "Tech Mahindra", "sector": "IT"},
        {"symbol": "DRREDDY", "name": "Dr. Reddy's", "sector": "Pharma"},
        {"symbol": "CIPLA", "name": "Cipla", "sector": "Pharma"},
        {"symbol": "HEROMOTOCO", "name": "Hero MotoCorp", "sector": "Auto"},
        {"symbol": "EICHERMOT", "name": "Eicher Motors", "sector": "Auto"},
        {"symbol": "APOLLOHOSP", "name": "Apollo Hospitals", "sector": "Healthcare"},
        {"symbol": "BPCL", "name": "BPCL", "sector": "Energy"},
        {"symbol": "HINDALCO", "name": "Hindalco", "sector": "Metals"},
        {"symbol": "SHRIRAMFIN", "name": "Shriram Finance", "sector": "NBFC"},
        {"symbol": "BRITANNIA", "name": "Britannia", "sector": "FMCG"},
        {"symbol": "SBILIFE", "name": "SBI Life Insurance", "sector": "Insurance"},
        {"symbol": "HDFCLIFE", "name": "HDFC Life", "sector": "Insurance"},
        {"symbol": "TRENT", "name": "Trent", "sector": "Retail"},
    ]
    return starter_symbols


# ─── Technical Indicator Calculations ────────────────────────────────────────

def calc_rsi(close: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    """Wilder's RSI calculation."""
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calc_macd(close: pd.Series) -> dict:
    """MACD line, signal line, histogram."""
    ema_fast = close.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = close.ewm(span=MACD_SLOW, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
    histogram = macd_line - signal_line
    return {
        "macd_line": macd_line,
        "signal_line": signal_line,
        "histogram": histogram
    }


def calc_bollinger_bands(close: pd.Series) -> dict:
    """Bollinger Bands with squeeze detection."""
    sma = close.rolling(BB_PERIOD).mean()
    std = close.rolling(BB_PERIOD).std()
    upper = sma + (BB_STD * std)
    lower = sma - (BB_STD * std)
    bandwidth = (upper - lower) / sma * 100  # as percentage
    return {
        "upper": upper,
        "lower": lower,
        "sma": sma,
        "bandwidth": bandwidth
    }


def calc_ema(close: pd.Series, period: int) -> pd.Series:
    """Exponential Moving Average."""
    return close.ewm(span=period, adjust=False).mean()


# ─── Scanner Logic ───────────────────────────────────────────────────────────

def scan_stock(symbol_info: dict, df: pd.DataFrame, info: dict) -> dict | None:
    """
    Run all 8 scanners on a single stock.
    Returns a dict with all scan results, or None if data is insufficient.
    """
    if df is None or len(df) < 200:
        return None

    try:
        close = df["Close"]
        high = df["High"]
        low = df["Low"]
        volume = df["Volume"]

        latest_close = float(close.iloc[-1])
        prev_close = float(close.iloc[-2]) if len(close) > 1 else latest_close
        change_pct = round((latest_close - prev_close) / prev_close * 100, 2) if prev_close else 0

        # ── RSI ──
        rsi_series = calc_rsi(close)
        rsi_val = round(float(rsi_series.iloc[-1]), 2) if not rsi_series.empty else None
        rsi_prev = round(float(rsi_series.iloc[-2]), 2) if len(rsi_series) > 1 else None

        rsi_signal = "neutral"
        if rsi_val is not None:
            if rsi_val >= 70:
                rsi_signal = "overbought"
            elif rsi_val <= 30:
                rsi_signal = "oversold"
            elif rsi_val >= 60:
                rsi_signal = "bullish"
            elif rsi_val <= 40:
                rsi_signal = "bearish"

        # ── MACD ──
        macd = calc_macd(close)
        macd_val = round(float(macd["macd_line"].iloc[-1]), 2)
        macd_signal_val = round(float(macd["signal_line"].iloc[-1]), 2)
        macd_hist = round(float(macd["histogram"].iloc[-1]), 2)
        macd_hist_prev = round(float(macd["histogram"].iloc[-2]), 2) if len(macd["histogram"]) > 1 else 0

        macd_crossover = "none"
        if macd_hist > 0 and macd_hist_prev <= 0:
            macd_crossover = "bullish_crossover"
        elif macd_hist < 0 and macd_hist_prev >= 0:
            macd_crossover = "bearish_crossover"
        elif macd_hist > 0:
            macd_crossover = "bullish"
        elif macd_hist < 0:
            macd_crossover = "bearish"

        # ── EMA Crossovers ──
        ema9 = calc_ema(close, EMA_SHORT_1)
        ema21 = calc_ema(close, EMA_LONG_1)
        ema50 = calc_ema(close, EMA_SHORT_2)
        ema200 = calc_ema(close, EMA_LONG_2)

        ema9_val = round(float(ema9.iloc[-1]), 2)
        ema21_val = round(float(ema21.iloc[-1]), 2)
        ema50_val = round(float(ema50.iloc[-1]), 2)
        ema200_val = round(float(ema200.iloc[-1]), 2)

        # 9/21 crossover
        ema_9_21_signal = "none"
        if len(ema9) > 1 and len(ema21) > 1:
            curr_diff = float(ema9.iloc[-1] - ema21.iloc[-1])
            prev_diff = float(ema9.iloc[-2] - ema21.iloc[-2])
            if curr_diff > 0 and prev_diff <= 0:
                ema_9_21_signal = "golden_cross"
            elif curr_diff < 0 and prev_diff >= 0:
                ema_9_21_signal = "death_cross"
            elif curr_diff > 0:
                ema_9_21_signal = "bullish"
            else:
                ema_9_21_signal = "bearish"

        # 50/200 crossover
        ema_50_200_signal = "none"
        if len(ema50) > 1 and len(ema200) > 1:
            curr_diff = float(ema50.iloc[-1] - ema200.iloc[-1])
            prev_diff = float(ema50.iloc[-2] - ema200.iloc[-2])
            if curr_diff > 0 and prev_diff <= 0:
                ema_50_200_signal = "golden_cross"
            elif curr_diff < 0 and prev_diff >= 0:
                ema_50_200_signal = "death_cross"
            elif curr_diff > 0:
                ema_50_200_signal = "bullish"
            else:
                ema_50_200_signal = "bearish"

        # Price vs EMAs
        above_ema50 = latest_close > ema50_val
        above_ema200 = latest_close > ema200_val

        # ── 52-Week High/Low ──
        high_52w = float(high.tail(252).max()) if len(high) >= 252 else float(high.max())
        low_52w = float(low.tail(252).min()) if len(low) >= 252 else float(low.min())
        pct_from_52w_high = round((latest_close - high_52w) / high_52w * 100, 2)
        pct_from_52w_low = round((latest_close - low_52w) / low_52w * 100, 2)

        breakout_signal = "none"
        if pct_from_52w_high >= -2:  # within 2% of 52w high
            breakout_signal = "near_52w_high"
        if pct_from_52w_high >= 0:
            breakout_signal = "new_52w_high"
        if pct_from_52w_low <= 2:  # within 2% of 52w low
            breakout_signal = "near_52w_low"
        if pct_from_52w_low <= 0:
            breakout_signal = "new_52w_low"

        # ── Volume Spike ──
        vol_avg = float(volume.tail(VOL_AVG_PERIOD).mean())
        vol_latest = float(volume.iloc[-1])
        vol_ratio = round(vol_latest / vol_avg, 2) if vol_avg > 0 else 0

        vol_signal = "normal"
        if vol_ratio >= 3.0:
            vol_signal = "extreme_spike"
        elif vol_ratio >= VOL_SPIKE_MULTIPLIER:
            vol_signal = "spike"
        elif vol_ratio >= 1.5:
            vol_signal = "above_avg"
        elif vol_ratio <= 0.5:
            vol_signal = "dry"

        # ── Bollinger Bands ──
        bb = calc_bollinger_bands(close)
        bb_upper = round(float(bb["upper"].iloc[-1]), 2)
        bb_lower = round(float(bb["lower"].iloc[-1]), 2)
        bb_bandwidth = round(float(bb["bandwidth"].iloc[-1]), 2)
        bb_bandwidth_prev = round(float(bb["bandwidth"].iloc[-6]), 2) if len(bb["bandwidth"]) > 5 else bb_bandwidth

        bb_signal = "neutral"
        if latest_close >= bb_upper:
            bb_signal = "upper_breakout"
        elif latest_close <= bb_lower:
            bb_signal = "lower_breakout"
        elif bb_bandwidth < bb_bandwidth_prev * 0.7:
            bb_signal = "squeeze"

        # ── Fundamentals (from yfinance info) ──
        pe_ratio = info.get("trailingPE") or info.get("forwardPE")
        market_cap = info.get("marketCap")
        eps = info.get("trailingEps")
        pb_ratio = info.get("priceToBook")
        dividend_yield = info.get("dividendYield")
        roe = info.get("returnOnEquity")
        sector = info.get("sector") or symbol_info.get("sector", "Unknown")
        industry = info.get("industry", "Unknown")

        # Market cap category
        mcap_cr = round(market_cap / 1e7, 2) if market_cap else None  # Convert to Crores
        mcap_category = "unknown"
        if mcap_cr:
            if mcap_cr >= 100000:
                mcap_category = "mega_cap"
            elif mcap_cr >= 20000:
                mcap_category = "large_cap"
            elif mcap_cr >= 5000:
                mcap_category = "mid_cap"
            elif mcap_cr >= 1000:
                mcap_category = "small_cap"
            else:
                mcap_category = "micro_cap"

        # ── Composite Score ──
        # Simple scoring: each bullish signal adds +1, bearish -1
        score = 0
        if rsi_signal in ["oversold", "bullish"]:
            score += 1
        elif rsi_signal in ["overbought", "bearish"]:
            score -= 1
        if macd_crossover in ["bullish_crossover", "bullish"]:
            score += 1
        elif macd_crossover in ["bearish_crossover", "bearish"]:
            score -= 1
        if ema_9_21_signal in ["golden_cross", "bullish"]:
            score += 1
        elif ema_9_21_signal in ["death_cross", "bearish"]:
            score -= 1
        if ema_50_200_signal in ["golden_cross", "bullish"]:
            score += 1
        elif ema_50_200_signal in ["death_cross", "bearish"]:
            score -= 1
        if breakout_signal in ["new_52w_high", "near_52w_high"]:
            score += 1
        elif breakout_signal in ["new_52w_low", "near_52w_low"]:
            score -= 1
        if vol_signal in ["spike", "extreme_spike"]:
            score += 1
        if bb_signal == "upper_breakout":
            score += 1
        elif bb_signal == "lower_breakout":
            score -= 1

        # ── Returns ──
        ret_1d = change_pct
        ret_1w = round((latest_close - float(close.iloc[-5])) / float(close.iloc[-5]) * 100, 2) if len(close) >= 5 else None
        ret_1m = round((latest_close - float(close.iloc[-21])) / float(close.iloc[-21]) * 100, 2) if len(close) >= 21 else None
        ret_3m = round((latest_close - float(close.iloc[-63])) / float(close.iloc[-63]) * 100, 2) if len(close) >= 63 else None

        return {
            "symbol": symbol_info["symbol"],
            "name": symbol_info.get("name", symbol_info["symbol"]),
            "sector": sector,
            "industry": industry,
            "price": round(latest_close, 2),
            "change_pct": ret_1d,
            "returns": {
                "1d": ret_1d,
                "1w": ret_1w,
                "1m": ret_1m,
                "3m": ret_3m,
            },
            # Technical
            "rsi": {"value": rsi_val, "signal": rsi_signal},
            "macd": {
                "line": macd_val,
                "signal": macd_signal_val,
                "histogram": macd_hist,
                "crossover": macd_crossover,
            },
            "ema": {
                "ema9": ema9_val,
                "ema21": ema21_val,
                "ema50": ema50_val,
                "ema200": ema200_val,
                "cross_9_21": ema_9_21_signal,
                "cross_50_200": ema_50_200_signal,
                "above_ema50": above_ema50,
                "above_ema200": above_ema200,
            },
            "breakout": {
                "high_52w": round(high_52w, 2),
                "low_52w": round(low_52w, 2),
                "pct_from_high": pct_from_52w_high,
                "pct_from_low": pct_from_52w_low,
                "signal": breakout_signal,
            },
            "volume": {
                "latest": int(vol_latest),
                "avg_20d": int(vol_avg),
                "ratio": vol_ratio,
                "signal": vol_signal,
            },
            "bollinger": {
                "upper": bb_upper,
                "lower": bb_lower,
                "bandwidth": bb_bandwidth,
                "signal": bb_signal,
            },
            # Fundamental
            "fundamentals": {
                "pe_ratio": round(pe_ratio, 2) if pe_ratio else None,
                "pb_ratio": round(pb_ratio, 2) if pb_ratio else None,
                "eps": round(eps, 2) if eps else None,
                "market_cap_cr": mcap_cr,
                "mcap_category": mcap_category,
                "dividend_yield": round(dividend_yield * 100, 2) if dividend_yield else None,
                "roe": round(roe * 100, 2) if roe else None,
            },
            # Composite
            "composite_score": score,
        }
    except Exception as e:
        print(f"  ✗ Error scanning {symbol_info['symbol']}: {e}")
        traceback.print_exc()
        return None


# ─── Main Fetcher ────────────────────────────────────────────────────────────

def fetch_and_scan():
    """Main entry point: fetch data for all NSE stocks and run scanners."""
    print("=" * 60)
    print("  TradEdge Scanner v1.0")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    symbols_list = get_nse_symbols()
    total = len(symbols_list)
    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    est_minutes = total_batches * (BATCH_DELAY + 3) / 60  # rough estimate
    
    print(f"\n📊 Scanning {total} NSE stocks...")
    print(f"   Period: {HISTORY_PERIOD} | Batch size: {BATCH_SIZE}")
    print(f"   Fundamentals: {'SKIP (fast mode)' if SKIP_FUNDAMENTALS else 'ENABLED (slower)'}")
    print(f"   Estimated time: ~{est_minutes:.0f} minutes\n")

    results = []
    errors = []
    skipped = []
    scan_start = time.time()

    # Process in batches
    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch = symbols_list[batch_start:batch_end]
        batch_num = batch_start // BATCH_SIZE + 1

        # Build yfinance tickers (append .NS for NSE)
        yf_symbols = [f"{s['symbol']}.NS" for s in batch]
        elapsed = time.time() - scan_start
        pct = batch_start / total * 100
        print(f"  [{pct:5.1f}%] Batch {batch_num}/{total_batches} ({len(results)} done, {elapsed:.0f}s): {', '.join(s['symbol'] for s in batch[:4])}{'...' if len(batch) > 4 else ''}")

        # Retry logic for batch download
        data = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                data = yf.download(
                    yf_symbols,
                    period=HISTORY_PERIOD,
                    group_by="ticker",
                    auto_adjust=True,
                    threads=True,
                    progress=False,
                )
                break
            except Exception as e:
                if attempt < MAX_RETRIES:
                    print(f"    ⟳ Retry {attempt+1}/{MAX_RETRIES} after error: {e}")
                    time.sleep(BATCH_DELAY * 2)
                else:
                    print(f"  ✗ Batch download failed after {MAX_RETRIES} retries: {e}")
                    errors.extend([s["symbol"] for s in batch])
                    data = None

        if data is None:
            continue

        for sym_info in batch:
            yf_sym = f"{sym_info['symbol']}.NS"
            try:
                # Extract individual stock data from batch result
                if len(yf_symbols) == 1:
                    stock_df = data.copy()
                else:
                    if yf_sym in data.columns.get_level_values(0):
                        stock_df = data[yf_sym].dropna(how="all")
                    else:
                        skipped.append(sym_info["symbol"])
                        continue

                if stock_df.empty or len(stock_df) < 50:
                    skipped.append(sym_info["symbol"])
                    continue

                # Fetch fundamental info (optional — slow for 2000+ stocks)
                info = {}
                if not SKIP_FUNDAMENTALS:
                    try:
                        ticker = yf.Ticker(yf_sym)
                        info = ticker.info or {}
                    except Exception:
                        pass

                result = scan_stock(sym_info, stock_df, info)
                if result:
                    results.append(result)
                else:
                    skipped.append(sym_info["symbol"])

            except Exception as e:
                errors.append(sym_info["symbol"])
                print(f"    ✗ {sym_info['symbol']}: {e}")

        # Rate limit delay between batches
        if batch_end < total:
            time.sleep(BATCH_DELAY)

    # ── Sort by composite score (descending) ──
    results.sort(key=lambda x: x["composite_score"], reverse=True)

    # ── Build output ──
    output = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "total_scanned": len(results),
            "total_errors": len(errors),
            "total_skipped": len(skipped),
            "version": "1.0",
            "scanners": [
                "RSI", "MACD", "EMA Crossovers", "52W Breakout",
                "Volume Spike", "Bollinger Bands", "PE Ratio", "Market Cap"
            ],
        },
        "scanner_summary": build_summary(results),
        "stocks": results,
    }

    # ── Write JSON ──
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n{'=' * 60}")
    print(f"  ✅ Done! {len(results)} stocks scanned successfully")
    print(f"  ⚠  {len(errors)} errors, {len(skipped)} skipped")
    print(f"  📁 Output: {OUTPUT_FILE}")
    print(f"{'=' * 60}")

    return output


def build_summary(results: list[dict]) -> dict:
    """Build scanner summary counts for the dashboard."""
    summary = {
        "rsi_overbought": [],
        "rsi_oversold": [],
        "macd_bullish_crossover": [],
        "macd_bearish_crossover": [],
        "ema_golden_cross_9_21": [],
        "ema_death_cross_9_21": [],
        "ema_golden_cross_50_200": [],
        "ema_death_cross_50_200": [],
        "new_52w_high": [],
        "near_52w_high": [],
        "new_52w_low": [],
        "near_52w_low": [],
        "volume_spike": [],
        "volume_extreme_spike": [],
        "bb_upper_breakout": [],
        "bb_lower_breakout": [],
        "bb_squeeze": [],
        "bullish_composite": [],  # score >= 3
        "bearish_composite": [],  # score <= -3
    }

    for stock in results:
        sym = stock["symbol"]

        if stock["rsi"]["signal"] == "overbought":
            summary["rsi_overbought"].append(sym)
        elif stock["rsi"]["signal"] == "oversold":
            summary["rsi_oversold"].append(sym)

        if stock["macd"]["crossover"] == "bullish_crossover":
            summary["macd_bullish_crossover"].append(sym)
        elif stock["macd"]["crossover"] == "bearish_crossover":
            summary["macd_bearish_crossover"].append(sym)

        if stock["ema"]["cross_9_21"] == "golden_cross":
            summary["ema_golden_cross_9_21"].append(sym)
        elif stock["ema"]["cross_9_21"] == "death_cross":
            summary["ema_death_cross_9_21"].append(sym)

        if stock["ema"]["cross_50_200"] == "golden_cross":
            summary["ema_golden_cross_50_200"].append(sym)
        elif stock["ema"]["cross_50_200"] == "death_cross":
            summary["ema_death_cross_50_200"].append(sym)

        bs = stock["breakout"]["signal"]
        if bs == "new_52w_high":
            summary["new_52w_high"].append(sym)
        elif bs == "near_52w_high":
            summary["near_52w_high"].append(sym)
        elif bs == "new_52w_low":
            summary["new_52w_low"].append(sym)
        elif bs == "near_52w_low":
            summary["near_52w_low"].append(sym)

        vs = stock["volume"]["signal"]
        if vs == "spike":
            summary["volume_spike"].append(sym)
        elif vs == "extreme_spike":
            summary["volume_extreme_spike"].append(sym)

        bbs = stock["bollinger"]["signal"]
        if bbs == "upper_breakout":
            summary["bb_upper_breakout"].append(sym)
        elif bbs == "lower_breakout":
            summary["bb_lower_breakout"].append(sym)
        elif bbs == "squeeze":
            summary["bb_squeeze"].append(sym)

        if stock["composite_score"] >= 3:
            summary["bullish_composite"].append(sym)
        elif stock["composite_score"] <= -3:
            summary["bearish_composite"].append(sym)

    # Convert to counts + symbols for dashboard
    return {k: {"count": len(v), "symbols": v} for k, v in summary.items()}


if __name__ == "__main__":
    fetch_and_scan()
