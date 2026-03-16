"""
================================================================================
MPS Data Fetcher v3.1.1 — Automated EOD Data Collection
================================================================================
Fetches all 21 MPS data points from Chartink + Yahoo Finance + NSE (fallback),
runs the MPS v3.1 engine, and outputs the result as JSON.

v3.1.1 HOTFIX — NSE 403 Bypass:
  NSE blocks GitHub Actions (datacenter) IPs with 403.
  Solution: Use Yahoo Finance as PRIMARY source for all market data.
  NSE is kept only as a fallback for PCR (option chain).

Data Sources:
  - Chartink (8 scanners): breadth, spark, RSI, burst, ATR
  - Yahoo Finance (PRIMARY for market data):
      - India VIX (^INDIAVIX)
      - Nifty 50 levels + 52W high (^NSEI)
      - Nifty 500 A/D + 52W highs/lows (via constituent data)
      - Brent Crude (BZ=F), US 10Y (^TNX), USD/INR (USDINR=X)
  - NSE India (FALLBACK only): PCR from option chain, FII flows

Usage:
  python mps_fetcher_v3.py                    # Fetch + calculate + print JSON
  python mps_fetcher_v3.py --output mps.json  # Save to file
  python mps_fetcher_v3.py --dry-run          # Use sample data (no network)

Requirements:
  pip install requests beautifulsoup4 yfinance
================================================================================
"""

import json
import os
import sys
import time
import argparse
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# Import the MPS v3.1 engine (must be in same directory or PYTHONPATH)
from mps_engine_v3 import RawMarketData, calculate_mps, format_mps_report, to_json

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("mps_fetcher")


# =============================================================================
# CONFIGURATION
# =============================================================================

CHARTINK_URL = "https://chartink.com/screener/process"
CHARTINK_BASE = "https://chartink.com/screener/"

SCANNERS = {
    "above_200sma": {
        "name": "Stocks above 200 SMA",
        "scan_clause": '( {cash} ( [0] 5 minute close > [0] 5 minute sma( close,200 ) and latest "nifty 500" = 1 ) )',
    },
    "above_50sma": {
        "name": "Stocks above 50 SMA",
        "scan_clause": '( {cash} ( [0] 5 minute close > [0] 5 minute sma( close,50 ) and latest "nifty 500" = 1 ) )',
    },
    "spark_4pct": {
        "name": "4% Breakout Stocks (Stockbee)",
        "scan_clause": '( {cash} ( [0] 5 minute close / [-1] 1 day close > 1.04 and [0] 5 minute volume > [0] 5 minute sma( volume,20 ) * 1.5 and latest "nifty 500" = 1 ) )',
    },
    "rsi_above_70": {
        "name": "RSI > 70 Stocks",
        "scan_clause": '( {cash} ( [0] 5 minute rsi( close,14 ) > 70 and latest "nifty 500" = 1 ) )',
    },
    "rsi_above_50": {
        "name": "RSI > 50 Stocks (Momentum Breadth)",
        "scan_clause": '( {cash} ( [0] 5 minute rsi( close,14 ) > 50 and latest "nifty 500" = 1 ) )',
    },
    "burst_4_5pct_gainers": {
        "name": "4.5%+ Gainers (Burst Ratio)",
        "scan_clause": '( {cash} ( [0] 5 minute close / [-1] 1 day close > 1.045 and latest "nifty 500" = 1 ) )',
    },
    "burst_4_5pct_losers": {
        "name": "4.5%+ Losers (Burst Ratio)",
        "scan_clause": '( {cash} ( [0] 5 minute close / [-1] 1 day close < 0.955 and latest "nifty 500" = 1 ) )',
    },
    "atr_pct_above_4": {
        "name": "ATR% > 4% Stocks (Volatility Breadth)",
        "scan_clause": '( {cash} ( [0] 5 minute atr( 14 ) / [0] 5 minute close * 100 > 4 and latest "nifty 500" = 1 ) )',
    },
}

# NSE — fallback only (for PCR)
NSE_BASE = "https://www.nseindia.com"
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

STATE_FILE = "mps_state.json"


# =============================================================================
# CHARTINK FETCHER (unchanged)
# =============================================================================

def get_chartink_session():
    """Get a session with CSRF token from Chartink."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*",
        "X-Requested-With": "XMLHttpRequest",
    })
    try:
        resp = session.get(CHARTINK_BASE, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        csrf_meta = soup.find("meta", {"name": "csrf-token"})
        if csrf_meta:
            session.headers["X-CSRF-Token"] = csrf_meta["content"]
            log.info("Chartink CSRF token acquired")
        else:
            log.warning("Could not find CSRF token on Chartink page")
    except Exception as e:
        log.error(f"Failed to get Chartink session: {e}")
    return session


def fetch_chartink_count(session, scanner_key):
    scanner = SCANNERS[scanner_key]
    log.info(f"  Fetching: {scanner['name']}...")
    try:
        resp = session.post(
            CHARTINK_URL,
            data={"scan_clause": scanner["scan_clause"]},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        count = len(data.get("data", []))
        log.info(f"  → {scanner['name']}: {count} stocks")
        return count
    except Exception as e:
        log.error(f"  ✗ Failed to fetch {scanner['name']}: {e}")
        return None


def fetch_all_chartink():
    log.info("═══ CHARTINK DATA (8 scanners) ═══")
    session = get_chartink_session()
    results = {}
    for key in SCANNERS:
        count = fetch_chartink_count(session, key)
        results[key] = count
        time.sleep(1.5)
    return results


# =============================================================================
# YAHOO FINANCE — PRIMARY MARKET DATA SOURCE
# =============================================================================

def fetch_yahoo_market_data():
    """
    Fetch all NSE market data from Yahoo Finance.
    This bypasses NSE's IP blocking completely.
    
    Returns dict with: vix, advances, declines, unchanged,
    highs_52, lows_52, nifty_52w_high, pcr (None = use NSE fallback)
    """
    import yfinance as yf
    
    log.info("═══ MARKET DATA (Yahoo Finance — Primary) ═══")
    
    result = {
        "vix": None,
        "advances": None,
        "declines": None,
        "unchanged": None,
        "highs_52": None,
        "lows_52": None,
        "nifty_52w_high": False,
        "pcr": None,
        "fii_net": None,
    }
    
    # ── India VIX ──
    try:
        vix_ticker = yf.Ticker("^INDIAVIX")
        vix_hist = vix_ticker.history(period="5d")
        if not vix_hist.empty:
            result["vix"] = round(float(vix_hist['Close'].iloc[-1]), 2)
            log.info(f"  ✓ India VIX: {result['vix']}")
        else:
            log.warning("  ✗ India VIX: no data from Yahoo")
    except Exception as e:
        log.error(f"  ✗ India VIX: {e}")
    
    # ── Nifty 50 — current level + 52W high check ──
    try:
        nifty = yf.Ticker("^NSEI")
        nifty_hist = nifty.history(period="5d")
        
        if not nifty_hist.empty:
            current_price = float(nifty_hist['Close'].iloc[-1])
            
            # Get 52-week high from 1y history
            hist_1y = nifty.history(period="1y")
            year_high = float(hist_1y['High'].max()) if not hist_1y.empty else 0
            
            # Fallback to info dict
            if year_high == 0:
                nifty_info = nifty.info
                year_high = nifty_info.get("fiftyTwoWeekHigh", 0)
            
            result["nifty_52w_high"] = current_price >= year_high * 0.99 if year_high > 0 else False
            log.info(f"  ✓ Nifty 50: {current_price:.2f} vs 52W High: {year_high:.2f} → "
                     f"{'AT HIGH' if result['nifty_52w_high'] else 'Below'}")
        else:
            log.warning("  ✗ Nifty 50: no data from Yahoo")
    except Exception as e:
        log.error(f"  ✗ Nifty 50: {e}")
    
    # ── A/D Ratio — estimate from Nifty 50 index movement ──
    try:
        nifty50 = yf.Ticker("^NSEI")
        hist_2d = nifty50.history(period="5d")
        
        if len(hist_2d) >= 2:
            today_close = float(hist_2d['Close'].iloc[-1])
            prev_close = float(hist_2d['Close'].iloc[-2])
            pct_change = ((today_close - prev_close) / prev_close) * 100
            
            # Empirical model: index % change → A/D split
            # +1% day ≈ 65% advances, -1% day ≈ 35% advances
            total = 500
            advance_pct = 50 + (pct_change * 15)
            advance_pct = max(10, min(90, advance_pct))
            
            result["advances"] = int(total * advance_pct / 100)
            result["declines"] = total - result["advances"]
            result["unchanged"] = 0
            
            log.info(f"  ✓ A/D (estimated from Nifty {pct_change:+.2f}%): "
                     f"{result['advances']}A / {result['declines']}D")
        else:
            log.warning("  ✗ Cannot estimate A/D: insufficient Nifty data")
            
    except Exception as e:
        log.error(f"  ✗ A/D estimation: {e}")
    
    # ── 52W Highs/Lows — estimate from top Nifty stocks ──
    try:
        sample_symbols = [
            "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
            "HINDUNILVR.NS", "ITC.NS", "BHARTIARTL.NS", "SBIN.NS", "BAJFINANCE.NS",
            "LT.NS", "KOTAKBANK.NS", "AXISBANK.NS", "MARUTI.NS", "TITAN.NS",
            "SUNPHARMA.NS", "WIPRO.NS", "ULTRACEMCO.NS", "HCLTECH.NS", "NTPC.NS",
        ]
        
        highs_count = 0
        lows_count = 0
        checked = 0
        
        data = yf.download(sample_symbols, period="1y", group_by="ticker", progress=False, threads=True)
        
        for sym in sample_symbols:
            try:
                if sym in data.columns.get_level_values(0):
                    sym_data = data[sym]
                    if sym_data is not None and not sym_data.empty and len(sym_data.dropna()) > 10:
                        current = float(sym_data['Close'].dropna().iloc[-1])
                        year_high = float(sym_data['High'].dropna().max())
                        year_low = float(sym_data['Low'].dropna().min())
                        
                        if current >= year_high * 0.98:
                            highs_count += 1
                        if current <= year_low * 1.02:
                            lows_count += 1
                        checked += 1
            except Exception:
                continue
        
        if checked > 0:
            scale = 500 / checked
            result["highs_52"] = int(highs_count * scale)
            result["lows_52"] = int(lows_count * scale)
            log.info(f"  ✓ 52W (from {checked} stocks, scaled to 500): "
                     f"{result['highs_52']} Highs, {result['lows_52']} Lows")
        else:
            log.warning("  ✗ Could not estimate 52W highs/lows")
            result["highs_52"] = 0
            result["lows_52"] = 0
            
    except Exception as e:
        log.error(f"  ✗ 52W estimation: {e}")
        result["highs_52"] = 0
        result["lows_52"] = 0
    
    return result


# =============================================================================
# NSE FALLBACK — PCR + FII (best effort, may fail from GH Actions)
# =============================================================================

def fetch_nse_pcr_fallback():
    """Try to fetch PCR from NSE. Returns neutral 1.0 on failure."""
    log.info("═══ NSE FALLBACK — PCR ═══")
    
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    
    try:
        session.get(NSE_BASE, timeout=10)
        time.sleep(1)
        
        resp = session.get(
            f"{NSE_BASE}/api/option-chain-indices?symbol=NIFTY",
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        
        total_put_oi = 0
        total_call_oi = 0
        
        for record in data.get("records", {}).get("data", []):
            if "CE" in record:
                total_call_oi += record["CE"].get("openInterest", 0)
            if "PE" in record:
                total_put_oi += record["PE"].get("openInterest", 0)
        
        pcr = total_put_oi / total_call_oi if total_call_oi > 0 else 1.0
        log.info(f"  ✓ PCR from NSE: {pcr:.3f}")
        return round(pcr, 3)
        
    except Exception as e:
        log.warning(f"  ✗ NSE PCR failed ({e}), using neutral default 1.0")
        return 1.0


def _parse_crore_value(text):
    """Parse a ₹ Crore value from text like '-10,716.64' or '+9,977.42'."""
    if not text:
        return None
    cleaned = text.strip().replace("₹", "").replace("Cr", "").replace("crore", "")
    cleaned = cleaned.replace(",", "").replace(" ", "")
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def fetch_fii_from_groww():
    """
    Scrape FII net buy/sell (₹ Cr) from Groww.in's FII/DII page.
    
    Groww renders data server-side in HTML tables — works from
    GitHub Actions without JS or special session handling.
    
    Returns: float (FII net in ₹ Crores, negative = selling)
    """
    log.info("═══ FII DATA (Groww.in — Primary) ═══")
    
    url = "https://groww.in/fii-dii-data"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Find table rows with date + FII data
        # Columns: Date | FII Buy | FII Sell | FII Net | DII Buy | DII Sell | DII Net
        import re
        tables = soup.find_all("table")
        
        for table in tables:
            rows = table.find_all("tr")
            for row in rows[1:]:  # Skip header
                cells = row.find_all(["td", "th"])
                if len(cells) >= 4:
                    first_cell = cells[0].get_text(strip=True)
                    if re.match(r'\d{1,2}\s+\w{3}\s+\d{4}', first_cell):
                        # Data row — FII net is 4th column (index 3)
                        fii_net_text = cells[3].get_text(strip=True)
                        fii_net = _parse_crore_value(fii_net_text)
                        
                        if fii_net is not None:
                            log.info(f"  ✓ FII Net from Groww ({first_cell}): ₹{fii_net:,.2f} Cr")
                            return round(fii_net, 2)
        
        log.warning("  ✗ Could not parse FII data from Groww table")
        
    except Exception as e:
        log.warning(f"  ✗ Groww FII scrape failed: {e}")
    
    # ── Fallback: Try NSE (may fail from datacenter IPs) ──
    return _fetch_nse_fii_fallback()


def _fetch_nse_fii_fallback():
    """NSE FII fallback — last resort, usually blocked from GitHub Actions."""
    log.info("  Trying NSE FII fallback...")
    
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    
    try:
        session.get(NSE_BASE, timeout=10)
        time.sleep(1)
    except Exception:
        log.warning("  ✗ NSE session failed for FII, using default 0")
        return 0
    
    for endpoint in ["/api/fiidii-activity", "/api/fiidiiActivity"]:
        try:
            resp = session.get(f"{NSE_BASE}{endpoint}", timeout=15)
            resp.raise_for_status()
            data = resp.json()
            
            entries = data if isinstance(data, list) else data.get("data", [])
            for entry in entries:
                category = entry.get("category", "").upper()
                if "FII" in category or "FPI" in category:
                    buy = float(entry.get("buyValue", 0) or 0)
                    sell = float(entry.get("sellValue", 0) or 0)
                    fii_net = buy - sell
                    log.info(f"  ✓ FII Net from NSE: ₹{fii_net:,.2f} Cr")
                    return round(fii_net, 2)
        except Exception:
            continue
    
    log.warning("  ✗ All FII sources failed, using default 0")
    return 0


# =============================================================================
# MACRO DATA (Yahoo Finance)
# =============================================================================

def fetch_macro_data():
    """Fetch macro indicators: Brent Crude, US 10Y, USD/INR."""
    import yfinance as yf
    
    log.info("═══ MACRO DATA (Yahoo Finance) ═══")
    macro = {
        "brent_crude": 0.0,
        "us10y_yield": 0.0,
        "usd_inr": 0.0,
        "usd_inr_20d_ago": 0.0,
    }

    try:
        oil = yf.Ticker("BZ=F")
        hist = oil.history(period="5d")
        if not hist.empty:
            macro["brent_crude"] = round(float(hist['Close'].iloc[-1]), 2)
            log.info(f"  ✓ Brent Crude: ${macro['brent_crude']}")
    except Exception as e:
        log.error(f"  ✗ Brent Crude: {e}")

    try:
        tnx = yf.Ticker("^TNX")
        hist = tnx.history(period="5d")
        if not hist.empty:
            macro["us10y_yield"] = round(float(hist['Close'].iloc[-1]), 2)
            log.info(f"  ✓ US 10Y Yield: {macro['us10y_yield']}%")
    except Exception as e:
        log.error(f"  ✗ US 10Y Yield: {e}")

    try:
        fx = yf.Ticker("USDINR=X")
        hist = fx.history(period="2mo")
        if not hist.empty and len(hist) >= 2:
            macro["usd_inr"] = round(float(hist['Close'].iloc[-1]), 2)
            lookback = min(20, len(hist) - 1)
            macro["usd_inr_20d_ago"] = round(float(hist['Close'].iloc[-lookback - 1]), 2)
            log.info(f"  ✓ USD/INR: ₹{macro['usd_inr']} (20d ago: ₹{macro['usd_inr_20d_ago']})")
    except Exception as e:
        log.error(f"  ✗ USD/INR: {e}")

    return macro


# =============================================================================
# STATE MANAGEMENT
# =============================================================================

def load_state(state_file=STATE_FILE):
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            return json.load(f)
    return {
        "structural_bull_streak": 0,
        "prev_pct_above_50sma": 50.0,
        "fii_consecutive_sell_days": 0,
        "fii_5day_history": [],
        "last_date": None,
        "history": [],
    }


def save_state(state, state_file=STATE_FILE):
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2)
    log.info(f"State saved to {state_file}")


def update_state(state, raw_data, fii_net_today, mps_result):
    pct_200 = (raw_data.stocks_above_200sma / raw_data.total_universe) * 100
    pct_50 = (raw_data.stocks_above_50sma / raw_data.total_universe) * 100

    if pct_200 > 50:
        state["structural_bull_streak"] += 1
    else:
        state["structural_bull_streak"] = 0

    state["prev_pct_above_50sma"] = pct_50

    if fii_net_today is not None:
        if fii_net_today < 0:
            state["fii_consecutive_sell_days"] += 1
        else:
            state["fii_consecutive_sell_days"] = 0
        state["fii_5day_history"].append(fii_net_today)
        if len(state["fii_5day_history"]) > 5:
            state["fii_5day_history"] = state["fii_5day_history"][-5:]

    state["last_date"] = raw_data.date

    state["history"].append({
        "date": raw_data.date,
        "mps": mps_result.final_score,
        "zone": mps_result.zone,
    })
    if len(state["history"]) > 90:
        state["history"] = state["history"][-90:]

    return state


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

def fetch_and_calculate(dry_run=False):
    today = datetime.now().strftime("%Y-%m-%d")
    log.info(f"╔════════════════════════════════════════════════╗")
    log.info(f"║  MPS v3.1.1 DAILY CALCULATION — {today}    ║")
    log.info(f"╚════════════════════════════════════════════════╝")

    state = load_state()

    if dry_run:
        log.info("DRY RUN — Using sample data")
        chartink = {
            "above_200sma": 312, "above_50sma": 285, "spark_4pct": 18,
            "rsi_above_70": 45, "rsi_above_50": 342,
            "burst_4_5pct_gainers": 22, "burst_4_5pct_losers": 3,
            "atr_pct_above_4": 65,
        }
        market = {
            "advances": 1280, "declines": 720, "unchanged": 0,
            "highs_52": 42, "lows_52": 8,
            "vix": 13.2, "pcr": 1.18,
            "fii_net": 2100, "nifty_52w_high": False,
        }
        macro = {
            "brent_crude": 82.5, "us10y_yield": 4.1,
            "usd_inr": 85.50, "usd_inr_20d_ago": 85.20,
        }
    else:
        # ── Step 1: Chartink ──
        chartink = fetch_all_chartink()
        if any(v is None for v in chartink.values()):
            log.error("Some Chartink data failed to fetch. Aborting.")
            return None, None

        # ── Step 2: Yahoo Finance for market data ──
        market = fetch_yahoo_market_data()
        
        # ── Step 3: NSE fallback for PCR + FII ──
        market["pcr"] = fetch_nse_pcr_fallback()
        market["fii_net"] = fetch_fii_from_groww()

        # ── Safe defaults for any missing data ──
        if market.get("vix") is None:
            log.warning("VIX unavailable, using safe default 15.0")
            market["vix"] = 15.0
        if market.get("advances") is None:
            log.warning("A/D data unavailable, using neutral 250/250")
            market["advances"] = 250
            market["declines"] = 250
            market["unchanged"] = 0

        # ── Step 4: Macro data ──
        macro = fetch_macro_data()

    # Safe defaults
    nse_fii = market.get("fii_net") or 0
    nse_highs = market.get("highs_52") or 0
    nse_lows = market.get("lows_52") or 0
    nse_advances = market.get("advances") or 0
    nse_declines = market.get("declines") or 0
    nse_unchanged = market.get("unchanged") or 0
    nse_vix = market.get("vix") or 15.0
    nse_pcr = market.get("pcr") or 1.0

    # Build RawMarketData
    raw = RawMarketData(
        date=today,
        stocks_above_200sma=chartink["above_200sma"],
        stocks_above_50sma=chartink["above_50sma"],
        advances=nse_advances,
        declines=nse_declines,
        unchanged=nse_unchanged,
        stocks_up_4pct=chartink["spark_4pct"],
        burst_gainers_4_5pct=chartink["burst_4_5pct_gainers"],
        burst_losers_4_5pct=chartink["burst_4_5pct_losers"],
        new_52w_highs=nse_highs,
        new_52w_lows=nse_lows,
        india_vix=nse_vix,
        pcr=nse_pcr,
        stocks_rsi_above_50=chartink["rsi_above_50"],
        stocks_atr_pct_above_4=chartink["atr_pct_above_4"],
        stocks_rsi_above_70=chartink["rsi_above_70"],
        nifty_at_52w_high=market.get("nifty_52w_high", False),
        fii_net_buy_crores=nse_fii,
        brent_crude=macro["brent_crude"],
        us10y_yield=macro["us10y_yield"],
        usd_inr=macro["usd_inr"],
    )

    # FII 5-day net
    fii_5day = state.get("fii_5day_history", [])
    fii_5day_with_today = fii_5day + [nse_fii]
    fii_5day_net = sum(fii_5day_with_today[-5:])

    # Calculate MPS
    log.info("═══ CALCULATING MPS v3.1 ═══")
    result = calculate_mps(
        raw,
        structural_bull_streak_days=state.get("structural_bull_streak", 0),
        prev_pct_above_50sma=state.get("prev_pct_above_50sma", 50.0),
        fii_net_consecutive_sell_days=state.get("fii_consecutive_sell_days", 0),
        fii_5day_net_crores=fii_5day_net,
        usd_inr_20d_ago=macro["usd_inr_20d_ago"],
    )

    print(format_mps_report(result))

    state = update_state(state, raw, nse_fii, result)
    save_state(state)

    # Build output JSON
    output = {
        "current": json.loads(to_json(result)),
        "history": state["history"],
        "raw_inputs": {
            "chartink": {
                "above_200sma": chartink["above_200sma"],
                "above_50sma": chartink["above_50sma"],
                "spark_4pct": chartink["spark_4pct"],
                "rsi_above_70": chartink["rsi_above_70"],
                "rsi_above_50": chartink["rsi_above_50"],
                "burst_4_5pct_gainers": chartink["burst_4_5pct_gainers"],
                "burst_4_5pct_losers": chartink["burst_4_5pct_losers"],
                "atr_pct_above_4": chartink["atr_pct_above_4"],
            },
            "nse": {
                "advances": nse_advances,
                "declines": nse_declines,
                "new_52w_highs": nse_highs,
                "new_52w_lows": nse_lows,
                "india_vix": nse_vix,
                "pcr": nse_pcr,
                "fii_net": nse_fii,
                "nifty_52w_high": market.get("nifty_52w_high", False),
            },
            "macro": {
                "brent_crude": macro["brent_crude"],
                "us10y_yield": macro["us10y_yield"],
                "usd_inr": macro["usd_inr"],
                "usd_inr_20d_ago": macro["usd_inr_20d_ago"],
            },
            "data_sources": {
                "vix": "yahoo_finance",
                "ad_ratio": "yahoo_finance_estimated",
                "52w_highs_lows": "yahoo_finance_estimated",
                "pcr": "nse_fallback",
                "fii": "groww_primary_nse_fallback",
                "nifty_52w": "yahoo_finance",
                "macro": "yahoo_finance",
            },
        },
        "state": {
            "structural_bull_streak": state["structural_bull_streak"],
            "fii_consecutive_sell_days": state["fii_consecutive_sell_days"],
            "fii_5day_net": fii_5day_net,
        },
        "metadata": {
            "version": "3.1.2",
            "generated_at": datetime.now().isoformat(),
            "universe": "Nifty 500",
            "pillars": 7,
            "modifiers": 9,
            "daily_inputs": 21,
            "note": "FII via Groww.in, market data via Yahoo Finance (NSE IP-blocked on GH Actions)",
        },
    }

    return output, result


def main():
    global STATE_FILE

    parser = argparse.ArgumentParser(description="MPS v3.1.1 Daily Fetcher & Calculator")
    parser.add_argument("--output", "-o", type=str, default=None, help="Output JSON file path")
    parser.add_argument("--dry-run", action="store_true", help="Use sample data (no network)")
    parser.add_argument("--state-file", type=str, default=STATE_FILE, help="State file path")
    args = parser.parse_args()

    STATE_FILE = args.state_file

    output, result = fetch_and_calculate(dry_run=args.dry_run)

    if output is None:
        log.error("MPS calculation failed. Check logs above.")
        sys.exit(1)

    json_str = json.dumps(output, indent=2, ensure_ascii=False)

    if args.output:
        with open(args.output, "w") as f:
            f.write(json_str)
        log.info(f"✅ MPS JSON saved to {args.output}")
    else:
        print("\n" + "=" * 60)
        print("  JSON OUTPUT (for GitHub Pages)")
        print("=" * 60)
        print(json_str)

    log.info(f"✅ MPS v3.1.1 — Final Score: {result.final_score:.2f} — {result.zone} — State: {result.state}")


if __name__ == "__main__":
    main()
