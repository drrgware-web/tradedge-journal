"""
================================================================================
MPS Data Fetcher — Automated EOD Data Collection
================================================================================
Fetches all 14 MPS data points from Chartink + NSE India, runs the MPS v2
engine, and outputs the result as JSON.

Data Sources:
  - Chartink (4 scanners): Structural, Breadth SMA, Spark, RSI
  - NSE India: A/D data, 52W Highs/Lows, VIX, PCR, FII flows

Usage:
  python mps_fetcher.py                    # Fetch + calculate + print JSON
  python mps_fetcher.py --output mps.json  # Save to file
  python mps_fetcher.py --dry-run          # Use sample data (no network)

Requirements:
  pip install requests beautifulsoup4
================================================================================
"""

import json
import os
import sys
import time
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# Import the MPS engine (must be in same directory or PYTHONPATH)
from mps_engine_v2 import RawMarketData, calculate_mps, format_mps_report, to_json

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("mps_fetcher")


# =============================================================================
# CONFIGURATION
# =============================================================================

# Chartink scanner configurations
# You need to create these scanners on chartink.com and note the scan IDs
# The scan clause is sent via POST to their API
CHARTINK_URL = "https://chartink.com/screener/process"
CHARTINK_BASE = "https://chartink.com/screener/"

# Scanner queries (Chartink scan clause format)
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
        "name": "4% Breakout Stocks",
        "scan_clause": '( {cash} ( [0] 5 minute close / [-1] 1 day close > 1.04 and [0] 5 minute volume > [0] 5 minute sma( volume,20 ) * 1.5 and latest "nifty 500" = 1 ) )',
    },
    "rsi_above_70": {
        "name": "RSI > 70 Stocks",
        "scan_clause": '( {cash} ( [0] 5 minute rsi( close,14 ) > 70 and latest "nifty 500" = 1 ) )',
    },
}

# NSE endpoints
NSE_BASE = "https://www.nseindia.com"
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
}

# Historical state file (tracks streaks and previous values)
STATE_FILE = "mps_state.json"


# =============================================================================
# CHARTINK FETCHER
# =============================================================================

def get_chartink_session():
    """Get a session with CSRF token from Chartink."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*",
        "X-Requested-With": "XMLHttpRequest",
    })

    # Get CSRF token
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
    """Run a Chartink scanner and return the count of matching stocks."""
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
    """Fetch all 4 Chartink scanners."""
    log.info("═══ CHARTINK DATA ═══")
    session = get_chartink_session()
    results = {}

    for key in SCANNERS:
        count = fetch_chartink_count(session, key)
        results[key] = count
        time.sleep(1.5)  # Be polite to Chartink

    return results


# =============================================================================
# NSE FETCHER
# =============================================================================

def get_nse_session():
    """Get a session with cookies from NSE."""
    session = requests.Session()
    session.headers.update(NSE_HEADERS)

    try:
        # Hit the main page first to get cookies
        resp = session.get(NSE_BASE, timeout=15)
        resp.raise_for_status()
        log.info("NSE session established")
    except Exception as e:
        log.error(f"Failed to establish NSE session: {e}")

    return session


def fetch_nse_market_status(session):
    """Fetch advance/decline data from NSE."""
    log.info("  Fetching: Market A/D data...")
    try:
        resp = session.get(
            f"{NSE_BASE}/api/market-data-pre-open?key=NIFTY%20500",
            timeout=15,
        )
        # Fallback: try the market turnover / equity market status
        if resp.status_code != 200:
            resp = session.get(f"{NSE_BASE}/api/equity-stockIndices?index=NIFTY%20500", timeout=15)

        resp.raise_for_status()
        data = resp.json()

        # Parse advance/decline from the response
        # NSE returns this in different formats depending on the endpoint
        advances = 0
        declines = 0
        unchanged = 0

        if "advance" in data:
            advances = int(data.get("advance", {}).get("advances", 0))
            declines = int(data.get("advance", {}).get("declines", 0))
            unchanged = int(data.get("advance", {}).get("unchanged", 0))
        elif "data" in data:
            # Count from individual stock data
            for stock in data["data"]:
                change = stock.get("pChange", 0) or stock.get("change", 0)
                if isinstance(change, str):
                    change = float(change) if change else 0
                if change > 0:
                    advances += 1
                elif change < 0:
                    declines += 1
                else:
                    unchanged += 1

        log.info(f"  → A/D: {advances}A / {declines}D / {unchanged}U")
        return advances, declines, unchanged

    except Exception as e:
        log.error(f"  ✗ Failed to fetch A/D data: {e}")
        return None, None, None


def fetch_nse_52w_highlow(session):
    """Fetch 52-week high/low counts from NSE."""
    log.info("  Fetching: 52-week Highs/Lows...")
    try:
        resp = session.get(f"{NSE_BASE}/api/live-analysis-52Week?type=high", timeout=15)
        resp.raise_for_status()
        highs_data = resp.json()
        highs = len(highs_data.get("data", []))

        time.sleep(0.5)

        resp = session.get(f"{NSE_BASE}/api/live-analysis-52Week?type=low", timeout=15)
        resp.raise_for_status()
        lows_data = resp.json()
        lows = len(lows_data.get("data", []))

        log.info(f"  → 52W: {highs} Highs, {lows} Lows")
        return highs, lows

    except Exception as e:
        log.error(f"  ✗ Failed to fetch 52W data: {e}")
        return None, None


def fetch_nse_vix(session):
    """Fetch India VIX from NSE."""
    log.info("  Fetching: India VIX...")
    try:
        resp = session.get(f"{NSE_BASE}/api/allIndices", timeout=15)
        resp.raise_for_status()
        data = resp.json()

        for idx in data.get("data", []):
            if "VIX" in idx.get("index", "").upper() or "VIX" in idx.get("indexSymbol", "").upper():
                vix = float(idx.get("last", 0) or idx.get("closePrice", 0))
                log.info(f"  → VIX: {vix:.2f}")
                return vix

        log.warning("  ✗ VIX not found in index data")
        return None

    except Exception as e:
        log.error(f"  ✗ Failed to fetch VIX: {e}")
        return None


def fetch_nse_pcr(session):
    """Fetch Nifty Put-Call Ratio from NSE option chain."""
    log.info("  Fetching: Nifty PCR...")
    try:
        resp = session.get(f"{NSE_BASE}/api/option-chain-indices?symbol=NIFTY", timeout=15)
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
        log.info(f"  → PCR: {pcr:.3f} (Put OI: {total_put_oi:,} / Call OI: {total_call_oi:,})")
        return round(pcr, 3)

    except Exception as e:
        log.error(f"  ✗ Failed to fetch PCR: {e}")
        return None


def fetch_nse_fii(session):
    """Fetch FII/FPI net buy/sell data from NSE."""
    log.info("  Fetching: FII/FPI flow...")
    try:
        resp = session.get(f"{NSE_BASE}/api/fiidiiActivity", timeout=15)
        resp.raise_for_status()
        data = resp.json()

        fii_net = 0
        for entry in data.get("data", []):
            category = entry.get("category", "").upper()
            if "FII" in category or "FPI" in category:
                buy = float(entry.get("buyValue", 0) or 0)
                sell = float(entry.get("sellValue", 0) or 0)
                fii_net = buy - sell  # in crores
                break

        log.info(f"  → FII Net: ₹{fii_net:,.2f} Cr")
        return round(fii_net, 2)

    except Exception as e:
        log.error(f"  ✗ Failed to fetch FII data: {e}")
        return None


def fetch_nifty_52w_check(session):
    """Check if Nifty is at/near its 52-week high."""
    log.info("  Fetching: Nifty 52W high check...")
    try:
        resp = session.get(f"{NSE_BASE}/api/allIndices", timeout=15)
        resp.raise_for_status()
        data = resp.json()

        for idx in data.get("data", []):
            if idx.get("index") == "NIFTY 50" or idx.get("indexSymbol") == "NIFTY 50":
                current = float(idx.get("last", 0))
                high_52w = float(idx.get("yearHigh", 0))
                is_at_high = current >= high_52w * 0.99  # within 1% of 52W high
                log.info(f"  → Nifty: {current:.2f} vs 52W High: {high_52w:.2f} → {'AT HIGH' if is_at_high else 'Below'}")
                return is_at_high

        return False

    except Exception as e:
        log.error(f"  ✗ Failed to check Nifty 52W: {e}")
        return False


def fetch_all_nse():
    """Fetch all NSE data points."""
    log.info("═══ NSE DATA ═══")
    session = get_nse_session()
    time.sleep(1)

    advances, declines, unchanged = fetch_nse_market_status(session)
    time.sleep(0.8)

    highs_52, lows_52 = fetch_nse_52w_highlow(session)
    time.sleep(0.8)

    vix = fetch_nse_vix(session)
    time.sleep(0.8)

    pcr = fetch_nse_pcr(session)
    time.sleep(0.8)

    fii_net = fetch_nse_fii(session)
    time.sleep(0.8)

    nifty_52w_high = fetch_nifty_52w_check(session)

    return {
        "advances": advances,
        "declines": declines,
        "unchanged": unchanged,
        "highs_52": highs_52,
        "lows_52": lows_52,
        "vix": vix,
        "pcr": pcr,
        "fii_net": fii_net,
        "nifty_52w_high": nifty_52w_high,
    }


# =============================================================================
# STATE MANAGEMENT (streaks + history)
# =============================================================================

def load_state(state_file=STATE_FILE):
    """Load the historical state (streaks, previous values)."""
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            return json.load(f)
    return {
        "structural_bull_streak": 0,
        "prev_pct_above_50sma": 50.0,
        "fii_consecutive_sell_days": 0,
        "fii_5day_history": [],  # last 5 days of FII net values
        "last_date": None,
        "history": [],  # list of daily MPS results
    }


def save_state(state, state_file=STATE_FILE):
    """Save updated state."""
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2)
    log.info(f"State saved to {state_file}")


def update_state(state, raw_data, fii_net_today, mps_result):
    """Update streaks and historical tracking."""
    pct_200 = (raw_data.stocks_above_200sma / raw_data.total_universe) * 100
    pct_50 = (raw_data.stocks_above_50sma / raw_data.total_universe) * 100

    # Structural bull streak
    if pct_200 > 50:
        state["structural_bull_streak"] += 1
    else:
        state["structural_bull_streak"] = 0

    # Previous breadth for divergence check
    state["prev_pct_above_50sma"] = pct_50

    # FII streak
    if fii_net_today is not None:
        if fii_net_today < 0:
            state["fii_consecutive_sell_days"] += 1
        else:
            state["fii_consecutive_sell_days"] = 0

        # 5-day rolling history
        state["fii_5day_history"].append(fii_net_today)
        if len(state["fii_5day_history"]) > 5:
            state["fii_5day_history"] = state["fii_5day_history"][-5:]

    state["last_date"] = raw_data.date

    # Append to history (keep last 90 days)
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
    """Main function: fetch data, calculate MPS, return result."""
    today = datetime.now().strftime("%Y-%m-%d")
    log.info(f"╔══════════════════════════════════════════╗")
    log.info(f"║  MPS DAILY CALCULATION — {today}     ║")
    log.info(f"╚══════════════════════════════════════════╝")

    # Load state
    state = load_state()

    if dry_run:
        log.info("DRY RUN — Using sample data")
        chartink = {"above_200sma": 142, "above_50sma": 120, "spark_4pct": 3, "rsi_above_70": 10}
        nse = {"advances": 130, "declines": 340, "unchanged": 30, "highs_52": 8, "lows_52": 95,
               "vix": 22.5, "pcr": 1.45, "fii_net": -2800, "nifty_52w_high": False}
    else:
        # Fetch from Chartink
        chartink = fetch_all_chartink()
        if any(v is None for v in chartink.values()):
            log.error("Some Chartink data failed to fetch. Aborting.")
            return None, None

        # Fetch from NSE
        nse = fetch_all_nse()
        critical_fields = ["vix", "pcr", "advances", "declines"]
        if any(nse.get(f) is None for f in critical_fields):
            log.error("Critical NSE data missing. Aborting.")
            return None, None

    # Handle None values with safe defaults
    nse_fii = nse.get("fii_net") or 0
    nse_highs = nse.get("highs_52") or 0
    nse_lows = nse.get("lows_52") or 0
    nse_advances = nse.get("advances") or 0
    nse_declines = nse.get("declines") or 0
    nse_unchanged = nse.get("unchanged") or 0
    nse_vix = nse.get("vix") or 15.0
    nse_pcr = nse.get("pcr") or 1.0

    # Build RawMarketData
    raw = RawMarketData(
        date=today,
        stocks_above_200sma=chartink["above_200sma"],
        stocks_above_50sma=chartink["above_50sma"],
        advances=nse_advances,
        declines=nse_declines,
        unchanged=nse_unchanged,
        stocks_up_4pct=chartink["spark_4pct"],
        new_52w_highs=nse_highs,
        new_52w_lows=nse_lows,
        india_vix=nse_vix,
        pcr=nse_pcr,
        stocks_rsi_above_70=chartink["rsi_above_70"],
        nifty_at_52w_high=nse.get("nifty_52w_high", False),
        fii_net_buy_crores=nse_fii,
    )

    # Calculate FII 5-day net
    fii_5day = state.get("fii_5day_history", [])
    fii_5day_with_today = fii_5day + [nse_fii]
    fii_5day_net = sum(fii_5day_with_today[-5:])

    # Calculate MPS
    log.info("═══ CALCULATING MPS ═══")
    result = calculate_mps(
        raw,
        structural_bull_streak_days=state.get("structural_bull_streak", 0),
        prev_pct_above_50sma=state.get("prev_pct_above_50sma", 50.0),
        fii_net_consecutive_sell_days=state.get("fii_consecutive_sell_days", 0),
        fii_5day_net_crores=fii_5day_net,
    )

    # Print report
    print(format_mps_report(result))

    # Update state
    state = update_state(state, raw, nse_fii, result)
    save_state(state)

    # Build output JSON (includes history for the chart)
    output = {
        "current": json.loads(to_json(result)),
        "history": state["history"],
        "raw_inputs": {
            "chartink": chartink,
            "nse": {k: v for k, v in nse.items() if v is not None},
        },
        "state": {
            "structural_bull_streak": state["structural_bull_streak"],
            "fii_consecutive_sell_days": state["fii_consecutive_sell_days"],
            "fii_5day_net": fii_5day_net,
        },
        "metadata": {
            "version": "2.0",
            "generated_at": datetime.now().isoformat(),
            "universe": "Nifty 500",
        },
    }

    return output, result


def main():
    global STATE_FILE

    parser = argparse.ArgumentParser(description="MPS Daily Fetcher & Calculator")
    parser.add_argument("--output", "-o", type=str, default=None, help="Output JSON file path")
    parser.add_argument("--dry-run", action="store_true", help="Use sample data (no network)")
    parser.add_argument("--state-file", type=str, default=STATE_FILE, help="State file path")
    args = parser.parse_args()

    STATE_FILE = args.state_file

    output, result = fetch_and_calculate(dry_run=args.dry_run)

    if output is None:
        log.error("MPS calculation failed. Check logs above.")
        sys.exit(1)

    # Output JSON
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

    log.info(f"✅ MPS v2.0 — Final Score: {result.final_score:.2f} — {result.zone}")


if __name__ == "__main__":
    main()
