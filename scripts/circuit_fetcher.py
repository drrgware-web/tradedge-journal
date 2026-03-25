#!/usr/bin/env python3
"""
TradEdge Circuit Limit Fetcher v1.0
Fetches real circuit limit bands from NSE for all stocks.
Outputs: data/circuit_limits.json

Sources:
  1. NSE CM-UDiFF (preferred) — daily price bands CSV
  2. NSE Bhavcopy — upper/lower circuit prices
  3. Fallback — F&O stocks = No Band, others = heuristic from market cap
"""

import json, os, sys, time, csv, io
from datetime import datetime, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    os.system("pip install requests --break-system-packages -q")
    import requests

DATA_DIR = Path("data")
CIRCUIT_FILE = DATA_DIR / "circuit_limits.json"
SCANNER_FILE = DATA_DIR / "scanner_results.json"

# NSE headers to avoid blocking
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.nseindia.com/",
}

# Known F&O stocks — these have NO circuit limit
FNO_STOCKS = {
    "RELIANCE","HDFCBANK","ICICIBANK","SBIN","TCS","INFY","BHARTIARTL","HINDUNILVR",
    "ITC","KOTAKBANK","LT","AXISBANK","BAJFINANCE","MARUTI","SUNPHARMA","TITAN",
    "ASIANPAINT","WIPRO","HCLTECH","TATAMOTORS","M&M","NTPC","POWERGRID",
    "ULTRACEMCO","BAJAJFINSV","NESTLEIND","JSWSTEEL","TATASTEEL","ONGC","COALINDIA",
    "TECHM","DRREDDY","CIPLA","HEROMOTOCO","EICHERMOT","BPCL","HINDALCO","GRASIM",
    "DIVISLAB","BRITANNIA","ADANIENT","ADANIPORTS","DLF","INDIGO","ZOMATO","HAL",
    "BEL","TATAPOWER","RECLTD","PFC","NHPC","IRFC","JINDALSTEL","VEDL","NMDC",
    "SAIL","TRENT","BAJAJ-AUTO","APOLLOHOSP","GODREJPROP","SBILIFE","HDFCLIFE",
    "SHRIRAMFIN","CHOLAFIN","PIDILITIND","BERGEPAINT","HAVELLS","DABUR","MARICO",
    "COLPAL","TATACONSUM","LUPIN","BIOCON","AUROPHARMA","TORNTPHARM","DMART",
    "POLYCAB","DIXON","ABB","SIEMENS","CUMMINSIND","PERSISTENT","LTIM","MPHASIS",
    "COFORGE","NAUKRI","AMBUJACEM","ACC","DALMIACEM","JKCEMENT","RAMCOCEM",
    "MUTHOOTFIN","MANAPPURAM","PVRINOX","VOLTAS","CROMPTON","IRCTC","RVNL","BHEL",
    "BANKBARODA","PNB","CANBK","IDFCFIRSTB","FEDERALBNK","BANDHANBNK","AUBANK",
    "MRF","BALKRISIND","APOLLOTYRE","EXIDEIND","MOTHERSON","IOC","HPCL","GAIL",
    "PETRONET","CONCOR","BOSCHLTD","MAXHEALTH","LICI","PAYTM","SBICARD",
    "INDUSTOWER","INDHOTEL","OBEROIRLTY","PHOENIXLTD","PRESTIGE","PAGEIND",
    "WHIRLPOOL","BATAINDIA","MFSL","LICHSGFIN","CANFINHOME","ABCAPITAL",
    "TATACHEM","PIIND","ATUL","DEEPAKNTR","NAVINFLUOR","SRF","ASTRAL",
    "SUPREMEIND","KPITTECH","LTTS","TATAELXSI","SONACOMS","LALPATHLAB",
    "METROPOLIS","IPCALAB","ALKEM","LAURUSLABS","GRANULES","NATCOPHARMA",
    "ESCORTS","ASHOKLEY","TVSMOTOR","BHARATFORG","SUNTV","ABFRL","JUBLFOOD",
    "ZYDUSLIFE","GLENMARK","HINDPETRO","IDEA","GMRAIRPORT","FACT",
    "JSWENERGY","ADANIGREEN","ADANIPOWER","ATGL","AWL","SJVN","HUDCO",
    "CESC","TORNTPOWER","NIACL","GICRE","STARHEALTH","ICICIPRULI",
    "HDFCAMC","ICICIGI","SBILIFE","MCX","BSE","CDSL","CAMS",
    "CGPOWER","KAYNES","AFFLE","CLEAN","ROUTE","SOLARINDS",
}


def get_nse_session():
    """Create a requests session with NSE cookies."""
    s = requests.Session()
    s.headers.update(NSE_HEADERS)
    try:
        s.get("https://www.nseindia.com", timeout=10)
    except Exception:
        pass
    return s


def fetch_nse_price_bands(session):
    """
    Fetch circuit limits from NSE's security-wise price bands.
    URL: https://nsearchives.nseindia.com/content/nsccl/fao_participant_vol_{date}.csv
    Alt: https://www.nseindia.com/api/equity-stockIndices?csv=true&index=SECURITIES%20IN%20F%26O
    """
    stocks = {}

    # Method 1: Try NSE equity bhavcopy (has upper/lower circuit prices)
    today = datetime.now()
    for days_back in range(0, 5):
        d = today - timedelta(days=days_back)
        if d.weekday() >= 5:  # Skip weekends
            continue
        date_str = d.strftime("%d%m%Y")
        date_str2 = d.strftime("%d-%b-%Y").upper()

        # Try CM bhavcopy format
        urls = [
            f"https://nsearchives.nseindia.com/content/cm/BhsecCMALLDATA{date_str}.csv",
            f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv",
        ]

        for url in urls:
            try:
                r = session.get(url, timeout=15)
                if r.ok and len(r.text) > 1000:
                    reader = csv.DictReader(io.StringIO(r.text))
                    for row in reader:
                        sym = (row.get("SYMBOL") or row.get("TckrSymb") or "").strip()
                        series = (row.get("SERIES") or row.get("SctySrs") or "").strip()
                        if not sym or series not in ("EQ", "BE", "SM", "ST", ""):
                            continue

                        close = float(row.get("CLOSE_PRICE") or row.get("CLOSE") or row.get("ClsPric") or 0)
                        upper = float(row.get("HI_52_WK") or row.get("UPPER_LIMIT") or row.get("UpprPricBnd") or 0)
                        lower = float(row.get("LO_52_WK") or row.get("LOWER_LIMIT") or row.get("LwrPricBnd") or 0)

                        # Try price band columns specifically
                        if upper == 0:
                            upper = float(row.get("UpprPricBnd") or row.get("UPPER_BAND") or 0)
                        if lower == 0:
                            lower = float(row.get("LwrPricBnd") or row.get("LOWER_BAND") or 0)

                        if sym and close > 0:
                            band = determine_band(upper, lower, close)
                            stocks[sym] = {
                                "symbol": sym,
                                "band": band,
                                "upper_limit": round(upper, 2),
                                "lower_limit": round(lower, 2),
                                "close_price": round(close, 2),
                                "is_restricted": band in ("2%", "5%"),
                                "in_fno": sym in FNO_STOCKS,
                            }

                    if stocks:
                        print(f"  Loaded {len(stocks)} stocks from NSE bhavcopy ({url.split('/')[-1]})")
                        return stocks
            except Exception as e:
                continue

    return stocks


def determine_band(upper, lower, close):
    """Determine circuit band from price limits."""
    if close <= 0:
        return "Unknown"
    if upper <= 0 or lower <= 0:
        return "Unknown"

    upper_pct = abs((upper - close) / close * 100)
    lower_pct = abs((close - lower) / close * 100)
    band_pct = round(min(upper_pct, lower_pct))

    if band_pct <= 2:
        return "2%"
    elif band_pct <= 5:
        return "5%"
    elif band_pct <= 10:
        return "10%"
    elif band_pct <= 20:
        return "20%"
    else:
        return "No Band"


def load_all_symbols():
    """Load stock universe from scanner_results.json."""
    symbols = []
    if SCANNER_FILE.exists():
        try:
            data = json.loads(SCANNER_FILE.read_text())
            for s in data.get("all_stocks", []):
                sym = s.get("symbol", "")
                if sym:
                    symbols.append({
                        "symbol": sym,
                        "market_cap_cr": s.get("market_cap_cr") or 0,
                    })
        except Exception:
            pass
    return symbols


def estimate_band_from_mcap(symbol, market_cap_cr):
    """Heuristic fallback: estimate circuit band from market cap."""
    if symbol in FNO_STOCKS:
        return "No Band"
    if market_cap_cr >= 20000:
        return "20%"  # Large cap
    elif market_cap_cr >= 5000:
        return "20%"  # Mid cap
    elif market_cap_cr >= 1000:
        return "10%"  # Small cap
    elif market_cap_cr >= 100:
        return "5%"   # Micro cap likely restricted
    elif market_cap_cr > 0:
        return "5%"   # Very small
    return "Unknown"


def main():
    print("=" * 60)
    print("  TradEdge Circuit Limit Fetcher v1.0")
    print("=" * 60)

    # 1. Load stock universe
    print("\n[1/3] Loading stock universe...")
    all_stocks = load_all_symbols()
    print(f"  {len(all_stocks)} stocks loaded")

    # 2. Try to fetch real NSE data
    print("\n[2/3] Fetching NSE circuit limits...")
    session = get_nse_session()
    nse_data = fetch_nse_price_bands(session)
    print(f"  NSE data: {len(nse_data)} stocks")

    # 3. Build complete circuit data
    print("\n[3/3] Building circuit limits...")
    circuit = {}
    from_nse = 0
    from_fno = 0
    from_heuristic = 0

    # Load previous data for change detection
    prev_data = {}
    if CIRCUIT_FILE.exists():
        try:
            old = json.loads(CIRCUIT_FILE.read_text())
            prev_data = old.get("stocks", {})
        except Exception:
            pass

    for stock in all_stocks:
        sym = stock["symbol"]
        mcap = stock.get("market_cap_cr", 0)

        if sym in nse_data:
            # Real NSE data available
            circuit[sym] = nse_data[sym]
            from_nse += 1
        elif sym in FNO_STOCKS:
            # F&O stock — no circuit limit
            circuit[sym] = {
                "symbol": sym,
                "band": "No Band",
                "upper_limit": 0,
                "lower_limit": 0,
                "close_price": 0,
                "is_restricted": False,
                "in_fno": True,
            }
            from_fno += 1
        else:
            # Heuristic from market cap
            band = estimate_band_from_mcap(sym, mcap)
            circuit[sym] = {
                "symbol": sym,
                "band": band,
                "upper_limit": 0,
                "lower_limit": 0,
                "close_price": 0,
                "is_restricted": band in ("2%", "5%"),
                "in_fno": False,
            }
            from_heuristic += 1

        # Detect band changes
        if sym in prev_data:
            prev_band = prev_data[sym].get("band", "Unknown")
            curr_band = circuit[sym]["band"]
            if prev_band != curr_band and prev_band != "Unknown" and curr_band != "Unknown":
                circuit[sym]["band_changed"] = True
                circuit[sym]["prev_band"] = prev_band
                band_order = {"2%": 2, "5%": 5, "10%": 10, "20%": 20, "No Band": 100}
                prev_n = band_order.get(prev_band, 0)
                curr_n = band_order.get(curr_band, 0)
                circuit[sym]["change_direction"] = "tightened" if curr_n < prev_n else "loosened"

    # Count stats
    summary = {"2%": 0, "5%": 0, "10%": 0, "20%": 0, "No Band": 0, "Unknown": 0}
    for s in circuit.values():
        b = s.get("band", "Unknown")
        summary[b] = summary.get(b, 0) + 1

    restricted = [s for s, v in circuit.items() if v.get("is_restricted")]
    band_changes = [v for v in circuit.values() if v.get("band_changed")]

    # Save
    DATA_DIR.mkdir(exist_ok=True)
    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "total_stocks": len(circuit),
        "summary": summary,
        "restricted_count": len(restricted),
        "data_sources": {
            "nse_bhavcopy": from_nse,
            "fno_list": from_fno,
            "heuristic": from_heuristic,
        },
        "band_changes": band_changes,
        "stocks": circuit,
    }
    CIRCUIT_FILE.write_text(json.dumps(output, separators=(",", ":")))
    file_size = CIRCUIT_FILE.stat().st_size / 1024

    print(f"\n{'=' * 60}")
    print(f"  ✓ Circuit Limits Updated!")
    print(f"  Total: {len(circuit)} stocks")
    print(f"  Sources: NSE={from_nse}, F&O={from_fno}, Heuristic={from_heuristic}")
    print(f"  Bands: {summary}")
    print(f"  Restricted (2%/5%): {len(restricted)}")
    print(f"  Band changes: {len(band_changes)}")
    print(f"  File: {CIRCUIT_FILE} ({file_size:.0f} KB)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
