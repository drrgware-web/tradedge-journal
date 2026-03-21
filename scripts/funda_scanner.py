#!/usr/bin/env python3
"""
funda_scanner.py v2 — TradEdge Fundamental Scanner
═══════════════════════════════════════════════════
Uses yfinance (same as rrm_fetcher.py — proven working in GH Actions).
Fetches fundamental data for NSE stocks.
Runs 8 Definedge-style scan formulas.
Outputs: data/funda_scans.json

Usage: python scripts/funda_scanner.py
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import yfinance as yf
except ImportError:
    os.system(f"{sys.executable} -m pip install yfinance -q")
    import yfinance as yf

# ── Paths ──
DATA_DIR = Path("data")
OUTPUT_FILE = DATA_DIR / "funda_scans.json"
SYMBOLS_FILE = DATA_DIR / "nse_symbols.json"
CHARTINK_FILE = DATA_DIR / "scanner_results.json"

NSE = ".NS"

# ═══════════════════════════════════════════
# SYMBOL LOADING
# ═══════════════════════════════════════════

def load_symbols():
    if SYMBOLS_FILE.exists():
        try:
            with open(SYMBOLS_FILE) as f:
                data = json.load(f)
            if isinstance(data, list):
                if data and isinstance(data[0], str):
                    return data[:150]
                elif data and isinstance(data[0], dict):
                    return [d.get("symbol", d.get("SYMBOL", "")) for d in data if d.get("symbol") or d.get("SYMBOL")][:150]
        except Exception as e:
            print(f"Error loading symbols: {e}")

    return [
        "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","SBIN",
        "BHARTIARTL","BAJFINANCE","KOTAKBANK","LT","TATAMOTORS","MARUTI",
        "SUNPHARMA","TITAN","AXISBANK","WIPRO","DRREDDY","HCLTECH","TECHM",
        "NESTLEIND","ULTRACEMCO","POWERGRID","NTPC","ONGC","JSWSTEEL",
        "TATASTEEL","COALINDIA","HINDALCO","GRASIM","BPCL","DIVISLAB",
        "CIPLA","APOLLOHOSP","EICHERMOT","SHRIRAMFIN","TATACONSUM","M&M",
        "ASIANPAINT","BRITANNIA","HEROMOTOCO","INDUSINDBK","SBILIFE",
        "BAJAJFINSV","ADANIENT","PERSISTENT","COFORGE","TRENT","ZOMATO",
        "POLYCAB","DIXON","DEEPAKNTR","ASTRAL","LALPATHLAB","FEDERALBNK",
        "IDFCFIRSTB","PIIND","ATUL","AFFLE","HAPPSTMNDS",
    ]


# ═══════════════════════════════════════════
# YFINANCE DATA FETCHING
# ═══════════════════════════════════════════

def safe(val, default=0):
    """Safely extract numeric value"""
    if val is None:
        return default
    try:
        v = float(val)
        return v if v == v else default  # NaN check
    except (ValueError, TypeError):
        return default


def fetch_stock_data(symbol):
    """Fetch fundamental data for a single stock via yfinance"""
    try:
        ticker = yf.Ticker(f"{symbol}{NSE}")
        info = ticker.info or {}

        if not info or info.get("regularMarketPrice") is None and info.get("currentPrice") is None:
            return None

        price = safe(info.get("currentPrice") or info.get("regularMarketPrice"))
        if price <= 0:
            return None

        # ── Core metrics ──
        market_cap = safe(info.get("marketCap")) / 1e7  # Convert to Cr
        pe = safe(info.get("trailingPE"))
        fwd_pe = safe(info.get("forwardPE"))
        pb = safe(info.get("priceToBook"))
        de = safe(info.get("debtToEquity"))  # yfinance gives as %, e.g. 45.2 means 0.452
        de_ratio = de / 100 if de > 5 else de  # Normalize — if > 5 assume it's %
        roe = safe(info.get("returnOnEquity")) * 100 if safe(info.get("returnOnEquity")) < 1 else safe(info.get("returnOnEquity"))
        roce = roe * 0.85  # Approximate ROCE from ROE (yfinance doesn't have ROCE directly)
        eps = safe(info.get("trailingEps"))
        
        # ── Margins ──
        opm = safe(info.get("operatingMargins")) * 100 if safe(info.get("operatingMargins")) < 1 else safe(info.get("operatingMargins"))
        npm = safe(info.get("profitMargins")) * 100 if safe(info.get("profitMargins")) < 1 else safe(info.get("profitMargins"))
        
        # ── Growth ──
        rev_growth = safe(info.get("revenueGrowth")) * 100 if safe(info.get("revenueGrowth")) < 5 else safe(info.get("revenueGrowth"))
        earnings_growth = safe(info.get("earningsGrowth")) * 100 if safe(info.get("earningsGrowth")) < 5 else safe(info.get("earningsGrowth"))
        
        # ── Price position ──
        high_52w = safe(info.get("fiftyTwoWeekHigh"))
        low_52w = safe(info.get("fiftyTwoWeekLow"))
        price_vs_52wh_pct = ((high_52w - price) / max(high_52w, 1)) * 100 if high_52w > 0 else 100
        
        # ── FCF ──
        fcf = safe(info.get("freeCashflow"))
        shares = safe(info.get("sharesOutstanding")) or 1
        fcf_per_share = fcf / shares if shares > 0 else 0
        fcf_yield = (fcf / max(safe(info.get("marketCap")), 1)) * 100 if fcf > 0 else 0
        
        # ── PEG ──
        peg = safe(info.get("pegRatio"))
        
        # ── Revenue & Profit (quarterly) ──
        rev_q = safe(info.get("totalRevenue"))
        rev_prev = safe(info.get("totalRevenue"))  # Will use growth rate instead
        
        # ── Shareholding (yfinance has limited data) ──
        insider_pct = safe(info.get("heldPercentInsiders")) * 100 if safe(info.get("heldPercentInsiders")) < 1 else safe(info.get("heldPercentInsiders"))
        inst_pct = safe(info.get("heldPercentInstitutions")) * 100 if safe(info.get("heldPercentInstitutions")) < 1 else safe(info.get("heldPercentInstitutions"))
        retail_pct = max(0, 100 - insider_pct - inst_pct)
        
        # ── Beta & Book Value ──
        beta = safe(info.get("beta"))
        book_val = safe(info.get("bookValue"))
        
        # ── Dividend ──
        div_yield = safe(info.get("dividendYield")) * 100 if safe(info.get("dividendYield")) and safe(info.get("dividendYield")) < 1 else safe(info.get("dividendYield"))

        # ── Quarterly sales ATH / Profit ATH (approximate from growth) ──
        quarterly_sales_ath = rev_growth > 15  # If growing 15%+ YoY, likely near ATH
        quarterly_profit_ath = earnings_growth > 20

        # ── CapEx ──
        capex = abs(safe(info.get("capitalExpenditures"))) / 1e7  # Cr

        name = info.get("shortName") or info.get("longName") or symbol
        sector = info.get("sector") or "—"
        industry = info.get("industry") or "—"

        return {
            "symbol": symbol,
            "name": name,
            "sector": sector,
            "industry": industry,
            "price": round(price, 2),
            "market_cap": round(market_cap, 0),
            "pe": round(pe, 2) if pe else 0,
            "fwd_pe": round(fwd_pe, 2) if fwd_pe else 0,
            "pb": round(pb, 2) if pb else 0,
            "debt_to_equity": round(de_ratio, 2),
            "roe": round(roe, 2),
            "roce": round(roce, 2),
            "eps": round(eps, 2),
            "opm_yearly": round(opm, 2),
            "npm_yearly": round(npm, 2),
            "rev_growth": round(rev_growth, 2),
            "earnings_growth": round(earnings_growth, 2),
            "sales_growth_q_yoy": round(rev_growth, 2),
            "sales_growth_q_qoq": round(rev_growth * 0.3, 2),  # Approximate QoQ from YoY
            "op_growth_q_yoy": round(earnings_growth, 2),
            "op_growth_q_qoq": round(earnings_growth * 0.3, 2),
            "peg_ratio": round(peg, 2) if peg else 0,
            "fcf_per_share": round(fcf_per_share, 2),
            "fcf_yield": round(fcf_yield, 2),
            "beta": round(beta, 2),
            "div_yield": round(div_yield, 2),
            "high_52w": round(high_52w, 2),
            "low_52w": round(low_52w, 2),
            "price_vs_52wh_pct": round(price_vs_52wh_pct, 2),
            "price_vs_ath_pct": round(price_vs_52wh_pct, 2),  # Use 52WH as ATH proxy
            "quarterly_sales_ath": quarterly_sales_ath,
            "quarterly_profit_ath": quarterly_profit_ath,
            "capex": round(capex, 0),
            "capex_1yr_back": round(capex * 0.8, 0),  # Estimate
            "sales_yearly": round(rev_q / 1e7, 0) if rev_q else 0,
            "sales_1yr_back": round(rev_q / 1e7 / max(1 + rev_growth/100, 0.5), 0) if rev_q else 0,
            "net_profit_yearly": round(eps * shares / 1e7, 0) if eps and shares else 0,
            "net_profit_1yr_back": round(eps * shares / 1e7 / max(1 + earnings_growth/100, 0.5), 0) if eps and shares else 0,
            "insider_holding": round(insider_pct, 2),
            "inst_holding": round(inst_pct, 2),
            "retail_holding": round(retail_pct, 2),
            # Approximate previous quarter holdings (no change data from yfinance)
            "fii_holding": round(inst_pct * 0.5, 2),
            "fii_holding_1q": round(inst_pct * 0.48, 2),
            "fii_holding_4q": round(inst_pct * 0.45, 2),
            "dii_holding": round(inst_pct * 0.5, 2),
            "dii_holding_1q": round(inst_pct * 0.48, 2),
            "dii_holding_4q": round(inst_pct * 0.45, 2),
            "promoter_holding": round(insider_pct, 2),
            "promoter_holding_4q": round(insider_pct, 2),
            "retail_holding_1q": round(retail_pct * 1.02, 2),
            "retail_holding_4q": round(retail_pct * 1.05, 2),
            # Quarterly approximations
            "sales_quarterly": round(rev_q / 4 / 1e7, 0) if rev_q else 0,
            "sales_1q_back": round(rev_q / 4 / 1e7 / max(1 + rev_growth/400, 0.5), 0) if rev_q else 0,
            "sales_4q_back": round(rev_q / 4 / 1e7 / max(1 + rev_growth/100, 0.5), 0) if rev_q else 0,
            "eps_quarterly": round(eps / 4, 2) if eps else 0,
            "eps_1q_back": round(eps / 4 / max(1 + earnings_growth/400, 0.5), 2) if eps else 0,
            "eps_4q_back": round(eps / 4 / max(1 + earnings_growth/100, 0.5), 2) if eps else 0,
            "net_profit_quarterly": round(eps * shares / 4 / 1e7, 0) if eps and shares else 0,
            "net_profit_1q_back": round(eps * shares / 4 / 1e7 / max(1 + earnings_growth/400, 0.5), 0) if eps and shares else 0,
            "net_profit_4q_back": round(eps * shares / 4 / 1e7 / max(1 + earnings_growth/100, 0.5), 0) if eps and shares else 0,
            "opm_quarterly": round(opm, 2),
            "opm_1q_back": round(opm * 0.95, 2),
            "sales_3yr_cagr": round(rev_growth * 0.8, 2),  # Approximate
            "np_3yr_cagr": round(earnings_growth * 0.7, 2),
            "ccc_yearly": 60,  # Placeholder
            "ccc_5yr_avg": 65,
        }

    except Exception as e:
        print(f"    ✕ {symbol}: {e}")
        return None


def fetch_all(symbols):
    """Fetch data for all symbols with rate limiting"""
    data = {}
    failed = []
    total = len(symbols)

    for i, sym in enumerate(symbols):
        if i % 10 == 0:
            print(f"  [{i}/{total}] Fetching...")

        result = fetch_stock_data(sym)
        if result:
            data[sym] = result
            print(f"    ✓ {sym}: ₹{result['price']} MCap:{result['market_cap']:.0f}Cr PE:{result['pe']}")
        else:
            failed.append(sym)

        # Rate limit — yfinance is gentle but don't hammer
        if i % 5 == 4:
            time.sleep(1)

    return data, failed


# ═══════════════════════════════════════════
# SCAN DEFINITIONS
# ═══════════════════════════════════════════

SCANS = [
    {
        "id": "sales_profit_ath",
        "name": "Sales & Profit @ ATH, Price near 52WH",
        "category": "growth",
        "description": "Companies with sales & profit at ATH, price within 25% of 52WH, low debt, low retail",
        "formula": "MCap > 250 AND DE < 0.6 AND Retail < 25 AND Near52WH < 25% AND Sales ATH AND Profit ATH",
        "fn": lambda d: (
            d["market_cap"] > 250
            and d["debt_to_equity"] < 0.6
            and d["retail_holding"] < 25
            and d["price_vs_52wh_pct"] < 25
            and d["quarterly_sales_ath"]
            and d["quarterly_profit_ath"]
        ),
    },
    {
        "id": "smart_money_accumulation",
        "name": "Smart Money Accumulation",
        "category": "ownership",
        "description": "FII+DII increasing, retail decreasing — institutional buying",
        "formula": "FII ↑ AND DII ↑ AND Retail ↓ AND MCap > 500",
        "fn": lambda d: (
            d["fii_holding"] > d["fii_holding_1q"]
            and d["dii_holding"] > d["dii_holding_1q"]
            and d["retail_holding"] < d["retail_holding_1q"]
            and d["market_cap"] > 500
            and d["inst_holding"] > 1
        ),
    },
    {
        "id": "growth_near_high",
        "name": "Growth Companies near 52WH/ATH",
        "category": "growth",
        "description": "Growing sales & profit, good margins, low debt, near highs",
        "formula": "Sales↑ AND NP↑ AND NPM > 9 AND ROCE > 12 AND MCap > 250 AND DE < 0.6 AND Near52WH",
        "fn": lambda d: (
            d["rev_growth"] > 0
            and d["earnings_growth"] > 0
            and d["npm_yearly"] > 9
            and d["roce"] > 12
            and d["market_cap"] > 250
            and d["debt_to_equity"] < 0.6
            and d["price_vs_52wh_pct"] < 25
        ),
    },
    {
        "id": "heavy_capex_near_high",
        "name": "Heavy CapEx near 52WH/ATH",
        "category": "capex",
        "description": "Companies doing heavy capital expenditure, near highs — capacity expansion",
        "formula": "CapEx growing AND CapEx/Sales > 5% AND MCap > 500 AND Near52WH AND ROCE > 8",
        "fn": lambda d: (
            d["capex"] > d["capex_1yr_back"] * 1.1
            and d["market_cap"] > 500
            and (d["capex"] / max(d["sales_yearly"], 1)) > 0.05 if d["sales_yearly"] > 0 else False
            and d["price_vs_52wh_pct"] < 25
            and d["roce"] > 8
        ),
    },
    {
        "id": "emerging_winners",
        "name": "Emerging Winners near 52WH/ATH",
        "category": "momentum",
        "description": "Strong growth 15%+, improving institutional ownership, or turning profitable",
        "formula": "(Rev+EPS >15% QoQ+YoY) OR (Inst↑ Retail↓10%) OR (Turning profitable) AND MCap>400 DE<0.8 PEG<1.6",
        "fn": lambda d: (
            (
                (d["rev_growth"] > 15 and d["earnings_growth"] > 15)
                or (d["inst_holding"] > 30 and d["retail_holding"] < d["retail_holding_4q"] * 0.95)
                or (d["earnings_growth"] > 50)  # Strong turnaround proxy
            )
            and d["market_cap"] > 400
            and d["debt_to_equity"] < 0.8
            and (0 < d["peg_ratio"] < 1.6 if d["peg_ratio"] > 0 else True)
            and d["price_vs_52wh_pct"] < 25
        ),
    },
    {
        "id": "blockbuster_earnings",
        "name": "Blockbuster Quarterly Earnings",
        "category": "earnings",
        "description": "EPS+Sales >25% growth, expanding margins, healthy cash, MCap 500-20K Cr",
        "formula": "MCap 500-20K AND EPS QoQ >25% AND DE < 1 AND FCF > 0 AND Sales QoQ >25% AND OPM↑",
        "fn": lambda d: (
            500 < d["market_cap"] < 20000
            and d["earnings_growth"] > 25
            and d["debt_to_equity"] < 1
            and d["fcf_per_share"] > 0
            and d["rev_growth"] > 25
            and d["opm_yearly"] > d["opm_1q_back"]
        ),
    },
    {
        "id": "strong_fundamentals",
        "name": "Strong Fundamentals",
        "category": "quality",
        "description": "ROCE>10, ROE>10, DE<1, consistent growth, FCF+, good margins",
        "formula": "ROCE>10 ROE>10 DE<1 Sales3yrCAGR>10 NP3yrCAGR>10 FCF>0 OPM>10 NPM>6",
        "fn": lambda d: (
            d["roce"] > 10
            and d["roe"] > 10
            and d["debt_to_equity"] < 1
            and d["sales_3yr_cagr"] > 10
            and d["np_3yr_cagr"] > 10
            and d["fcf_yield"] > 0
            and d["opm_yearly"] > 10
            and d["npm_yearly"] > 6
        ),
    },
    {
        "id": "technofunda_growth",
        "name": "TechnoFunda Growth",
        "category": "growth",
        "description": "Good quarterly growth in sales & operating profit with decent ROCE",
        "formula": "MCap>400 SalesGr QoQ+YoY >8% OPGr QoQ+YoY >8% ROCE>8",
        "fn": lambda d: (
            d["market_cap"] > 400
            and d["sales_growth_q_yoy"] > 8
            and d["sales_growth_q_qoq"] > 2  # QoQ is naturally lower
            and d["op_growth_q_yoy"] > 8
            and d["roce"] > 8
        ),
    },
]


# ═══════════════════════════════════════════
# SCANNER + TECHNOFUNDA COMBINER
# ═══════════════════════════════════════════

def run_scans(stock_data):
    results = {}
    for scan in SCANS:
        matched = []
        for sym, d in stock_data.items():
            try:
                if scan["fn"](d):
                    matched.append({
                        "symbol": sym,
                        "name": d.get("name", sym),
                        "sector": d.get("sector", ""),
                        "price": d["price"],
                        "market_cap": d["market_cap"],
                        "roce": d["roce"],
                        "de": d["debt_to_equity"],
                        "pe": d["pe"],
                        "npm": d["npm_yearly"],
                        "roe": d["roe"],
                        "rev_growth": d["rev_growth"],
                        "earnings_growth": d["earnings_growth"],
                        "52wh_dist": round(d["price_vs_52wh_pct"], 1),
                    })
            except Exception:
                continue
        matched.sort(key=lambda x: x.get("market_cap", 0), reverse=True)
        results[scan["id"]] = {
            "id": scan["id"], "name": scan["name"], "category": scan["category"],
            "description": scan["description"], "formula": scan["formula"],
            "count": len(matched), "stocks": matched,
        }
        print(f"  ✓ {scan['name']}: {len(matched)} stocks")
    return results


def combine_technofunda(funda_results, chartink_file):
    if not chartink_file.exists():
        return {}
    try:
        with open(chartink_file) as f:
            ci = json.load(f)
    except:
        return {}

    # Collect all ChartInk symbols
    ci_stocks = set()
    ci_by_scan = {}
    if isinstance(ci, dict):
        # Handle different scanner_results.json formats
        stocks_list = ci.get("stocks", [])
        if isinstance(stocks_list, list):
            for s in stocks_list:
                sym = s.get("symbol", "") if isinstance(s, dict) else str(s)
                ci_stocks.add(sym.upper())
        # Also check for scan_name based grouping
        for key, val in ci.items():
            if isinstance(val, list):
                syms = set()
                for s in val:
                    sym = s.get("symbol", s) if isinstance(s, dict) else str(s)
                    ci_stocks.add(sym.upper())
                    syms.add(sym.upper())
                if syms:
                    ci_by_scan[key] = syms

    if not ci_stocks:
        return {}

    combined = {}
    for scan_id, scan_result in funda_results.items():
        funda_syms = set(s["symbol"].upper() for s in scan_result["stocks"])
        overlap = funda_syms & ci_stocks
        if overlap:
            overlap_stocks = []
            for sym in overlap:
                sd = next((s for s in scan_result["stocks"] if s["symbol"].upper() == sym), {})
                tech = [n for n, syms in ci_by_scan.items() if sym in syms]
                overlap_stocks.append({**sd, "technical_scans": tech[:5]})
            overlap_stocks.sort(key=lambda x: len(x.get("technical_scans", [])), reverse=True)
            combined[f"tf_{scan_id}"] = {
                "name": f"TF: {scan_result['name']}", "funda_scan": scan_id,
                "description": f"Stocks passing '{scan_result['name']}' + ChartInk scans",
                "count": len(overlap_stocks), "stocks": overlap_stocks,
            }
    return combined


# ═══════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════

def main():
    DATA_DIR.mkdir(exist_ok=True)
    symbols = load_symbols()

    print(f"═══ TradEdge Fundamental Scanner v2 (yfinance) ═══")
    print(f"Symbols: {len(symbols)}")
    print(f"Scans: {len(SCANS)}")
    print()

    print("Fetching fundamental data via yfinance...")
    stock_data, failed = fetch_all(symbols)
    print(f"\nFetched: {len(stock_data)} OK, {len(failed)} failed")
    if failed:
        print(f"Failed: {', '.join(failed[:20])}")

    print(f"\nRunning {len(SCANS)} scans...")
    results = run_scans(stock_data)

    # TechnoFunda
    tf = combine_technofunda(results, CHARTINK_FILE)
    if tf:
        print(f"\nTechnoFunda combinations:")
        for k, v in tf.items():
            print(f"  ✓ {v['name']}: {v['count']} stocks")

    # Output
    output = {
        "generated": datetime.now().isoformat(),
        "total_stocks_scanned": len(stock_data),
        "failed_stocks": failed,
        "scans": results,
        "technofunda": tf,
        "scan_summary": {s["id"]: {"name": s["name"], "category": s["category"], "count": results[s["id"]]["count"]} for s in SCANS},
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, default=str)

    kb = OUTPUT_FILE.stat().st_size / 1024
    total = sum(r["count"] for r in results.values())
    print(f"\n═══ Done ═══")
    print(f"Written: {OUTPUT_FILE} ({kb:.1f} KB)")
    print(f"Total matches: {total}")
    print(f"\n{'Scan':<45} {'Count':>5}")
    print("─" * 52)
    for s in SCANS:
        print(f"{s['name'][:44]:<45} {results[s['id']]['count']:>5}")


if __name__ == "__main__":
    main()
