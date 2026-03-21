#!/usr/bin/env python3
"""
funda_scanner.py — TradEdge Fundamental Scanner Engine
═══════════════════════════════════════════════════════
Fetches fundamental data from Screener.in for NSE stocks.
Runs 8 Definedge-style scan formulas.
Outputs: data/funda_scans.json

Cross-references with ChartInk technical scan results (if available)
to produce TechnoFunda combined results.

Usage:
  python scripts/funda_scanner.py

Add to GitHub Actions (scanner-daily.yml):
  - name: Run Fundamental Scanner
    run: python scripts/funda_scanner.py
"""

import json
import os
import sys
import time
import re
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

# ── Paths ──
DATA_DIR = Path("data")
OUTPUT_FILE = DATA_DIR / "funda_scans.json"
SYMBOLS_FILE = DATA_DIR / "nse_symbols.json"
CHARTINK_FILE = DATA_DIR / "scanner_results.json"  # ChartInk technical scan results

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json, text/html",
}


# ═══════════════════════════════════════════
# SCAN DEFINITIONS (Definedge-style formulas)
# ═══════════════════════════════════════════

SCANS = [
    {
        "id": "sales_profit_ath",
        "name": "Sales @ ATH, Profit @ ATH, Price @ ATH/52WH",
        "category": "growth",
        "description": "Companies with sales and profit at all-time highs, price near ATH/52WH, low debt, low retail holding",
        "formula": "Market Cap > 250 AND DE < 0.6 AND Retail Holding < 25 AND (Price Within 52WH < 25 OR Price Within ATH < 25) AND Quarterly Sales ATH AND Quarterly Profit ATH",
        "conditions": lambda d: (
            d.get("market_cap", 0) > 250
            and d.get("debt_to_equity", 99) < 0.6
            and d.get("retail_holding", 100) < 25
            and (d.get("price_vs_52wh_pct", 100) < 25 or d.get("price_vs_ath_pct", 100) < 25)
            and d.get("quarterly_sales_ath", False)
            and d.get("quarterly_profit_ath", False)
        ),
    },
    {
        "id": "smart_money_accumulation",
        "name": "Smart Money Accumulation",
        "category": "ownership",
        "description": "FII & DII increasing holdings, retail decreasing — smart money moving in",
        "formula": "FII Holding > FII Holding 1Q Back AND FII > 1 AND MCap > 500 AND DII > DII 1Q Back AND DII > 1 AND Retail < Retail 1Q Back",
        "conditions": lambda d: (
            d.get("fii_holding", 0) > d.get("fii_holding_1q", 0)
            and d.get("fii_holding", 0) > 1
            and d.get("market_cap", 0) > 500
            and d.get("dii_holding", 0) > d.get("dii_holding_1q", 0)
            and d.get("dii_holding", 0) > 1
            and d.get("retail_holding", 100) < d.get("retail_holding_1q", 100)
        ),
    },
    {
        "id": "growth_near_high",
        "name": "Growth Companies @ near 25% 52WH/ATH",
        "category": "growth",
        "description": "Growing sales & profit, good margins, low debt, near highs",
        "formula": "Sales Yearly > Sales 1yr Back AND NP Yearly > NP 1yr Back AND NPM > 9 AND ROCE > 12 AND MCap > 250 AND DE < 0.6 AND Retail < 25 AND (Within 52WH < 25 OR Within ATH < 25)",
        "conditions": lambda d: (
            d.get("sales_yearly", 0) > d.get("sales_1yr_back", 0)
            and d.get("net_profit_yearly", 0) > d.get("net_profit_1yr_back", 0)
            and d.get("npm_yearly", 0) > 9
            and d.get("roce", 0) > 12
            and d.get("market_cap", 0) > 250
            and d.get("debt_to_equity", 99) < 0.6
            and d.get("retail_holding", 100) < 25
            and (d.get("price_vs_52wh_pct", 100) < 25 or d.get("price_vs_ath_pct", 100) < 25)
        ),
    },
    {
        "id": "heavy_capex_near_high",
        "name": "Heavy CapEx Companies @ near 52WH/ATH",
        "category": "capex",
        "description": "Companies doing heavy capital expenditure, near price highs — capacity expansion plays",
        "formula": "CapEx > CapEx 1yr Back * 1.2 AND CapEx/Sales > 0.05 AND MCap > 500 AND (Within 52WH < 25 OR Within ATH < 25) AND ROCE > 8",
        "conditions": lambda d: (
            d.get("capex", 0) > d.get("capex_1yr_back", 0) * 1.2
            and d.get("market_cap", 0) > 500
            and (d.get("capex", 0) / max(d.get("sales_yearly", 1), 1)) > 0.05
            and (d.get("price_vs_52wh_pct", 100) < 25 or d.get("price_vs_ath_pct", 100) < 25)
            and d.get("roce", 0) > 8
        ),
    },
    {
        "id": "emerging_winners",
        "name": "Emerging Winners @ 52WH/ATH",
        "category": "momentum",
        "description": "Consistent strong growth in sales/earnings, improving institutional ownership, turning profitable, financially healthy",
        "formula": "((Sales QoQ > 15% AND Sales YoY > 15% AND EPS QoQ > 15% AND EPS YoY > 15%) OR (Inst Holding Up AND Retail Down 10%+) OR (NP turning positive)) AND MCap > 400 AND DE < 0.8 AND PEG < 1.6 AND Near 52WH/ATH",
        "conditions": lambda d: (
            (
                # Strong growth path
                (
                    d.get("sales_quarterly", 0) > d.get("sales_1q_back", 1) * 1.15
                    and d.get("sales_quarterly", 0) > d.get("sales_4q_back", 1) * 1.15
                    and d.get("eps_quarterly", 0) > d.get("eps_1q_back", -999) * 1.15
                    and d.get("eps_quarterly", 0) > d.get("eps_4q_back", -999) * 1.15
                )
                or
                # Improving institutional ownership
                (
                    (
                        d.get("fii_holding", 0) + d.get("dii_holding", 0)
                        > d.get("fii_holding_4q", 0) + d.get("dii_holding_4q", 0)
                        or d.get("promoter_holding", 0) > d.get("promoter_holding_4q", 0)
                    )
                    and d.get("retail_holding", 100) < 0.9 * d.get("retail_holding_4q", 100)
                )
                or
                # Turning profitable
                (
                    (d.get("net_profit_quarterly", 0) > 0 and d.get("net_profit_1q_back", 0) < 0)
                    or (d.get("net_profit_quarterly", 0) > 0 and d.get("net_profit_4q_back", 0) < 0)
                )
            )
            and d.get("market_cap", 0) > 400
            and d.get("debt_to_equity", 99) < 0.8
            and 0 < d.get("peg_ratio", 99) < 1.6
            and (d.get("price_vs_52wh_pct", 100) < 25 or d.get("price_vs_ath_pct", 100) < 25)
        ),
    },
    {
        "id": "blockbuster_earnings",
        "name": "Blockbuster Quarterly Earnings",
        "category": "earnings",
        "description": "Growth companies with improving earnings, rising sales, expanding margins, healthy cash & debt",
        "formula": "MCap 500-20000 AND EPS QoQ > 25% AND DE < 1 AND FCF/share > 0 AND Sales QoQ > 25% AND OPM improving",
        "conditions": lambda d: (
            500 < d.get("market_cap", 0) < 20000
            and d.get("eps_quarterly", 0) > d.get("eps_1q_back", 1) * 1.25
            and d.get("debt_to_equity", 99) < 1
            and d.get("fcf_per_share", -1) > 0
            and d.get("sales_quarterly", 0) > d.get("sales_1q_back", 1) * 1.25
            and d.get("opm_quarterly", 0) > d.get("opm_1q_back", 0)
        ),
    },
    {
        "id": "strong_fundamentals",
        "name": "Strong Fundamentals",
        "category": "quality",
        "description": "ROCE > 10, ROE > 10, low debt, consistent growth, positive FCF, good margins",
        "formula": "ROCE > 10 AND ROE > 10 AND DE < 1 AND Sales 3yr CAGR > 10 AND NP 3yr CAGR > 10 AND CCC improving AND FCF Yield > 0 AND OPM > 10 AND NPM > 6",
        "conditions": lambda d: (
            d.get("roce", 0) > 10
            and d.get("roe", 0) > 10
            and d.get("debt_to_equity", 99) < 1
            and d.get("sales_3yr_cagr", 0) > 10
            and d.get("np_3yr_cagr", 0) > 10
            and d.get("ccc_yearly", 999) > d.get("ccc_5yr_avg", 999)  # improving = lower
            and d.get("fcf_yield", -1) > 0
            and d.get("opm_yearly", 0) > 10
            and d.get("npm_yearly", 0) > 6
        ),
    },
    {
        "id": "technofunda_growth",
        "name": "TechnoFunda Growth",
        "category": "growth",
        "description": "Good quarterly growth in sales & operating profit with decent ROCE",
        "formula": "MCap > 400 AND Sales Growth Q YoY > 8 AND Sales Growth Q QoQ > 8 AND OP Growth Q YoY > 8 AND OP Growth Q QoQ > 8 AND ROCE > 8",
        "conditions": lambda d: (
            d.get("market_cap", 0) > 400
            and d.get("sales_growth_q_yoy", 0) > 8
            and d.get("sales_growth_q_qoq", 0) > 8
            and d.get("op_growth_q_yoy", 0) > 8
            and d.get("op_growth_q_qoq", 0) > 8
            and d.get("roce", 0) > 8
        ),
    },
]


# ═══════════════════════════════════════
# DATA FETCHING — Screener.in
# ═══════════════════════════════════════

def fetch_screener_data(symbol, retries=2):
    """Fetch fundamental data for a stock from Screener.in"""
    url = f"https://www.screener.in/api/company/{symbol}/consolidated/"
    
    for attempt in range(retries):
        try:
            req = Request(url, headers=HEADERS)
            with urlopen(req, timeout=15) as resp:
                raw = json.loads(resp.read().decode())
            return parse_screener_response(symbol, raw)
        except HTTPError as e:
            if e.code == 404:
                # Try standalone (non-consolidated)
                try:
                    url2 = f"https://www.screener.in/api/company/{symbol}/"
                    req2 = Request(url2, headers=HEADERS)
                    with urlopen(req2, timeout=15) as resp2:
                        raw2 = json.loads(resp2.read().decode())
                    return parse_screener_response(symbol, raw2)
                except:
                    return None
            if attempt < retries - 1:
                time.sleep(2)
        except (URLError, Exception) as e:
            if attempt < retries - 1:
                time.sleep(2)
    return None


def safe_float(val, default=0):
    """Safely convert to float"""
    if val is None:
        return default
    try:
        if isinstance(val, str):
            val = val.replace(",", "").replace("%", "").strip()
        return float(val)
    except (ValueError, TypeError):
        return default


def parse_screener_response(symbol, raw):
    """Parse Screener.in API response into our flat format"""
    try:
        # Extract warehouse data (key ratios)
        wh = raw.get("warehouse_set", {})
        if isinstance(wh, list) and len(wh) > 0:
            wh = wh[0] if isinstance(wh[0], dict) else {}
        elif not isinstance(wh, dict):
            wh = {}

        # Number data
        nd = raw.get("number", {}) or {}

        # Quarterly results
        qr = raw.get("quarters", {}) or {}
        qr_data = qr.get("data", []) if isinstance(qr, dict) else []

        # Annual P&L
        annual = raw.get("annual", {}) or {}
        annual_data = annual.get("data", []) if isinstance(annual, dict) else []

        # Shareholding
        sh = raw.get("shareholding", {}) or {}
        sh_data = sh.get("data", []) if isinstance(sh, dict) else []

        # ── Extract quarterly figures ──
        sales_q = []
        np_q = []
        eps_q = []
        opm_q = []

        for row in qr_data:
            vals = row.get("values", []) if isinstance(row, dict) else []
            name = row.get("name", "") if isinstance(row, dict) else ""
            if "sales" in name.lower() or "revenue" in name.lower():
                sales_q = [safe_float(v) for v in vals[-8:]]
            elif "net profit" in name.lower():
                np_q = [safe_float(v) for v in vals[-8:]]
            elif "eps" in name.lower():
                eps_q = [safe_float(v) for v in vals[-8:]]
            elif "opm" in name.lower():
                opm_q = [safe_float(v) for v in vals[-8:]]

        # ── Extract annual figures ──
        sales_y = []
        np_y = []
        for row in annual_data:
            vals = row.get("values", []) if isinstance(row, dict) else []
            name = row.get("name", "") if isinstance(row, dict) else ""
            if "sales" in name.lower() or "revenue" in name.lower():
                sales_y = [safe_float(v) for v in vals[-5:]]
            elif "net profit" in name.lower():
                np_y = [safe_float(v) for v in vals[-5:]]

        # ── Extract shareholding ──
        fii_h = []
        dii_h = []
        promoter_h = []
        retail_h = []
        for row in sh_data:
            vals = row.get("values", []) if isinstance(row, dict) else []
            name = (row.get("name", "") if isinstance(row, dict) else "").lower()
            if "fii" in name or "foreign" in name:
                fii_h = [safe_float(v) for v in vals[-8:]]
            elif "dii" in name or "domestic" in name:
                dii_h = [safe_float(v) for v in vals[-8:]]
            elif "promoter" in name:
                promoter_h = [safe_float(v) for v in vals[-8:]]
            elif "public" in name or "retail" in name:
                retail_h = [safe_float(v) for v in vals[-8:]]

        # ── Key ratios from warehouse ──
        market_cap = safe_float(wh.get("market_capitalization") or nd.get("market_cap"))
        pe = safe_float(wh.get("price_to_earning"))
        pb = safe_float(wh.get("price_to_book"))
        de = safe_float(wh.get("debt_to_equity"))
        roce = safe_float(wh.get("roce"))
        roe = safe_float(wh.get("roe") or wh.get("return_on_equity"))
        opm = safe_float(wh.get("opm") or wh.get("operating_profit_margin"))
        npm = safe_float(wh.get("npm") or wh.get("net_profit_margin"))
        peg = safe_float(wh.get("peg_ratio"))
        fcf_yield = safe_float(wh.get("free_cash_flow_yield"))
        price = safe_float(nd.get("current_price") or wh.get("current_price"))
        high_52w = safe_float(nd.get("high_price") or wh.get("high_price"))
        ath = safe_float(wh.get("all_time_high") or high_52w)

        # Growth CAGRs
        sales_3yr_cagr = safe_float(wh.get("sales_growth_3yr") or wh.get("revenue_growth_3yr"))
        np_3yr_cagr = safe_float(wh.get("profit_growth_3yr") or wh.get("net_profit_growth_3yr"))

        # FCF per share
        fcf_ps = safe_float(wh.get("free_cash_flow_per_share"))

        # CCC
        ccc = safe_float(wh.get("cash_conversion_cycle"))

        # CapEx
        capex = safe_float(wh.get("capex") or 0)

        # ── Derived metrics ──
        price_vs_52wh_pct = ((high_52w - price) / max(high_52w, 1)) * 100 if high_52w > 0 else 100
        price_vs_ath_pct = ((ath - price) / max(ath, 1)) * 100 if ath > 0 else 100

        # Quarterly growth
        sales_growth_q_yoy = 0
        sales_growth_q_qoq = 0
        op_growth_q_yoy = 0
        op_growth_q_qoq = 0

        if len(sales_q) >= 5:
            if sales_q[-5] > 0:
                sales_growth_q_yoy = ((sales_q[-1] - sales_q[-5]) / sales_q[-5]) * 100
            if sales_q[-2] > 0:
                sales_growth_q_qoq = ((sales_q[-1] - sales_q[-2]) / sales_q[-2]) * 100

        # Check if quarterly sales/profit at ATH
        quarterly_sales_ath = len(sales_q) >= 2 and sales_q[-1] >= max(sales_q[:-1]) if sales_q else False
        quarterly_profit_ath = len(np_q) >= 2 and np_q[-1] >= max(np_q[:-1]) if np_q else False

        return {
            "symbol": symbol,
            "price": price,
            "market_cap": market_cap,
            "pe": pe,
            "pb": pb,
            "debt_to_equity": de,
            "roce": roce,
            "roe": roe,
            "opm_yearly": opm,
            "npm_yearly": npm,
            "peg_ratio": peg,
            "fcf_yield": fcf_yield,
            "fcf_per_share": fcf_ps,
            "sales_3yr_cagr": sales_3yr_cagr,
            "np_3yr_cagr": np_3yr_cagr,
            "ccc_yearly": ccc,
            "ccc_5yr_avg": ccc,  # Simplified — same as current
            "capex": capex,
            "capex_1yr_back": capex * 0.8,  # Estimate
            "high_52w": high_52w,
            "ath": ath,
            "price_vs_52wh_pct": price_vs_52wh_pct,
            "price_vs_ath_pct": price_vs_ath_pct,
            # Quarterly
            "sales_quarterly": sales_q[-1] if sales_q else 0,
            "sales_1q_back": sales_q[-2] if len(sales_q) >= 2 else 0,
            "sales_4q_back": sales_q[-5] if len(sales_q) >= 5 else 0,
            "net_profit_quarterly": np_q[-1] if np_q else 0,
            "net_profit_1q_back": np_q[-2] if len(np_q) >= 2 else 0,
            "net_profit_4q_back": np_q[-5] if len(np_q) >= 5 else 0,
            "eps_quarterly": eps_q[-1] if eps_q else 0,
            "eps_1q_back": eps_q[-2] if len(eps_q) >= 2 else 0,
            "eps_4q_back": eps_q[-5] if len(eps_q) >= 5 else 0,
            "opm_quarterly": opm_q[-1] if opm_q else 0,
            "opm_1q_back": opm_q[-2] if len(opm_q) >= 2 else 0,
            "sales_growth_q_yoy": sales_growth_q_yoy,
            "sales_growth_q_qoq": sales_growth_q_qoq,
            "op_growth_q_yoy": op_growth_q_yoy,  # Will be derived
            "op_growth_q_qoq": op_growth_q_qoq,
            "quarterly_sales_ath": quarterly_sales_ath,
            "quarterly_profit_ath": quarterly_profit_ath,
            # Annual
            "sales_yearly": sales_y[-1] if sales_y else 0,
            "sales_1yr_back": sales_y[-2] if len(sales_y) >= 2 else 0,
            "net_profit_yearly": np_y[-1] if np_y else 0,
            "net_profit_1yr_back": np_y[-2] if len(np_y) >= 2 else 0,
            # Shareholding
            "fii_holding": fii_h[-1] if fii_h else 0,
            "fii_holding_1q": fii_h[-2] if len(fii_h) >= 2 else 0,
            "fii_holding_4q": fii_h[-5] if len(fii_h) >= 5 else 0,
            "dii_holding": dii_h[-1] if dii_h else 0,
            "dii_holding_1q": dii_h[-2] if len(dii_h) >= 2 else 0,
            "dii_holding_4q": dii_h[-5] if len(dii_h) >= 5 else 0,
            "promoter_holding": promoter_h[-1] if promoter_h else 0,
            "promoter_holding_4q": promoter_h[-5] if len(promoter_h) >= 5 else 0,
            "retail_holding": retail_h[-1] if retail_h else 100,
            "retail_holding_1q": retail_h[-2] if len(retail_h) >= 2 else 100,
            "retail_holding_4q": retail_h[-5] if len(retail_h) >= 5 else 100,
        }

    except Exception as e:
        print(f"    Parse error for {symbol}: {e}")
        return None


# ═══════════════════════════════════════
# SCANNER EXECUTION
# ═══════════════════════════════════════

def run_scans(stock_data):
    """Run all scan formulas against fetched data"""
    results = {}
    
    for scan in SCANS:
        scan_id = scan["id"]
        matched = []
        
        for symbol, data in stock_data.items():
            try:
                if scan["conditions"](data):
                    matched.append({
                        "symbol": symbol,
                        "price": data.get("price", 0),
                        "market_cap": data.get("market_cap", 0),
                        "roce": data.get("roce", 0),
                        "de": data.get("debt_to_equity", 0),
                        "pe": data.get("pe", 0),
                        "npm": data.get("npm_yearly", 0),
                        "52wh_dist": round(data.get("price_vs_52wh_pct", 0), 1),
                    })
            except Exception:
                continue
        
        # Sort by market cap descending
        matched.sort(key=lambda x: x.get("market_cap", 0), reverse=True)
        
        results[scan_id] = {
            "id": scan_id,
            "name": scan["name"],
            "category": scan["category"],
            "description": scan["description"],
            "formula": scan["formula"],
            "count": len(matched),
            "stocks": matched,
        }
        
        print(f"  ✓ {scan['name']}: {len(matched)} stocks")
    
    return results


# ═══════════════════════════════════════
# TECHNOFUNDA COMBINER
# ═══════════════════════════════════════

def combine_technofunda(funda_results, chartink_data):
    """Cross-reference fundamental scan results with ChartInk technical scans"""
    if not chartink_data:
        return {}
    
    combined = {}
    
    # Get all ChartInk stock symbols
    chartink_stocks = set()
    chartink_by_scan = {}
    
    if isinstance(chartink_data, dict):
        for scan_name, stocks in chartink_data.items():
            if isinstance(stocks, list):
                scan_syms = set()
                for s in stocks:
                    sym = s.get("symbol", s) if isinstance(s, dict) else str(s)
                    sym = sym.replace(".NS", "").replace("NSE:", "").strip()
                    chartink_stocks.add(sym)
                    scan_syms.add(sym)
                chartink_by_scan[scan_name] = scan_syms
    
    # For each fundamental scan, find stocks that also appear in ANY ChartInk scan
    for scan_id, scan_result in funda_results.items():
        funda_syms = set(s["symbol"] for s in scan_result["stocks"])
        overlap = funda_syms & chartink_stocks
        
        if overlap:
            # Find which ChartInk scans each overlapping stock appears in
            overlap_stocks = []
            for sym in overlap:
                stock_data = next((s for s in scan_result["stocks"] if s["symbol"] == sym), {})
                tech_scans = [name for name, syms in chartink_by_scan.items() if sym in syms]
                overlap_stocks.append({
                    **stock_data,
                    "technical_scans": tech_scans[:5],  # Top 5 matching tech scans
                })
            
            overlap_stocks.sort(key=lambda x: len(x.get("technical_scans", [])), reverse=True)
            
            combined[f"tf_{scan_id}"] = {
                "name": f"TF: {scan_result['name']}",
                "description": f"Stocks passing '{scan_result['name']}' + ChartInk technical scans",
                "funda_scan": scan_id,
                "count": len(overlap_stocks),
                "stocks": overlap_stocks,
            }
    
    return combined


# ═══════════════════════════════════════
# SYMBOL LOADING
# ═══════════════════════════════════════

def load_symbols():
    """Load stock symbols from nse_symbols.json"""
    if SYMBOLS_FILE.exists():
        try:
            with open(SYMBOLS_FILE) as f:
                data = json.load(f)
            if isinstance(data, list):
                if data and isinstance(data[0], str):
                    return data
                elif data and isinstance(data[0], dict):
                    return [d.get("symbol", d.get("SYMBOL", "")) for d in data if d.get("symbol") or d.get("SYMBOL")]
        except Exception as e:
            print(f"Error loading symbols: {e}")
    
    # Fallback — Nifty 200 core
    return [
        "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
        "SBIN", "BHARTIARTL", "BAJFINANCE", "KOTAKBANK", "LT", "TATAMOTORS",
        "MARUTI", "SUNPHARMA", "TITAN", "AXISBANK", "WIPRO", "DRREDDY",
        "HCLTECH", "TECHM", "NESTLEIND", "ULTRACEMCO", "POWERGRID",
        "NTPC", "ONGC", "JSWSTEEL", "TATASTEEL", "COALINDIA", "HINDALCO",
        "GRASIM", "BPCL", "DIVISLAB", "CIPLA", "APOLLOHOSP", "EICHERMOT",
        "SHRIRAMFIN", "TATACONSUM", "M&M", "ASIANPAINT", "BRITANNIA",
        "HEROMOTOCO", "INDUSINDBK", "SBILIFE", "BAJAJFINSV", "ADANIENT",
        "PERSISTENT", "COFORGE", "TRENT", "ZOMATO", "PAYTM",
        "POLYCAB", "DIXON", "AFFLE", "DEEPAKNTR", "ATUL",
        "ASTRAL", "METROPOLIS", "LALPATHLAB", "IDFCFIRSTB", "FEDERALBNK",
    ]


def load_chartink_results():
    """Load ChartInk scan results if available"""
    if CHARTINK_FILE.exists():
        try:
            with open(CHARTINK_FILE) as f:
                return json.load(f)
        except:
            pass
    return None


# ═══════════════════════════════════════
# MAIN
# ═══════════════════════════════════════

def main():
    DATA_DIR.mkdir(exist_ok=True)
    
    symbols = load_symbols()
    print(f"═══ TradEdge Fundamental Scanner ═══")
    print(f"Symbols: {len(symbols)}")
    print(f"Scans: {len(SCANS)}")
    print()
    
    # Fetch fundamental data
    print("Fetching fundamental data from Screener.in...")
    stock_data = {}
    failed = []
    
    for i, sym in enumerate(symbols):
        if i > 0 and i % 10 == 0:
            print(f"  Progress: {i}/{len(symbols)}...")
            time.sleep(1)  # Rate limiting
        
        data = fetch_screener_data(sym)
        if data:
            stock_data[sym] = data
            # Brief delay for rate limiting
            time.sleep(0.5)
        else:
            failed.append(sym)
    
    print(f"\nFetched: {len(stock_data)} OK, {len(failed)} failed")
    if failed:
        print(f"Failed: {', '.join(failed[:15])}{'...' if len(failed) > 15 else ''}")
    
    # Run scans
    print(f"\nRunning {len(SCANS)} fundamental scans...")
    funda_results = run_scans(stock_data)
    
    # TechnoFunda combination
    chartink_data = load_chartink_results()
    technofunda = {}
    if chartink_data:
        print(f"\nCombining with ChartInk technical scans...")
        technofunda = combine_technofunda(funda_results, chartink_data)
        for k, v in technofunda.items():
            print(f"  ✓ {v['name']}: {v['count']} stocks (TechnoFunda)")
    else:
        print("\nNo ChartInk data found — skipping TechnoFunda combination")
    
    # Output
    output = {
        "generated": datetime.now().isoformat(),
        "total_stocks_scanned": len(stock_data),
        "failed_stocks": failed,
        "scans": funda_results,
        "technofunda": technofunda,
        "scan_summary": {
            scan["id"]: {
                "name": scan["name"],
                "category": scan["category"],
                "count": funda_results[scan["id"]]["count"],
            }
            for scan in SCANS
        },
    }
    
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, default=str)
    
    size_kb = OUTPUT_FILE.stat().st_size / 1024
    print(f"\n═══ Results ═══")
    print(f"Written: {OUTPUT_FILE} ({size_kb:.1f} KB)")
    total_matches = sum(r["count"] for r in funda_results.values())
    print(f"Total scan matches: {total_matches}")
    print(f"TechnoFunda combinations: {len(technofunda)}")
    
    # Summary table
    print(f"\n{'Scan':<40} {'Count':>6}")
    print("─" * 48)
    for scan in SCANS:
        r = funda_results[scan["id"]]
        print(f"{scan['name'][:39]:<40} {r['count']:>6}")
    if technofunda:
        print("─" * 48)
        for k, v in technofunda.items():
            print(f"{'TF: ' + v['name'][:35]:<40} {v['count']:>6}")


if __name__ == "__main__":
    main()
