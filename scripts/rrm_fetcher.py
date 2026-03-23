"""
================================================================================
RRM Data Fetcher v4.3 — Thematic Indices + Multi-Benchmark + RSI
================================================================================
UPGRADE from v4.2:
  - NEW: 47 Nifty Thematic/Sectoral indices as a separate group
  - Appears as 'thematic_indices' in rrm_data.json
  - Frontend renders this as a new "Thematic" tab
  - All v4.2 features preserved (RSI, multi-benchmark, global indices, etc.)

Requirements:  pip install yfinance numpy
================================================================================
"""

import json, os, sys, argparse, logging, hashlib
from datetime import datetime
import yfinance as yf
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("rrm_fetcher")

# =============================================================================
# RSI COMPUTATION (v4.2)
# =============================================================================
def compute_rsi(prices, period=14):
    if len(prices) < period + 1:
        return None
    prices = np.array(prices, dtype=float)
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi, 1)

def rsi_zone(rsi_val):
    if rsi_val is None: return 'unknown'
    if rsi_val >= 70: return 'overbought'
    elif rsi_val >= 50: return 'bullish'
    elif rsi_val >= 30: return 'bearish'
    else: return 'oversold'

# =============================================================================
# CONFIG
# =============================================================================
def load_config(config_path):
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path) as f:
                cfg = json.load(f)
            log.info(f"Config: {config_path}")
            return cfg
        except Exception as e:
            log.warning(f"Config load failed: {e}")
    log.info("Using built-in defaults (v4.3 — 16 sectors, 47 thematic, 54 ETFs, 25 global + RSI)")
    return DEFAULT_CONFIG()

def DEFAULT_CONFIG():
    return {
        "benchmarks": {"^NSEI": "Nifty 50", "^NSEBANK": "Nifty Bank", "^CRSLDX": "Nifty 500"},
        "default_benchmark": "^NSEI",

        # ═══════════════════════════════════════════════════════
        # 16 CORE SECTORS (v4.0 — unchanged)
        # ═══════════════════════════════════════════════════════
        "sectors": {
            "^CNXAUTO":     {"name": "Nifty Auto",         "color": "#ef4444"},
            "^NSEBANK":     {"name": "Nifty Bank",         "color": "#3b82f6"},
            "^CNXFIN":      {"name": "Nifty Fin Service",  "color": "#6366f1"},
            "^CNXFMCG":     {"name": "Nifty FMCG",         "color": "#22c55e"},
            "^CNXPHARMA":   {"name": "Nifty Pharma",       "color": "#f59e0b"},
            "^CNXIT":       {"name": "Nifty IT",           "color": "#06b6d4"},
            "^CNXMETAL":    {"name": "Nifty Metal",        "color": "#8b5cf6"},
            "^CNXREALTY":   {"name": "Nifty Realty",       "color": "#ec4899"},
            "^CNXENERGY":   {"name": "Nifty Energy",       "color": "#f97316"},
            "^CNXINFRA":    {"name": "Nifty Infra",        "color": "#14b8a6"},
            "^CNXMEDIA":    {"name": "Nifty Media",        "color": "#a855f7"},
            "^CNXPSUBANK":  {"name": "Nifty PSU Bank",     "color": "#0ea5e9"},
            "^CNXSERVICE":  {"name": "Nifty Services",     "color": "#84cc16"},
            "^CNXCONSUM":   {"name": "Nifty Consumption",  "color": "#e879f9"},
            "^CNXCMDT":     {"name": "Nifty Commodities",  "color": "#d97706"},
            "^CNXMNC":      {"name": "Nifty MNC",          "color": "#64748b"},
            "^CNXPSE":      {"name": "Nifty PSE",          "color": "#fde68a"},
        },

        # ═══════════════════════════════════════════════════════
        # 47 NIFTY THEMATIC / SECTORAL INDICES (NEW v4.3)
        # Yahoo Finance format: NIFTY_XXXXX.NS
        # ═══════════════════════════════════════════════════════
        "thematic_indices": {
            # ── Sectoral / Industry ──
            "NIFTY_CONSR_DURBL.NS":     {"name": "Nifty Consumer Durables",   "color": "#f472b6", "group": "Sectoral"},
            "NIFTY_HEALTHCARE.NS":      {"name": "Nifty Healthcare",          "color": "#34d399", "group": "Sectoral"},
            "NIFTY_OIL_AND_GAS.NS":     {"name": "Nifty Oil & Gas",          "color": "#f97316", "group": "Sectoral"},
            "NIFTY_PHARMA.NS":          {"name": "Nifty Pharma",             "color": "#f59e0b", "group": "Sectoral"},
            "NIFTY_AUTO.NS":            {"name": "Nifty Auto",               "color": "#ef4444", "group": "Sectoral"},
            "NIFTY_BANK.NS":            {"name": "Nifty Bank",               "color": "#3b82f6", "group": "Sectoral"},
            "NIFTY_ENERGY.NS":          {"name": "Nifty Energy",             "color": "#f97316", "group": "Sectoral"},
            "NIFTY_FMCG.NS":            {"name": "Nifty FMCG",              "color": "#22c55e", "group": "Sectoral"},
            "NIFTY_IT.NS":              {"name": "Nifty IT",                 "color": "#06b6d4", "group": "Sectoral"},
            "NIFTY_MEDIA.NS":           {"name": "Nifty Media",              "color": "#a855f7", "group": "Sectoral"},
            "NIFTY_METAL.NS":           {"name": "Nifty Metal",              "color": "#8b5cf6", "group": "Sectoral"},
            "NIFTY_REALTY.NS":          {"name": "Nifty Realty",             "color": "#ec4899", "group": "Sectoral"},
            "NIFTY_PSU_BANK.NS":        {"name": "Nifty PSU Bank",          "color": "#0ea5e9", "group": "Sectoral"},
            "NIFTY_PVT_BANK.NS":        {"name": "Nifty Pvt Bank",          "color": "#60a5fa", "group": "Sectoral"},
            "NIFTY_CHEMICALS.NS":       {"name": "Nifty Chemicals",          "color": "#d946ef", "group": "Sectoral"},

            # ── Thematic ──
            "NIFTY_CPSE.NS":            {"name": "Nifty CPSE",              "color": "#4ade80", "group": "Thematic"},
            "NIFTY_IND_DIGITAL.NS":     {"name": "Nifty India Digital",     "color": "#22d3ee", "group": "Thematic"},
            "NIFTY_INDIA_MFG.NS":       {"name": "Nifty India Mfg",        "color": "#bef264", "group": "Thematic"},
            "NIFTY_COMMODITIES.NS":     {"name": "Nifty Commodities",       "color": "#d97706", "group": "Thematic"},
            "NIFTY_CONSUMPTION.NS":     {"name": "Nifty Consumption",       "color": "#e879f9", "group": "Thematic"},
            "NIFTY_FIN_SERVICE.NS":     {"name": "Nifty Fin Service",       "color": "#6366f1", "group": "Thematic"},
            "NIFTY_GROWSECT_15.NS":     {"name": "Nifty Growth Sect 15",    "color": "#86efac", "group": "Thematic"},
            "NIFTY_INFRA.NS":           {"name": "Nifty Infra",             "color": "#14b8a6", "group": "Thematic"},
            "NIFTY_MNC.NS":             {"name": "Nifty MNC",               "color": "#64748b", "group": "Thematic"},
            "NIFTY_PSE.NS":             {"name": "Nifty PSE",               "color": "#fde68a", "group": "Thematic"},
            "NIFTY_SERV_SECTOR.NS":     {"name": "Nifty Services",          "color": "#84cc16", "group": "Thematic"},
            "NIFTY_IND_DEFENCE.NS":     {"name": "Nifty India Defence",     "color": "#84cc16", "group": "Thematic"},
            "NIFTY_IND_TOURISM.NS":     {"name": "Nifty India Tourism",     "color": "#fb923c", "group": "Thematic"},
            "NIFTY_CAPITAL_MKT.NS":     {"name": "Nifty Capital Mkt",       "color": "#f472b6", "group": "Thematic"},
            "NIFTY_EV.NS":              {"name": "Nifty EV",                "color": "#67e8f9", "group": "Thematic"},
            "NIFTY_HOUSING.NS":         {"name": "Nifty Housing",           "color": "#fb7185", "group": "Thematic"},
            "NIFTY_COREHOUSING.NS":     {"name": "Nifty CoreHousing",       "color": "#fda4af", "group": "Thematic"},
            "NIFTY_INTERNET.NS":        {"name": "Nifty Internet",          "color": "#38bdf8", "group": "Thematic"},
            "NIFTY_MOBILITY.NS":        {"name": "Nifty Mobility",          "color": "#c084fc", "group": "Thematic"},
            "NIFTY_RURAL.NS":           {"name": "Nifty Rural",             "color": "#a3e635", "group": "Thematic"},
            "NIFTY_WAVES.NS":           {"name": "Nifty Waves",             "color": "#5eead4", "group": "Thematic"},

            # ── Strategy / Multi-Sector ──
            "NIFTY_FIN_SRV_25_50.NS":   {"name": "Nifty FinSrv 25/50",     "color": "#818cf8", "group": "Strategy"},
            "NIFTY_FINSRV_EX_BANK.NS":  {"name": "Nifty FinSrv ExBank",    "color": "#a5b4fc", "group": "Strategy"},
            "NIFTY_MS_FIN_SERV.NS":     {"name": "Nifty MS Fin Serv",      "color": "#c4b5fd", "group": "Strategy"},
            "NIFTY_MS_IT_TELECOM.NS":   {"name": "Nifty MS IT Telecom",    "color": "#99f6e4", "group": "Strategy"},
            "NIFTY_MS_IND_CONS.NS":     {"name": "Nifty MS Ind Cons",      "color": "#fcd34d", "group": "Strategy"},
            "NIFTY_MIDSML_HLTH.NS":     {"name": "Nifty MidSml Health",    "color": "#6ee7b7", "group": "Strategy"},
            "NIFTY_NEW_CONSUMP.NS":     {"name": "Nifty New Consumption",   "color": "#fca5a5", "group": "Strategy"},
            "NIFTY_NONCYC_CONS.NS":     {"name": "Nifty NonCyc Cons",      "color": "#86efac", "group": "Strategy"},
            "NIFTY_TRANS_LOGIS.NS":     {"name": "Nifty Trans & Logis",    "color": "#fdba74", "group": "Strategy"},
            "NIFTY_INFRA_LOG.NS":       {"name": "Nifty Infra & Log",      "color": "#2dd4bf", "group": "Strategy"},
            "NIFTY_CORP_MAATR.NS":      {"name": "Nifty Corp MAATR",       "color": "#fb923c", "group": "Strategy"},
            "NIFTY_TELECOM.NS":         {"name": "Nifty Telecom",          "color": "#38bdf8", "group": "Strategy"},
        },

        # ═══════════════════════════════════════════════════════
        # 54 ETFs (v4.0 — unchanged)
        # ═══════════════════════════════════════════════════════
        "etfs": {
            "NIFTYBEES.NS":   {"name": "Nifty 50 ETF",        "color": "#3b82f6"},
            "BANKBEES.NS":    {"name": "Bank ETF",             "color": "#6366f1"},
            "ITBEES.NS":      {"name": "IT ETF",               "color": "#06b6d4"},
            "PHARMABEES.NS":  {"name": "Pharma ETF",           "color": "#f59e0b"},
            "PSUBNKBEES.NS":  {"name": "PSU Bank ETF",         "color": "#0ea5e9"},
            "JUNIORBEES.NS":  {"name": "Next 50 ETF",          "color": "#8b5cf6"},
            "AUTOBEES.NS":    {"name": "Auto ETF",             "color": "#ef4444"},
            "MID150BEES.NS":  {"name": "Midcap 150 ETF",       "color": "#ec4899"},
            "GOLDBEES.NS":    {"name": "Gold ETF",             "color": "#eab308"},
            "SILVERBEES.NS":  {"name": "Silver ETF",           "color": "#94a3b8"},
            "MON100.NS":      {"name": "NASDAQ 100 ETF",       "color": "#22d3ee"},
            "ABSLPSE.NS":     {"name": "PSE ETF",              "color": "#fde68a"},
            "ALPHA.NS":       {"name": "Alpha ETF",            "color": "#a78bfa"},
            "AONETOTAL.NS":   {"name": "Top 750 ETF",          "color": "#67e8f9"},
            "BFSI.NS":        {"name": "BFSI ETF",             "color": "#c084fc"},
            "COMMOIETF.NS":   {"name": "Commodities ETF",      "color": "#d97706"},
            "CONSUMBEES.NS":  {"name": "Consumption ETF",      "color": "#e879f9"},
            "CONSUMER.NS":    {"name": "New Age Consumption ETF","color": "#fb7185"},
            "CPSEETF.NS":     {"name": "CPSE ETF",             "color": "#4ade80"},
            "DIVOPPBEES.NS":  {"name": "Dividend Opp 50 ETF",  "color": "#fbbf24"},
            "ESG.NS":         {"name": "ESG Leaders ETF",      "color": "#34d399"},
            "FINIETF.NS":     {"name": "FinServ Ex-Bank ETF",  "color": "#60a5fa"},
            "FMCGIETF.NS":    {"name": "FMCG ETF",             "color": "#22c55e"},
            "GILT5YBEES.NS":  {"name": "Gilt 5Y ETF",          "color": "#0ea5e9"},
            "GROWWEV.NS":     {"name": "EV Auto ETF",          "color": "#67e8f9"},
            "GROWWRAIL.NS":   {"name": "Railways PSU ETF",     "color": "#bef264"},
            "HDFCGROWTH.NS":  {"name": "Growth Sectors ETF",   "color": "#f472b6"},
            "HDFCSML250.NS":  {"name": "SmallCap 250 ETF",     "color": "#fca5a5"},
            "HEALTHIETF.NS":  {"name": "Healthcare ETF",       "color": "#34d399"},
            "HNGSNGBEES.NS":  {"name": "Hang Seng ETF",        "color": "#fb923c"},
            "ICICIB22.NS":    {"name": "Bharat 22 ETF",        "color": "#14b8a6"},
            "INFRAIETF.NS":   {"name": "Infra ETF",            "color": "#14b8a6"},
            "LIQUIDCASE.NS":  {"name": "Liquid Assets ETF",    "color": "#06b6d4"},
            "LOWVOLIETF.NS":  {"name": "Low Vol Top 100 ETF",  "color": "#93c5fd"},
            "LTGILTBEES.NS":  {"name": "Long Term Gilt ETF",   "color": "#7dd3fc"},
            "MAFANG.NS":      {"name": "NYSE FANG ETF",        "color": "#a855f7"},
            "MAHKTECH.NS":    {"name": "Hang Seng Tech ETF",   "color": "#fcd34d"},
            "MAKEINDIA.NS":   {"name": "Make in India ETF",    "color": "#bef264"},
            "MASPTOP50.NS":   {"name": "S&P 500 Top 50 ETF",   "color": "#6366f1"},
            "METALIETF.NS":   {"name": "Metal ETF",            "color": "#8b5cf6"},
            "MIDSMALL.NS":    {"name": "MidSmallCap ETF",      "color": "#d946ef"},
            "MNC.NS":         {"name": "MNC ETF",              "color": "#64748b"},
            "MOCAPITAL.NS":   {"name": "Capital Markets ETF",  "color": "#f472b6"},
            "MODEFENCE.NS":   {"name": "Defence ETF",          "color": "#84cc16"},
            "MOM30IETF.NS":   {"name": "Momentum Top 200 ETF", "color": "#fdba74"},
            "MOMENTUM50.NS":  {"name": "Momentum Top 500 ETF", "color": "#c4b5fd"},
            "MONQ50.NS":      {"name": "NASDAQ Q50 ETF",       "color": "#5eead4"},
            "MOREALTY.NS":    {"name": "Realty ETF",            "color": "#ec4899"},
            "MSCIINDIA.NS":   {"name": "MSCI India ETF",       "color": "#86efac"},
            "MULTICAP.NS":    {"name": "Multicap ETF",          "color": "#a5b4fc"},
            "OILIETF.NS":     {"name": "Oil & Gas ETF",        "color": "#fbbf24"},
            "PVTBANIETF.NS":  {"name": "Pvt Bank ETF",         "color": "#99f6e4"},
            "SELECTIPO.NS":   {"name": "Select IPO ETF",       "color": "#fda4af"},
            "TOP10ADD.NS":    {"name": "Top 10 ETF",            "color": "#fed7aa"},
        },

        # ═══════════════════════════════════════════════════════
        # 12 ASSET CLASSES (unchanged)
        # ═══════════════════════════════════════════════════════
        "asset_classes": {
            "GC=F":           {"name": "Gold",              "color": "#eab308"},
            "SI=F":           {"name": "Silver",            "color": "#94a3b8"},
            "CL=F":           {"name": "Crude Oil",         "color": "#f97316"},
            "USDINR=X":       {"name": "USD/INR",           "color": "#22c55e"},
            "BTC-USD":        {"name": "Bitcoin",           "color": "#f59e0b"},
            "^TNX":           {"name": "US 10Y Yield",      "color": "#ef4444"},
            "ICICIB22.NS":    {"name": "ICICI G-Sec 2027",  "color": "#14b8a6"},
            "GILT5YBEES.NS":  {"name": "Gilt 5Y ETF",       "color": "#0ea5e9"},
            "LIQUIDBEES.NS":  {"name": "Liquid Fund",        "color": "#06b6d4"},
            "^DJI":           {"name": "Dow Jones",          "color": "#8b5cf6"},
            "^GSPC":          {"name": "S&P 500",            "color": "#6366f1"},
            "^IXIC":          {"name": "NASDAQ",             "color": "#a855f7"},
        },

        # ═══════════════════════════════════════════════════════
        # 11 MARKET SEGMENTS (unchanged)
        # ═══════════════════════════════════════════════════════
        "market_segments": {
            "^NSEI":              {"name": "Nifty 50",            "color": "#22d3ee"},
            "^NSMIDCP":           {"name": "Nifty Next 50",       "color": "#6366f1"},
            "^CRSLDX":            {"name": "Nifty 500",           "color": "#3b82f6"},
            "NIFTYMIDCAP150.NS":  {"name": "Midcap 150",          "color": "#a855f7"},
            "^NSEMDCP50":         {"name": "Midcap 50",            "color": "#d946ef"},
            "NIFTYSMLCAP250.NS":  {"name": "Smallcap 250",         "color": "#f43f5e"},
            "NIFTYMSML400.NS":    {"name": "MidSmallcap 400",      "color": "#fb923c"},
            "NIFTYMICRO250.NS":   {"name": "Microcap 250",         "color": "#fbbf24"},
            "NIFTYTOTALMARKET.NS":{"name": "Total Market 750",     "color": "#14b8a6"},
            "NIFTYLARGEMID250.NS":{"name": "LargeMidcap 250",      "color": "#84cc16"},
            "^NSEBANK":           {"name": "Bank Index",           "color": "#0ea5e9"},
        },

        # ═══════════════════════════════════════════════════════
        # 25 GLOBAL INDICES (unchanged)
        # ═══════════════════════════════════════════════════════
        "global_indices": {
            "^GSPC":      {"name": "S&P 500",          "color": "#6366f1", "group": "US"},
            "^DJI":       {"name": "Dow Jones",        "color": "#8b5cf6", "group": "US"},
            "^IXIC":      {"name": "NASDAQ Composite",  "color": "#a855f7", "group": "US"},
            "^RUT":       {"name": "Russell 2000",      "color": "#c084fc", "group": "US"},
            "^GDAXI":     {"name": "DAX (Germany)",    "color": "#f59e0b", "group": "Europe"},
            "^FTSE":      {"name": "FTSE 100 (UK)",    "color": "#ef4444", "group": "Europe"},
            "^FCHI":      {"name": "CAC 40 (France)",  "color": "#3b82f6", "group": "Europe"},
            "^STOXX50E":  {"name": "Euro Stoxx 50",    "color": "#14b8a6", "group": "Europe"},
            "^N225":      {"name": "Nikkei 225",       "color": "#ec4899", "group": "Asia"},
            "^HSI":       {"name": "Hang Seng",        "color": "#fb923c", "group": "Asia"},
            "000001.SS":  {"name": "Shanghai Comp",    "color": "#ef4444", "group": "Asia"},
            "^KS11":      {"name": "KOSPI (Korea)",    "color": "#22c55e", "group": "Asia"},
            "^TWII":      {"name": "Taiwan Weighted",  "color": "#06b6d4", "group": "Asia"},
            "^STI":       {"name": "Straits Times",    "color": "#84cc16", "group": "Asia"},
            "GC=F":       {"name": "Gold",             "color": "#eab308", "group": "Commodities"},
            "SI=F":       {"name": "Silver",           "color": "#94a3b8", "group": "Commodities"},
            "CL=F":       {"name": "Crude Oil WTI",    "color": "#f97316", "group": "Commodities"},
            "HG=F":       {"name": "Copper",           "color": "#d97706", "group": "Commodities"},
            "BTC-USD":    {"name": "Bitcoin",          "color": "#f59e0b", "group": "Crypto"},
            "ETH-USD":    {"name": "Ethereum",         "color": "#6366f1", "group": "Crypto"},
            "DX-Y.NYB":  {"name": "US Dollar Index",  "color": "#22c55e", "group": "Forex"},
            "USDINR=X":  {"name": "USD/INR",          "color": "#14b8a6", "group": "Forex"},
            "EURUSD=X":  {"name": "EUR/USD",          "color": "#3b82f6", "group": "Forex"},
            "GBPUSD=X":  {"name": "GBP/USD",          "color": "#ef4444", "group": "Forex"},
            "USDJPY=X":  {"name": "USD/JPY",          "color": "#ec4899", "group": "Forex"},
        },
        "sector_constituents": {},
    }

# =============================================================================
# CUSTOM STOCKS LOADER (unchanged)
# =============================================================================
def load_custom_stocks(config_dir=None):
    search_paths = ["data/custom_stocks.json", "../data/custom_stocks.json", "custom_stocks.json"]
    if config_dir: search_paths.insert(0, os.path.join(config_dir, "custom_stocks.json"))
    for path in search_paths:
        if os.path.exists(path):
            try:
                with open(path) as f: data = json.load(f)
                stocks = data.get("custom_stocks", [])
                log.info(f"Custom stocks: loaded {len(stocks)} from {path}")
                return stocks
            except Exception as e: log.warning(f"Custom stocks load failed ({path}): {e}")
    log.info("Custom stocks: none found")
    return []

# =============================================================================
# JdK RS-RATIO / RS-MOMENTUM (unchanged)
# =============================================================================
def calc_rs(sector_prices, bench_prices, window=10):
    if len(sector_prices) < window * 3 or len(bench_prices) < window * 3:
        return None, None
    n = min(len(sector_prices), len(bench_prices))
    sec = np.array(sector_prices[-n:], dtype=float)
    ben = np.array(bench_prices[-n:], dtype=float)
    ben[ben == 0] = 1e-10
    rs_raw = sec / ben
    rs_norm = np.full(len(rs_raw), np.nan)
    for i in range(window - 1, len(rs_raw)):
        sma = np.mean(rs_raw[max(0, i - window + 1):i + 1])
        rs_norm[i] = (rs_raw[i] / sma * 100) if sma > 0 else 100
    alpha = 2.0 / (window + 1)
    rs_ratio = np.full(len(rs_norm), np.nan)
    fv = window - 1
    rs_ratio[fv] = rs_norm[fv]
    for i in range(fv + 1, len(rs_norm)):
        if not (np.isnan(rs_norm[i]) or np.isnan(rs_ratio[i-1])):
            rs_ratio[i] = alpha * rs_norm[i] + (1 - alpha) * rs_ratio[i-1]
    rs_mom = np.full(len(rs_ratio), np.nan)
    for i in range(window, len(rs_ratio)):
        if not (np.isnan(rs_ratio[i]) or np.isnan(rs_ratio[i-window])):
            p = rs_ratio[i - window]
            rs_mom[i] = (rs_ratio[i] / p * 100) if p > 0 else 100
    return rs_ratio, rs_mom

def quadrant(r, m):
    if r >= 100 and m >= 100: return "Leading"
    if r < 100 and m >= 100: return "Improving"
    if r < 100 and m < 100: return "Lagging"
    return "Weakening"

# =============================================================================
# FETCH PRICES (unchanged)
# =============================================================================
def fetch_prices(symbols, period="5y"):
    log.info(f"Fetching {len(symbols)} symbols from Yahoo Finance...")
    out = {}
    for sym in symbols:
        try:
            # Try requested period first (5y default)
            h = yf.Ticker(sym).history(period=period, interval="1d")
            # If empty or too short, try "max" to get ALL available history
            # This catches newer indices launched in 2022-2024
            if (h.empty or len(h) < 30) and period != "max":
                h = yf.Ticker(sym).history(period="max", interval="1d")
                if not h.empty and len(h) >= 30:
                    log.info(f"  ✓ {sym}: {len(h)} days (fallback: max)")
            if h.empty or len(h) < 30:
                log.warning(f"  ✗ {sym}: {len(h) if not h.empty else 0} rows")
                continue
            out[sym] = {
                "closes": h['Close'].dropna().tolist(),
                "dates": [d.strftime("%Y-%m-%d") for d in h.index],
            }
            if sym not in [s for s in out if 'fallback' not in str(s)]:
                log.info(f"  ✓ {sym}: {len(out[sym]['closes'])} days")
        except Exception as e:
            log.error(f"  ✗ {sym}: {e}")
    log.info(f"  Total fetched: {len(out)}/{len(symbols)} symbols")
    return out

def resample_weekly(closes, dates):
    from datetime import datetime as dt
    wc, wd, cw = [], [], None
    for c, d in zip(closes, dates):
        wk = dt.strptime(d, "%Y-%m-%d").isocalendar()[:2]
        if cw is not None and wk != cw: wc.append(lc); wd.append(ld)
        cw = wk; lc = c; ld = d
    if cw is not None: wc.append(lc); wd.append(ld)
    return wc, wd

def resample_monthly(closes, dates):
    from datetime import datetime as dt
    mc, md, cm = [], [], None
    for c, d in zip(closes, dates):
        ym = dt.strptime(d, "%Y-%m-%d").strftime("%Y-%m")
        if cm is not None and ym != cm: mc.append(lc); md.append(ld)
        cm = ym; lc = c; ld = d
    if cm is not None: mc.append(lc); md.append(ld)
    return mc, md

# =============================================================================
# SECTOR CONSTITUENTS (unchanged)
# =============================================================================
_SECTOR_CONSTITUENTS_CACHE = None

def load_sector_constituents():
    global _SECTOR_CONSTITUENTS_CACHE
    if _SECTOR_CONSTITUENTS_CACHE is not None: return _SECTOR_CONSTITUENTS_CACHE
    for path in ["data/sector_constituents.json", "../data/sector_constituents.json", "sector_constituents.json"]:
        if os.path.exists(path):
            try:
                with open(path) as f: data = json.load(f)
                result = {k: v for k, v in data.items() if not k.startswith("_")}
                log.info(f"Sector constituents: {len(result)} sectors from {path}")
                _SECTOR_CONSTITUENTS_CACHE = result
                return result
            except Exception as e: log.warning(f"Sector constituents failed ({path}): {e}")
    _SECTOR_CONSTITUENTS_CACHE = {}
    return {}

def auto_fetch_constituents(sector_symbol):
    try:
        t = yf.Ticker(sector_symbol)
        if hasattr(t, 'components') and t.components is not None:
            comps = list(t.components)
            if comps: return [{"symbol": s, "name": s.replace(".NS", "")} for s in comps[:30]]
    except: pass
    return None

def get_constituents(sector_symbol, config):
    sc_data = load_sector_constituents()
    if sector_symbol in sc_data and sc_data[sector_symbol]: return sc_data[sector_symbol]
    auto = auto_fetch_constituents(sector_symbol)
    if auto and len(auto) >= 3: return auto
    static = config.get("sector_constituents", {}).get(sector_symbol, [])
    if static: return static
    return []

PALETTE = ["#ef4444","#f97316","#f59e0b","#eab308","#84cc16","#22c55e","#14b8a6","#06b6d4","#0ea5e9","#3b82f6","#6366f1","#8b5cf6","#a855f7","#d946ef","#ec4899","#f43f5e","#fb923c","#a3e635","#2dd4bf","#38bdf8"]

def stock_color(name, idx):
    h = int(hashlib.md5(name.encode()).hexdigest()[:8], 16)
    return PALETTE[(h + idx) % len(PALETTE)]

# =============================================================================
# RRM FOR A SET OF ITEMS (unchanged)
# =============================================================================
def calc_rrm_items(price_data, items_cfg, bench_closes, bench_dates, tail_len, window, resample_fn=None):
    results = []
    for entry in items_cfg:
        sym = entry["symbol"]; name = entry.get("name", sym); color = entry.get("color", "#94a3b8")
        extra = {k: v for k, v in entry.items() if k not in ("symbol", "name", "color")}
        if sym not in price_data: continue
        sc, sd = price_data[sym]["closes"], price_data[sym]["dates"]
        bc, bd = bench_closes, bench_dates
        raw_daily_closes = sc[:]
        if resample_fn: sc, sd = resample_fn(sc, sd); bc, bd = resample_fn(bc, bd)
        rs_r, rs_m = calc_rs(sc, bc, window)
        if rs_r is None: continue
        valid = [i for i in range(len(rs_r)) if not (np.isnan(rs_r[i]) or np.isnan(rs_m[i]))]
        tail_idx = valid[-(tail_len + 1):]
        tail = [{"date": sd[i] if i < len(sd) else "", "rs_ratio": round(float(rs_r[i]), 2), "rs_momentum": round(float(rs_m[i]), 2)} for i in tail_idx]
        if not tail: continue
        cur = tail[-1]
        rsi_val = compute_rsi(sc, 14)
        cur["rsi"] = rsi_val; cur["rsi_zone"] = rsi_zone(rsi_val)
        cur["daily_rsi"] = compute_rsi(raw_daily_closes, 14) if resample_fn else rsi_val
        item = {"symbol": sym, "name": name, "color": color, "quadrant": quadrant(cur["rs_ratio"], cur["rs_momentum"]), "current": cur, "tail": tail}
        item.update(extra)
        results.append(item)
    return results

def qsum(items):
    return {q: [s["name"] for s in items if s["quadrant"] == q] for q in ["Leading", "Improving", "Lagging", "Weakening"]}

# =============================================================================
# CALCULATE RRM FOR ONE BENCHMARK — UPDATED with thematic_indices
# =============================================================================
def calc_for_benchmark(bench_sym, config, price_data, sector_stocks, custom_stock_items, daily_tail, weekly_tail, monthly_tail, window):
    if bench_sym not in price_data:
        log.warning(f"Benchmark {bench_sym} not in price data, skipping")
        return None

    bc = price_data[bench_sym]["closes"]
    bd = price_data[bench_sym]["dates"]

    def to_list(cfg):
        return [{"symbol": k, **v} for k, v in cfg.items() if k != bench_sym]

    sec_list = to_list(config.get("sectors", {}))
    thm_list = to_list(config.get("thematic_indices", {}))  # NEW v4.3
    etf_list = to_list(config.get("etfs", {}))
    ac_list  = to_list(config.get("asset_classes", {}))
    ms_list  = to_list(config.get("market_segments", {}))
    gi_list  = to_list(config.get("global_indices", {}))
    cs_list  = [{"symbol": s["symbol"], "name": s.get("name", s["symbol"]), "color": stock_color(s.get("name", s["symbol"]), i), "sector": s.get("sector", ""), "group": s.get("group", "Custom")} for i, s in enumerate(custom_stock_items)]

    # ── Daily ──
    d_sec = calc_rrm_items(price_data, sec_list, bc, bd, daily_tail, window)
    d_thm = calc_rrm_items(price_data, thm_list, bc, bd, daily_tail, window)  # NEW
    d_etf = calc_rrm_items(price_data, etf_list, bc, bd, daily_tail, window)
    d_ac  = calc_rrm_items(price_data, ac_list,  bc, bd, daily_tail, window)
    d_ms  = calc_rrm_items(price_data, ms_list,  bc, bd, daily_tail, window)
    d_gi  = calc_rrm_items(price_data, gi_list,  bc, bd, daily_tail, window)
    d_cs  = calc_rrm_items(price_data, cs_list,  bc, bd, daily_tail, window)

    # ── Weekly ──
    w_sec = calc_rrm_items(price_data, sec_list, bc, bd, weekly_tail, window, resample_weekly)
    w_thm = calc_rrm_items(price_data, thm_list, bc, bd, weekly_tail, window, resample_weekly)  # NEW
    w_etf = calc_rrm_items(price_data, etf_list, bc, bd, weekly_tail, window, resample_weekly)
    w_ac  = calc_rrm_items(price_data, ac_list,  bc, bd, weekly_tail, window, resample_weekly)
    w_ms  = calc_rrm_items(price_data, ms_list,  bc, bd, weekly_tail, window, resample_weekly)
    w_gi  = calc_rrm_items(price_data, gi_list,  bc, bd, weekly_tail, window, resample_weekly)
    w_cs  = calc_rrm_items(price_data, cs_list,  bc, bd, weekly_tail, window, resample_weekly)

    # ── Monthly ──
    m_sec = calc_rrm_items(price_data, sec_list, bc, bd, monthly_tail, window, resample_monthly)
    m_thm = calc_rrm_items(price_data, thm_list, bc, bd, monthly_tail, window, resample_monthly)  # NEW
    m_etf = calc_rrm_items(price_data, etf_list, bc, bd, monthly_tail, window, resample_monthly)
    m_ac  = calc_rrm_items(price_data, ac_list,  bc, bd, monthly_tail, window, resample_monthly)
    m_ms  = calc_rrm_items(price_data, ms_list,  bc, bd, monthly_tail, window, resample_monthly)
    m_gi  = calc_rrm_items(price_data, gi_list,  bc, bd, monthly_tail, window, resample_monthly)
    m_cs  = calc_rrm_items(price_data, cs_list,  bc, bd, monthly_tail, window, resample_monthly)

    # ── Drill-down ──
    sectors = config.get("sectors", {})
    drilldown = {}
    for sec_sym, stocks in sector_stocks.items():
        if sec_sym not in price_data: continue
        sbc, sbd = price_data[sec_sym]["closes"], price_data[sec_sym]["dates"]
        sec_name = sectors.get(sec_sym, {}).get("name", sec_sym)
        stock_items = [{"symbol": s["symbol"], "name": s["name"], "color": stock_color(s["name"], i)} for i, s in enumerate(stocks)]
        dd_d = calc_rrm_items(price_data, stock_items, sbc, sbd, daily_tail, window)
        dd_w = calc_rrm_items(price_data, stock_items, sbc, sbd, weekly_tail, window, resample_weekly)
        dd_m = calc_rrm_items(price_data, stock_items, sbc, sbd, monthly_tail, window, resample_monthly)
        if dd_d or dd_w or dd_m:
            drilldown[sec_sym] = {"sector_name": sec_name, "benchmark": sec_sym, "daily": dd_d, "weekly": dd_w, "monthly": dd_m}

    return {
        "daily": {
            "sectors": d_sec, "thematic_indices": d_thm, "etfs": d_etf,
            "asset_classes": d_ac, "market_segments": d_ms, "global_indices": d_gi, "custom_stocks": d_cs,
            "quadrant_summary": qsum(d_sec), "tail_length": daily_tail,
        },
        "weekly": {
            "sectors": w_sec, "thematic_indices": w_thm, "etfs": w_etf,
            "asset_classes": w_ac, "market_segments": w_ms, "global_indices": w_gi, "custom_stocks": w_cs,
            "quadrant_summary": qsum(w_sec), "tail_length": weekly_tail,
        },
        "monthly": {
            "sectors": m_sec, "thematic_indices": m_thm, "etfs": m_etf,
            "asset_classes": m_ac, "market_segments": m_ms, "global_indices": m_gi, "custom_stocks": m_cs,
            "quadrant_summary": qsum(m_sec), "tail_length": monthly_tail,
        },
        "drilldown": drilldown,
    }

# =============================================================================
# MAIN — UPDATED metadata for v4.3
# =============================================================================
def calculate_rrm(config, daily_tail=5, weekly_tail=5, monthly_tail=5, window=10):
    today = datetime.now().strftime("%Y-%m-%d")
    log.info(f"╔════════════════════════════════════════════════════════════╗")
    log.info(f"║  RRM v4.3 + THEMATIC INDICES + RSI — {today}       ║")
    log.info(f"╚════════════════════════════════════════════════════════════╝")

    benchmarks = config.get("benchmarks", {})
    sectors = config.get("sectors", {})
    thematic = config.get("thematic_indices", {})  # NEW
    etfs = config.get("etfs", {})
    asset_classes = config.get("asset_classes", {})
    market_segments = config.get("market_segments", {})
    global_indices = config.get("global_indices", {})
    custom_stocks = load_custom_stocks()

    all_syms = set()
    all_syms.update(benchmarks.keys(), sectors.keys(), thematic.keys(), etfs.keys(),
                    asset_classes.keys(), market_segments.keys(), global_indices.keys())
    for cs in custom_stocks: all_syms.add(cs["symbol"])

    sector_stocks = {}
    for sec_sym in sectors:
        constituents = get_constituents(sec_sym, config)
        if constituents:
            sector_stocks[sec_sym] = constituents
            for s in constituents: all_syms.add(s["symbol"])

    log.info(f"Total symbols to fetch: {len(all_syms)}")
    log.info(f"  {len(sectors)} sectors, {len(thematic)} thematic, {len(etfs)} ETFs")
    log.info(f"  {len(asset_classes)} assets, {len(market_segments)} segments, {len(global_indices)} global, {len(custom_stocks)} custom")

    price_data = fetch_prices(list(all_syms), period="5y")

    benchmarks_data = {}
    for bench_sym, bench_name in benchmarks.items():
        log.info(f"\n═══ BENCHMARK: {bench_name} ({bench_sym}) ═══")
        result = calc_for_benchmark(bench_sym, config, price_data, sector_stocks, custom_stocks, daily_tail, weekly_tail, monthly_tail, window)
        if result:
            benchmarks_data[bench_sym] = result
            log.info(f"  Sectors: {len(result['daily']['sectors'])}, Thematic: {len(result['daily']['thematic_indices'])}, ETFs: {len(result['daily']['etfs'])}")
            log.info(f"  Globals: {len(result['daily']['global_indices'])}, Custom: {len(result['daily']['custom_stocks'])}")
            log.info(f"  Drilldowns: {len(result['drilldown'])}")

    output = {
        "benchmarks_data": benchmarks_data,
        "available_benchmarks": benchmarks,
        "default_benchmark": config.get("default_benchmark", "^NSEI"),
        "config": {"window": window, "center": 100},
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "date": today,
            "version": "4.3",
            "features": ["rs_ratio", "rs_momentum", "rsi_14", "multi_tf", "global_indices", "thematic_indices", "custom_stocks"],
            "benchmarks_calculated": list(benchmarks_data.keys()),
            "total_sectors": max((len(b["daily"]["sectors"]) for b in benchmarks_data.values()), default=0),
            "total_thematic": max((len(b["daily"]["thematic_indices"]) for b in benchmarks_data.values()), default=0),
            "total_etfs": max((len(b["daily"]["etfs"]) for b in benchmarks_data.values()), default=0),
            "total_global_indices": max((len(b["daily"]["global_indices"]) for b in benchmarks_data.values()), default=0),
            "total_custom_stocks": max((len(b["daily"]["custom_stocks"]) for b in benchmarks_data.values()), default=0),
            "total_drilldown_sectors": max((len(b["drilldown"]) for b in benchmarks_data.values()), default=0),
            "timeframes": ["daily", "weekly", "monthly"],
        },
    }

    log.info(f"\n═══ FINAL SUMMARY ═══")
    log.info(f"Benchmarks: {', '.join(benchmarks_data.keys())}")
    log.info(f"Sectors: {output['metadata']['total_sectors']}, Thematic: {output['metadata']['total_thematic']}")
    log.info(f"ETFs: {output['metadata']['total_etfs']}, Globals: {output['metadata']['total_global_indices']}")

    ai_note = generate_ai_analysis(output)
    if ai_note: output["ai_analysis"] = ai_note

    return output

# =============================================================================
# AI EOD MARKET RESEARCH NOTE (unchanged from v4.2)
# =============================================================================
def generate_ai_analysis(rrm_output):
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        log.warning("GROQ_API_KEY not set, skipping AI analysis")
        return None
    try:
        import urllib.request
        bm_data = rrm_output.get("benchmarks_data", {})
        nifty = bm_data.get("^NSEI", {})
        today = rrm_output.get("metadata", {}).get("date", "")
        if not nifty: return None

        w_sectors = nifty.get("weekly", {}).get("sectors", [])
        d_sectors = nifty.get("daily", {}).get("sectors", [])
        w_globals = nifty.get("weekly", {}).get("global_indices", [])

        q_dist = {"Leading": 0, "Improving": 0, "Weakening": 0, "Lagging": 0}
        for s in w_sectors:
            q = s.get("quadrant", "")
            if q in q_dist: q_dist[q] += 1
        total = sum(q_dist.values()) or 1
        right_pct = round((q_dist["Leading"] + q_dist["Improving"]) / total * 100)
        if right_pct >= 70: regime = "STRONG BULL"
        elif right_pct >= 55: regime = "BULL"
        elif right_pct >= 45: regime = "TRANSITIONAL"
        elif right_pct >= 30: regime = "BEAR"
        else: regime = "STRONG BEAR"

        sorted_by_mom = sorted(w_sectors, key=lambda s: s.get("current", {}).get("rs_momentum", 100), reverse=True)
        top3, bot3 = sorted_by_mom[:3], sorted_by_mom[-3:]
        d_map = {s["symbol"]: s.get("quadrant", "") for s in d_sectors}
        rotating = []
        for s in w_sectors:
            dq = d_map.get(s["symbol"], "")
            wq = s.get("quadrant", "")
            if dq and wq and dq != wq:
                rotating.append(s["name"] + ": " + wq + " → " + dq)
        global_summary = []
        for g in w_globals[:10]:
            cur = g.get("current", {})
            global_summary.append(g["name"] + ": RS=" + str(round(cur.get("rs_ratio", 100), 1)) + ", Q=" + g.get("quadrant", "?"))

        # Build context parts without backslashes in f-strings
        top3_str = "; ".join([s["name"] + ":RS" + str(round(s["current"]["rs_ratio"], 1)) + ",M" + str(round(s["current"]["rs_momentum"], 1)) for s in top3])
        bot3_str = "; ".join([s["name"] + ":RS" + str(round(s["current"]["rs_ratio"], 1)) + ",M" + str(round(s["current"]["rs_momentum"], 1)) for s in bot3])
        rot_str = "; ".join(rotating) if rotating else "None"
        glob_str = "; ".join(global_summary)
        all_str = "; ".join([s["name"] + ":" + s["quadrant"] + ",RSI" + str(s["current"].get("rsi", "?")) for s in w_sectors])

        context = (
            "DATE:" + today + "\n"
            "Quadrant:L" + str(q_dist["Leading"]) + " I" + str(q_dist["Improving"]) +
            " W" + str(q_dist["Weakening"]) + " Lg" + str(q_dist["Lagging"]) + "\n"
            "Right:" + str(right_pct) + "% Regime:" + regime + "\n"
            "Top3:" + top3_str + "\n"
            "Bot3:" + bot3_str + "\n"
            "Rotation:" + rot_str + "\n"
            "Global:" + glob_str + "\n"
            "All:" + all_str
        )

        prompt = f"You are a senior equity analyst. Write an EOD note for Indian markets. Bloomberg style, opinionated, specific numbers, 2-3 themes, OW/UW recs, 3 actionable calls for swing traders. Under 600 words. ## headers.\n\n{context}"

        payload = json.dumps({"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}], "temperature": 0.4, "max_tokens": 1500}).encode("utf-8")
        req = urllib.request.Request("https://api.groq.com/openai/v1/chat/completions", data=payload, headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            note = result.get("choices", [{}])[0].get("message", {}).get("content", "")
        if note:
            log.info(f"✅ AI Analysis generated ({len(note)} chars)")
            return {"note": note, "generated_at": datetime.now().isoformat(), "model": "llama-3.3-70b-versatile", "regime": regime, "right_pct": right_pct}
        return None
    except Exception as e:
        log.error(f"AI Analysis failed: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="RRM v4.3 Multi-Benchmark + Thematic + RSI")
    parser.add_argument("--output", "-o", type=str, default=None)
    parser.add_argument("--config", "-c", type=str, default=None)
    parser.add_argument("--daily-tail", type=int, default=5)
    parser.add_argument("--weekly-tail", type=int, default=5)
    parser.add_argument("--monthly-tail", type=int, default=5)
    parser.add_argument("--window", "-w", type=int, default=10)
    args = parser.parse_args()

    cp = args.config
    if not cp:
        for c in ["../data/rrm_config.json", "data/rrm_config.json", "rrm_config.json"]:
            if os.path.exists(c): cp = c; break

    cfg = load_config(cp)
    out = calculate_rrm(cfg, args.daily_tail, args.weekly_tail, args.monthly_tail, args.window)
    if not out: sys.exit(1)

    js = json.dumps(out, indent=2, ensure_ascii=False)
    if args.output:
        with open(args.output, "w") as f: f.write(js)
        log.info(f"✅ Saved to {args.output}")
    else:
        print(js)

if __name__ == "__main__":
    main()
