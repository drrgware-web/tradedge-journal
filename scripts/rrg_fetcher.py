"""
================================================================================
RRG Data Fetcher v2 — Relative Rotation Graph Engine
================================================================================
Config-driven: reads symbols from data/rrg_config.json
Calculates JdK RS-Ratio/Momentum for DAILY + WEEKLY timeframes.
Includes sector drill-down: stock-level RRG within each sector.
Auto-fetches sector constituents from Yahoo Finance, falls back to static JSON.

Requirements:  pip install yfinance numpy
================================================================================
"""

import json, os, sys, argparse, logging, hashlib
from datetime import datetime
import yfinance as yf
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("rrg_fetcher")

# =============================================================================
# CONFIG
# =============================================================================
DEFAULT_SECTORS = {
    "^CNXAUTO": {"name": "Nifty Auto", "color": "#ef4444"},
    "^NSEBANK": {"name": "Nifty Bank", "color": "#3b82f6"},
    "^CNXFIN": {"name": "Nifty Financial Services", "color": "#6366f1"},
    "^CNXFMCG": {"name": "Nifty FMCG", "color": "#22c55e"},
    "^CNXPHARMA": {"name": "Nifty Pharma", "color": "#f59e0b"},
    "^CNXIT": {"name": "Nifty IT", "color": "#06b6d4"},
    "^CNXMETAL": {"name": "Nifty Metal", "color": "#8b5cf6"},
    "^CNXREALTY": {"name": "Nifty Realty", "color": "#ec4899"},
    "^CNXENERGY": {"name": "Nifty Energy", "color": "#f97316"},
    "^CNXINFRA": {"name": "Nifty Infra", "color": "#14b8a6"},
    "^CNXMEDIA": {"name": "Nifty Media", "color": "#a855f7"},
    "^CNXPSUBANK": {"name": "Nifty PSU Bank", "color": "#0ea5e9"},
    "^CNXSERVICE": {"name": "Nifty Services", "color": "#84cc16"},
    "^CNXCONSUM": {"name": "Nifty Consumption", "color": "#e879f9"},
    "^CNXCMDT": {"name": "Nifty Commodities", "color": "#d97706"},
    "^CNXMNC": {"name": "Nifty MNC", "color": "#64748b"},
}

def load_config(config_path):
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path) as f:
                cfg = json.load(f)
            log.info(f"Config: {config_path} — {len(cfg.get('sectors',{}))} sectors, {len(cfg.get('etfs',{}))} ETFs")
            return cfg
        except Exception as e:
            log.warning(f"Config load failed: {e}")
    log.info("Using built-in defaults")
    return {
        "benchmarks": {"^NSEI": "Nifty 50", "^NSEBANK": "Nifty Bank", "^CRSLDX": "Nifty 500"},
        "default_benchmark": "^NSEI",
        "sectors": DEFAULT_SECTORS,
        "etfs": {},
        "sector_constituents": {},
    }

# =============================================================================
# JdK RS-RATIO / RS-MOMENTUM
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
# FETCH PRICES
# =============================================================================
def fetch_prices(symbols, period="2y"):
    log.info(f"Fetching {len(symbols)} symbols...")
    out = {}
    for sym in symbols:
        try:
            h = yf.Ticker(sym).history(period=period, interval="1d")
            if h.empty or len(h) < 30:
                log.warning(f"  ✗ {sym}: {len(h) if not h.empty else 0} rows")
                continue
            out[sym] = {
                "closes": h['Close'].dropna().tolist(),
                "dates": [d.strftime("%Y-%m-%d") for d in h.index],
            }
            log.info(f"  ✓ {sym}: {len(out[sym]['closes'])} days")
        except Exception as e:
            log.error(f"  ✗ {sym}: {e}")
    return out

def resample_weekly(closes, dates):
    from datetime import datetime as dt
    wc, wd, cw = [], [], None
    for c, d in zip(closes, dates):
        wk = dt.strptime(d, "%Y-%m-%d").isocalendar()[:2]
        if cw is not None and wk != cw:
            wc.append(lc); wd.append(ld)
        cw = wk; lc = c; ld = d
    if cw is not None:
        wc.append(lc); wd.append(ld)
    return wc, wd

# =============================================================================
# AUTO-FETCH SECTOR CONSTITUENTS
# =============================================================================
# Yahoo Finance index symbols → we try to get components from yfinance
# This may not work for all indices, hence the fallback to static config.

SECTOR_YF_MAP = {
    "^CNXIT": "NIFTY IT",
    "^NSEBANK": "NIFTY BANK",
    "^CNXAUTO": "NIFTY AUTO",
    "^CNXPHARMA": "NIFTY PHARMA",
    "^CNXFMCG": "NIFTY FMCG",
    "^CNXMETAL": "NIFTY METAL",
    "^CNXREALTY": "NIFTY REALTY",
    "^CNXENERGY": "NIFTY ENERGY",
    "^CNXFIN": "NIFTY FINANCIAL SERVICES",
    "^CNXPSUBANK": "NIFTY PSU BANK",
    "^CNXINFRA": "NIFTY INFRA",
    "^CNXMEDIA": "NIFTY MEDIA",
}

def auto_fetch_constituents(sector_symbol):
    """Try to fetch index constituents from Yahoo Finance."""
    try:
        ticker = yf.Ticker(sector_symbol)
        # yfinance doesn't reliably expose constituents for Indian indices
        # but we try anyway — some indices work
        if hasattr(ticker, 'components') and ticker.components is not None:
            components = list(ticker.components)
            if components:
                return [{"symbol": s, "name": s.replace(".NS", "")} for s in components[:30]]
    except Exception:
        pass
    return None

def get_constituents(sector_symbol, config):
    """Get sector constituents: try auto-fetch first, fall back to static config."""
    # Try auto-fetch
    auto = auto_fetch_constituents(sector_symbol)
    if auto and len(auto) >= 3:
        log.info(f"  Auto-fetched {len(auto)} constituents for {sector_symbol}")
        return auto

    # Fall back to static config
    static = config.get("sector_constituents", {}).get(sector_symbol, [])
    if static:
        log.info(f"  Using static config: {len(static)} constituents for {sector_symbol}")
        return static

    log.warning(f"  No constituents available for {sector_symbol}")
    return []

# =============================================================================
# GENERATE COLORS FOR STOCKS (deterministic from name)
# =============================================================================
STOCK_PALETTE = [
    "#ef4444","#f97316","#f59e0b","#eab308","#84cc16","#22c55e","#14b8a6",
    "#06b6d4","#0ea5e9","#3b82f6","#6366f1","#8b5cf6","#a855f7","#d946ef",
    "#ec4899","#f43f5e","#fb923c","#a3e635","#2dd4bf","#38bdf8",
]

def stock_color(name, idx):
    h = int(hashlib.md5(name.encode()).hexdigest()[:8], 16)
    return STOCK_PALETTE[(h + idx) % len(STOCK_PALETTE)]

# =============================================================================
# RRG FOR A SET OF ITEMS
# =============================================================================
def calc_rrg_items(price_data, items_config, bench_closes, bench_dates, tail_len, window, weekly=False):
    results = []
    for entry in items_config:
        sym = entry["symbol"]
        name = entry.get("name", sym)
        color = entry.get("color", "#94a3b8")
        if sym not in price_data:
            continue

        sc = price_data[sym]["closes"]
        sd = price_data[sym]["dates"]
        bc, bd = bench_closes, bench_dates

        if weekly:
            sc, sd = resample_weekly(sc, sd)
            bc, bd = resample_weekly(bc, bd)

        rs_r, rs_m = calc_rs(sc, bc, window)
        if rs_r is None:
            continue

        valid = [i for i in range(len(rs_r)) if not (np.isnan(rs_r[i]) or np.isnan(rs_m[i]))]
        tail_idx = valid[-(tail_len + 1):]
        tail = []
        for i in tail_idx:
            d = sd[i] if i < len(sd) else ""
            tail.append({"date": d, "rs_ratio": round(float(rs_r[i]), 2), "rs_momentum": round(float(rs_m[i]), 2)})

        if not tail:
            continue
        cur = tail[-1]
        results.append({
            "symbol": sym, "name": name, "color": color,
            "quadrant": quadrant(cur["rs_ratio"], cur["rs_momentum"]),
            "current": cur, "tail": tail,
        })
    return results

# =============================================================================
# MAIN
# =============================================================================
def calculate_rrg(config, benchmark_sym=None, daily_tail=5, weekly_tail=5, window=10):
    today = datetime.now().strftime("%Y-%m-%d")
    log.info(f"╔═══════════════════════════════════════════╗")
    log.info(f"║  RRG v2 CALCULATION — {today}          ║")
    log.info(f"╚═══════════════════════════════════════════╝")

    if not benchmark_sym:
        benchmark_sym = config.get("default_benchmark", "^NSEI")

    benchmarks = config.get("benchmarks", {})
    sectors = config.get("sectors", {})
    etfs = config.get("etfs", {})

    # Collect ALL symbols to fetch in one batch
    all_syms = set()
    all_syms.add(benchmark_sym)
    all_syms.update(sectors.keys())
    all_syms.update(etfs.keys())

    # Also collect stock constituents for drill-down
    sector_stocks = {}
    for sec_sym in sectors:
        constituents = get_constituents(sec_sym, config)
        if constituents:
            sector_stocks[sec_sym] = constituents
            for s in constituents:
                all_syms.add(s["symbol"])

    # Fetch all prices at once (efficient — single batch)
    price_data = fetch_prices(list(all_syms), period="2y")

    if benchmark_sym not in price_data:
        log.error(f"Benchmark {benchmark_sym} not available. Aborting.")
        return None

    bc = price_data[benchmark_sym]["closes"]
    bd = price_data[benchmark_sym]["dates"]

    # Convert sectors/etfs config to list format for calc
    def to_list(cfg):
        return [{"symbol": k, "name": v.get("name", k), "color": v.get("color", "#94a3b8")} for k, v in cfg.items() if k != benchmark_sym]

    sec_list = to_list(sectors)
    etf_list = to_list(etfs)

    # ── DAILY ──
    log.info("═══ DAILY RRG ═══")
    d_sec = calc_rrg_items(price_data, sec_list, bc, bd, daily_tail, window, weekly=False)
    d_etf = calc_rrg_items(price_data, etf_list, bc, bd, daily_tail, window, weekly=False)

    # ── WEEKLY ──
    log.info("═══ WEEKLY RRG ═══")
    w_sec = calc_rrg_items(price_data, sec_list, bc, bd, weekly_tail, window, weekly=True)
    w_etf = calc_rrg_items(price_data, etf_list, bc, bd, weekly_tail, window, weekly=True)

    # ── SECTOR DRILL-DOWN (daily + weekly for each sector's stocks) ──
    log.info("═══ SECTOR DRILL-DOWN ═══")
    drilldown = {}
    for sec_sym, stocks in sector_stocks.items():
        sec_name = sectors.get(sec_sym, {}).get("name", sec_sym)
        if sec_sym not in price_data:
            continue

        sec_bc = price_data[sec_sym]["closes"]
        sec_bd = price_data[sec_sym]["dates"]

        # Assign colors
        stock_items = []
        for i, s in enumerate(stocks):
            stock_items.append({
                "symbol": s["symbol"],
                "name": s["name"],
                "color": stock_color(s["name"], i),
            })

        dd_daily = calc_rrg_items(price_data, stock_items, sec_bc, sec_bd, daily_tail, window, weekly=False)
        dd_weekly = calc_rrg_items(price_data, stock_items, sec_bc, sec_bd, weekly_tail, window, weekly=True)

        if dd_daily or dd_weekly:
            drilldown[sec_sym] = {
                "sector_name": sec_name,
                "benchmark": sec_sym,
                "daily": dd_daily,
                "weekly": dd_weekly,
            }
            log.info(f"  {sec_name}: {len(dd_daily)} daily, {len(dd_weekly)} weekly stocks")

    def qsum(items):
        return {q: [s["name"] for s in items if s["quadrant"] == q]
                for q in ["Leading", "Improving", "Lagging", "Weakening"]}

    output = {
        "benchmark": {"symbol": benchmark_sym, "name": benchmarks.get(benchmark_sym, "Benchmark")},
        "available_benchmarks": benchmarks,
        "daily": {"sectors": d_sec, "etfs": d_etf, "quadrant_summary": qsum(d_sec), "tail_length": daily_tail},
        "weekly": {"sectors": w_sec, "etfs": w_etf, "quadrant_summary": qsum(w_sec), "tail_length": weekly_tail},
        "drilldown": drilldown,
        "config": {"window": window, "center": 100},
        "metadata": {
            "generated_at": datetime.now().isoformat(), "date": today,
            "total_sectors": len(d_sec), "total_etfs": len(d_etf),
            "total_drilldown_sectors": len(drilldown),
            "timeframes": ["daily", "weekly"],
        },
    }

    log.info(f"\n═══ SUMMARY ═══")
    log.info(f"Benchmark: {output['benchmark']['name']}")
    for tf in ["daily", "weekly"]:
        log.info(f"  [{tf.upper()}]")
        for q in ["Leading", "Improving", "Weakening", "Lagging"]:
            ns = output[tf]["quadrant_summary"].get(q, [])
            log.info(f"    {q}: {', '.join(ns) if ns else '(none)'}")
    log.info(f"Drilldown: {len(drilldown)} sectors with stock-level RRG")

    return output


def main():
    parser = argparse.ArgumentParser(description="RRG v2 Fetcher")
    parser.add_argument("--output", "-o", type=str, default=None)
    parser.add_argument("--config", "-c", type=str, default=None)
    parser.add_argument("--benchmark", "-b", type=str, default=None)
    parser.add_argument("--daily-tail", type=int, default=5)
    parser.add_argument("--weekly-tail", type=int, default=5)
    parser.add_argument("--window", "-w", type=int, default=10)
    args = parser.parse_args()

    cp = args.config
    if not cp:
        for c in ["../data/rrg_config.json", "data/rrg_config.json", "rrg_config.json"]:
            if os.path.exists(c):
                cp = c; break

    cfg = load_config(cp)
    out = calculate_rrg(cfg, args.benchmark, args.daily_tail, args.weekly_tail, args.window)

    if not out:
        sys.exit(1)

    js = json.dumps(out, indent=2, ensure_ascii=False)
    if args.output:
        with open(args.output, "w") as f:
            f.write(js)
        log.info(f"✅ Saved to {args.output}")
    else:
        print(js)


if __name__ == "__main__":
    main()
