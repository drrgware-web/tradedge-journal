#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════
TradEdge MATRIX — Position Sync to custom_stocks.json
═══════════════════════════════════════════════════════════════
Runs BEFORE rrm_fetcher.py in the daily workflow.
Fetches open positions from Supabase and writes them to
data/custom_stocks.json so the fetcher calculates their RRM.

Env: SUPABASE_URL, SUPABASE_ANON_KEY
═══════════════════════════════════════════════════════════════
"""

import json, os, sys, logging
from datetime import datetime
import urllib.request

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("pos_sync")


def fetch_positions(supabase_url, anon_key):
    """Fetch open positions from Supabase."""
    api_url = f"{supabase_url.rstrip('/')}/rest/v1/tradedge_trades?select=trades_json&order=updated_at.desc&limit=1"
    headers = {
        "apikey": anon_key,
        "Authorization": f"Bearer {anon_key}",
        "Content-Type": "application/json",
    }
    try:
        req = urllib.request.Request(api_url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            rows = json.loads(resp.read())
        if not rows or not rows[0].get("trades_json"):
            return []
        trades = json.loads(rows[0]["trades_json"])
        return [t for t in trades if t.get("status") == "Open"]
    except Exception as e:
        log.error(f"Supabase fetch failed: {e}")
        return []


def main():
    log.info("╔═════════════════════════════════════════╗")
    log.info("║  MATRIX Position Sync v1.0              ║")
    log.info("╚═════════════════════════════════════════╝")

    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = os.environ.get("SUPABASE_ANON_KEY", "")

    if not sb_url or not sb_key:
        log.warning("No Supabase credentials. Skipping position sync.")
        return

    # Fetch open positions
    positions = fetch_positions(sb_url, sb_key)
    log.info(f"Open positions from Supabase: {len(positions)}")

    if not positions:
        log.info("No open positions. Keeping existing custom_stocks.json.")
        return

    # Build custom_stocks entries
    today = datetime.now().strftime("%Y-%m-%d")
    stocks = []
    seen = set()

    for p in positions:
        sym = p.get("symbol", "").strip()
        if not sym or sym in seen:
            continue
        seen.add(sym)

        # Ensure .NS suffix
        sym_ns = sym if sym.endswith(".NS") else sym + ".NS"

        stocks.append({
            "symbol": sym_ns,
            "name": sym.replace(".NS", ""),
            "sector": "",
            "group": "Positions",
            "added_date": today,
        })

    log.info(f"Synced {len(stocks)} position stocks:")
    for s in stocks:
        log.info(f"  {s['symbol']} ({s['name']})")

    # Read existing custom_stocks.json to preserve non-position entries
    output_path = "data/custom_stocks.json"
    existing_non_pos = []

    if os.path.exists(output_path):
        try:
            with open(output_path) as f:
                existing = json.load(f)
            # Keep entries that aren't from "Positions" group (user-added watchlist etc.)
            existing_non_pos = [s for s in existing.get("custom_stocks", []) if s.get("group") != "Positions"]
            log.info(f"Preserved {len(existing_non_pos)} non-position custom stocks")
        except Exception as e:
            log.warning(f"Could not read existing custom_stocks.json: {e}")

    # Merge: non-position entries + fresh position entries
    all_stocks = existing_non_pos + stocks

    # Write
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump({"custom_stocks": all_stocks}, f, indent=2)

    log.info(f"Saved {len(all_stocks)} total stocks → {output_path}")


if __name__ == "__main__":
    main()
