#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════
TradEdge MATRIX — Telegram Alert Engine v1.0
═══════════════════════════════════════════════════════════════
Runs after rrm_fetcher.py in GitHub Actions.
Compares current signals vs previous snapshot.

4 Alert Triggers:
  1. Signal color change  (e.g. RED → GREEN)
  2. Score jumps to 4–5   (high conviction entry)
  3. Score drops to 1–2   (exit warning on held positions)
  4. Weekly quadrant flip  (Leading → Weakening etc.)

Usage:
  python scripts/rrm_alerts.py \
    --data data/rrm_data.json \
    --prev data/rrm_signals_prev.json \
    --positions data/positions.json   (optional)

Env vars: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
═══════════════════════════════════════════════════════════════
"""

import json, os, sys, logging, argparse
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("rrm_alerts")

# ═══ SIGNAL COMPUTATION (mirrors dashboard logic) ═══

def compute_signal(daily, weekly, monthly):
    """Compute traffic light, score, and action from D/W/M quadrant data."""
    def right(q):
        return q in ("Leading", "Improving")

    d_q = daily.get("quadrant", "")
    w_q = weekly.get("quadrant", "")
    m_q = monthly.get("quadrant", "")
    d_rsr = daily.get("current", {}).get("rs_ratio", 100)
    d_rsm = daily.get("current", {}).get("rs_momentum", 100)
    w_rsr = weekly.get("current", {}).get("rs_ratio", 100)
    w_rsm = weekly.get("current", {}).get("rs_momentum", 100)
    m_rsr = monthly.get("current", {}).get("rs_ratio", 100)
    m_rsm = monthly.get("current", {}).get("rs_momentum", 100)

    # Score 1–5
    score = 0
    if right(m_q):
        score += 2 if (m_rsr >= 100 and m_rsm >= 100) else 1
    if right(w_q):
        score += 2 if (w_rsr >= 100 and w_rsm >= 100) else 1
    if right(d_q):
        score += 1
    score = min(score, 5)

    # Traffic light
    if right(m_q) and right(w_q) and right(d_q):
        light = "GREEN"
    elif right(m_q) and not right(w_q) and right(d_q):
        light = "YELLOW"
    elif not right(m_q) and right(w_q) and right(d_q):
        light = "BLUE"
    else:
        light = "RED"

    # RSI check (weekly momentum proxy)
    rsi_ok = right(w_q) and 98 <= w_rsm <= 106

    # Action
    if score >= 4 and rsi_ok:
        action = "AGGRESSIVE BUY"
    elif score >= 3 and rsi_ok:
        action = "BUY ON DIP"
    elif light == "YELLOW":
        action = "HOOK ENTRY"
    elif light == "BLUE":
        action = "SCALP ONLY"
    elif score >= 3:
        action = "HOLD"
    else:
        action = "AVOID"

    return {
        "light": light, "score": score, "action": action,
        "rsi_ok": rsi_ok, "weekly_q": w_q, "daily_q": d_q, "monthly_q": m_q,
    }


def build_signal_snapshot(rrm_data, benchmark="^NSEI"):
    """Build a flat dict of {symbol: signal_data} from rrm_data.json."""
    bm = rrm_data.get("benchmarks_data", {}).get(benchmark, {})
    if not bm:
        return {}

    groups = ["sectors", "etfs", "asset_classes", "market_segments", "global_indices", "custom_stocks"]
    snapshot = {}

    for group in groups:
        daily_items = bm.get("daily", {}).get(group, [])
        weekly_items = bm.get("weekly", {}).get(group, [])
        monthly_items = bm.get("monthly", {}).get(group, [])

        w_map = {s["symbol"]: s for s in weekly_items}
        m_map = {s["symbol"]: s for s in monthly_items}

        for d in daily_items:
            sym = d["symbol"]
            w = w_map.get(sym, {})
            m = m_map.get(sym, {})

            sig = compute_signal(d, w, m)
            sig["name"] = d.get("name", sym)
            sig["symbol"] = sym
            sig["group"] = group
            snapshot[sym] = sig

    return snapshot


# ═══ ALERT DETECTION ═══

LIGHT_EMOJI = {"GREEN": "🟢", "YELLOW": "🟡", "BLUE": "🔵", "RED": "🔴"}
SCORE_EMOJI = {5: "⭐⭐⭐⭐⭐", 4: "⭐⭐⭐⭐", 3: "⭐⭐⭐", 2: "⭐⭐", 1: "⭐"}

def detect_alerts(current, previous, held_symbols=None):
    """Compare current vs previous signals and detect alert triggers."""
    alerts = []
    held = set(held_symbols or [])

    for sym, cur in current.items():
        prev = previous.get(sym)
        if not prev:
            continue  # New symbol, no comparison

        name = cur.get("name", sym)
        c_light = cur["light"]
        p_light = prev.get("light", "")
        c_score = cur["score"]
        p_score = prev.get("score", 0)
        c_wq = cur.get("weekly_q", "")
        p_wq = prev.get("weekly_q", "")

        # ── Trigger 1: Signal color change ──
        if c_light != p_light and p_light:
            emoji_old = LIGHT_EMOJI.get(p_light, "⚪")
            emoji_new = LIGHT_EMOJI.get(c_light, "⚪")
            severity = "🚨" if c_light == "RED" or p_light == "RED" else "📊"
            is_held = "📌 HELD " if sym.replace(".NS", "") in held or sym in held else ""
            alerts.append({
                "type": "signal_change",
                "symbol": sym,
                "name": name,
                "message": f"{severity} {is_held}Signal Change: {name}\n"
                           f"   {emoji_old} {p_light} → {emoji_new} {c_light}\n"
                           f"   Score: {c_score}/5 | Action: {cur['action']}",
                "priority": 3 if "RED" in (c_light, p_light) else 1,
            })

        # ── Trigger 2: Score jumps to 4–5 ──
        if c_score >= 4 and p_score < 4:
            alerts.append({
                "type": "score_jump",
                "symbol": sym,
                "name": name,
                "message": f"🚀 High Conviction: {name}\n"
                           f"   Score: {p_score} → {c_score}/5\n"
                           f"   {LIGHT_EMOJI.get(c_light, '')} {c_light} | Action: {cur['action']}",
                "priority": 2,
            })

        # ── Trigger 3: Score drops to 1–2 on held position ──
        if c_score <= 2 and p_score > 2:
            sym_clean = sym.replace(".NS", "")
            if sym_clean in held or sym in held:
                alerts.append({
                    "type": "score_drop_held",
                    "symbol": sym,
                    "name": name,
                    "message": f"⚠️ EXIT WARNING: {name} (HELD)\n"
                               f"   Score: {p_score} → {c_score}/5\n"
                               f"   {LIGHT_EMOJI.get(c_light, '')} {c_light} | Action: {cur['action']}\n"
                               f"   ⛔ Consider reducing position",
                    "priority": 4,
                })

        # ── Trigger 4: Weekly quadrant flip ──
        if c_wq != p_wq and p_wq:
            bullish_flip = c_wq in ("Leading", "Improving") and p_wq in ("Lagging", "Weakening")
            bearish_flip = c_wq in ("Lagging", "Weakening") and p_wq in ("Leading", "Improving")
            if bullish_flip or bearish_flip:
                emoji = "📈" if bullish_flip else "📉"
                is_held = "📌 HELD " if sym.replace(".NS", "") in held or sym in held else ""
                alerts.append({
                    "type": "quadrant_flip",
                    "symbol": sym,
                    "name": name,
                    "message": f"{emoji} {is_held}Weekly Flip: {name}\n"
                               f"   {p_wq} → {c_wq}\n"
                               f"   Score: {c_score}/5 | {LIGHT_EMOJI.get(c_light, '')} {c_light}",
                    "priority": 2 if bearish_flip else 1,
                })

    # Sort by priority (highest first)
    alerts.sort(key=lambda a: -a["priority"])
    return alerts


# ═══ TELEGRAM SENDER ═══

def send_telegram(token, chat_id, message):
    """Send a message via Telegram Bot API."""
    import urllib.request
    import urllib.parse

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()

    try:
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if result.get("ok"):
                log.info(f"  ✓ Telegram sent ({len(message)} chars)")
                return True
            else:
                log.error(f"  ✗ Telegram error: {result}")
                return False
    except Exception as e:
        log.error(f"  ✗ Telegram failed: {e}")
        return False


def send_alerts(alerts, token, chat_id, max_alerts=20):
    """Send alert messages to Telegram, batched."""
    if not alerts:
        log.info("No alerts to send.")
        return

    # Header
    date = datetime.now().strftime("%Y-%m-%d %H:%M")
    header = (
        f"🔮 <b>MATRIX ALERT</b> — {date}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 {len(alerts)} signal(s) detected\n"
    )

    # Group alerts into batches (Telegram has 4096 char limit)
    batch = header + "\n"
    sent = 0

    for alert in alerts[:max_alerts]:
        entry = alert["message"] + "\n\n"
        if len(batch) + len(entry) > 3800:
            send_telegram(token, chat_id, batch)
            batch = f"🔮 <b>MATRIX ALERT</b> (cont.)\n\n"
            sent += 1

        batch += entry

    if batch.strip():
        send_telegram(token, chat_id, batch)
        sent += 1

    log.info(f"Sent {sent} Telegram message(s) with {len(alerts)} alerts.")


# ═══ SUPABASE POSITIONS FETCH ═══

def fetch_positions_from_supabase(supabase_url, anon_key):
    """Fetch open positions from Supabase tradedge_trades table."""
    import urllib.request

    api_url = f"{supabase_url.rstrip('/')}/rest/v1/tradedge_trades?select=trades_json&limit=1"
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
            log.info("Supabase: no trades_json found")
            return []

        trades = json.loads(rows[0]["trades_json"])
        open_trades = [t for t in trades if t.get("status") == "Open"]
        log.info(f"Supabase: fetched {len(open_trades)} open positions from {len(trades)} total trades")
        return open_trades

    except Exception as e:
        log.warning(f"Supabase fetch failed: {e}")
        return []


# ═══ MAIN ═══

def main():
    parser = argparse.ArgumentParser(description="TradEdge MATRIX — Telegram Alert Engine")
    parser.add_argument("--data", "-d", default="data/rrm_data.json", help="Current RRM data")
    parser.add_argument("--prev", "-p", default="data/rrm_signals_prev.json", help="Previous signals snapshot")
    parser.add_argument("--positions", default="data/positions.json", help="Held positions (optional)")
    parser.add_argument("--benchmark", "-b", default="^NSEI", help="Benchmark symbol")
    parser.add_argument("--dry-run", action="store_true", help="Print alerts without sending")
    args = parser.parse_args()

    # Load current RRM data
    if not os.path.exists(args.data):
        log.error(f"RRM data not found: {args.data}")
        sys.exit(1)

    with open(args.data) as f:
        rrm_data = json.load(f)

    log.info(f"╔═══════════════════════════════════════════╗")
    log.info(f"║  MATRIX Alert Engine v1.0                 ║")
    log.info(f"╚═══════════════════════════════════════════╝")
    log.info(f"Data: {args.data} (v{rrm_data.get('metadata', {}).get('version', '?')})")

    # Build current signal snapshot
    current = build_signal_snapshot(rrm_data, args.benchmark)
    log.info(f"Current snapshot: {len(current)} symbols")

    # Load previous snapshot
    previous = {}
    if os.path.exists(args.prev):
        with open(args.prev) as f:
            previous = json.load(f)
        log.info(f"Previous snapshot: {len(previous)} symbols")
    else:
        log.info("No previous snapshot found — first run, saving baseline.")

    # Load held positions — Supabase first, then local file fallback
    held_symbols = set()
    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_ANON_KEY", "")

    if supabase_url and supabase_key:
        log.info("Fetching positions from Supabase...")
        positions = fetch_positions_from_supabase(supabase_url, supabase_key)
        held_symbols = {p.get("symbol", "").replace(".NS", "") for p in positions}
        if held_symbols:
            log.info(f"Held positions (Supabase): {len(held_symbols)} open — {', '.join(sorted(held_symbols)[:10])}{'...' if len(held_symbols)>10 else ''}")
    elif os.path.exists(args.positions):
        try:
            with open(args.positions) as f:
                positions = json.load(f)
            held_symbols = {p.get("symbol", "").replace(".NS", "") for p in positions if p.get("status") == "Open"}
            log.info(f"Held positions (file): {len(held_symbols)} open")
        except Exception as e:
            log.warning(f"Could not load positions: {e}")
    else:
        log.info("No position source available (no Supabase creds, no positions.json)")

    # Detect alerts
    alerts = detect_alerts(current, previous, held_symbols)
    log.info(f"Alerts detected: {len(alerts)}")

    for a in alerts:
        log.info(f"  [{a['type']}] {a['name']}: {a['message'][:60]}...")

    # Send via Telegram
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

    if args.dry_run:
        log.info("DRY RUN — not sending Telegram messages.")
        for a in alerts:
            print(f"\n{'='*50}")
            print(a["message"])
    elif token and chat_id:
        if alerts:
            send_alerts(alerts, token, chat_id)
        else:
            # Send daily summary even if no alerts
            date = rrm_data.get("metadata", {}).get("date", "")
            summary_counts = {"GREEN": 0, "YELLOW": 0, "BLUE": 0, "RED": 0}
            for s in current.values():
                summary_counts[s["light"]] = summary_counts.get(s["light"], 0) + 1

            pos_line = f"\n📌 {len(held_symbols)} open positions tracked" if held_symbols else ""
            summary = (
                f"✅ <b>MATRIX Daily</b> — {date}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"🟢 {summary_counts['GREEN']} | 🟡 {summary_counts['YELLOW']} | "
                f"🔵 {summary_counts['BLUE']} | 🔴 {summary_counts['RED']}\n"
                f"📊 {len(current)} assets scanned{pos_line}\n"
                f"✨ No signal changes detected"
            )
            send_telegram(token, chat_id, summary)
    else:
        log.warning("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set. Skipping Telegram.")

    # Save current snapshot as previous for next run
    with open(args.prev, "w") as f:
        json.dump(current, f, indent=2)
    log.info(f"Saved snapshot → {args.prev}")


if __name__ == "__main__":
    main()
