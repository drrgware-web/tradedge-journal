#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════
TradEdge MATRIX — Position Monitor v1.0
═══════════════════════════════════════════════════════════════
Lightweight alert — runs every 30 min during market hours.
Checks held positions (from Supabase) against saved RRM signals.

NO Yahoo Finance calls. Uses rrm_signals_prev.json (already saved).

3 Triggers:
  1. Score ≤ 2 on held position  → EXIT WARNING
  2. Signal RED on held position → DANGER
  3. P&L below -5%               → LOSS ALERT

Env: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, SUPABASE_URL, SUPABASE_ANON_KEY
═══════════════════════════════════════════════════════════════
"""

import json, os, sys, logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("pos_monitor")


def fetch_positions_from_supabase(supabase_url, anon_key):
    """Fetch open positions from Supabase."""
    import urllib.request

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
        open_trades = [t for t in trades if t.get("status") == "Open"]
        return open_trades
    except Exception as e:
        log.warning(f"Supabase fetch failed: {e}")
        return []


def parse_position(trade):
    """Parse a trade into position data with P&L."""
    entries = trade.get("entries", [])
    total_qty = sum(e.get("qty", 0) for e in entries)
    total_cost = sum(e.get("qty", 0) * e.get("price", 0) for e in entries)
    avg_price = total_cost / total_qty if total_qty > 0 else 0
    cmp = trade.get("cmp", avg_price)
    pnl_pct = ((cmp - avg_price) / avg_price * 100) if avg_price > 0 else 0

    return {
        "symbol": trade.get("symbol", ""),
        "qty": total_qty,
        "avg_price": round(avg_price, 2),
        "cmp": round(cmp, 2),
        "pnl_pct": round(pnl_pct, 2),
        "invested": round(total_cost),
        "sl": trade.get("sl", 0),
        "side": trade.get("side", "Buy"),
    }


def check_position_alerts(positions, signals, prev_alerts_file):
    """Check positions against RRM signals and detect triggers."""
    alerts = []

    # Load previously sent alerts to avoid spam
    prev_sent = set()
    if os.path.exists(prev_alerts_file):
        try:
            with open(prev_alerts_file) as f:
                prev_sent = set(json.load(f))
        except:
            pass

    new_sent = set()

    for pos in positions:
        sym = pos["symbol"]
        sym_clean = sym.replace(".NS", "")

        # Find signal (try multiple key formats)
        sig = signals.get(sym) or signals.get(sym + ".NS") or signals.get(sym_clean) or None

        # ── Trigger 1: Score ≤ 2 → EXIT WARNING ──
        if sig and sig.get("score", 5) <= 2:
            alert_key = f"score_low_{sym}"
            if alert_key not in prev_sent:
                alerts.append({
                    "type": "exit_warning",
                    "priority": 4,
                    "message": (
                        f"⚠️ <b>EXIT WARNING</b>: {sym_clean}\n"
                        f"   Score: {sig['score']}/5 | {sig.get('light', '?')}\n"
                        f"   Avg: ₹{pos['avg_price']} → CMP: ₹{pos['cmp']} ({'+' if pos['pnl_pct']>=0 else ''}{pos['pnl_pct']}%)\n"
                        f"   ⛔ Consider reducing/exiting"
                    ),
                })
            new_sent.add(alert_key)

        # ── Trigger 2: Signal RED → DANGER ──
        if sig and sig.get("light") == "RED":
            alert_key = f"red_{sym}"
            if alert_key not in prev_sent:
                alerts.append({
                    "type": "red_signal",
                    "priority": 3,
                    "message": (
                        f"🔴 <b>RED SIGNAL</b>: {sym_clean}\n"
                        f"   Score: {sig['score']}/5 | Action: {sig.get('action', '?')}\n"
                        f"   P&L: {'+' if pos['pnl_pct']>=0 else ''}{pos['pnl_pct']}% | Invested: ₹{pos['invested']:,}"
                    ),
                })
            new_sent.add(alert_key)

        # ── Trigger 3: P&L below -5% → LOSS ALERT ──
        if pos["pnl_pct"] < -5:
            # Alert at each -5% increment: -5%, -10%, -15%...
            loss_level = int(pos["pnl_pct"] / -5) * 5
            alert_key = f"loss_{sym}_{loss_level}"
            if alert_key not in prev_sent:
                alerts.append({
                    "type": "loss_alert",
                    "priority": 3,
                    "message": (
                        f"📉 <b>LOSS ALERT</b>: {sym_clean}\n"
                        f"   P&L: {pos['pnl_pct']}% (below -{loss_level}% threshold)\n"
                        f"   Avg: ₹{pos['avg_price']} → CMP: ₹{pos['cmp']}\n"
                        f"   Invested: ₹{pos['invested']:,} | Qty: {pos['qty']}"
                        + (f"\n   SL: ₹{pos['sl']}" if pos['sl'] > 0 else "")
                    ),
                })
            new_sent.add(alert_key)

    # Save sent alerts (reset daily at 9 AM — allow re-alerting next day)
    with open(prev_alerts_file, "w") as f:
        json.dump(list(new_sent), f)

    alerts.sort(key=lambda a: -a["priority"])
    return alerts


def send_telegram(token, chat_id, message):
    """Send Telegram message."""
    import urllib.request, urllib.parse

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
            return result.get("ok", False)
    except Exception as e:
        log.error(f"Telegram failed: {e}")
        return False


def main():
    log.info("╔═════════════════════════════════════╗")
    log.info("║  MATRIX Position Monitor v1.0       ║")
    log.info("╚═════════════════════════════════════╝")

    # Load env
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = os.environ.get("SUPABASE_ANON_KEY", "")

    if not sb_url or not sb_key:
        log.error("SUPABASE_URL or SUPABASE_ANON_KEY not set.")
        sys.exit(1)

    if not token or not chat_id:
        log.error("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set.")
        sys.exit(1)

    # Fetch positions from Supabase
    trades = fetch_positions_from_supabase(sb_url, sb_key)
    if not trades:
        log.info("No open positions found. Skipping.")
        return

    positions = [parse_position(t) for t in trades]
    log.info(f"Open positions: {len(positions)}")
    for p in positions:
        log.info(f"  {p['symbol']}: avg ₹{p['avg_price']}, cmp ₹{p['cmp']}, P&L {p['pnl_pct']}%")

    # Load saved RRM signals
    signals_file = "data/rrm_signals_prev.json"
    signals = {}
    if os.path.exists(signals_file):
        with open(signals_file) as f:
            signals = json.load(f)
        log.info(f"RRM signals loaded: {len(signals)} symbols")
    else:
        log.warning("No rrm_signals_prev.json found. Signal-based alerts disabled.")

    # Check for alerts
    prev_alerts_file = "data/pos_alerts_sent.json"
    alerts = check_position_alerts(positions, signals, prev_alerts_file)
    log.info(f"Alerts to send: {len(alerts)}")

    if alerts:
        now = datetime.now().strftime("%H:%M")
        header = (
            f"🔮 <b>MATRIX Position Alert</b> — {now}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
        )
        body = "\n\n".join(a["message"] for a in alerts[:10])
        send_telegram(token, chat_id, header + body)
        log.info(f"Sent {len(alerts)} position alert(s)")
    else:
        log.info("No new position alerts.")


if __name__ == "__main__":
    main()
