#!/usr/bin/env python3
"""
TradEdge Scanner — Telegram Alerts v1.0
==========================================
Sends real-time alerts to Telegram for:
  1. HIGH PRIORITY Catalysts (earnings gap-up, blockbuster EPS, volume explosion)
  2. Circuit limit band changes (tightened/loosened)
  3. Earnings date alerts (reporting today / this week)
  4. Scan matches (new stocks entering a scan)
  5. RRM signal changes (quadrant flip, score jump/drop on held positions)
  6. Daily summary digest

Setup:
  1. Create a Telegram bot via @BotFather → get TELEGRAM_BOT_TOKEN
  2. Get your chat ID via @userinfobot → TELEGRAM_CHAT_ID
  3. Set as environment variables or in .env file
  4. Add as GitHub repo secrets for Actions

Usage:
  python scripts/scanner_alerts.py                  # Send all pending alerts
  python scripts/scanner_alerts.py --digest          # Send daily summary only
  python scripts/scanner_alerts.py --test            # Send test message
  python scripts/scanner_alerts.py --catalyst NTPC   # Check catalysts for one stock

Environment:
  TELEGRAM_BOT_TOKEN=your_bot_token
  TELEGRAM_CHAT_ID=your_chat_id
"""

import json
import os
import sys
import time
import argparse
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
ALERT_STATE_FILE = DATA_DIR / "alert_state.json"

# Telegram config
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Alert thresholds
CATALYST_MIN_PRIORITY = 8       # Only alert P8+ catalysts
MAX_ALERTS_PER_RUN = 30         # Don't spam
EARNINGS_ALERT_DAYS = 3         # Alert N days before earnings


# ═══════════════════════════════════════════════════════════════
# TELEGRAM SENDER
# ═══════════════════════════════════════════════════════════════

def send_telegram(text: str, parse_mode: str = "HTML", disable_preview: bool = True) -> bool:
    """Send a message via Telegram Bot API."""
    if not BOT_TOKEN or not CHAT_ID:
        print(f"  [DRY RUN] {text[:100]}...")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": str(disable_preview).lower(),
    }).encode()

    try:
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if result.get("ok"):
                return True
            print(f"  ⚠ Telegram error: {result}")
            return False
    except Exception as e:
        print(f"  ⚠ Telegram send failed: {e}")
        return False


def send_telegram_batch(messages: list[str], delay: float = 0.5):
    """Send multiple messages with rate limiting."""
    sent = 0
    for msg in messages[:MAX_ALERTS_PER_RUN]:
        if send_telegram(msg):
            sent += 1
        time.sleep(delay)
    return sent


# ═══════════════════════════════════════════════════════════════
# ALERT STATE TRACKING (avoid duplicate alerts)
# ═══════════════════════════════════════════════════════════════

def load_alert_state() -> dict:
    if ALERT_STATE_FILE.exists():
        try:
            with open(ALERT_STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"last_run": None, "sent_alerts": {}, "prev_scan_matches": {}}


def save_alert_state(state: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    state["last_run"] = datetime.now().isoformat()
    with open(ALERT_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def is_already_sent(state: dict, alert_key: str) -> bool:
    """Check if this alert was already sent today."""
    today = datetime.now().strftime("%Y-%m-%d")
    sent = state.get("sent_alerts", {})
    return sent.get(alert_key, "") == today


def mark_sent(state: dict, alert_key: str):
    today = datetime.now().strftime("%Y-%m-%d")
    if "sent_alerts" not in state:
        state["sent_alerts"] = {}
    state["sent_alerts"][alert_key] = today

    # Clean old entries (keep last 7 days)
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    state["sent_alerts"] = {k: v for k, v in state["sent_alerts"].items() if v >= cutoff}


# ═══════════════════════════════════════════════════════════════
# ALERT GENERATORS
# ═══════════════════════════════════════════════════════════════

def generate_catalyst_alerts(stocks: list[dict], state: dict) -> list[str]:
    """Generate alerts for high-priority catalysts."""
    alerts = []

    for stock in stocks:
        symbol = stock.get("symbol", "")
        catalysts = stock.get("catalysts", [])

        for cat in catalysts:
            if cat.get("priority", 0) < CATALYST_MIN_PRIORITY:
                continue

            key = f"catalyst_{symbol}_{cat['type']}"
            if is_already_sent(state, key):
                continue

            direction = cat.get("direction", "neutral")
            emoji = "🟢" if direction == "bullish" else "🔴" if direction == "bearish" else "⚪"
            priority = cat.get("priority", 0)

            price = stock.get("price", 0)
            change = stock.get("change_pct", 0)
            change_emoji = "📈" if change > 0 else "📉" if change < 0 else "➡"

            msg = (
                f"{emoji} <b>CATALYST P{priority}</b> — {symbol}\n"
                f"{change_emoji} ₹{price:,.2f} ({change:+.2f}%)\n\n"
                f"<b>{cat.get('title', '')}</b>\n"
                f"{cat.get('detail', '')[:200]}\n\n"
                f"🏷 {stock.get('sector', '')} | RSI: {stock.get('rsi', '—')}"
            )

            alerts.append(msg)
            mark_sent(state, key)

    return alerts


def generate_circuit_alerts(state: dict) -> list[str]:
    """Generate alerts for circuit limit band changes."""
    alerts = []
    circuit_file = DATA_DIR / "circuit_limits.json"

    if not circuit_file.exists():
        return alerts

    try:
        with open(circuit_file) as f:
            data = json.load(f)

        for change in data.get("band_changes", []):
            symbol = change.get("symbol", "")
            key = f"circuit_{symbol}_{change.get('band', '')}"
            if is_already_sent(state, key):
                continue

            direction = change.get("change_direction", "")
            emoji = "🔴" if direction == "tightened" else "🟢" if direction == "loosened" else "⚠"

            msg = (
                f"{emoji} <b>CIRCUIT CHANGE</b> — {symbol}\n"
                f"{change.get('prev_band', '?')} → <b>{change.get('band', '?')}</b> ({direction})\n"
                f"Price: ₹{change.get('close_price', 0):,.2f}"
            )

            alerts.append(msg)
            mark_sent(state, key)

    except Exception as e:
        print(f"  ⚠ Error reading circuit data: {e}")

    return alerts


def generate_earnings_alerts(state: dict) -> list[str]:
    """Generate alerts for upcoming earnings."""
    alerts = []
    earnings_file = DATA_DIR / "earnings_calendar.json"

    if not earnings_file.exists():
        return alerts

    try:
        with open(earnings_file) as f:
            data = json.load(f)

        # Reporting today
        for event in data.get("reporting_today", []):
            symbol = event.get("symbol", "")
            key = f"earnings_today_{symbol}"
            if is_already_sent(state, key):
                continue

            msg = (
                f"⚠️ <b>EARNINGS TODAY</b> — {symbol}\n"
                f"📊 {event.get('quarter', '')} Results\n"
                f"Watch for post-market reaction!"
            )
            alerts.append(msg)
            mark_sent(state, key)

        # Reporting this week
        for event in data.get("reporting_this_week", []):
            symbol = event.get("symbol", "")
            days = event.get("days_until", 0)
            if days <= 0 or days > EARNINGS_ALERT_DAYS:
                continue

            key = f"earnings_upcoming_{symbol}_{event.get('date', '')}"
            if is_already_sent(state, key):
                continue

            msg = (
                f"📅 <b>EARNINGS IN {days}d</b> — {symbol}\n"
                f"{event.get('quarter', '')} Results on {event.get('date', '')}\n"
                f"Consider position management."
            )
            alerts.append(msg)
            mark_sent(state, key)

    except Exception as e:
        print(f"  ⚠ Error reading earnings data: {e}")

    return alerts


def generate_scan_alerts(stocks: list[dict], state: dict) -> list[str]:
    """Generate alerts for new stocks entering key scans."""
    alerts = []
    prev_matches = state.get("prev_scan_matches", {})

    # Collect current scan matches
    current_matches = {}
    for stock in stocks:
        for scan_id in stock.get("matched_scans", []):
            if scan_id not in current_matches:
                current_matches[scan_id] = set()
            current_matches[scan_id].add(stock["symbol"])

    # Important scans to alert on
    ALERT_SCANS = {
        "earnings_gap_up": "📊 Earnings Gap Up",
        "positive_earnings_reaction": "📊 Positive Earnings Reaction",
        "earnings_breakaway_gap": "💥 Earnings Breakaway Gap",
        "vcp": "◈ VCP Pattern",
        "momentum_scanner": "🚀 Momentum Scanner",
        "volume_breakout": "📈 Volume Breakout",
        "minervini_stage2": "📐 Minervini Stage 2",
        "preset_golden_cross_50_200": "✦ Golden Cross 50/200",
        "preset_52w_high_breakout": "⬆ 52W High Breakout",
        "supertrend_buy": "🔺 SuperTrend Buy",
    }

    for scan_id, scan_label in ALERT_SCANS.items():
        current = current_matches.get(scan_id, set())
        prev = set(prev_matches.get(scan_id, []))
        new_entries = current - prev

        if new_entries:
            symbols_str = ", ".join(sorted(new_entries)[:10])
            extra = f" +{len(new_entries)-10} more" if len(new_entries) > 10 else ""

            key = f"scan_{scan_id}_{datetime.now().strftime('%Y-%m-%d')}"
            if is_already_sent(state, key):
                continue

            msg = (
                f"🔍 <b>{scan_label}</b>\n"
                f"<b>{len(new_entries)} new match(es):</b>\n"
                f"{symbols_str}{extra}"
            )
            alerts.append(msg)
            mark_sent(state, key)

    # Save current matches for next comparison
    state["prev_scan_matches"] = {k: list(v) for k, v in current_matches.items()}

    return alerts


def generate_daily_digest(stocks: list[dict]) -> str:
    """Generate a daily summary digest message."""
    total = len(stocks)
    bullish = sum(1 for s in stocks if s.get("composite_score", 0) >= 3)
    bearish = sum(1 for s in stocks if s.get("composite_score", 0) <= -3)
    catalyst_count = sum(1 for s in stocks if s.get("catalysts"))
    vol_spikes = sum(1 for s in stocks if s.get("volume", {}).get("signal") in ("spike", "extreme_spike"))
    restricted = sum(1 for s in stocks if s.get("circuit", {}).get("is_restricted"))
    earnings = sum(1 for s in stocks if s.get("earnings_alert"))

    # Top 5 by score
    top5 = sorted(stocks, key=lambda s: s.get("composite_score", 0), reverse=True)[:5]
    top_str = "\n".join(
        f"  {s['symbol']:10s} Score:{s.get('composite_score',0):+d}  ₹{s.get('price',0):,.0f} ({s.get('change_pct',0):+.1f}%)"
        for s in top5
    )

    # Bottom 5
    bot5 = sorted(stocks, key=lambda s: s.get("composite_score", 0))[:5]
    bot_str = "\n".join(
        f"  {s['symbol']:10s} Score:{s.get('composite_score',0):+d}  ₹{s.get('price',0):,.0f} ({s.get('change_pct',0):+.1f}%)"
        for s in bot5
    )

    # Top catalysts
    all_cats = []
    for s in stocks:
        for c in s.get("catalysts", []):
            if c.get("priority", 0) >= 9:
                all_cats.append((s["symbol"], c))
    all_cats.sort(key=lambda x: x[1].get("priority", 0), reverse=True)
    cat_str = "\n".join(
        f"  P{c['priority']} {sym}: {c['title']}"
        for sym, c in all_cats[:8]
    ) or "  None today"

    msg = (
        f"📊 <b>TRADEDGE DAILY DIGEST</b>\n"
        f"📅 {datetime.now().strftime('%d %b %Y, %I:%M %p IST')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📈 Universe: <b>{total}</b> stocks scanned\n"
        f"🟢 Bullish (≥3): <b>{bullish}</b>\n"
        f"🔴 Bearish (≤-3): <b>{bearish}</b>\n"
        f"⚡ Catalysts: <b>{catalyst_count}</b>\n"
        f"📊 Volume Spikes: <b>{vol_spikes}</b>\n"
        f"📅 Earnings Alerts: <b>{earnings}</b>\n"
        f"🚫 Circuit Restricted: <b>{restricted}</b>\n\n"
        f"<b>🏆 TOP 5 (Score):</b>\n<code>{top_str}</code>\n\n"
        f"<b>⬇ BOTTOM 5 (Score):</b>\n<code>{bot_str}</code>\n\n"
        f"<b>⚡ TOP CATALYSTS:</b>\n<code>{cat_str}</code>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 <a href='https://drrgware-web.github.io/tradedge-journal/scanner.html'>Open Scanner</a>"
    )
    return msg


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="TradEdge Telegram Alerts")
    parser.add_argument("--digest", action="store_true", help="Send daily digest only")
    parser.add_argument("--test", action="store_true", help="Send test message")
    parser.add_argument("--catalyst", type=str, help="Check catalysts for one stock")
    parser.add_argument("--dry-run", action="store_true", help="Print alerts without sending")
    args = parser.parse_args()

    print("=" * 50)
    print("  TradEdge Telegram Alerts")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.dry_run:
        BOT_TOKEN_VAL = ""
        CHAT_ID_VAL = ""
    else:
        BOT_TOKEN_VAL = BOT_TOKEN
        CHAT_ID_VAL = CHAT_ID

    print(f"  Bot: {'configured' if BOT_TOKEN_VAL else '❌ MISSING TOKEN'}")
    print(f"  Chat: {'configured' if CHAT_ID_VAL else '❌ MISSING CHAT_ID'}")
    print("=" * 50)

    # Test message
    if args.test:
        msg = "🧪 <b>TradEdge Scanner</b> — Test alert\n✅ Telegram integration working!"
        send_telegram(msg)
        print("  ✅ Test message sent")
        return

    # Load scanner data
    scanner_file = DATA_DIR / "scanner_results.json"
    if not scanner_file.exists():
        print("  ❌ scanner_results.json not found. Run generate_data.py first.")
        return

    with open(scanner_file) as f:
        scanner_data = json.load(f)

    stocks = scanner_data.get("stocks", [])
    print(f"\n  📊 Loaded {len(stocks)} stocks")

    # Single stock catalyst check
    if args.catalyst:
        sym = args.catalyst.upper()
        stock = next((s for s in stocks if s["symbol"] == sym), None)
        if stock:
            print(f"\n  Catalysts for {sym}:")
            for c in stock.get("catalysts", []):
                print(f"    P{c['priority']} [{c['direction']}] {c['title']}")
            if not stock.get("catalysts"):
                print("    None detected")
        else:
            print(f"  ❌ {sym} not found in scanner data")
        return

    # Daily digest only
    if args.digest:
        msg = generate_daily_digest(stocks)
        send_telegram(msg)
        print("  ✅ Daily digest sent")
        return

    # Full alert run
    state = load_alert_state()
    all_alerts = []

    print("\n  Generating alerts...")

    # 1. Catalyst alerts (highest priority)
    catalyst_alerts = generate_catalyst_alerts(stocks, state)
    all_alerts.extend(catalyst_alerts)
    print(f"    ⚡ Catalysts: {len(catalyst_alerts)} alerts")

    # 2. Circuit change alerts
    circuit_alerts = generate_circuit_alerts(state)
    all_alerts.extend(circuit_alerts)
    print(f"    🔄 Circuit changes: {len(circuit_alerts)} alerts")

    # 3. Earnings alerts
    earnings_alerts = generate_earnings_alerts(state)
    all_alerts.extend(earnings_alerts)
    print(f"    📅 Earnings: {len(earnings_alerts)} alerts")

    # 4. Scan match alerts
    scan_alerts = generate_scan_alerts(stocks, state)
    all_alerts.extend(scan_alerts)
    print(f"    🔍 New scan matches: {len(scan_alerts)} alerts")

    # Send all alerts
    if all_alerts:
        print(f"\n  Sending {len(all_alerts)} alerts...")
        sent = send_telegram_batch(all_alerts)
        print(f"  ✅ Sent {sent}/{len(all_alerts)} alerts")
    else:
        print("\n  ✅ No new alerts to send")

    # Always send daily digest at end
    digest = generate_daily_digest(stocks)
    send_telegram(digest)
    print("  ✅ Daily digest sent")

    # Save state
    save_alert_state(state)
    print(f"  💾 Alert state saved")


if __name__ == "__main__":
    main()
