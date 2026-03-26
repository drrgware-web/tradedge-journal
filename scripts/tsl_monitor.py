#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════
 EDGE PILOT — TSL MONITOR v1.0
 Phase-aware Trailing Stop-Loss Engine
═══════════════════════════════════════════════════════════════

 Runs as a cron job (GitHub Actions or local) post-market.
 
 1. Fetches open positions from Supabase
 2. Gets live/EOD price data via yfinance
 3. Calculates phase-aware TSL using:
    - 21 EMA trail (for +1.5R to +3R positions)
    - 10 DMA trail (for +3R to +5R positions)
    - Chandelier Exit (for +5R+ extended positions)
    - ATR cushion: 0.5×/1×/1.5×/2× based on phase
    - Low-of-day trail (for climax 5×+ ATR days)
    - 3-day low trail (for parabolic moves)
 4. Compares new TSL vs current TSL
 5. If TSL should move UP → sends Telegram alert + updates Supabase
 6. Pushes updated TSL levels to data/tsl_levels.json for frontend

 USAGE:
   python tsl_monitor.py                    # Run once (EOD)
   python tsl_monitor.py --dry-run          # Preview without updating
   python tsl_monitor.py --symbol TCS       # Check single stock

 GITHUB ACTIONS:
   Schedule: Mon-Fri at 4:00 PM IST (after market close)
   See tsl-monitor.yml workflow

 ENV VARS:
   SUPABASE_URL, SUPABASE_KEY
   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
═══════════════════════════════════════════════════════════════
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

# ── Dependencies ──
try:
    import yfinance as yf
    import requests
    import pandas as pd
    import numpy as np
except ImportError:
    print("Installing dependencies...")
    os.system("pip install yfinance requests pandas numpy --break-system-packages -q")
    import yfinance as yf
    import requests
    import pandas as pd
    import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('TSL')

# ═══ CONFIG ═══
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://urnrdpyhncezljirpnmy.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVybnJkcHlobmNlemxqaXJwbm15Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI3NzY5MDIsImV4cCI6MjA4ODM1MjkwMn0.eExEFw1XGAlYBGECqCpl928UvXv5Jchuyr1YYkcrbdw')
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '8659936599:AAFKV6MKfHOSJKKTVqISJI-SwQ_cerTaAbQ')
TELEGRAM_CHAT = os.environ.get('TELEGRAM_CHAT_ID', '183752078')

DATA_DIR = Path('data')
TSL_OUTPUT = DATA_DIR / 'tsl_levels.json'

# ═══ TSL PHASE CONFIG ═══
# Each phase defines: method, ATR cushion multiplier, description
TSL_PHASES = {
    'initial': {
        'r_range': (0, 1.0),
        'method': 'fixed',
        'atr_cushion': 0,
        'desc': 'Initial hard stop — no trail',
    },
    'breakeven': {
        'r_range': (1.0, 1.5),
        'method': 'breakeven',
        'atr_cushion': 0,
        'desc': 'Move SL to entry price (breakeven)',
    },
    'ema21_loose': {
        'r_range': (1.5, 2.0),
        'method': '21ema',
        'atr_cushion': 1.0,  # 1× ATR below 21 EMA
        'desc': '21 EMA - 1×ATR (loose trail)',
    },
    'ema21_tight': {
        'r_range': (2.0, 3.0),
        'method': '21ema',
        'atr_cushion': 0.5,  # 0.5× ATR below 21 EMA
        'desc': '21 EMA - 0.5×ATR (tightening)',
    },
    'dma10_standard': {
        'r_range': (3.0, 5.0),
        'method': '10dma',
        'atr_cushion': 1.0,  # 1× ATR below 10 DMA
        'desc': '10 DMA - 1×ATR (standard trail)',
    },
    'chandelier': {
        'r_range': (5.0, 999),
        'method': 'chandelier',
        'atr_cushion': 2.0,  # 2× ATR from highest close
        'desc': 'Chandelier Exit: Highest Close - 2×ATR',
    },
    'climax': {
        'r_range': None,  # Triggered by ATR expansion, not R-multiple
        'method': 'low_of_day',
        'atr_cushion': 0.5,  # 0.5× ATR below low of day
        'desc': 'Low of climax day - 0.5×ATR (emergency trail)',
    },
    'parabolic': {
        'r_range': None,
        'method': '3day_low',
        'atr_cushion': 0,  # No cushion — tightest possible
        'desc': '3-day low (parabolic move — tightest trail)',
    },
}


# ═══ SUPABASE CLIENT ═══
class SupabaseClient:
    def __init__(self):
        self.url = SUPABASE_URL
        self.key = SUPABASE_KEY
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
        }

    def fetch_open_trades(self):
        """Fetch all OPEN and PARTIAL trades."""
        try:
            r = requests.get(
                f'{self.url}/rest/v1/trades?status=in.(OPEN,PARTIAL)&select=*',
                headers=self.headers, timeout=10
            )
            if r.ok:
                trades = r.json()
                log.info(f'Fetched {len(trades)} open positions from Supabase')
                return trades
            else:
                log.warning(f'Supabase fetch failed: {r.status_code}')
                return []
        except Exception as e:
            log.error(f'Supabase connection error: {e}')
            return []

    def update_trade(self, trade_id, data):
        """Update a trade record."""
        try:
            r = requests.patch(
                f'{self.url}/rest/v1/trades?trade_id=eq.{trade_id}',
                headers=self.headers, json=data, timeout=10
            )
            return r.ok
        except Exception as e:
            log.error(f'Supabase update error for {trade_id}: {e}')
            return False

    def insert_event(self, event):
        """Insert a trade event."""
        try:
            r = requests.post(
                f'{self.url}/rest/v1/trade_events',
                headers={**self.headers, 'Prefer': 'resolution=merge-duplicates'},
                json=event, timeout=10
            )
            return r.ok
        except:
            return False


# ═══ TELEGRAM ═══
def send_telegram(message):
    """Send Telegram alert."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        log.warning('Telegram not configured — skipping alert')
        return False
    try:
        url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
        r = requests.post(url, json={
            'chat_id': TELEGRAM_CHAT,
            'text': message,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True,
        }, timeout=10)
        return r.ok
    except Exception as e:
        log.error(f'Telegram error: {e}')
        return False


# ═══ PRICE DATA ═══
def fetch_price_data(symbol, period='3mo'):
    """Fetch OHLCV data via yfinance and compute indicators."""
    ticker = f'{symbol}.NS'
    try:
        df = yf.download(ticker, period=period, progress=False)
        if df.empty:
            log.warning(f'No data for {ticker}')
            return None

        # Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # Compute indicators
        df['EMA21'] = df['Close'].ewm(span=21, adjust=False).mean()
        df['DMA10'] = df['Close'].rolling(10).mean()
        df['ATR14'] = compute_atr(df, 14)
        df['HighestClose22'] = df['Close'].rolling(22).max()
        df['Low3d'] = df['Low'].rolling(3).min()
        df['DayRange'] = df['High'] - df['Low']
        df['ATR_Multiple'] = df['DayRange'] / df['ATR14']

        return df
    except Exception as e:
        log.error(f'yfinance error for {symbol}: {e}')
        return None


def compute_atr(df, period=14):
    """Compute Average True Range."""
    high = df['High']
    low = df['Low']
    close = df['Close'].shift(1)
    tr = pd.concat([high - low, (high - close).abs(), (low - close).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()


# ═══ TSL CALCULATOR ═══
def determine_phase(trade, df):
    """
    Determine the current TSL phase based on:
    1. R-multiple (unrealized P&L vs risk)
    2. ATR expansion (climax/parabolic detection)
    """
    entry = float(trade.get('entry_price', 0))
    initial_sl = float(trade.get('stop_loss', 0))
    risk_per_share = entry - initial_sl
    if risk_per_share <= 0:
        return 'initial', TSL_PHASES['initial']

    latest = df.iloc[-1]
    cmp = float(latest['Close'])
    unrealized_pnl = cmp - entry
    r_multiple = unrealized_pnl / risk_per_share if risk_per_share > 0 else 0

    atr14 = float(latest['ATR14']) if not pd.isna(latest['ATR14']) else 0
    atr_multiple = float(latest['ATR_Multiple']) if not pd.isna(latest['ATR_Multiple']) else 0

    # Check for climax/parabolic first (overrides R-based phase)
    if atr_multiple >= 7:
        return 'parabolic', TSL_PHASES['parabolic']
    if atr_multiple >= 5:
        return 'climax', TSL_PHASES['climax']

    # R-multiple based phase
    for phase_name, config in TSL_PHASES.items():
        if config['r_range'] is None:
            continue
        r_min, r_max = config['r_range']
        if r_min <= r_multiple < r_max:
            return phase_name, config

    return 'initial', TSL_PHASES['initial']


def calculate_tsl(trade, df, phase_name, phase_config):
    """Calculate the TSL price based on phase method + ATR cushion."""
    entry = float(trade.get('entry_price', 0))
    current_sl = float(trade.get('stop_loss', 0))
    latest = df.iloc[-1]

    cmp = float(latest['Close'])
    ema21 = float(latest['EMA21']) if not pd.isna(latest['EMA21']) else 0
    dma10 = float(latest['DMA10']) if not pd.isna(latest['DMA10']) else 0
    atr14 = float(latest['ATR14']) if not pd.isna(latest['ATR14']) else 0
    highest_close = float(latest['HighestClose22']) if not pd.isna(latest['HighestClose22']) else cmp
    low_of_day = float(latest['Low'])
    low_3d = float(latest['Low3d']) if not pd.isna(latest['Low3d']) else low_of_day
    atr_cushion = phase_config['atr_cushion'] * atr14
    method = phase_config['method']

    if method == 'fixed':
        new_tsl = current_sl  # Don't change

    elif method == 'breakeven':
        new_tsl = max(current_sl, entry)  # Move to entry, never below

    elif method == '21ema':
        raw_tsl = ema21 - atr_cushion
        new_tsl = max(current_sl, raw_tsl)  # Only move UP

    elif method == '10dma':
        raw_tsl = dma10 - atr_cushion
        new_tsl = max(current_sl, raw_tsl)

    elif method == 'chandelier':
        raw_tsl = highest_close - atr_cushion
        new_tsl = max(current_sl, raw_tsl)

    elif method == 'low_of_day':
        raw_tsl = low_of_day - atr_cushion
        new_tsl = max(current_sl, raw_tsl)

    elif method == '3day_low':
        new_tsl = max(current_sl, low_3d)

    else:
        new_tsl = current_sl

    return round(new_tsl, 2), {
        'method': method,
        'phase': phase_name,
        'atr14': round(atr14, 2),
        'atr_cushion': round(atr_cushion, 2),
        'ema21': round(ema21, 2),
        'dma10': round(dma10, 2),
        'chandelier_level': round(highest_close - phase_config['atr_cushion'] * atr14, 2),
        'cmp': round(cmp, 2),
        'day_range': round(float(latest['DayRange']), 2),
        'atr_multiple': round(float(latest['ATR_Multiple']), 2) if not pd.isna(latest['ATR_Multiple']) else 0,
        'highest_close_22d': round(highest_close, 2),
        'low_3d': round(low_3d, 2),
    }


# ═══ MAIN MONITOR ═══
def run_monitor(dry_run=False, single_symbol=None):
    """Main TSL monitoring loop."""
    log.info('═══ EDGE PILOT TSL MONITOR v1.0 ═══')
    log.info(f'Mode: {"DRY RUN" if dry_run else "LIVE"} | Time: {datetime.now().strftime("%Y-%m-%d %H:%M")}')

    db = SupabaseClient()

    # Fetch open positions
    if SUPABASE_KEY:
        trades = db.fetch_open_trades()
    else:
        # Fallback: read from local file
        local_path = Path('data/open_trades.json')
        if local_path.exists():
            trades = json.loads(local_path.read_text())
            log.info(f'Loaded {len(trades)} trades from local file')
        else:
            log.warning('No Supabase key and no local trades file. Exiting.')
            return

    if single_symbol:
        trades = [t for t in trades if t.get('symbol') == single_symbol]

    if not trades:
        log.info('No open positions. Nothing to monitor.')
        return

    results = []
    alerts = []

    for trade in trades:
        symbol = trade.get('symbol', '?')
        trade_id = trade.get('trade_id', '?')
        entry_price = float(trade.get('entry_price', 0))
        current_sl = float(trade.get('stop_loss', 0))
        remaining_qty = int(trade.get('remaining_qty', trade.get('qty', 0)))
        risk_per_share = entry_price - current_sl

        log.info(f'─── {symbol} ({trade_id}) ───')
        log.info(f'  Entry: ₹{entry_price} | Current SL: ₹{current_sl} | Qty: {remaining_qty}')

        # Fetch price data
        df = fetch_price_data(symbol)
        if df is None or df.empty:
            log.warning(f'  Skipping {symbol} — no price data')
            continue

        # Determine phase
        phase_name, phase_config = determine_phase(trade, df)
        log.info(f'  Phase: {phase_name} — {phase_config["desc"]}')

        # Calculate new TSL
        new_tsl, details = calculate_tsl(trade, df, phase_name, phase_config)

        cmp = details['cmp']
        r_mult = (cmp - entry_price) / risk_per_share if risk_per_share > 0 else 0

        log.info(f'  CMP: ₹{cmp} | R-Multiple: {r_mult:.1f}R')
        log.info(f'  Method: {details["method"]} | ATR(14): ₹{details["atr14"]} | Cushion: ₹{details["atr_cushion"]}')
        log.info(f'  21 EMA: ₹{details["ema21"]} | 10 DMA: ₹{details["dma10"]} | Chandelier: ₹{details["chandelier_level"]}')

        # Check if TSL should move
        tsl_moved = new_tsl > current_sl
        tsl_delta = new_tsl - current_sl

        result = {
            'trade_id': trade_id,
            'symbol': symbol,
            'entry': entry_price,
            'cmp': cmp,
            'current_sl': current_sl,
            'new_tsl': new_tsl,
            'tsl_moved': tsl_moved,
            'tsl_delta': round(tsl_delta, 2),
            'phase': phase_name,
            'method': details['method'],
            'r_multiple': round(r_mult, 2),
            'remaining_qty': remaining_qty,
            **details,
            'timestamp': datetime.now().isoformat(),
        }
        results.append(result)

        if tsl_moved:
            log.info(f'  🔺 TSL MOVE: ₹{current_sl} → ₹{new_tsl} (+₹{tsl_delta:.2f}) [{phase_name}]')

            alert_msg = (
                f'🛡️ <b>TSL UPDATE — {symbol}</b>\n'
                f'━━━━━━━━━━━━━━━━━━\n'
                f'📊 CMP: ₹{cmp:,.2f} ({r_mult:+.1f}R)\n'
                f'🔴 Old SL: ₹{current_sl:,.2f}\n'
                f'🟢 New SL: ₹{new_tsl:,.2f} (+₹{tsl_delta:,.2f})\n'
                f'📐 Phase: {phase_name}\n'
                f'📏 Method: {phase_config["desc"]}\n'
                f'📈 ATR(14): ₹{details["atr14"]} | Day ×ATR: {details["atr_multiple"]:.1f}×\n'
                f'🔢 Remaining: {remaining_qty} shares\n'
                f'━━━━━━━━━━━━━━━━━━\n'
                f'⚡ <b>ACTION:</b> Update SL in your broker to ₹{new_tsl:,.2f}'
            )
            alerts.append(alert_msg)

            if not dry_run:
                # Update Supabase
                sl_moves = json.loads(trade.get('sl_moves', '[]'))
                sl_moves.append({
                    'date': datetime.now().isoformat(),
                    'from': current_sl,
                    'to': new_tsl,
                    'reason': f'{phase_name}: {phase_config["desc"]}',
                    'method': details['method'],
                    'atr14': details['atr14'],
                })

                db.update_trade(trade_id, {
                    'stop_loss': new_tsl,
                    'sl_moves': json.dumps(sl_moves),
                })

                db.insert_event({
                    'event_id': f'TSL-{int(datetime.now().timestamp())}',
                    'trade_id': trade_id,
                    'symbol': symbol,
                    'event_type': 'TSL_MOVE',
                    'price': new_tsl,
                    'reason': f'{phase_name}: {phase_config["desc"]}',
                    'atr_multiple': details['atr_multiple'],
                    'remaining_qty': remaining_qty,
                    'created_at': datetime.now().isoformat(),
                })
        else:
            log.info(f'  ── No TSL change (current ₹{current_sl} ≥ calculated ₹{new_tsl})')

        # Check if CMP < current SL (stop-loss HIT)
        if cmp <= current_sl:
            hit_msg = (
                f'🚨 <b>SL HIT — {symbol}</b>\n'
                f'CMP ₹{cmp:,.2f} ≤ SL ₹{current_sl:,.2f}\n'
                f'⚡ EXIT {remaining_qty} shares immediately!'
            )
            alerts.append(hit_msg)
            log.warning(f'  🚨 SL HIT! CMP ₹{cmp} ≤ SL ₹{current_sl}')

        # Check for ATR climax sell signals
        if details['atr_multiple'] >= 5:
            atr_x = details['atr_multiple']
            sell_pct = 25 if atr_x < 6 else 50 if atr_x < 7 else 75
            sell_msg = (
                f'📈 <b>ATR SELL SIGNAL — {symbol}</b>\n'
                f'Day Range: {atr_x:.1f}× ATR (₹{details["day_range"]:,.0f} vs ATR ₹{details["atr14"]:,.0f})\n'
                f'⚡ Sell {sell_pct}% ({int(remaining_qty * sell_pct / 100)} shares) into strength!\n'
                f'Move TSL to low of day: ₹{float(df.iloc[-1]["Low"]):,.2f}'
            )
            alerts.append(sell_msg)

    # ── Send Telegram Alerts ──
    if alerts:
        # Summary header
        header = (
            f'⚡ <b>EDGE PILOT TSL MONITOR</b>\n'
            f'📅 {datetime.now().strftime("%d %b %Y, %I:%M %p")}\n'
            f'📊 {len(trades)} positions monitored\n'
            f'🔔 {len(alerts)} alerts\n'
            f'{"─" * 30}\n'
        )
        full_msg = header + '\n\n'.join(alerts)

        if dry_run:
            log.info('DRY RUN — would send Telegram:')
            print(full_msg)
        else:
            send_telegram(full_msg)
            log.info(f'Sent {len(alerts)} Telegram alerts')
    else:
        log.info('No alerts to send.')

    # ── Save TSL levels JSON for frontend ──
    DATA_DIR.mkdir(exist_ok=True)
    tsl_data = {
        'generated_at': datetime.now().isoformat(),
        'positions': results,
    }
    TSL_OUTPUT.write_text(json.dumps(tsl_data, indent=2))
    log.info(f'TSL levels saved to {TSL_OUTPUT}')

    # ── Summary ──
    moved = sum(1 for r in results if r['tsl_moved'])
    log.info(f'═══ SUMMARY: {len(results)} checked, {moved} TSL moved, {len(alerts)} alerts ═══')

    return results


# ═══ CLI ═══
if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv
    symbol = None
    for i, arg in enumerate(sys.argv):
        if arg == '--symbol' and i + 1 < len(sys.argv):
            symbol = sys.argv[i + 1]

    run_monitor(dry_run=dry_run, single_symbol=symbol)
