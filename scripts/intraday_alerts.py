#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════
 EDGE PILOT — INTRADAY ALERT MONITOR v2.0
 Dual Data Source: Dhan API (real-time) + Google Finance (fallback)
═══════════════════════════════════════════════════════════════

 DATA SOURCES (cascade):
   1. Dhan Market Quote API — real-time OHLC, up to 1000 instruments
   2. Google Finance — free, real-time, no API key needed
   3. yfinance — 15-min delayed last resort

 ENV VARS:
   DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN
   SUPABASE_URL, SUPABASE_ANON_KEY
   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
═══════════════════════════════════════════════════════════════
"""
import os, sys, json, re, logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import requests, pandas as pd
except ImportError:
    os.system("pip install requests pandas --break-system-packages -q")
    import requests, pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('INTRADAY')

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://urnrdpyhncezljirpnmy.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_ANON_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVybnJkcHlobmNlemxqaXJwbm15Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI3NzY5MDIsImV4cCI6MjA4ODM1MjkwMn0.eExEFw1XGAlYBGECqCpl928UvXv5Jchuyr1YYkcrbdw')
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '8659936599:AAFKV6MKfHOSJKKTVqISJI-SwQ_cerTaAbQ')
TELEGRAM_CHAT = os.environ.get('TELEGRAM_CHAT_ID', '183752078')
DHAN_CLIENT_ID = os.environ.get('DHAN_CLIENT_ID', '')
DHAN_ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN', '')
DHAN_SECID_FILE = Path('data/dhan_security_ids.json')
ALERT_STATE_FILE = Path('data/intraday_alerts_sent.json')

def supabase_fetch(table, filt=''):
    try:
        r = requests.get(f'{SUPABASE_URL}/rest/v1/{table}?select=*{filt}',
            headers={'apikey':SUPABASE_KEY,'Authorization':f'Bearer {SUPABASE_KEY}'}, timeout=10)
        return r.json() if r.ok else []
    except: return []

def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT: return False
    try:
        return requests.post(f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage',
            json={'chat_id':TELEGRAM_CHAT,'text':msg,'parse_mode':'HTML','disable_web_page_preview':True}, timeout=10).ok
    except: return False

def load_alert_state():
    try:
        if ALERT_STATE_FILE.exists():
            d = json.loads(ALERT_STATE_FILE.read_text())
            cut = (datetime.now()-timedelta(hours=8)).isoformat()
            return {k:v for k,v in d.items() if v.get('time','')>cut}
    except: pass
    return {}

def save_alert_state(s):
    ALERT_STATE_FILE.parent.mkdir(exist_ok=True)
    ALERT_STATE_FILE.write_text(json.dumps(s, indent=2))

# ═══ SOURCE 1: DHAN API ═══
def fetch_dhan_prices(symbols):
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        log.info('Dhan not configured'); return {}
    sids = json.loads(DHAN_SECID_FILE.read_text()) if DHAN_SECID_FILE.exists() else {}
    if not sids: log.warning('No dhan_security_ids.json'); return {}
    nse_ids = []; smap = {}
    for sym in symbols:
        sid = sids.get(sym)
        if sid: nse_ids.append(str(sid)); smap[str(sid)] = sym
    if not nse_ids: return {}
    try:
        r = requests.post('https://api.dhan.co/v2/marketfeed/ohlc',
            headers={'Content-Type':'application/json','Accept':'application/json',
                     'access-token':DHAN_ACCESS_TOKEN,'client-id':DHAN_CLIENT_ID},
            json={"NSE_EQ":nse_ids}, timeout=10)
        if not r.ok: log.warning(f'Dhan {r.status_code}'); return {}
        prices = {}
        for sid, info in r.json().get('data',{}).get('NSE_EQ',{}).items():
            sym = smap.get(sid)
            if sym and 'ohlc' in info:
                o = info['ohlc']; ltp = float(info.get('last_price',0))
                h = float(o.get('high',0)); l = float(o.get('low',0))
                prices[sym] = {'cmp':ltp,'high':h,'low':l,'day_range':h-l,'source':'dhan'}
        log.info(f'Dhan: {len(prices)}/{len(symbols)} (real-time)')
        return prices
    except Exception as e: log.error(f'Dhan error: {e}'); return {}

# ═══ SOURCE 2: GOOGLE FINANCE ═══
def fetch_google_price(sym):
    try:
        r = requests.get(f'https://www.google.com/finance/quote/{sym}:NSE',
            headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}, timeout=8)
        if not r.ok: return None
        pm = re.search(r'data-last-price="([\d.]+)"', r.text)
        if not pm: return None
        cmp = float(pm.group(1))
        rm = re.search(r'data-low="([\d.]+)".*?data-high="([\d.]+)"', r.text, re.DOTALL)
        low = float(rm.group(1)) if rm else cmp
        high = float(rm.group(2)) if rm else cmp
        return {'cmp':cmp,'high':high,'low':low,'day_range':high-low,'source':'google'}
    except: return None

def fetch_google_prices(symbols):
    p = {}
    for s in symbols:
        d = fetch_google_price(s)
        if d: p[s] = d
    log.info(f'Google: {len(p)}/{len(symbols)}')
    return p

# ═══ SOURCE 3: YFINANCE ═══
def fetch_yfinance_prices(symbols):
    try: import yfinance as yf
    except:
        os.system("pip install yfinance --break-system-packages -q")
        import yfinance as yf
    prices = {}
    tickers = [f'{s}.NS' for s in symbols]
    try:
        data = yf.download(tickers, period='1d', progress=False, group_by='ticker')
        if len(tickers)==1:
            if isinstance(data.columns,pd.MultiIndex): data.columns=data.columns.get_level_values(0)
            if not data.empty:
                l=data.iloc[-1]
                prices[symbols[0]]={'cmp':float(l['Close']),'high':float(l['High']),'low':float(l['Low']),'day_range':float(l['High']-l['Low']),'source':'yfinance'}
        else:
            for sym,tk in zip(symbols,tickers):
                try:
                    df=data[tk].dropna()
                    if not df.empty:
                        l=df.iloc[-1]
                        prices[sym]={'cmp':float(l['Close']),'high':float(l['High']),'low':float(l['Low']),'day_range':float(l['High']-l['Low']),'source':'yfinance'}
                except: pass
    except Exception as e: log.error(f'yfinance: {e}')
    log.info(f'yfinance: {len(prices)}/{len(symbols)} (delayed)')
    return prices

# ═══ CASCADE FETCHER ═══
def fetch_all_prices(symbols):
    prices = {}; remaining = list(symbols)
    if DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN:
        prices.update(fetch_dhan_prices(remaining))
        remaining = [s for s in remaining if s not in prices]
    if remaining:
        prices.update(fetch_google_prices(remaining))
        remaining = [s for s in remaining if s not in prices]
    if remaining:
        prices.update(fetch_yfinance_prices(remaining))
    src = {}
    for p in prices.values(): src[p.get('source','?')] = src.get(p.get('source','?'),0)+1
    log.info(f'Sources: {src} | Total: {len(prices)}/{len(symbols)}')
    return prices

# ═══ MAIN ═══
def run(dry_run=False):
    ist = timezone(timedelta(hours=5,minutes=30))
    now = datetime.now(ist)
    log.info(f'=== EDGE PILOT INTRADAY v2.0 === {now.strftime("%I:%M %p IST")}')
    log.info(f'Dhan:{"ON" if DHAN_CLIENT_ID else "OFF"} | Google:ON | yfinance:ON')

    mo = now.replace(hour=9,minute=15,second=0)
    mc = now.replace(hour=15,minute=45,second=0)
    if not (0<=now.weekday()<=4 and mo<=now<=mc):
        if '--force' not in sys.argv: log.info('Outside market hours.'); return

    trades = supabase_fetch('trades','&status=in.(OPEN,PARTIAL)')
    if not trades: log.info('No open positions.'); return

    symbols = list(set(t.get('symbol') for t in trades if t.get('symbol')))
    log.info(f'{len(trades)} positions, {len(symbols)} symbols')
    prices = fetch_all_prices(symbols)
    alert_state = load_alert_state()
    alerts = []

    for t in trades:
        sym=t.get('symbol','?'); tid=t.get('trade_id','?')
        ep=float(t.get('entry_price',0)); sl=float(t.get('stop_loss',0))
        atr=float(t.get('atr_14',0)); rq=int(t.get('remaining_qty',t.get('qty',0)))
        pd2=prices.get(sym)
        if not pd2: continue
        cmp=pd2['cmp']; dr=pd2['day_range']; hi=pd2['high']; lo=pd2['low']; src=pd2.get('source','?')
        pnl=((cmp-ep)/ep*100) if ep>0 else 0

        # SL HIT
        if cmp<=sl and cmp>0:
            k=f'sl_hit_{sym}_{tid}'
            if k not in alert_state:
                alerts.append(f'🚨 <b>SL HIT — {sym}</b>\nCMP ₹{cmp:,.2f} ≤ SL ₹{sl:,.2f}\nEntry ₹{ep:,.2f} | {pnl:+.1f}% | {rq} shares\n⚡ <b>EXIT NOW</b> [{src}]')
                alert_state[k]={'time':now.isoformat(),'type':'sl_hit'}

        # SL APPROACHING
        elif cmp>0 and sl>0:
            sd=((cmp-sl)/cmp*100)
            if 0<sd<=1.5:
                k=f'sl_near_{sym}_{now.strftime("%Y%m%d_%H")}'
                if k not in alert_state:
                    alerts.append(f'⚠️ <b>SL NEAR — {sym}</b>\nCMP ₹{cmp:,.2f} | SL ₹{sl:,.2f} ({sd:.1f}% away) [{src}]')
                    alert_state[k]={'time':now.isoformat(),'type':'sl_near'}

        # ATR CLIMAX
        if atr>0 and dr>0:
            ax=dr/atr
            if ax>=5:
                sp=75 if ax>=7 else 50 if ax>=6 else 25
                lv='7x' if ax>=7 else '6x' if ax>=6 else '5x'
                em='🔴' if ax>=7 else '🟠' if ax>=6 else '🟡'
                sq=int(rq*sp/100)
                k=f'atr_{lv}_{sym}_{now.strftime("%Y%m%d")}'
                if k not in alert_state:
                    alerts.append(f'{em} <b>ATR {lv.upper()} — {sym}</b>\nRange ₹{dr:,.0f} = <b>{ax:.1f}× ATR</b>\nCMP ₹{cmp:,.2f} | {pnl:+.1f}%\n⚡ <b>Sell {sp}% ({sq}/{rq})</b> | TSL→₹{lo:,.2f} [{src}]')
                    alert_state[k]={'time':now.isoformat(),'type':f'atr_{lv}','atr_x':round(ax,1)}

    if alerts:
        hdr=f'⚡ <b>EDGE PILOT</b> {now.strftime("%I:%M %p · %d %b")}\n{len(trades)} pos | {len(alerts)} alerts\n{"━"*28}\n'
        full=hdr+'\n\n'.join(alerts)
        if dry_run: print(full)
        else: send_telegram(full); log.info(f'{len(alerts)} alerts sent')
    else: log.info('All normal.')

    save_alert_state(alert_state)

if __name__=='__main__': run(dry_run='--dry-run' in sys.argv)
