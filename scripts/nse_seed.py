"""
NSE Thematic Seeder — Run this on your Mac to fetch the 28 missing indices
Usage: python3 nse_seed.py
"""
import requests, json, time, os
from datetime import datetime, timedelta

print("=" * 60)
print("  NSE THEMATIC INDEX SEEDER")
print("  Fetches directly from nseindia.com using your home IP")
print("=" * 60)

# Setup session with browser headers
s = requests.Session()
s.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nseindia.com/market-data/live-equity-market',
})

print("\nGetting NSE cookies...")
try:
    s.get('https://www.nseindia.com', timeout=15)
    print("Session ready!\n")
except Exception as e:
    print(f"FAILED to connect to NSE: {e}")
    print("Check your internet connection and try again.")
    exit(1)

# The 28 missing indices
indices = {
    'NIFTY CONSUMER DURABLES': 'NIFTY_CONSR_DURBL.NS',
    'NIFTY HEALTHCARE': 'NIFTY_HEALTHCARE.NS',
    'NIFTY OIL & GAS': 'NIFTY_OIL_AND_GAS.NS',
    'NIFTY CHEMICALS': 'NIFTY_CHEMICALS.NS',
    'NIFTY INDIA DIGITAL': 'NIFTY_IND_DIGITAL.NS',
    'NIFTY INDIA MANUFACTURING': 'NIFTY_INDIA_MFG.NS',
    'NIFTY GROWTH SECTORS 15': 'NIFTY_GROWSECT_15.NS',
    'NIFTY INDIA DEFENCE': 'NIFTY_IND_DEFENCE.NS',
    'NIFTY INDIA TOURISM': 'NIFTY_IND_TOURISM.NS',
    'NIFTY CAPITAL MARKETS': 'NIFTY_CAPITAL_MKT.NS',
    'NIFTY EV & NEW AGE AUTOMOTIVE': 'NIFTY_EV.NS',
    'NIFTY HOUSING': 'NIFTY_HOUSING.NS',
    'NIFTY CORE HOUSING': 'NIFTY_COREHOUSING.NS',
    'NIFTY INTERNET': 'NIFTY_INTERNET.NS',
    'NIFTY MOBILITY': 'NIFTY_MOBILITY.NS',
    'NIFTY RURAL': 'NIFTY_RURAL.NS',
    'NIFTY WAVES': 'NIFTY_WAVES.NS',
    'NIFTY FINANCIAL SERVICES EX-BANK': 'NIFTY_FINSRV_EX_BANK.NS',
    'NIFTY FINANCIAL SERVICES 25/50': 'NIFTY_FIN_SRV_25_50.NS',
    'NIFTY MIDSMALL FINANCIAL SERVICES': 'NIFTY_MS_FIN_SERV.NS',
    'NIFTY MIDSMALL IT & TELECOM': 'NIFTY_MS_IT_TELECOM.NS',
    'NIFTY MIDSMALL INDIA CONSUMPTION': 'NIFTY_MS_IND_CONS.NS',
    'NIFTY MIDSMALL HEALTHCARE': 'NIFTY_MIDSML_HLTH.NS',
    'NIFTY NON-CYCLICAL CONSUMER': 'NIFTY_NONCYC_CONS.NS',
    'NIFTY TRANSPORTATION & LOGISTICS': 'NIFTY_TRANS_LOGIS.NS',
    'NIFTY TELECOM': 'NIFTY_TELECOM.NS',
    'NIFTY CORPORATE MAATR': 'NIFTY_CORP_MAATR.NS',
    'NIFTY INFRASTRUCTURE & LOGISTICS': 'NIFTY_INFRA_LOG.NS',
}

def refresh_session():
    """Re-fetch cookies if NSE returns 403"""
    try:
        s.get('https://www.nseindia.com', timeout=15)
        time.sleep(1)
    except:
        pass

def fetch_index(nse_name):
    """Fetch 5 years of history for one index, chunked by year"""
    closes, dates = [], []
    now = datetime.now()
    
    for yr in range(4, -1, -1):
        fr = now - timedelta(days=365 * (yr + 1))
        to = now - timedelta(days=365 * yr) if yr > 0 else now
        
        encoded = nse_name.replace(' ', '+').replace('&', '%26')
        url = f'https://www.nseindia.com/api/historical/indicesHistory?indexType={encoded}&from={fr.strftime("%d-%m-%Y")}&to={to.strftime("%d-%m-%Y")}'
        
        try:
            time.sleep(0.5)
            r = s.get(url, timeout=20)
            
            if r.status_code == 403:
                refresh_session()
                r = s.get(url, timeout=20)
            
            if r.status_code == 200:
                data = r.json().get('data', {})
                recs = data.get('indexCloseOnlineRecords') or data.get('indexTurnoverRecords') or []
                for rec in recs:
                    d = rec.get('EOD_TIMESTAMP') or rec.get('TIMESTAMP', '')
                    c = rec.get('EOD_CLOSE_INDEX_VAL') or rec.get('CLOSE', '')
                    if d and c:
                        for fmt in ['%d-%b-%Y', '%d-%m-%Y', '%d %b %Y']:
                            try:
                                dt = datetime.strptime(d.strip(), fmt)
                                dates.append(dt.strftime('%Y-%m-%d'))
                                closes.append(float(str(c).replace(',', '')))
                                break
                            except ValueError:
                                continue
        except Exception as e:
            pass
    
    # Sort and deduplicate
    if closes:
        pairs = sorted(set(zip(dates, closes)), key=lambda x: x[0])
        dates = [p[0] for p in pairs]
        closes = [p[1] for p in pairs]
    
    return closes, dates

# Fetch all 28
cache = {}
ok, fail = 0, 0

for nse_name, ticker in indices.items():
    short = nse_name.replace('NIFTY ', '')
    print(f"  [{ok+fail+1}/28] {short}...", end=' ', flush=True)
    
    closes, dates = fetch_index(nse_name)
    
    if len(closes) >= 30:
        cache[ticker] = {'closes': closes, 'dates': dates}
        print(f"{len(closes)} days OK")
        ok += 1
    else:
        print(f"FAILED ({len(closes)} days)")
        fail += 1

print(f"\n{'=' * 60}")
print(f"  RESULT: {ok} succeeded, {fail} failed out of 28")
print(f"{'=' * 60}")

if ok > 0:
    out_path = 'data/nse_thematic_cache.json'
    os.makedirs('data', exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(cache, f)
    print(f"\nSaved to {out_path}")
    print(f"Now re-run: python3 scripts/rrm_fetcher.py -o data/rrm_data.json")
else:
    print("\nNo data fetched. NSE might be down or blocking your IP too.")
