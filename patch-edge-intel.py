"""
Edge Intel Live Price Fix — Auto-patcher
Run: python3 patch-edge-intel.py
Applies 3 changes to edge-intel.html to fix stale price bug.
"""
import re, sys, os

FILE = 'edge-intel.html'
if not os.path.exists(FILE):
    print(f"ERROR: {FILE} not found. Run this in the same folder as edge-intel.html")
    sys.exit(1)

with open(FILE, 'r', encoding='utf-8') as f:
    code = f.read()

original = code
changes = 0

# ═══ CHANGE 1: Add liveCMP state + auto-refresh to EdgeIntel component ═══
FIND1 = "const inputRef=useRef(null);"
INSERT1 = """const inputRef=useRef(null);

  const[liveCMP,setLiveCMP]=useState(0);

  // ═══ LIVE PRICE FIX: Auto-refresh CMP every 30s in main app ═══
  useEffect(()=>{
    if(!symbol) return;
    const wUrl=localStorage.getItem('zd_worker_url');
    if(!wUrl) return;
    const fetchLive=async()=>{
      try{
        const res=await fetch(wUrl,{method:'POST',headers:{'Content-Type':'application/json','X-Kite-Action':'yahoo-proxy'},body:JSON.stringify({ticker:`${symbol}.NS`,range:'1d',interval:'1m'})});
        const d=await res.json();const r=d?.chart?.result?.[0];
        const mp=r?.meta?.regularMarketPrice;
        if(mp&&mp>0){setLiveCMP(+mp.toFixed(2));setStockData(prev=>prev?{...prev,price:+mp.toFixed(2)}:prev);return;}
        const c=r?.indicators?.quote?.[0]?.close?.filter(x=>x!=null&&x>0);
        if(c&&c.length>0){const p=+(c[c.length-1]).toFixed(2);setLiveCMP(p);setStockData(prev=>prev?{...prev,price:p}:prev);}
      }catch(e){}
    };
    fetchLive();
    const iv=setInterval(fetchLive,30000);
    return()=>clearInterval(iv);
  },[symbol]);"""

if FIND1 in code:
    code = code.replace(FIND1, INSERT1, 1)
    changes += 1
    print("✅ Change 1: Added liveCMP state + 30s auto-refresh")
else:
    print("⚠️  Change 1: Pattern not found (inputRef)")

# ═══ CHANGE 2: Fix header price to use liveCMP ═══
FIND2 = """<div style={{fontSize:24,fontWeight:800,fontFamily:'var(--font-mono)',color:'#fff'}}>₹{stockData.price?.toLocaleString()}</div>"""
REPLACE2 = """<div style={{fontSize:24,fontWeight:800,fontFamily:'var(--font-mono)',color:'#fff'}}>₹{(liveCMP||stockData.price)?.toLocaleString()}</div>"""

if FIND2 in code:
    code = code.replace(FIND2, REPLACE2, 1)
    changes += 1
    print("✅ Change 2: Header price now uses liveCMP")
else:
    print("⚠️  Change 2: Pattern not found (header price)")

# ═══ CHANGE 3: Fix change % to use live price ═══
FIND3 = """{stockData.change_pct>=0?'▲':'▼'} {stockData.change_pct>=0?'+':''}{stockData.change_pct?.toFixed(2)}%"""
REPLACE3 = """{(()=>{const curr=liveCMP||stockData.price;const prev=stockData.price/(1+(stockData.change_pct||0)/100);const chg=prev>0?((curr-prev)/prev*100):(stockData.change_pct||0);return`${chg>=0?'▲':'▼'} ${chg>=0?'+':''}${chg.toFixed(2)}%`})()}"""

if FIND3 in code:
    code = code.replace(FIND3, REPLACE3, 1)
    changes += 1
    print("✅ Change 3: Change % now uses live price")
else:
    print("⚠️  Change 3: Pattern not found (change %)")

# Save
if changes > 0:
    # Backup original
    backup = FILE + '.backup'
    with open(backup, 'w', encoding='utf-8') as f:
        f.write(original)
    print(f"\n📁 Backup saved: {backup}")
    
    with open(FILE, 'w', encoding='utf-8') as f:
        f.write(code)
    print(f"✅ Saved {changes}/3 changes to {FILE}")
    print(f"\nPush to GitHub and the live price will work correctly!")
else:
    print("\n❌ No changes applied. File may already be patched or structure differs.")
