"""
Stock.html Search Bar Fix — Auto-patcher
Run: python3 patch-stock-search.py
Adds autocomplete search bar when no ?s= param is provided.
Also fixes rrg.html nav link (removed).
"""
import sys, os, re

FILE = 'stock.html'
if not os.path.exists(FILE):
    print(f"ERROR: {FILE} not found. Run this in the repo root folder.")
    sys.exit(1)

with open(FILE, 'r', encoding='utf-8') as f:
    code = f.read()

original = code
changes = 0

# ═══ FIX 1: Change SYMBOL default from 'RELIANCE' to null when no ?s= param ═══
FIND1 = "const SYMBOL = (urlParams.get('s') || 'RELIANCE').toUpperCase();"
REPLACE1 = "const SYMBOL = urlParams.get('s') ? urlParams.get('s').toUpperCase() : null;"

if FIND1 in code:
    code = code.replace(FIND1, REPLACE1, 1)
    changes += 1
    print("✅ Fix 1: SYMBOL defaults to null when no ?s= param")
else:
    print("⚠️  Fix 1: Pattern not found")

# ═══ FIX 2: Remove rrg.html nav links ═══
# Remove all <a href="rrg.html" ...>RRG</a> links
rrg_pattern = r'\s*<a href="rrg\.html"[^>]*>RRG</a>'
if re.search(rrg_pattern, code):
    code = re.sub(rrg_pattern, '', code)
    changes += 1
    print("✅ Fix 2: Removed rrg.html nav links")
else:
    print("ℹ️  Fix 2: No rrg.html nav links found")

# ═══ FIX 3: Add StockSearchPage component + NSE_STOCKS list before StockPage ═══
# We insert a search page component that shows when SYMBOL is null

SEARCH_COMPONENT = r'''
// ═══ NSE STOCK LIST FOR AUTOCOMPLETE ═══
const NSE_STOCKS_SEARCH=[
  {s:"RELIANCE",n:"Reliance Industries",sec:"Oil & Gas"},{s:"TCS",n:"Tata Consultancy Services",sec:"IT"},
  {s:"HDFCBANK",n:"HDFC Bank",sec:"Banking"},{s:"INFY",n:"Infosys",sec:"IT"},
  {s:"ICICIBANK",n:"ICICI Bank",sec:"Banking"},{s:"HINDUNILVR",n:"Hindustan Unilever",sec:"FMCG"},
  {s:"SBIN",n:"State Bank of India",sec:"Banking"},{s:"BHARTIARTL",n:"Bharti Airtel",sec:"Telecom"},
  {s:"ITC",n:"ITC Limited",sec:"FMCG"},{s:"KOTAKBANK",n:"Kotak Mahindra Bank",sec:"Banking"},
  {s:"LT",n:"Larsen & Toubro",sec:"Infrastructure"},{s:"AXISBANK",n:"Axis Bank",sec:"Banking"},
  {s:"ASIANPAINT",n:"Asian Paints",sec:"Paints"},{s:"MARUTI",n:"Maruti Suzuki",sec:"Auto"},
  {s:"SUNPHARMA",n:"Sun Pharma",sec:"Pharma"},{s:"TITAN",n:"Titan Company",sec:"Consumer"},
  {s:"BAJFINANCE",n:"Bajaj Finance",sec:"NBFC"},{s:"WIPRO",n:"Wipro",sec:"IT"},
  {s:"ULTRACEMCO",n:"UltraTech Cement",sec:"Cement"},{s:"HCLTECH",n:"HCL Technologies",sec:"IT"},
  {s:"NTPC",n:"NTPC Limited",sec:"Power"},{s:"POWERGRID",n:"Power Grid Corp",sec:"Power"},
  {s:"TATAMOTORS",n:"Tata Motors",sec:"Auto"},{s:"M&M",n:"Mahindra & Mahindra",sec:"Auto"},
  {s:"TATASTEEL",n:"Tata Steel",sec:"Metals"},{s:"ONGC",n:"ONGC",sec:"Oil & Gas"},
  {s:"JSWSTEEL",n:"JSW Steel",sec:"Metals"},{s:"ADANIENT",n:"Adani Enterprises",sec:"Diversified"},
  {s:"ADANIPORTS",n:"Adani Ports",sec:"Infrastructure"},{s:"COALINDIA",n:"Coal India",sec:"Mining"},
  {s:"BAJAJFINSV",n:"Bajaj Finserv",sec:"NBFC"},{s:"TECHM",n:"Tech Mahindra",sec:"IT"},
  {s:"INDUSINDBK",n:"IndusInd Bank",sec:"Banking"},{s:"HDFCLIFE",n:"HDFC Life",sec:"Insurance"},
  {s:"SBILIFE",n:"SBI Life Insurance",sec:"Insurance"},{s:"DIVISLAB",n:"Divi's Labs",sec:"Pharma"},
  {s:"DRREDDY",n:"Dr. Reddy's Labs",sec:"Pharma"},{s:"CIPLA",n:"Cipla",sec:"Pharma"},
  {s:"APOLLOHOSP",n:"Apollo Hospitals",sec:"Healthcare"},{s:"EICHERMOT",n:"Eicher Motors",sec:"Auto"},
  {s:"GRASIM",n:"Grasim Industries",sec:"Cement"},{s:"BRITANNIA",n:"Britannia Industries",sec:"FMCG"},
  {s:"NESTLEIND",n:"Nestle India",sec:"FMCG"},{s:"HEROMOTOCO",n:"Hero MotoCorp",sec:"Auto"},
  {s:"HINDALCO",n:"Hindalco Industries",sec:"Metals"},{s:"TATACONSUM",n:"Tata Consumer",sec:"FMCG"},
  {s:"BPCL",n:"BPCL",sec:"Oil & Gas"},{s:"VEDL",n:"Vedanta",sec:"Mining"},
  {s:"HAL",n:"Hindustan Aeronautics",sec:"Defence"},{s:"BEL",n:"Bharat Electronics",sec:"Defence"},
  {s:"IRCTC",n:"IRCTC",sec:"Railways"},{s:"ZOMATO",n:"Zomato",sec:"Consumer Tech"},
  {s:"DMART",n:"Avenue Supermarts",sec:"Retail"},{s:"PIDILITIND",n:"Pidilite Industries",sec:"Chemicals"},
  {s:"SIEMENS",n:"Siemens India",sec:"Capital Goods"},{s:"ABB",n:"ABB India",sec:"Capital Goods"},
  {s:"HAVELLS",n:"Havells India",sec:"Consumer Durables"},{s:"TRENT",n:"Trent Limited",sec:"Retail"},
  {s:"JINDALSTEL",n:"Jindal Steel",sec:"Metals"},{s:"SAIL",n:"Steel Authority",sec:"Metals"},
  {s:"GAIL",n:"GAIL India",sec:"Oil & Gas"},{s:"IOC",n:"Indian Oil Corp",sec:"Oil & Gas"},
  {s:"PNB",n:"Punjab National Bank",sec:"Banking"},{s:"BANKBARODA",n:"Bank of Baroda",sec:"Banking"},
  {s:"POLYCAB",n:"Polycab India",sec:"Cables"},{s:"KEI",n:"KEI Industries",sec:"Cables"},
  {s:"DIXON",n:"Dixon Technologies",sec:"Electronics"},{s:"DEEPAKNI",n:"Deepak Nitrite",sec:"Chemicals"},
  {s:"SRF",n:"SRF Limited",sec:"Chemicals"},{s:"ASTRAL",n:"Astral Limited",sec:"Pipes"},
  {s:"AMBUJACEM",n:"Ambuja Cements",sec:"Cement"},{s:"ACC",n:"ACC Limited",sec:"Cement"},
  {s:"INDIGO",n:"InterGlobe Aviation",sec:"Aviation"},{s:"TATAPOWER",n:"Tata Power",sec:"Power"},
  {s:"RECLTD",n:"REC Limited",sec:"NBFC"},{s:"PFC",n:"Power Finance Corp",sec:"NBFC"},
  {s:"CHOLAFIN",n:"Cholamandalam Inv",sec:"NBFC"},{s:"MUTHOOTFIN",n:"Muthoot Finance",sec:"NBFC"},
  {s:"LTIM",n:"LTIMindtree",sec:"IT"},{s:"PERSISTENT",n:"Persistent Systems",sec:"IT"},
  {s:"COFORGE",n:"Coforge",sec:"IT"},{s:"BIOCON",n:"Biocon",sec:"Pharma"},
  {s:"LUPIN",n:"Lupin",sec:"Pharma"},{s:"AUROPHARMA",n:"Aurobindo Pharma",sec:"Pharma"},
  {s:"DLF",n:"DLF Limited",sec:"Real Estate"},{s:"GODREJPROP",n:"Godrej Properties",sec:"Real Estate"},
  {s:"OBEROIRLTY",n:"Oberoi Realty",sec:"Real Estate"},{s:"LODHA",n:"Macrotech Developers",sec:"Real Estate"},
  {s:"COCHINSHIP",n:"Cochin Shipyard",sec:"Defence"},{s:"MAZAGON",n:"Mazagon Dock Ship",sec:"Defence"},
  {s:"RVNL",n:"Rail Vikas Nigam",sec:"Railways"},{s:"IRFC",n:"IRFC",sec:"NBFC"},
  {s:"NHPC",n:"NHPC Limited",sec:"Power"},{s:"CDSL",n:"CDSL",sec:"Financial Services"},
  {s:"BSE",n:"BSE Limited",sec:"Financial Services"},{s:"MCX",n:"MCX India",sec:"Financial Services"},
  {s:"KPITTECH",n:"KPIT Technologies",sec:"IT"},{s:"TATAELXSI",n:"Tata Elxsi",sec:"IT"},
  {s:"MARICO",n:"Marico",sec:"FMCG"},{s:"DABUR",n:"Dabur India",sec:"FMCG"},
  {s:"COLPAL",n:"Colgate Palmolive",sec:"FMCG"},{s:"VBL",n:"Varun Beverages",sec:"Beverages"},
  {s:"MAXHEALTH",n:"Max Healthcare",sec:"Healthcare"},{s:"FORTIS",n:"Fortis Healthcare",sec:"Healthcare"},
  {s:"MOTHERSON",n:"Samvardhana Motherson",sec:"Auto Ancillary"},{s:"BHARATFORG",n:"Bharat Forge",sec:"Auto Ancillary"},
  {s:"MRF",n:"MRF Limited",sec:"Tyres"},{s:"APOLLOTYRE",n:"Apollo Tyres",sec:"Tyres"},
  {s:"NAUKRI",n:"Info Edge India",sec:"Internet"},{s:"ZYDUSLIFE",n:"Zydus Lifesciences",sec:"Pharma"},
].sort((a,b)=>a.s.localeCompare(b.s));

// ═══ STOCK SEARCH PAGE (shown when no ?s= param) ═══
const StockSearchPage = () => {
  const[input,setInput]=useState('');
  const[suggestions,setSuggestions]=useState([]);
  const[showList,setShowList]=useState(false);
  const ref=useRef(null);

  useEffect(()=>{
    if(input.length>=1){
      const q=input.toUpperCase();
      const m=NSE_STOCKS_SEARCH.filter(s=>s.s.startsWith(q)||s.n.toUpperCase().includes(q)).slice(0,15);
      setSuggestions(m);setShowList(m.length>0);
    }else{setSuggestions([]);setShowList(false)}
  },[input]);

  useEffect(()=>{
    const h=e=>{if(ref.current&&!ref.current.contains(e.target))setShowList(false)};
    document.addEventListener('click',h);return()=>document.removeEventListener('click',h);
  },[]);

  const go=(sym)=>window.location.href=`stock.html?s=${sym}`;

  return(
    <div>
      <nav className="nav">
        <a href="index.html" style={{display:"flex",alignItems:"center"}}><img src="assets/logo.jpeg" alt="TradEdge" style={{height:80,marginRight:16}} onError={e=>e.target.style.display='none'}/></a>
        <a href="scanner.html" className="nav-link">Scanner</a>
        <a href="sectors.html" className="nav-link">Sectors</a>
        <a href="matrix.html" className="nav-link">Matrix</a>
        <a href="index.html" className="nav-link">Journal</a>
        <a href="watchlist.html" className="nav-link">Watchlist</a>
        <a href="#" className="nav-link active">Stock</a>
      </nav>
      <div style={{maxWidth:700,margin:"0 auto",padding:"60px 20px",textAlign:"center"}}>
        <div style={{fontSize:48,marginBottom:16,opacity:0.4}}>📋</div>
        <h1 style={{fontSize:28,fontWeight:900,color:"#fff",marginBottom:8,fontFamily:"var(--font-display)"}}>Stock Drilldown</h1>
        <p style={{fontSize:13,color:"var(--text-muted)",marginBottom:32,fontFamily:"var(--font-mono)"}}>
          FUNDAMENTALS • O'NEIL SCORE • GURU RATINGS • SURVEILLANCE
        </p>
        <div ref={ref} style={{position:"relative",maxWidth:500,margin:"0 auto"}}>
          <form onSubmit={e=>{e.preventDefault();if(input.trim())go(input.trim().toUpperCase())}}>
            <input
              type="text" value={input}
              onChange={e=>setInput(e.target.value.toUpperCase())}
              onFocus={()=>input.length>=1&&suggestions.length>0&&setShowList(true)}
              placeholder="Enter NSE Symbol (e.g., RELIANCE, TCS, INFY)"
              autoComplete="off" autoFocus
              style={{width:"100%",padding:"16px 20px",fontSize:15,fontFamily:"var(--font-mono)",
                background:"var(--bg-tertiary)",border:"1px solid var(--border-med)",borderRadius:showList?"10px 10px 0 0":"10px",
                color:"var(--accent)",outline:"none"}}
            />
          </form>
          {showList&&<div style={{position:"absolute",top:"100%",left:0,right:0,maxHeight:400,overflowY:"auto",
            background:"var(--bg-secondary)",border:"1px solid var(--border-med)",borderTop:"none",
            borderRadius:"0 0 10px 10px",zIndex:50,boxShadow:"0 10px 40px rgba(0,0,0,0.5)"}}>
            {suggestions.map((s,i)=>(
              <div key={i} onClick={()=>go(s.s)} style={{padding:"10px 16px",cursor:"pointer",display:"flex",
                justifyContent:"space-between",alignItems:"center",borderBottom:"1px solid var(--border-dim)",
                fontFamily:"var(--font-mono)",fontSize:12,transition:"background 0.1s"}}
                onMouseEnter={e=>e.target.style.background="rgba(0,255,136,0.05)"}
                onMouseLeave={e=>e.target.style.background="transparent"}>
                <div style={{display:"flex",alignItems:"center",gap:10}}>
                  <span style={{fontWeight:700,color:"var(--accent)"}}>{s.s}</span>
                  <span style={{color:"var(--text-muted)",fontSize:10,maxWidth:200,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{s.n}</span>
                </div>
                {s.sec&&<span style={{fontSize:9,padding:"2px 6px",background:"rgba(0,200,255,0.1)",color:"var(--accent2)",borderRadius:3}}>{s.sec}</span>}
              </div>
            ))}
          </div>}
        </div>
        <div style={{marginTop:40,fontSize:11,color:"var(--text-muted)"}}>
          Or open from <a href="scanner.html" style={{color:"var(--accent)",textDecoration:"none"}}>Scanner</a>,
          <a href="watchlist.html" style={{color:"var(--accent)",textDecoration:"none",marginLeft:4}}>Watchlist</a>, or
          <a href="edge-intel.html" style={{color:"var(--accent)",textDecoration:"none",marginLeft:4}}>Edge Intel</a>
        </div>
      </div>
    </div>
  );
};

'''

# Find the insertion point: just before "const StockPage = () => {"
FIND3 = "const StockPage = () => {"
if FIND3 in code:
    code = code.replace(FIND3, SEARCH_COMPONENT + "\n" + FIND3, 1)
    changes += 1
    print("✅ Fix 3: Added StockSearchPage component with autocomplete")
else:
    print("⚠️  Fix 3: StockPage component not found")

# ═══ FIX 4: Update the render to show search page when SYMBOL is null ═══
FIND4 = 'ReactDOM.createRoot(document.getElementById("root")).render(<StockPage/>);'
REPLACE4 = 'ReactDOM.createRoot(document.getElementById("root")).render(SYMBOL ? <StockPage/> : <StockSearchPage/>);'

if FIND4 in code:
    code = code.replace(FIND4, REPLACE4, 1)
    changes += 1
    print("✅ Fix 4: Renders search page when no ?s= param")
else:
    print("⚠️  Fix 4: Render line not found")

# Save
if changes > 0:
    backup = FILE + '.backup'
    with open(backup, 'w', encoding='utf-8') as f:
        f.write(original)
    print(f"\n📁 Backup: {backup}")
    with open(FILE, 'w', encoding='utf-8') as f:
        f.write(code)
    print(f"✅ Saved {changes}/4 changes to {FILE}")
    print("\nPush:")
    print("  git add stock.html")
    print('  git commit -m "feat: stock search bar + remove rrg nav"')
    print("  git push")
else:
    print("\n❌ No changes applied.")
