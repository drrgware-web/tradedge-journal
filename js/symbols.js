// ═══════════════════════════════════════════════════════════════
// TradEdge NSE Symbols — js/symbols.js
// Built-in 150+ stocks + dynamic NSE EQUITY_L.csv extension
// Autocomplete UI engine
// ═══════════════════════════════════════════════════════════════

'use strict';

// ── Built-in seed list (top 150 NSE stocks) ──────────────────
TE.NSE_BUILTIN = [
  ["RELIANCE","Reliance Industries","Energy"],["TCS","Tata Consultancy","IT"],
  ["HDFCBANK","HDFC Bank","Banking"],["INFY","Infosys","IT"],
  ["ICICIBANK","ICICI Bank","Banking"],["HINDUNILVR","HUL","FMCG"],
  ["ITC","ITC Limited","FMCG"],["SBIN","State Bank","Banking"],
  ["BHARTIARTL","Bharti Airtel","Telecom"],["KOTAKBANK","Kotak Bank","Banking"],
  ["LT","Larsen & Toubro","Infra"],["AXISBANK","Axis Bank","Banking"],
  ["MARUTI","Maruti Suzuki","Auto"],["SUNPHARMA","Sun Pharma","Pharma"],
  ["BAJFINANCE","Bajaj Finance","Finance"],["WIPRO","Wipro","IT"],
  ["HCLTECH","HCL Tech","IT"],["TITAN","Titan","Consumer"],
  ["TATAMOTORS","Tata Motors","Auto"],["TATASTEEL","Tata Steel","Metal"],
  ["JSWSTEEL","JSW Steel","Metal"],["HINDALCO","Hindalco","Metal"],
  ["ADANIENT","Adani Enterprises","Conglomerate"],["ADANIPORTS","Adani Ports","Logistics"],
  ["ONGC","ONGC","Energy"],["COALINDIA","Coal India","Mining"],
  ["NTPC","NTPC","Power"],["POWERGRID","Power Grid","Power"],
  ["TECHM","Tech Mahindra","IT"],["DRREDDY","Dr Reddy's","Pharma"],
  ["CIPLA","Cipla","Pharma"],["APOLLOHOSP","Apollo Hospitals","Healthcare"],
  ["EICHERMOT","Eicher Motors","Auto"],["M&M","Mahindra","Auto"],
  ["TATACONSUM","Tata Consumer","FMCG"],["BRITANNIA","Britannia","FMCG"],
  ["HAVELLS","Havells","Electrical"],["PIDILITIND","Pidilite","Chemical"],
  ["BEL","Bharat Electronics","Defence"],["HAL","HAL","Defence"],
  ["IRFC","IRFC","Finance"],["PFC","PFC","Finance"],["REC","REC","Finance"],
  ["TATAPOWER","Tata Power","Power"],["BPCL","BPCL","Energy"],
  ["HPCL","HPCL","Energy"],["IOC","Indian Oil","Energy"],
  ["GAIL","GAIL","Energy"],["DLF","DLF","Real Estate"],
  ["ZOMATO","Zomato","Internet"],["DMART","DMart","Retail"],
  ["TRENT","Trent","Retail"],["DIXON","Dixon Tech","Electronics"],
  ["POLYCAB","Polycab","Electrical"],["KEI","KEI Industries","Electrical"],
  ["COFORGE","Coforge","IT"],["MPHASIS","Mphasis","IT"],["LTIM","LTIMindtree","IT"],
  ["PERSISTENT","Persistent","IT"],["TATAELXSI","Tata Elxsi","IT"],
  ["MARICO","Marico","FMCG"],["DABUR","Dabur","FMCG"],["COLPAL","Colgate","FMCG"],
  ["VBL","Varun Beverages","FMCG"],["LUPIN","Lupin","Pharma"],
  ["AUROPHARMA","Aurobindo","Pharma"],["BIOCON","Biocon","Pharma"],
  ["FORTIS","Fortis","Healthcare"],["MAXHEALTH","Max Healthcare","Healthcare"],
  ["IRCTC","IRCTC","Travel"],["INDIGO","IndiGo","Aviation"],
  ["RVNL","RVNL","Infra"],["SAIL","SAIL","Metal"],["NMDC","NMDC","Mining"],
  ["VEDL","Vedanta","Metal"],["CONCOR","Container Corp","Logistics"],
  ["BSE","BSE","Finance"],["CDSL","CDSL","Finance"],["MCX","MCX","Finance"],
  ["ANGELONE","Angel One","Finance"],["HDFCAMC","HDFC AMC","Finance"],
  ["NIFTYBEES","ETF - Nifty 50","ETF"],["BANKBEES","ETF - Bank","ETF"],
  ["GOLDBEES","ETF - Gold","ETF"],["ITBEES","ETF - IT","ETF"],
  ["JUNIORBEES","ETF - Next 50","ETF"],
];

// ── Extended DB (loaded from localStorage or NSE fetch) ──────
TE._nseExtended = null;
try {
  const cached = localStorage.getItem('te_nse_ext');
  if (cached) TE._nseExtended = JSON.parse(cached);
} catch (e) {}

TE.getSymbolDB = function() {
  if (!TE._nseExtended) return TE.NSE_BUILTIN;
  if (!TE._nseExtended._merged) {
    const extSet = new Set(TE._nseExtended.map(s => s[0]));
    const sectorMap = {};
    TE.NSE_BUILTIN.forEach(([s, n, sec]) => sectorMap[s] = sec);
    TE._nseExtended.forEach(e => { if (sectorMap[e[0]] && !e[2]) e[2] = sectorMap[e[0]]; });
    TE.NSE_BUILTIN.forEach(([s, n, sec]) => { if (!extSet.has(s)) TE._nseExtended.push([s, n, sec]); });
    TE._nseExtended._merged = true;
  }
  return TE._nseExtended;
};

// ── Score-based fuzzy matching ───────────────────────────────
TE.scoreMatch = function(sym, name, q) {
  if (sym === q) return 100;
  if (sym.startsWith(q)) return 90 - sym.length;
  if (sym.includes(q)) return 70;
  if (name.toUpperCase().includes(q)) return 50;
  let si = 0;
  for (let ci = 0; ci < q.length && si < sym.length; ci++) {
    if (sym[si] === q[ci]) si++;
  }
  if (si >= q.length) return 20;
  return -1;
};

TE.searchSymbols = function(query, limit = 10) {
  const q = query.trim().toUpperCase();
  if (!q) return [];
  return TE.getSymbolDB()
    .map(([sym, name, sector]) => ({ sym, name, sector, score: TE.scoreMatch(sym, name.toUpperCase(), q) }))
    .filter(x => x.score >= 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);
};

TE.highlightMatch = function(sym, q) {
  const idx = sym.indexOf(q);
  if (idx < 0) return sym;
  return sym.slice(0, idx) + `<span style="color:var(--y)">${sym.slice(idx, idx + q.length)}</span>` + sym.slice(idx + q.length);
};

// ── Autocomplete UI Engine ───────────────────────────────────
// Creates a reusable autocomplete for any input + dropdown pair.
// Usage: TE.createAutocomplete({ inputId, dropId, onSelect, onPrice })

TE.createAutocomplete = function(opts) {
  const { inputId, dropId, onSelect, fetchPrice } = opts;
  let focusIdx = -1;

  const input = document.getElementById(inputId);
  const drop  = document.getElementById(dropId);
  if (!input || !drop) return;

  function render(q) {
    if (!q || q.length < 1) { drop.classList.remove('open'); return; }
    const matches = TE.searchSymbols(q, 10);
    focusIdx = -1;

    const customRow = `<div class="sym-opt sym-opt-custom" data-sym="${q}" onmousedown="event.preventDefault()">
      <div class="sym-opt-icon" style="background:var(--yd);border-color:rgba(245,197,66,.3);color:var(--y)">+</div>
      <div class="sym-opt-info"><div class="sym-opt-sym" style="color:var(--y)">${q}</div><div class="sym-opt-name" style="color:var(--t3)">Use as custom symbol</div></div>
      <div style="font-size:9px;color:var(--y);border:1px solid rgba(245,197,66,.4);padding:2px 6px;border-radius:4px;font-weight:700">CUSTOM</div>
    </div>`;

    drop.innerHTML =
      (matches.length ? `<div class="sym-dropdown-header">NSE — ${matches.length} found</div>` : `<div class="sym-dropdown-header" style="color:var(--t3)">No NSE match</div>`) +
      matches.map(s => `<div class="sym-opt" data-sym="${s.sym}" onmousedown="event.preventDefault()">
        <div class="sym-opt-icon">${s.sym.slice(0, 3)}</div>
        <div class="sym-opt-info"><div class="sym-opt-sym">${TE.highlightMatch(s.sym, q)}</div><div class="sym-opt-name">${s.name}</div></div>
        <div style="text-align:right"><div class="sym-opt-sector">${s.sector || ''}</div><div style="font-size:9px;color:var(--b);font-family:'IBM Plex Mono',monospace">NSE</div></div>
      </div>`).join('') +
      `<div style="border-top:1px solid var(--b1)"></div>` + customRow;

    drop.classList.add('open');

    // Bind click events
    drop.querySelectorAll('.sym-opt').forEach(el => {
      el.addEventListener('mousedown', e => {
        e.preventDefault();
        select(el.dataset.sym);
      });
    });
  }

  function select(sym) {
    input.value = sym;
    drop.classList.remove('open');
    if (onSelect) onSelect(sym);
    if (fetchPrice) fetchPrice(sym);
  }

  function close() { drop.classList.remove('open'); }

  input.addEventListener('input', () => render(input.value.trim().toUpperCase()));
  input.addEventListener('focus', () => { if (input.value.trim()) render(input.value.trim().toUpperCase()); });
  input.addEventListener('blur', () => setTimeout(close, 200));

  input.addEventListener('keydown', e => {
    const opts = drop.querySelectorAll('.sym-opt');
    if (!opts.length) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      focusIdx = Math.min(focusIdx + 1, opts.length - 1);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      focusIdx = Math.max(focusIdx - 1, 0);
    } else if (e.key === 'Enter') {
      e.preventDefault();
      if (focusIdx >= 0) { select(opts[focusIdx].dataset.sym); return; }
      const val = input.value.trim().toUpperCase();
      if (val) select(val);
      return;
    } else if (e.key === 'Escape') { close(); return; }
    opts.forEach((o, i) => o.classList.toggle('focused', i === focusIdx));
    if (opts[focusIdx]) opts[focusIdx].scrollIntoView({ block: 'nearest' });
  });

  return { render, select, close };
};

// ── Yahoo Finance Price Fetcher ──────────────────────────────
TE.fetchCMP = async function(symbol) {
  const ticker = symbol.includes('.') ? symbol : symbol + '.NS';
  try {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?interval=1m&range=1d`;
    const proxy = `https://api.allorigins.win/get?url=${encodeURIComponent(url)}`;
    const res = await fetch(proxy, { signal: AbortSignal.timeout(8000) });
    const data = await res.json();
    const parsed = JSON.parse(data.contents);
    return parsed?.chart?.result?.[0]?.meta?.regularMarketPrice || null;
  } catch (e) { return null; }
};

console.log(`[TE Symbols] ${TE.getSymbolDB().length} symbols loaded`);
