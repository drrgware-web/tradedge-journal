/**
 * TradEdge — Dhan Live CMP Module v2
 * js/dhan-live.js
 *
 * Real-time LTP via Dhan API through Cloudflare Worker.
 * Falls back to Yahoo proxy if Dhan unavailable.
 *
 * Worker actions used:
 *   X-Kite-Action: dhan-ltp      → POST { NSE_EQ: [secId, ...] }
 *   X-Kite-Action: dhan-symbols  → GET  (scrip master CSV)
 *   X-Kite-Action: yahoo-proxy   → POST { ticker }
 *
 * Auth headers: X-Dhan-Token, X-Dhan-ID
 *
 * Usage:
 *   await DhanLive.init();
 *   DhanLive.getCMP('RELIANCE');        // cached price
 *   DhanLive.onUpdate(prices => {...}); // subscribe
 *   DhanLive.destroy();                 // cleanup
 *
 * localStorage keys: dhan_id, dhan_tk, zd_worker_url
 */

window.DhanLive = (() => {
  // --- Config ---
  const POLL_MS = 30_000;
  const MKT_OPEN  = 9 * 60 + 15;   // 9:15 IST
  const MKT_CLOSE = 15 * 60 + 45;  // 15:45 IST (+15 min buffer)
  const BATCH = 50;
  const SYM_KEY = 'dhan_symbol_map';
  const SYM_TTL = 24 * 60 * 60 * 1000;

  // --- State ---
  let _map = {};         // { RELIANCE: { secId: 2885, exch: 'NSE_EQ' }, ... }
  let _prices = {};      // { RELIANCE: 1234.50, ... }
  let _timer = null;
  let _cbs = [];
  let _ready = false;
  let _url = '', _tk = '', _id = '';
  let _useDhan = false;

  // --- Helpers ---
  function isMarket() {
    const now = new Date();
    const ist = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
    const d = ist.getDay();
    if (d === 0 || d === 6) return false;
    const m = ist.getHours() * 60 + ist.getMinutes();
    return m >= MKT_OPEN && m <= MKT_CLOSE;
  }

  function openSymbols() {
    try {
      const t = JSON.parse(localStorage.getItem('te_trades') || '[]');
      return [...new Set(t.filter(x => x.status === 'Open').map(x => x.symbol))];
    } catch { return []; }
  }

  function workerFetch(action, opts = {}) {
    const headers = { 'Content-Type': 'application/json', 'X-Kite-Action': action };
    if (_tk) headers['X-Dhan-Token'] = _tk;
    if (_id) headers['X-Dhan-ID'] = _id;
    return fetch(_url, { method: 'POST', ...opts, headers: { ...headers, ...(opts.headers || {}) } });
  }

  // --- Symbol Map ---
  async function loadMap() {
    // Cache check
    try {
      const c = JSON.parse(localStorage.getItem(SYM_KEY) || '{}');
      if (c._ts && Date.now() - c._ts < SYM_TTL && Object.keys(c).length > 100) {
        delete c._ts;
        _map = c;
        console.log(`[DhanLive] Symbol map from cache (${Object.keys(_map).length})`);
        return;
      }
    } catch {}

    // Fetch fresh
    try {
      const res = await workerFetch('dhan-symbols', { body: '{}' });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const csv = await res.text();
      const lines = csv.split('\n');
      const hdr = lines[0].split(',');

      const iSym = Math.max(hdr.indexOf('SEM_TRADING_SYMBOL'), hdr.indexOf('SEM_CUSTOM_SYMBOL'));
      const iSec = hdr.indexOf('SEM_SMST_SECURITY_ID');
      const iExch = hdr.indexOf('SEM_EXM_EXCH_ID');
      const iSeg = hdr.indexOf('SEM_SEGMENT');
      const iSeries = hdr.indexOf('SEM_SERIES');

      if (iSym === -1 || iSec === -1) throw new Error('Column mismatch in scrip master');

      const map = {};
      for (let i = 1; i < lines.length; i++) {
        const c = lines[i].split(',');
        if (!c[iSym] || !c[iSec]) continue;
        const exch = (c[iExch] || '').trim();
        const seg = (c[iSeg] || '').trim();
        const series = (c[iSeries] || '').trim();

        if (exch === 'NSE' && (series === 'EQ' || seg === 'E')) {
          const sym = c[iSym].trim().replace(/-EQ$/, '');
          map[sym] = { secId: parseInt(c[iSec].trim()), exch: 'NSE_EQ' };
        }
      }

      _map = map;
      try { localStorage.setItem(SYM_KEY, JSON.stringify({ ...map, _ts: Date.now() })); } catch {}
      console.log(`[DhanLive] Symbol map built (${Object.keys(map).length} NSE equities)`);
    } catch (err) {
      console.warn('[DhanLive] Symbol map failed:', err.message);
      _map = {};
    }
  }

  // --- Dhan LTP ---
  async function fetchDhan(symbols) {
    if (!symbols.length) return {};
    const valid = symbols.filter(s => _map[s]);
    if (!valid.length) return {};

    const out = {};
    for (let i = 0; i < valid.length; i += BATCH) {
      const batch = valid.slice(i, i + BATCH);
      const ids = batch.map(s => _map[s].secId);

      try {
        const res = await workerFetch('dhan-ltp', {
          body: JSON.stringify({ NSE_EQ: ids })
        });
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const data = await res.json();

        if (data?.data) {
          for (const sym of batch) {
            const id = _map[sym].secId;
            const entry = data.data[String(id)] || data.data[id];
            if (entry?.last_price != null) out[sym] = entry.last_price;
          }
        }
      } catch (err) {
        console.warn('[DhanLive] LTP batch failed:', err.message);
      }
    }
    return out;
  }

  // --- Yahoo Fallback ---
  async function fetchYahoo(symbols) {
    const out = {};
    const promises = symbols.map(async (sym) => {
      try {
        const res = await workerFetch('yahoo-proxy', {
          body: JSON.stringify({ ticker: sym + '.NS', range: '1d', interval: '1d' })
        });
        if (!res.ok) return;
        const data = await res.json();
        const price = data?.chart?.result?.[0]?.meta?.regularMarketPrice;
        if (price) out[sym] = price;
      } catch {}
    });
    await Promise.allSettled(promises);
    return out;
  }

  // --- Poll ---
  async function poll() {
    const syms = openSymbols();
    if (!syms.length) return;

    let prices = {};

    if (_useDhan && Object.keys(_map).length > 0) {
      prices = await fetchDhan(syms);
    }

    const missing = syms.filter(s => prices[s] == null);
    if (missing.length > 0) {
      const yp = await fetchYahoo(missing);
      prices = { ...prices, ...yp };
    }

    let changed = false;
    for (const [sym, price] of Object.entries(prices)) {
      if (price != null && _prices[sym] !== price) {
        _prices[sym] = price;
        changed = true;
      }
    }

    if (changed) {
      try {
        const trades = JSON.parse(localStorage.getItem('te_trades') || '[]');
        let upd = false;
        for (const t of trades) {
          if (t.status === 'Open' && prices[t.symbol] != null) {
            t.cmp = prices[t.symbol];
            upd = true;
          }
        }
        if (upd) localStorage.setItem('te_trades', JSON.stringify(trades));
      } catch {}

      const snapshot = { ...prices };
      for (const cb of _cbs) { try { cb(snapshot); } catch {} }
      window.dispatchEvent(new CustomEvent('dhan-cmp-update', { detail: { prices: snapshot } }));
    }

    return prices;
  }

  function startPoll() {
    stopPoll();
    poll();
    _timer = setInterval(() => { if (isMarket()) poll(); }, POLL_MS);
    console.log('[DhanLive] Polling started (30s, market hours)');
  }

  function stopPoll() {
    if (_timer) { clearInterval(_timer); _timer = null; }
  }

  // --- Public API ---
  return {
    async init() {
      if (_ready) return;
      _url = (localStorage.getItem('zd_worker_url') || '').replace(/\/$/, '');
      _tk = localStorage.getItem('dhan_tk') || '';
      _id = localStorage.getItem('dhan_id') || '';
      _useDhan = !!(_url && _tk && _id);

      if (!_url) { console.warn('[DhanLive] No zd_worker_url set'); return; }

      if (_useDhan) {
        await loadMap();
        console.log('[DhanLive] Dhan mode active');
      } else {
        console.log('[DhanLive] No Dhan creds — Yahoo only');
      }

      _ready = true;
      startPoll();
    },

    getCMP(sym) { return _prices[sym] ?? null; },
    getAllCMP() { return { ..._prices }; },
    async refresh() { return await poll(); },

    onUpdate(cb) { if (typeof cb === 'function') _cbs.push(cb); },
    offUpdate(cb) { _cbs = _cbs.filter(x => x !== cb); },

    isDhanActive() { return _useDhan && Object.keys(_map).length > 0; },
    isMarketOpen() { return isMarket(); },
    getSymbolInfo(sym) { return _map[sym] || null; },

    destroy() { stopPoll(); _cbs = []; _ready = false; },

    reconfigure() {
      _tk = localStorage.getItem('dhan_tk') || '';
      _id = localStorage.getItem('dhan_id') || '';
      _useDhan = !!(_url && _tk && _id);
      console.log(`[DhanLive] Reconfigured — Dhan ${_useDhan ? 'ON' : 'OFF'}`);
    }
  };
})();

// Auto-init
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => DhanLive.init());
} else {
  DhanLive.init();
}
