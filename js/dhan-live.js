/**
 * TradEdge — Dhan Live CMP Module
 * js/dhan-live.js
 *
 * Provides real-time Last Traded Price (LTP) via Dhan API.
 * Falls back to Yahoo proxy if Dhan is unavailable.
 *
 * Usage:
 *   await DhanLive.init();                    // Load symbol map, start polling
 *   const price = DhanLive.getCMP('RELIANCE'); // Get latest cached price
 *   DhanLive.onUpdate(callback);              // Subscribe to price updates
 *   DhanLive.destroy();                       // Stop polling, cleanup
 *
 * Requires in localStorage:
 *   dhan_id    — Dhan Client ID
 *   dhan_tk    — Dhan Access Token
 *   zd_worker_url — Cloudflare Worker base URL
 */

window.DhanLive = (() => {
  // --- Config ---
  const POLL_INTERVAL = 30_000;       // 30 seconds
  const MARKET_OPEN_H = 9;
  const MARKET_OPEN_M = 15;
  const MARKET_CLOSE_H = 15;
  const MARKET_CLOSE_M = 30;
  const BATCH_SIZE = 50;              // Dhan LTP max per request
  const SYMBOL_MAP_KEY = 'dhan_symbol_map';
  const SYMBOL_MAP_TTL = 24 * 60 * 60 * 1000; // 24h cache

  // --- State ---
  let _symbolMap = {};       // { 'RELIANCE': { secId: 2885, exchange: 'NSE_EQ' }, ... }
  let _priceCache = {};      // { 'RELIANCE': 1234.50, ... }
  let _pollTimer = null;
  let _listeners = [];
  let _initialized = false;
  let _workerUrl = '';
  let _token = '';
  let _clientId = '';
  let _useDhan = false;

  // --- Helpers ---
  function isMarketHours() {
    const now = new Date();
    const ist = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
    const day = ist.getDay();
    if (day === 0 || day === 6) return false; // Weekend
    const mins = ist.getHours() * 60 + ist.getMinutes();
    return mins >= (MARKET_OPEN_H * 60 + MARKET_OPEN_M) &&
           mins <= (MARKET_CLOSE_H * 60 + MARKET_CLOSE_M + 15); // +15 min buffer for closing
  }

  function getOpenSymbols() {
    try {
      const trades = JSON.parse(localStorage.getItem('te_trades') || '[]');
      return [...new Set(trades.filter(t => t.status === 'Open').map(t => t.symbol))];
    } catch { return []; }
  }

  // --- Symbol Map: NSE symbol → Dhan securityId ---
  async function loadSymbolMap() {
    // Check localStorage cache first
    try {
      const cached = JSON.parse(localStorage.getItem(SYMBOL_MAP_KEY) || '{}');
      if (cached._ts && Date.now() - cached._ts < SYMBOL_MAP_TTL && Object.keys(cached).length > 100) {
        delete cached._ts;
        _symbolMap = cached;
        console.log(`[DhanLive] Symbol map loaded from cache (${Object.keys(_symbolMap).length} symbols)`);
        return;
      }
    } catch {}

    // Fetch fresh from worker → Dhan master CSV
    try {
      const resp = await fetch(`${_workerUrl}/dhan-symbols`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const csv = await resp.text();
      const lines = csv.split('\n');
      const header = lines[0].split(',');

      // Find column indices
      const iSym = header.indexOf('SEM_TRADING_SYMBOL') !== -1
        ? header.indexOf('SEM_TRADING_SYMBOL')
        : header.indexOf('SEM_CUSTOM_SYMBOL');
      const iSecId = header.indexOf('SEM_SMST_SECURITY_ID');
      const iExch = header.indexOf('SEM_EXM_EXCH_ID');
      const iSeg = header.indexOf('SEM_SEGMENT');
      const iSeries = header.indexOf('SEM_SERIES');

      if (iSym === -1 || iSecId === -1) {
        // Try alternate column names
        console.warn('[DhanLive] Could not find expected columns, trying alternate parse');
        throw new Error('Column mismatch');
      }

      const map = {};
      for (let i = 1; i < lines.length; i++) {
        const cols = lines[i].split(',');
        if (!cols[iSym] || !cols[iSecId]) continue;

        const exch = (cols[iExch] || '').trim();
        const seg = (cols[iSeg] || '').trim();
        const series = (cols[iSeries] || '').trim();

        // Only NSE equity (EQ series)
        if (exch === 'NSE' && (series === 'EQ' || seg === 'E')) {
          const sym = cols[iSym].trim().replace(/-EQ$/, '');
          map[sym] = {
            secId: parseInt(cols[iSecId].trim()),
            exchange: 'NSE_EQ'
          };
        }
      }

      _symbolMap = map;
      // Cache with timestamp
      const toStore = { ...map, _ts: Date.now() };
      try { localStorage.setItem(SYMBOL_MAP_KEY, JSON.stringify(toStore)); } catch {}
      console.log(`[DhanLive] Symbol map built (${Object.keys(map).length} NSE equities)`);
    } catch (err) {
      console.warn('[DhanLive] Symbol map fetch failed, Dhan LTP unavailable:', err.message);
      _symbolMap = {};
    }
  }

  // --- Fetch LTP via Dhan ---
  async function fetchDhanLTP(symbols) {
    if (!symbols.length) return {};

    // Map symbols to security IDs
    const validSymbols = symbols.filter(s => _symbolMap[s]);
    if (!validSymbols.length) return {};

    const results = {};
    // Batch requests (Dhan limits per call)
    for (let i = 0; i < validSymbols.length; i += BATCH_SIZE) {
      const batch = validSymbols.slice(i, i + BATCH_SIZE);
      const secIds = batch.map(s => _symbolMap[s].secId);

      try {
        const resp = await fetch(`${_workerUrl}/dhan-ltp?token=${encodeURIComponent(_token)}&client_id=${encodeURIComponent(_clientId)}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ NSE_EQ: secIds })
        });

        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();

        // Map security IDs back to symbols
        if (data && data.data) {
          for (const sym of batch) {
            const secId = _symbolMap[sym].secId;
            const entry = data.data[String(secId)] || data.data[secId];
            if (entry && entry.last_price != null) {
              results[sym] = entry.last_price;
            }
          }
        }
      } catch (err) {
        console.warn(`[DhanLive] Dhan LTP batch failed:`, err.message);
      }
    }

    return results;
  }

  // --- Fallback: Yahoo via Cloudflare Worker ---
  async function fetchYahooLTP(symbols) {
    const results = {};
    // Yahoo can handle comma-separated symbols
    const yahooSymbols = symbols.map(s => `${s}.NS`).join(',');

    try {
      const resp = await fetch(`${_workerUrl}/yahoo-proxy?symbols=${encodeURIComponent(yahooSymbols)}`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();

      if (data && data.quoteResponse && data.quoteResponse.result) {
        for (const q of data.quoteResponse.result) {
          const sym = q.symbol.replace('.NS', '').replace('.BO', '');
          if (q.regularMarketPrice) {
            results[sym] = q.regularMarketPrice;
          }
        }
      }
    } catch (err) {
      console.warn('[DhanLive] Yahoo fallback failed:', err.message);
    }

    return results;
  }

  // --- Poll: Fetch prices and notify listeners ---
  async function poll() {
    const symbols = getOpenSymbols();
    if (!symbols.length) return;

    let prices = {};

    // Try Dhan first
    if (_useDhan && Object.keys(_symbolMap).length > 0) {
      prices = await fetchDhanLTP(symbols);
    }

    // Fallback to Yahoo for any missing symbols
    const missing = symbols.filter(s => prices[s] == null);
    if (missing.length > 0) {
      const yahooPrices = await fetchYahooLTP(missing);
      prices = { ...prices, ...yahooPrices };
    }

    // Update cache
    let changed = false;
    for (const [sym, price] of Object.entries(prices)) {
      if (price != null && _priceCache[sym] !== price) {
        _priceCache[sym] = price;
        changed = true;
      }
    }

    // Update te_trades CMP field
    if (changed) {
      try {
        const trades = JSON.parse(localStorage.getItem('te_trades') || '[]');
        let updated = false;
        for (const t of trades) {
          if (t.status === 'Open' && prices[t.symbol] != null) {
            t.cmp = prices[t.symbol];
            updated = true;
          }
        }
        if (updated) {
          localStorage.setItem('te_trades', JSON.stringify(trades));
        }
      } catch {}

      // Notify listeners
      for (const cb of _listeners) {
        try { cb({ ...prices }); } catch {}
      }

      // Dispatch custom event for other modules
      window.dispatchEvent(new CustomEvent('dhan-cmp-update', { detail: { prices: { ...prices } } }));
    }

    return prices;
  }

  // --- Start/Stop Polling ---
  function startPolling() {
    stopPolling();
    // Immediate first fetch
    poll();
    _pollTimer = setInterval(() => {
      if (isMarketHours()) {
        poll();
      }
    }, POLL_INTERVAL);
    console.log('[DhanLive] Polling started (30s interval, market hours only)');
  }

  function stopPolling() {
    if (_pollTimer) {
      clearInterval(_pollTimer);
      _pollTimer = null;
    }
  }

  // --- Public API ---
  return {
    /**
     * Initialize Dhan Live module.
     * Loads symbol map, starts polling if credentials available.
     */
    async init() {
      if (_initialized) return;

      _workerUrl = (localStorage.getItem('zd_worker_url') || '').replace(/\/$/, '');
      _token = localStorage.getItem('dhan_tk') || '';
      _clientId = localStorage.getItem('dhan_id') || '';
      _useDhan = !!(_workerUrl && _token && _clientId);

      if (!_workerUrl) {
        console.warn('[DhanLive] No worker URL configured. Set zd_worker_url in localStorage.');
        return;
      }

      if (_useDhan) {
        await loadSymbolMap();
        console.log('[DhanLive] Dhan mode active');
      } else {
        console.log('[DhanLive] Dhan creds missing — Yahoo fallback only');
      }

      _initialized = true;
      startPolling();
    },

    /**
     * Get cached CMP for a symbol. Returns null if not available.
     */
    getCMP(symbol) {
      return _priceCache[symbol] ?? null;
    },

    /**
     * Get all cached prices. Returns { symbol: price, ... }
     */
    getAllCMP() {
      return { ..._priceCache };
    },

    /**
     * Force an immediate price refresh. Returns prices object.
     */
    async refresh() {
      return await poll();
    },

    /**
     * Subscribe to price updates.
     * Callback receives { symbol: price, ... } on each update.
     */
    onUpdate(callback) {
      if (typeof callback === 'function') {
        _listeners.push(callback);
      }
    },

    /**
     * Remove a listener.
     */
    offUpdate(callback) {
      _listeners = _listeners.filter(cb => cb !== callback);
    },

    /**
     * Check if Dhan is the active source (vs Yahoo fallback).
     */
    isDhanActive() {
      return _useDhan && Object.keys(_symbolMap).length > 0;
    },

    /**
     * Check if currently within market hours.
     */
    isMarketOpen() {
      return isMarketHours();
    },

    /**
     * Get symbol map info (for debugging).
     */
    getSymbolInfo(symbol) {
      return _symbolMap[symbol] || null;
    },

    /**
     * Stop polling and cleanup.
     */
    destroy() {
      stopPolling();
      _listeners = [];
      _initialized = false;
      console.log('[DhanLive] Destroyed');
    },

    /**
     * Reconfigure credentials without full re-init.
     */
    reconfigure() {
      _token = localStorage.getItem('dhan_tk') || '';
      _clientId = localStorage.getItem('dhan_id') || '';
      _useDhan = !!(_workerUrl && _token && _clientId);
      console.log(`[DhanLive] Reconfigured — Dhan ${_useDhan ? 'active' : 'inactive'}`);
    }
  };
})();

// Auto-init when DOM ready (pages can also call DhanLive.init() manually)
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => DhanLive.init());
} else {
  DhanLive.init();
}
