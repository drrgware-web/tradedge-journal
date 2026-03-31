// ═══════════════════════════════════════════════════════════════
//  TradEdge NSE Stock Master v1.1
//  Centralized stock list for ALL pages
//
//  Primary: data/nse_symbols.json (3045 stocks — {symbol, name, sector})
//  Enriched: NSE EQUITY_L.csv via worker (listing dates → IPO grouping)
//  Enriched: data/scanner_results.json (sector info)
//  Enriched: localStorage dhan_symbol_map (SME/new stocks)
//
//  Place in: js/nse-stock-master.js
//  Import:   <script src="js/nse-stock-master.js"></script>
//
//  API:
//    await NSEMaster.init()            // load everything
//    NSEMaster.getAll()                // [{s,n,sec,listingDate,ipoYears,...}]
//    NSEMaster.search('ccl', 15)       // prefix + fuzzy
//    NSEMaster.resolve('CCL Products') // name → entry
//    NSEMaster.getIPO(1)               // listed < 1 year
//    NSEMaster.getIPO(2)               // listed < 2 years
//    NSEMaster.getIPO(3)               // listed < 3 years
//    NSEMaster.refresh()               // force re-fetch
//    NSEMaster.stats()                 // {total, ipo1y, ipo2y, ipo3y, ...}
// ═══════════════════════════════════════════════════════════════

(function(global) {
  'use strict';

  var LISTING_CACHE_KEY = 'te_nse_listing_dates';
  var CACHE_TTL = 24 * 60 * 60 * 1000;
  var FALLBACK_WORKER = 'https://spring-fire-41a0.drrgware.workers.dev';

  var _stocks = [];
  var _ready = false;
  var _loading = false;
  var _listeners = [];

  function getWorkerUrl() {
    return localStorage.getItem('zd_worker_url')
        || localStorage.getItem('te_worker_url')
        || FALLBACK_WORKER;
  }

  // ═══ CSV LINE PARSER ═══
  function parseCSVLine(line) {
    var cells = [], current = '', inQ = false;
    for (var i = 0; i < line.length; i++) {
      var ch = line[i];
      if (ch === '"') inQ = !inQ;
      else if (ch === ',' && !inQ) { cells.push(current.trim()); current = ''; }
      else current += ch;
    }
    cells.push(current.trim());
    return cells;
  }

  // ═══ LOAD PRIMARY: data/nse_symbols.json ═══
  async function loadLocalJSON() {
    try {
      var res = await fetch('./data/nse_symbols.json?t=' + Math.floor(Date.now() / 3600000));
      if (!res.ok) return null;
      var data = await res.json();
      if (!Array.isArray(data) || data.length < 100) return null;

      return data.map(function(item) {
        return {
          s: (item.symbol || item.s || '').toUpperCase(),
          n: item.name || item.n || item.symbol || '',
          sec: (item.sector && item.sector !== 'Unknown') ? item.sector : (item.sec || ''),
          series: item.series || 'EQ',
          listingDate: item.listingDate || null,
          ipoYears: item.ipoYears || null,
          isin: item.isin || '',
          faceValue: item.faceValue || 0
        };
      }).filter(function(s) { return s.s.length > 0 && s.s.length < 30; });
    } catch (e) {
      console.warn('NSEMaster: Local JSON failed:', e.message);
      return null;
    }
  }

  // ═══ FETCH LISTING DATES from NSE CSV via worker ═══
  async function fetchListingDates() {
    // Check cache
    try {
      var cached = localStorage.getItem(LISTING_CACHE_KEY);
      if (cached) {
        var cData = JSON.parse(cached);
        if (Date.now() - cData.ts < CACHE_TTL && Object.keys(cData.dates).length > 500) {
          console.log('NSEMaster: Listing dates from cache (' + Object.keys(cData.dates).length + ')');
          return cData.dates;
        }
      }
    } catch (e) {}

    var wUrl = getWorkerUrl();
    try {
      var res = await fetch(wUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Kite-Action': 'nse-symbols' },
        body: '{}'
      });
      if (!res.ok) return {};
      var csv = await res.text();
      if (!csv || csv.length < 500) return {};

      var lines = csv.split('\n');
      var dates = {};
      var now = new Date();

      for (var i = 1; i < lines.length; i++) {
        var cells = parseCSVLine(lines[i]);
        if (cells.length < 5) continue;
        var symbol = (cells[0] || '').toUpperCase();
        var dateStr = (cells[3] || '').trim();
        if (symbol && dateStr) {
          var parsed = new Date(dateStr);
          if (!isNaN(parsed.getTime())) {
            dates[symbol] = {
              d: parsed.toISOString().split('T')[0],
              y: +((now - parsed) / (365.25 * 24 * 60 * 60 * 1000)).toFixed(2)
            };
          }
        }
      }

      // Cache it
      try { localStorage.setItem(LISTING_CACHE_KEY, JSON.stringify({ ts: Date.now(), dates: dates })); } catch (e) {}
      console.log('NSEMaster: Listing dates fetched —', Object.keys(dates).length);
      return dates;
    } catch (e) {
      console.warn('NSEMaster: Listing dates failed:', e.message);
      return {};
    }
  }

  // ═══ ENRICH: Dhan symbol map ═══
  function enrichDhan(stocks) {
    try {
      var dhanMap = JSON.parse(localStorage.getItem('dhan_symbol_map') || '{}');
      var existing = {};
      stocks.forEach(function(s) { existing[s.s] = true; });
      var added = 0;
      Object.keys(dhanMap).forEach(function(sym) {
        if (!existing[sym] && sym.length > 0 && sym.length < 30 && /^[A-Z]/.test(sym)) {
          existing[sym] = true;
          stocks.push({ s: sym, n: sym, sec: '', series: '', listingDate: null, ipoYears: null, isin: '', faceValue: 0, _dhanId: dhanMap[sym] });
          added++;
        }
      });
      if (added > 0) console.log('NSEMaster: +' + added + ' from Dhan');
    } catch (e) {}
    return stocks;
  }

  // ═══ ENRICH: Scanner sectors ═══
  async function enrichScanner(stocks) {
    try {
      var res = await fetch('./data/scanner_results.json');
      if (!res.ok) return stocks;
      var data = await res.json();
      var scanStocks = data.stocks || data.scan_results || [];
      var sectorMap = {};
      scanStocks.forEach(function(st) {
        var sym = (st.symbol || st.s || '').toUpperCase();
        if (sym && (st.sector || st.sec)) sectorMap[sym] = st.sector || st.sec;
      });
      stocks.forEach(function(s) { if (!s.sec && sectorMap[s.s]) s.sec = sectorMap[s.s]; });

      var existing = {};
      stocks.forEach(function(s) { existing[s.s] = true; });
      var added = 0;
      scanStocks.forEach(function(st) {
        var sym = (st.symbol || st.s || '').toUpperCase();
        if (sym && !existing[sym]) {
          existing[sym] = true;
          stocks.push({ s: sym, n: st.name || st.n || sym, sec: st.sector || st.sec || '', series: 'EQ', listingDate: null, ipoYears: null, isin: '', faceValue: 0 });
          added++;
        }
      });
      if (added > 0) console.log('NSEMaster: +' + added + ' from scanner');
    } catch (e) {}
    return stocks;
  }

  // ═══ INIT ═══
  async function init() {
    if (_ready) return _stocks;
    if (_loading) return new Promise(function(resolve) { _listeners.push(resolve); });
    _loading = true;

    try {
      // 1. Primary: nse_symbols.json
      var stocks = await loadLocalJSON();
      if (!stocks) stocks = [];
      console.log('NSEMaster: Primary —', stocks.length, 'stocks');

      // 2. Enrich (local, fast)
      stocks = enrichDhan(stocks);
      stocks = await enrichScanner(stocks);

      // 3. Listing dates (from worker, cached 24h)
      var dates = await fetchListingDates();
      var dateCount = Object.keys(dates).length;
      if (dateCount > 0) {
        stocks.forEach(function(s) {
          var ld = dates[s.s];
          if (ld) { s.listingDate = ld.d; s.ipoYears = ld.y; }
        });
        console.log('NSEMaster: Dates applied —', stocks.filter(function(s) { return s.listingDate; }).length);
      }

      // 4. Sort
      stocks.sort(function(a, b) { return a.s.localeCompare(b.s); });
      _stocks = stocks;
      _ready = true;
      _loading = false;
      _listeners.forEach(function(fn) { fn(_stocks); });
      _listeners = [];
      console.log('NSEMaster: Ready —', _stocks.length, 'total stocks');
      return _stocks;
    } catch (e) {
      _loading = false;
      console.error('NSEMaster: Init failed:', e);
      return [];
    }
  }

  // ═══ REFRESH (bypass all caches, re-fetch from NSE) ═══
  async function refresh() {
    console.log('NSEMaster: Force refresh...');
    localStorage.removeItem(LISTING_CACHE_KEY);
    _ready = false;
    _stocks = [];
    return await init();
  }

  // ═══ GETTERS ═══
  function getAll() { return _stocks; }
  function count() { return _stocks.length; }

  function search(query, limit) {
    if (!query || query.length < 1) return [];
    limit = limit || 15;
    var q = query.toUpperCase().trim();
    var a = _stocks.filter(function(s) { return s.s.startsWith(q); });
    var b = _stocks.filter(function(s) { return !s.s.startsWith(q) && s.s.indexOf(q) >= 0; });
    var c = _stocks.filter(function(s) { return !s.s.startsWith(q) && s.s.indexOf(q) < 0 && s.n.toUpperCase().indexOf(q) >= 0; });
    return [].concat(a, b, c).slice(0, limit);
  }

  function resolve(input) {
    if (!input) return null;
    var q = input.toUpperCase().trim();
    var noSpace = q.replace(/\s+/g, '');
    return _stocks.find(function(s) { return s.s === q; })
        || _stocks.find(function(s) { return s.s === noSpace; })
        || _stocks.find(function(s) { return s.n.toUpperCase() === q; })
        || _stocks.find(function(s) { return s.n.toUpperCase().indexOf(q) >= 0; })
        || null;
  }

  function getIPO(years) {
    if (!years || years <= 0) return [];
    return _stocks.filter(function(s) { return s.ipoYears !== null && s.ipoYears <= years; })
      .sort(function(a, b) { return (a.ipoYears || 999) - (b.ipoYears || 999); });
  }

  function getBySector(sector) {
    if (!sector) return [];
    var q = sector.toUpperCase();
    return _stocks.filter(function(s) { return s.sec && s.sec.toUpperCase().indexOf(q) >= 0; });
  }

  function stats() {
    var ipo1y = 0, ipo2y = 0, ipo3y = 0, withSec = 0, withDate = 0;
    _stocks.forEach(function(s) {
      if (s.ipoYears !== null && s.ipoYears <= 1) ipo1y++;
      if (s.ipoYears !== null && s.ipoYears <= 2) ipo2y++;
      if (s.ipoYears !== null && s.ipoYears <= 3) ipo3y++;
      if (s.sec) withSec++;
      if (s.listingDate) withDate++;
    });
    var cacheMin = null;
    try { var r = JSON.parse(localStorage.getItem(LISTING_CACHE_KEY) || '{}'); if (r.ts) cacheMin = Math.round((Date.now() - r.ts) / 60000); } catch (e) {}
    return { total: _stocks.length, withListingDate: withDate, withSector: withSec, ipo1y: ipo1y, ipo2y: ipo2y, ipo3y: ipo3y, cacheMinutes: cacheMin, ready: _ready };
  }

  // ═══ EXPORT ═══
  global.NSEMaster = {
    init: init, refresh: refresh,
    getAll: getAll, count: count,
    search: search, resolve: resolve,
    getIPO: getIPO, getBySector: getBySector,
    stats: stats,
    get stocks() { return _stocks; },
    get ready() { return _ready; }
  };

})(typeof window !== 'undefined' ? window : globalThis);
