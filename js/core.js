// ═══════════════════════════════════════════════════════════════
// TradEdge Core — js/core.js
// Shared state, trade calculations, localStorage, formatters
// Loaded by every page. Zero DOM dependencies.
// ═══════════════════════════════════════════════════════════════

'use strict';

// ── Global State ─────────────────────────────────────────────
window.TE = window.TE || {};

TE.trades = JSON.parse(localStorage.getItem('te_trades') || '[]');
TE.cfg    = JSON.parse(localStorage.getItem('te_cfg') || '{"cap":5000000,"risk":0.5}');

// ── Constants ────────────────────────────────────────────────
TE.ENTRY_COLORS = ['#00E5A0','#3B8BFF','#A855F7','#22D3EE','#F5C542'];
TE.EXIT_COLORS  = ['#FF4560','#FF7A50','#FFAA50','#FFD050','#FFE580'];

TE.SUPABASE_URL = 'https://urnrdpyhncezljirpnmy.supabase.co';
TE.SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVybnJkcHlobmNlemxqaXJwbm15Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI3NzY5MDIsImV4cCI6MjA4ODM1MjkwMn0.eExEFw1XGAlYBGECqCpl928UvXv5Jchuyr1YYkcrbdw';

TE.TELEGRAM_BOT  = '8659936599:AAFKV6MKfHOSJKKTVqISJI-SwQ_cerTaAbQ';
TE.TELEGRAM_CHAT = '183752078';

// ── Save Guard Flags ─────────────────────────────────────────
TE._saving   = false;
TE._pushing  = false;
TE._pulling  = false;
TE._pushTimer = null;

// ── Trade Calculator ─────────────────────────────────────────
TE.calc = function(t) {
  t = { ...t };
  // Entries
  let tQty = 0, tVal = 0;
  (t.entries || []).forEach(e => {
    if (e.price && e.qty) { tQty += +e.qty; tVal += +e.price * +e.qty; }
  });
  t.totalQty = tQty;
  t.avgEntry = tQty > 0 ? tVal / tQty : 0;

  // Exits
  let eQty = 0, eVal = 0;
  (t.exits || []).forEach(e => {
    if (e.price && e.qty) { eQty += +e.qty; eVal += +e.price * +e.qty; }
  });
  t.exitQty  = eQty;
  t.avgExit  = eQty > 0 ? eVal / eQty : 0;
  t.openQty  = Math.max(0, tQty - eQty);

  const m = t.side === 'Buy' ? 1 : -1;
  t.realisedPL   = eQty > 0 ? m * (t.avgExit - t.avgEntry) * eQty : 0;
  t.unrealisedPL = t.cmp && t.openQty > 0 ? m * (t.cmp - t.avgEntry) * t.openQty : 0;
  t.totalPL      = t.realisedPL + t.unrealisedPL;

  // R-multiple
  if (t.sl && t.avgEntry && t.sl !== t.avgEntry) {
    const risk = Math.abs(t.avgEntry - t.sl);
    t.rr = t.avgExit ? m * (t.avgExit - t.avgEntry) / risk :
           (t.cmp ? m * (t.cmp - t.avgEntry) / risk : 0);
  } else t.rr = 0;
  t.slPct = t.sl && t.avgEntry ? Math.abs(t.avgEntry - t.sl) / t.avgEntry * 100 : 0;

  // Hold days
  const start   = t.entries?.[0]?.date ? new Date(t.entries[0].date) : null;
  const lastExit = t.exits?.length ? new Date(t.exits[t.exits.length - 1].date) : null;
  const endDate  = t.status === 'Closed' && lastExit ? lastExit : new Date();
  t.holdDays = start ? Math.max(0, Math.round((endDate - start) / 86400000)) : 0;

  // Auto-derive status
  if (tQty > 0) {
    if (eQty <= 0)         t.status = 'Open';
    else if (eQty >= tQty) t.status = 'Closed';
    else                   t.status = 'Partial';
  }

  return t;
};

// ── Save — with debounced Supabase push ──────────────────────
TE.save = function() {
  if (TE._saving) return; // re-entrancy guard
  TE._saving = true;

  localStorage.setItem('te_trades', JSON.stringify(TE.trades));

  // Notify other tabs (Edge Pilot, etc.) via StorageEvent
  // StorageEvent fires automatically on other tabs when localStorage changes

  // Debounced Supabase push (3s cooldown)
  const autoSync = localStorage.getItem('sb_auto');
  if (autoSync && !TE._pushing && !TE._pulling && typeof TE.sbPush === 'function') {
    clearTimeout(TE._pushTimer);
    TE._pushTimer = setTimeout(() => TE.sbPush(), 3000);
  }

  TE._saving = false;

  // Fire custom event for same-tab listeners
  window.dispatchEvent(new CustomEvent('te:trades-changed', { detail: { count: TE.trades.length } }));
};

// ── Formatters ───────────────────────────────────────────────
TE.fmt = function(n) {
  n = +n;
  return (n < 0 ? '-₹' : '₹') + Math.abs(n).toLocaleString('en-IN', {
    minimumFractionDigits: 2, maximumFractionDigits: 2
  });
};

TE.fmtShort = function(n) {
  if (n >= 1e7) return '₹' + (n / 1e7).toFixed(1) + 'Cr';
  if (n >= 1e5) return '₹' + (n / 1e5).toFixed(1) + 'L';
  return '₹' + n.toLocaleString('en-IN');
};

TE.fmtINR = function(n) {
  return '₹ ' + Math.abs(n).toLocaleString('en-IN', {
    minimumFractionDigits: 2, maximumFractionDigits: 2
  });
};

TE.parseDate = function(raw) {
  if (!raw) return '';
  raw = String(raw).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) return raw.slice(0, 10);
  const m1 = raw.match(/(\d{2})[-\/](\d{2})[-\/](\d{4})/);
  if (m1) return `${m1[3]}-${m1[2]}-${m1[1]}`;
  const d = new Date(raw);
  return isNaN(d) ? raw.slice(0, 10) : d.toISOString().slice(0, 10);
};

TE.cleanNum = function(v) { return +(String(v || '').replace(/[₹,\s]/g, '')) || 0; };
TE.cleanSym = function(v) { return String(v || '').replace(/-EQ$|-BE$|-N$|-B$/, '').toUpperCase().trim(); };

// ── Device ID (for Supabase row key) ─────────────────────────
TE.deviceId = function() {
  let id = localStorage.getItem('te_device_id');
  if (!id) {
    id = 'device_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
    localStorage.setItem('te_device_id', id);
  }
  return id;
};

// ── Worker URL helper ────────────────────────────────────────
TE.getWorkerUrl = function() {
  return (localStorage.getItem('zd_worker_url') || '').trim();
};

// ═══ PRICE FETCHING ══════════════════════════════════════════

/**
 * Fetch CMP for a symbol - tries DhanLive first, then Yahoo via worker
 * @param {string} sym - Stock symbol (e.g., 'RELIANCE', 'INFY')
 * @returns {Promise<number|null>} - Price or null if unavailable
 */
TE.fetchCMP = async function(sym) {
  if (!sym) return null;
  sym = TE.cleanSym(sym);
  
  // 1. Try DhanLive cache first
  if (typeof DhanLive !== 'undefined') {
    const cached = DhanLive.getCMP(sym);
    if (cached) return cached;
  }
  
  // 2. Fetch from Yahoo via worker
  const url = TE.getWorkerUrl();
  if (!url) {
    console.warn('[TE.fetchCMP] No worker URL configured');
    return null;
  }
  
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Kite-Action': 'yahoo-proxy' },
      body: JSON.stringify({ ticker: sym + '.NS', range: '1d', interval: '1d' })
    });
    if (!res.ok) return null;
    const data = await res.json();
    return data?.chart?.result?.[0]?.meta?.regularMarketPrice || null;
  } catch (e) {
    console.warn('[TE.fetchCMP] Failed for', sym, e.message);
    return null;
  }
};

/**
 * Fetch CMP with source info - returns { price, source } or null
 * @param {string} sym - Stock symbol
 * @returns {Promise<{price: number, source: string}|null>}
 */
TE.fetchCMPWithSource = async function(sym) {
  if (!sym) return null;
  sym = TE.cleanSym(sym);
  
  // 1. Try DhanLive cache first
  if (typeof DhanLive !== 'undefined') {
    const cached = DhanLive.getCMP(sym);
    if (cached) return { price: cached, source: 'dhan' };
  }
  
  // 2. Fetch from Yahoo via worker
  const url = TE.getWorkerUrl();
  if (!url) return null;
  
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Kite-Action': 'yahoo-proxy' },
      body: JSON.stringify({ ticker: sym + '.NS', range: '1d', interval: '1d' })
    });
    if (!res.ok) return null;
    const data = await res.json();
    const price = data?.chart?.result?.[0]?.meta?.regularMarketPrice;
    return price ? { price, source: 'yahoo' } : null;
  } catch (e) {
    console.warn('[TE.fetchCMPWithSource] Failed for', sym, e.message);
    return null;
  }
};

/**
 * Batch fetch CMPs for multiple symbols
 * @param {string[]} symbols - Array of symbols
 * @returns {Promise<Object>} - { SYMBOL: price, ... }
 */
TE.fetchCMPBatch = async function(symbols) {
  if (!symbols?.length) return {};
  
  const results = {};
  const promises = symbols.map(async sym => {
    const price = await TE.fetchCMP(sym);
    if (price) results[TE.cleanSym(sym)] = price;
  });
  
  await Promise.allSettled(promises);
  return results;
};

// ── Telegram helper ──────────────────────────────────────────
TE.sendTelegram = async function(text) {
  const bot  = localStorage.getItem('te_tg_bot')  || TE.TELEGRAM_BOT;
  const chat = localStorage.getItem('te_tg_chat') || TE.TELEGRAM_CHAT;
  if (!bot || !chat) return;
  try {
    await fetch(`https://api.telegram.org/bot${bot}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chat, text, parse_mode: 'HTML', disable_web_page_preview: true })
    });
  } catch (e) { console.warn('[TG]', e.message); }
};

// ── Market Status ────────────────────────────────────────────
TE.isMarketOpen = function() {
  const ist = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
  const h = ist.getHours(), mi = ist.getMinutes(), day = ist.getDay();
  return day >= 1 && day <= 5 && (h > 9 || (h === 9 && mi >= 15)) && (h < 15 || (h === 15 && mi <= 30));
};

TE.getIST = function() {
  return new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
};

// ── Fund Data ────────────────────────────────────────────────
TE.getFundData = function() {
  try { return JSON.parse(localStorage.getItem('te_fund') || '{}'); }
  catch { return {}; }
};
TE.saveFundData = function(d) { localStorage.setItem('te_fund', JSON.stringify(d)); };

// ── Ledger ───────────────────────────────────────────────────
TE.getLedger = function() {
  try { return JSON.parse(localStorage.getItem('te_ledger') || '[]'); }
  catch { return []; }
};
TE.saveLedger = function(d) { localStorage.setItem('te_ledger', JSON.stringify(d)); };

// ── Alerts ───────────────────────────────────────────────────
TE.getAlerts = function() {
  try { return JSON.parse(localStorage.getItem('te_alerts') || '[]'); }
  catch { return []; }
};
TE.saveAlerts = function(arr) { localStorage.setItem('te_alerts', JSON.stringify(arr)); };

// ── GTT Orders ───────────────────────────────────────────────
TE.getGTTs = function() {
  try { return JSON.parse(localStorage.getItem('te_gtt') || '[]'); }
  catch { return []; }
};
TE.saveGTTs = function(arr) { localStorage.setItem('te_gtt', JSON.stringify(arr)); };

// ── Open Positions Helper ────────────────────────────────────
TE.getOpenTrades = function() {
  return TE.trades.filter(t => t.status === 'Open' || t.status === 'Partial');
};

TE.getClosedTrades = function() {
  return TE.trades.filter(t => t.status === 'Closed');
};

// ── Auto-fix trade statuses on load ──────────────────────────
(function fixStatuses() {
  let changed = false;
  TE.trades.forEach(t => {
    const tQty = (t.entries || []).reduce((s, e) => s + (+e.qty || 0), 0);
    const eQty = (t.exits || []).reduce((s, e) => s + (+e.qty || 0), 0);
    if (tQty > 0) {
      const correct = eQty <= 0 ? 'Open' : eQty >= tQty ? 'Closed' : 'Partial';
      if (t.status !== correct) { t.status = correct; changed = true; }
    }
  });
  if (changed) localStorage.setItem('te_trades', JSON.stringify(TE.trades));
})();

// ── Init Supabase Credentials ────────────────────────────────
(function initSbCreds() {
  if (!localStorage.getItem('sb_url')) localStorage.setItem('sb_url', TE.SUPABASE_URL);
  if (!localStorage.getItem('sb_key')) localStorage.setItem('sb_key', TE.SUPABASE_KEY);
})();

console.log(`[TE Core] Loaded — ${TE.trades.length} trades, cap ₹${TE.fmtShort(TE.cfg.cap)}`);
