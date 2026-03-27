// ═══════════════════════════════════════════════════════════════
// TradEdge Execution Engine — js/execution.js
// Order placement, positions, iceberg, bracket, auto-trail, GTT
// ═══════════════════════════════════════════════════════════════

'use strict';

TE.exec = {
  broker: 'zerodha',
  side: 'BUY',
  otype: 'MARKET',
  positions: [],
  orders: [],
  sortBy: 'pnl',
  autoTrailHighs: {},
};

// ── Sound Engine ──
TE.exec.playSound = function(name) {
  const sounds = {
    fill:     { freq:[523,659,784], dur:0.12, gap:0.15, vol:0.2, type:'sine' },
    slHit:    { freq:[880,660,440], dur:0.15, gap:0.18, vol:0.25, type:'sawtooth' },
    error:    { freq:[220,180], dur:0.2, gap:0.25, vol:0.15, type:'square' },
    squareOff:{ freq:[440,350,260], dur:0.18, gap:0.2, vol:0.2, type:'sawtooth' },
  };
  const s = sounds[name]; if (!s) return;
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    s.freq.forEach((freq, i) => {
      const osc = ctx.createOscillator(), gain = ctx.createGain();
      osc.connect(gain); gain.connect(ctx.destination);
      osc.frequency.value = freq; osc.type = s.type;
      const t = ctx.currentTime + i * s.gap;
      gain.gain.setValueAtTime(0, t);
      gain.gain.linearRampToValueAtTime(s.vol, t + 0.02);
      gain.gain.linearRampToValueAtTime(0, t + s.dur);
      osc.start(t); osc.stop(t + s.dur + 0.05);
    });
  } catch (e) {}
};

// ── Broker Headers ──
TE.exec.getHeaders = function() {
  const h = { 'Content-Type': 'application/json' };
  const b = TE.exec.broker;
  if (b === 'zerodha') {
    h['Authorization'] = `token ${localStorage.getItem('zd_key') || ''}:${localStorage.getItem('zd_tk') || ''}`;
  } else if (b === 'dhan') {
    h['X-Dhan-Token'] = localStorage.getItem('dhan_tk') || '';
    h['X-Dhan-ID'] = localStorage.getItem('dhan_id') || '';
  } else {
    h['X-DE-Session'] = localStorage.getItem('de_session') || '';
  }
  return h;
};

// ── Send Single Order ──
TE.exec.sendOrder = async function(sym, side, otype, qty, price, trigger, product, validity) {
  const workerUrl = TE.getWorkerUrl();
  if (!workerUrl) throw new Error('Set Worker URL in Settings');
  const headers = TE.exec.getHeaders();
  let action, body;

  if (TE.exec.broker === 'zerodha') {
    action = 'zd-place-order';
    body = { tradingsymbol: sym, exchange: 'NSE', transaction_type: side, order_type: otype, quantity: qty, product, validity, price: price || 0, trigger_price: trigger || 0 };
  } else if (TE.exec.broker === 'dhan') {
    action = 'dhan-place-order';
    body = { symbol: sym, side, order_type: otype, qty, price: price || 0, trigger_price: trigger || 0, product, validity };
  } else {
    action = 'de-place-order';
    body = { symbol: sym, side, order_type: otype, qty, price: price || 0, trigger_price: trigger || 0, product, validity };
  }

  headers['X-Kite-Action'] = action;
  const res = await fetch(workerUrl, { method: 'POST', headers, body: JSON.stringify(body) });
  const data = await res.json();
  const oid = data?.order_id || data?.orderId || data?.data?.order_id;
  if (oid) return { ok: true, orderId: oid };
  return { ok: false, error: data?.message || data?.error || JSON.stringify(data).slice(0, 150) };
};

// ── Refresh Positions ──
TE.exec.refreshPositions = async function() {
  const workerUrl = TE.getWorkerUrl(); if (!workerUrl) return;
  const headers = TE.exec.getHeaders();
  const b = TE.exec.broker;
  headers['X-Kite-Action'] = b === 'zerodha' ? 'zd-positions' : b === 'dhan' ? 'dhan-positions' : 'de-positions';
  try {
    const res = await fetch(workerUrl, { method: 'GET', headers });
    const data = await res.json();
    TE.exec.positions = (b === 'zerodha' ? (data?.data?.net || data?.net || []) : (data?.data || data || []))
      .filter(p => (p.quantity || p.netQty || 0) !== 0);
    return TE.exec.positions;
  } catch (e) {
    console.warn('[Exec] Position refresh failed:', e.message);
    return [];
  }
};

// ── Refresh Orders ──
TE.exec.refreshOrders = async function() {
  const workerUrl = TE.getWorkerUrl(); if (!workerUrl) return [];
  const headers = TE.exec.getHeaders();
  const b = TE.exec.broker;
  headers['X-Kite-Action'] = b === 'zerodha' ? 'zd-orders' : b === 'dhan' ? 'dhan-orders' : 'de-orders';
  try {
    const res = await fetch(workerUrl, { method: 'GET', headers });
    const data = await res.json();
    TE.exec.orders = data?.data || data || [];
    return TE.exec.orders;
  } catch (e) { return []; }
};

// ── Auto-Journal: log trade from execution ──
TE.exec.autoJournal = function(sym, side, qty, price, sl, tgt) {
  const today = new Date().toISOString().slice(0, 10);
  const existing = TE.trades.find(t => t.symbol === sym && t.side === (side === 'BUY' ? 'Buy' : 'Sell') && t.status === 'Open');
  if (existing) {
    existing.entries.push({ date: today, price: price || 0, qty });
    if (sl > 0) existing.sl = sl;
    if (tgt > 0) existing.target = tgt;
  } else {
    TE.trades.push({
      id: Date.now() + '' + Math.random().toString(36).slice(2),
      symbol: sym, setup: '', side: side === 'BUY' ? 'Buy' : 'Sell', status: 'Open',
      sl: sl || 0, target: tgt || 0, plan: '', exitTrigger: '', growth: '',
      notes: 'Auto-logged from Execution', source: TE.exec.broker,
      entries: [{ date: today, price: price || 0, qty }], exits: [], cmp: 0
    });
  }
  TE.save();
  TE.toast('📝 Auto-logged: ' + sym, '📝');
};

// ── Pre-Trade Risk Nudges ──
TE.exec.runNudges = function(sym, qty, price) {
  const nudges = [];
  let score = 100;
  const cap = TE.cfg.cap || 5000000;
  const orderVal = qty * price;

  if (orderVal > 0 && cap > 0) {
    const pct = orderVal / cap * 100;
    if (pct > 25) { nudges.push({ level: 'fail', icon: '🔴', text: `Order is ${pct.toFixed(1)}% of capital — BLOCKED (max 25%)` }); score -= 40; }
    else if (pct > 15) { nudges.push({ level: 'warn', icon: '⚠️', text: `${pct.toFixed(1)}% of capital — high concentration` }); score -= 15; }
    else nudges.push({ level: 'pass', icon: '✓', text: `Capital usage: ${pct.toFixed(1)}%` });
  }

  const existingPos = TE.exec.positions.find(p => (p.tradingsymbol || p.symbol) === sym);
  if (existingPos) {
    nudges.push({ level: 'info', icon: 'ℹ️', text: `Already holding ${Math.abs(existingPos.quantity || existingPos.netQty || 0)} shares` });
  }

  if (!nudges.length) nudges.push({ level: 'pass', icon: '✅', text: 'All clear' });
  score = Math.max(0, Math.min(100, score));
  return { score, nudges };
};

// ── P&L Computation ──
TE.exec.computePnl = function() {
  let totalPnl = 0, dayPnl = 0, totalValue = 0;
  TE.exec.positions.forEach(p => {
    const pnl = p.pnl || ((p.last_price || p.lastPrice || 0) - (p.average_price || p.averagePrice || 0)) * (p.quantity || p.netQty || 0);
    totalPnl += pnl;
    dayPnl += (p.day_m2m || p.dayPnl || pnl);
    totalValue += Math.abs((p.quantity || p.netQty || 0) * (p.last_price || p.lastPrice || 0));
  });
  return { totalPnl, dayPnl, totalValue, count: TE.exec.positions.length, riskPct: totalValue > 0 ? totalValue / (TE.cfg.cap || 5000000) * 100 : 0 };
};

// ── GTT Orders ──
TE.exec.gttOrders = TE.getGTTs();

TE.exec.saveGTTs = function() {
  TE.saveGTTs(TE.exec.gttOrders);
};

TE.exec.gttAutoFromJournal = function() {
  const openTrades = TE.getOpenTrades();
  let created = 0;
  openTrades.forEach(t => {
    const sym = t.symbol.toUpperCase();
    const sl = +(t.sl || 0), tgt = +(t.target || 0);
    const tQty = (t.entries || []).reduce((s, e) => s + (+e.qty || 0), 0);
    const eQty = (t.exits || []).reduce((s, e) => s + (+e.qty || 0), 0);
    const qty = tQty - eQty;
    if (!qty || (!sl && !tgt)) return;
    const exists = TE.exec.gttOrders.find(g => g.symbol === sym && g.status === 'active');
    if (exists) return;
    const side = t.side === 'Sell' ? 'BUY' : 'SELL';
    if (sl && tgt) {
      TE.exec.gttOrders.push({
        id: 'gtt_' + Date.now() + '_' + (created++), type: 'oco', symbol: sym, exchange: 'NSE', product: 'CNC',
        side, qty, slTrigger: sl, slLimit: sl, tgtTrigger: tgt, tgtLimit: tgt,
        note: 'Auto from journal', source: 'local', status: 'active', brokerId: null,
        createdAt: Date.now(), updatedAt: Date.now()
      });
    } else if (sl) {
      TE.exec.gttOrders.push({
        id: 'gtt_' + Date.now() + '_' + (created++), type: 'single', symbol: sym, exchange: 'NSE', product: 'CNC',
        side, qty, trigger1: sl, limit1: sl, note: 'SL from journal', source: 'local', status: 'active',
        brokerId: null, createdAt: Date.now(), updatedAt: Date.now()
      });
    }
  });
  TE.exec.saveGTTs();
  return created;
};

console.log('[TE Exec] Module loaded');
