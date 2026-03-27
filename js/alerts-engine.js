// ═══════════════════════════════════════════════════════════════
// TradEdge Alerts Engine — js/alerts-engine.js
// Price alerts, RS underperformance, sounds, browser notifs
// ═══════════════════════════════════════════════════════════════

'use strict';

TE.alerts = {
  custom: TE.getAlerts(),
  log: JSON.parse(localStorage.getItem('te_alert_log') || '[]'),
  triggered: new Set(JSON.parse(localStorage.getItem('te_triggered') || '[]')),
  timer: null,
};

TE.alerts.save = function() {
  TE.saveAlerts(TE.alerts.custom);
  localStorage.setItem('te_alert_log', JSON.stringify(TE.alerts.log));
  localStorage.setItem('te_triggered', JSON.stringify([...TE.alerts.triggered]));
};

// ── Sound ──
TE.alerts.playSound = function() {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    [523, 659, 784].forEach((freq, i) => {
      const osc = ctx.createOscillator(), gain = ctx.createGain();
      osc.connect(gain); gain.connect(ctx.destination);
      osc.frequency.value = freq; osc.type = 'sine';
      const t = ctx.currentTime + i * 0.18;
      gain.gain.setValueAtTime(0, t);
      gain.gain.linearRampToValueAtTime(0.3, t + 0.05);
      gain.gain.linearRampToValueAtTime(0, t + 0.25);
      osc.start(t); osc.stop(t + 0.3);
    });
  } catch (e) {}
};

// ── Fire Alert ──
TE.alerts.fire = function(symbol, price, level, label, color) {
  const key = `${symbol}_${label}_${level}`;
  if (TE.alerts.triggered.has(key)) return;
  TE.alerts.triggered.add(key);
  TE.alerts.playSound();

  // Browser notification
  if (typeof Notification !== 'undefined' && Notification.permission === 'granted') {
    try { new Notification(`🔔 ${symbol}`, { body: `${label} at ₹${price.toFixed(2)} (level ₹${level.toFixed(2)})`, tag: key }); } catch (e) {}
  }

  // Log
  TE.alerts.log.unshift({ ts: Date.now(), symbol, price, level, label, color });
  if (TE.alerts.log.length > 50) TE.alerts.log = TE.alerts.log.slice(0, 50);
  TE.alerts.save();

  // Toast banner
  TE.toast(`🔔 ${symbol} — ${label} hit at ₹${price.toFixed(2)}`, '🔔');
};

// ── Build trade-based alert levels ──
TE.alerts.getTradeAlertLevels = function() {
  const levels = [];
  TE.getOpenTrades().forEach(t => {
    const ct = TE.calc({ ...t });
    const sym = t.symbol;
    if (!sym) return;
    if (ct.sl > 0) levels.push({ symbol: sym, level: +t.sl, label: 'Stop Loss', type: 'below', color: 'var(--r)', auto: true });
    if (+t.target > 0) levels.push({ symbol: sym, level: +t.target, label: 'Target', type: 'above', color: 'var(--c)', auto: true });
    if (ct.avgEntry > 0) levels.push({ symbol: sym, level: ct.avgEntry, label: 'Avg Entry', type: 'below', color: 'var(--y)', auto: true });
  });
  return levels;
};

// ── Check all alerts against prices ──
TE.alerts.checkAll = function(prices) {
  const tradeAlerts = TE.alerts.getTradeAlertLevels();
  const allAlerts = [...tradeAlerts, ...TE.alerts.custom];
  allAlerts.forEach(a => {
    const cmp = prices[a.symbol];
    if (cmp == null) return;
    const hit = a.type === 'above' ? cmp >= a.level : cmp <= a.level;
    if (hit) TE.alerts.fire(a.symbol, cmp, a.level, a.label, a.color || 'var(--y)');
  });
};

// ── Fetch prices for alert symbols ──
TE.alerts.fetchPrices = async function() {
  const tradeAlerts = TE.alerts.getTradeAlertLevels();
  const allAlerts = [...tradeAlerts, ...TE.alerts.custom];
  const syms = [...new Set(allAlerts.map(a => a.symbol))];
  if (!syms.length) return {};
  const prices = {};
  for (const sym of syms) {
    const cmp = await TE.fetchCMP(sym);
    if (cmp != null) prices[sym] = cmp;
    await new Promise(r => setTimeout(r, 200));
  }
  return prices;
};

// ── Run full alert check cycle ──
TE.alerts.runCheck = async function() {
  try {
    const prices = await TE.alerts.fetchPrices();
    // Update CMP on trades
    Object.keys(prices).forEach(sym => {
      TE.trades.forEach(t => { if (t.symbol === sym && t.status !== 'Closed') t.cmp = prices[sym]; });
    });
    TE.alerts.checkAll(prices);
    return prices;
  } catch (e) { return {}; }
};

// ── Start periodic polling (15 min, market hours) ──
TE.alerts.startPolling = function() {
  if (TE.alerts.timer) clearInterval(TE.alerts.timer);
  TE.alerts.timer = setInterval(() => {
    if (TE.isMarketOpen()) TE.alerts.runCheck();
  }, 15 * 60 * 1000);
};

// ── Add custom alert ──
TE.alerts.addCustom = function(symbol, level, type, label, note) {
  TE.alerts.custom.push({
    id: Date.now() + Math.random().toString(36).slice(2),
    symbol, level, type, label: note || label || (type === 'above' ? 'Above ₹' + level : 'Below ₹' + level),
    color: type === 'above' ? 'var(--g)' : 'var(--r)', auto: false, createdAt: Date.now()
  });
  TE.alerts.save();
};

// ── Delete custom alert ──
TE.alerts.deleteCustom = function(id) {
  TE.alerts.custom = TE.alerts.custom.filter(a => a.id !== id);
  TE.alerts.save();
};

// ── Reset triggered alert ──
TE.alerts.reset = function(symbol, label) {
  const key = [...TE.alerts.triggered].find(k => k.startsWith(symbol + '_' + label));
  if (key) { TE.alerts.triggered.delete(key); TE.alerts.save(); }
};

// ── Clear log ──
TE.alerts.clearLog = function() {
  TE.alerts.log = [];
  TE.alerts.save();
};

// ── Boot ──
TE.alerts.startPolling();
setTimeout(() => TE.alerts.runCheck(), 10000);

console.log('[TE Alerts] Module loaded — ' + TE.alerts.custom.length + ' custom alerts');
