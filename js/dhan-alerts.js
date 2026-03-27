/**
 * TradEdge — Intraday Alerts Module
 * js/dhan-alerts.js
 *
 * Monitors open positions and sends Telegram alerts when:
 * - Price hits Stop Loss
 * - Price hits Target
 * - Price approaches SL (warning)
 * - Price moves significantly from entry
 *
 * Uses DhanLive for CMP, sends via TE.sendTelegram()
 *
 * Usage:
 *   DhanAlerts.start();           // Start monitoring
 *   DhanAlerts.stop();            // Stop monitoring
 *   DhanAlerts.checkNow();        // Manual check
 *   DhanAlerts.getTriggered();    // Get triggered alerts
 *   DhanAlerts.clearTriggered();  // Reset triggered list
 *
 * Config (localStorage):
 *   te_alerts_config: { enabled, slAlert, targetAlert, nearSlPct, trailingPct }
 */

window.DhanAlerts = (() => {
  'use strict';

  // ── Config ─────────────────────────────────────────────────
  const DEFAULT_CONFIG = {
    enabled: true,
    slAlert: true,          // Alert on SL hit
    targetAlert: true,      // Alert on target hit
    nearSlAlert: true,      // Alert when near SL
    nearSlPct: 1.5,         // % from SL to trigger warning
    trailingAlert: false,   // Alert on significant move from entry
    trailingPct: 5,         // % move from entry
    checkIntervalMs: 60000, // Check every 60 seconds
    cooldownMs: 300000      // 5 min cooldown per symbol per alert type
  };

  let _config = { ...DEFAULT_CONFIG };
  let _timer = null;
  let _triggered = {};  // { 'SYMBOL_sl': timestamp, 'SYMBOL_target': timestamp, ... }
  let _lastCheck = 0;

  // ── Load/Save Config ───────────────────────────────────────
  function loadConfig() {
    try {
      const saved = JSON.parse(localStorage.getItem('te_alerts_config') || '{}');
      _config = { ...DEFAULT_CONFIG, ...saved };
    } catch { _config = { ...DEFAULT_CONFIG }; }
    
    // Load triggered state
    try {
      _triggered = JSON.parse(localStorage.getItem('te_alerts_triggered') || '{}');
      // Clean old entries (older than 24h)
      const cutoff = Date.now() - 24 * 60 * 60 * 1000;
      Object.keys(_triggered).forEach(k => {
        if (_triggered[k] < cutoff) delete _triggered[k];
      });
    } catch { _triggered = {}; }
  }

  function saveConfig() {
    localStorage.setItem('te_alerts_config', JSON.stringify(_config));
  }

  function saveTriggered() {
    localStorage.setItem('te_alerts_triggered', JSON.stringify(_triggered));
  }

  // ── Market Hours Check ─────────────────────────────────────
  function isMarketOpen() {
    if (typeof DhanLive !== 'undefined') return DhanLive.isMarketOpen();
    if (typeof TE !== 'undefined' && TE.isMarketOpen) return TE.isMarketOpen();
    
    const ist = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
    const h = ist.getHours(), m = ist.getMinutes(), d = ist.getDay();
    return d >= 1 && d <= 5 && (h > 9 || (h === 9 && m >= 15)) && (h < 15 || (h === 15 && m <= 30));
  }

  // ── Get CMP ────────────────────────────────────────────────
  function getCMP(symbol) {
    // Try DhanLive first
    if (typeof DhanLive !== 'undefined') {
      const price = DhanLive.getCMP(symbol);
      if (price) return price;
    }
    
    // Fallback to trade's stored CMP
    const trade = TE.trades.find(t => t.symbol === symbol && t.status !== 'Closed');
    return trade?.cmp || null;
  }

  // ── Cooldown Check ─────────────────────────────────────────
  function isOnCooldown(symbol, alertType) {
    const key = `${symbol}_${alertType}`;
    const lastTime = _triggered[key];
    if (!lastTime) return false;
    return (Date.now() - lastTime) < _config.cooldownMs;
  }

  function markTriggered(symbol, alertType) {
    const key = `${symbol}_${alertType}`;
    _triggered[key] = Date.now();
    saveTriggered();
  }

  // ── Send Alert ─────────────────────────────────────────────
  async function sendAlert(type, symbol, data) {
    const { cmp, sl, target, entry, side, pnl, pnlPct } = data;
    
    let emoji, title, details;
    
    switch (type) {
      case 'sl_hit':
        emoji = '🔴';
        title = 'STOP LOSS HIT';
        details = `SL: ₹${sl?.toFixed(2)} | CMP: ₹${cmp?.toFixed(2)}`;
        break;
      case 'target_hit':
        emoji = '🎯';
        title = 'TARGET HIT';
        details = `Target: ₹${target?.toFixed(2)} | CMP: ₹${cmp?.toFixed(2)}`;
        break;
      case 'near_sl':
        emoji = '⚠️';
        title = 'APPROACHING SL';
        details = `SL: ₹${sl?.toFixed(2)} | CMP: ₹${cmp?.toFixed(2)} (${((cmp - sl) / sl * 100).toFixed(1)}% away)`;
        break;
      case 'trailing':
        emoji = '📈';
        title = `UP ${pnlPct?.toFixed(1)}% FROM ENTRY`;
        details = `Entry: ₹${entry?.toFixed(2)} | CMP: ₹${cmp?.toFixed(2)}`;
        break;
      default:
        return;
    }

    const pnlText = pnl >= 0 ? `+₹${pnl.toFixed(0)}` : `-₹${Math.abs(pnl).toFixed(0)}`;
    const pnlEmoji = pnl >= 0 ? '💚' : '❤️';
    
    const message = `
${emoji} <b>${title}</b>

<b>${symbol}</b> (${side})
${details}

${pnlEmoji} P&L: <b>${pnlText}</b> (${pnlPct >= 0 ? '+' : ''}${pnlPct?.toFixed(2)}%)

⏰ ${new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata' })} IST
    `.trim();

    console.log(`[DhanAlerts] ${type}: ${symbol}`, data);

    if (typeof TE !== 'undefined' && TE.sendTelegram) {
      await TE.sendTelegram(message);
    }

    // Dispatch event for UI updates
    window.dispatchEvent(new CustomEvent('te:alert-triggered', {
      detail: { type, symbol, data, message, timestamp: Date.now() }
    }));
  }

  // ── Check Single Position ──────────────────────────────────
  function checkPosition(trade) {
    const symbol = trade.symbol;
    const cmp = getCMP(symbol);
    if (!cmp) return [];

    const ct = typeof TE !== 'undefined' ? TE.calc({ ...trade }) : trade;
    const { avgEntry, openQty, sl, target, side } = { ...trade, ...ct };
    
    if (!avgEntry || openQty <= 0) return [];

    const alerts = [];
    const isBuy = side === 'Buy';
    const pnl = isBuy ? (cmp - avgEntry) * openQty : (avgEntry - cmp) * openQty;
    const pnlPct = isBuy ? (cmp - avgEntry) / avgEntry * 100 : (avgEntry - cmp) / avgEntry * 100;
    
    const alertData = { cmp, sl, target, entry: avgEntry, side, pnl, pnlPct, openQty };

    // ── SL Hit Check ──
    if (_config.slAlert && sl && sl > 0) {
      const slHit = isBuy ? (cmp <= sl) : (cmp >= sl);
      if (slHit && !isOnCooldown(symbol, 'sl_hit')) {
        alerts.push({ type: 'sl_hit', symbol, data: alertData });
        markTriggered(symbol, 'sl_hit');
      }
    }

    // ── Target Hit Check ──
    if (_config.targetAlert && target && target > 0) {
      const targetHit = isBuy ? (cmp >= target) : (cmp <= target);
      if (targetHit && !isOnCooldown(symbol, 'target_hit')) {
        alerts.push({ type: 'target_hit', symbol, data: alertData });
        markTriggered(symbol, 'target_hit');
      }
    }

    // ── Near SL Warning ──
    if (_config.nearSlAlert && sl && sl > 0 && !isOnCooldown(symbol, 'near_sl')) {
      const slDist = isBuy ? (cmp - sl) / sl * 100 : (sl - cmp) / cmp * 100;
      const slHit = isBuy ? (cmp <= sl) : (cmp >= sl);
      
      if (!slHit && slDist <= _config.nearSlPct && slDist > 0) {
        alerts.push({ type: 'near_sl', symbol, data: alertData });
        markTriggered(symbol, 'near_sl');
      }
    }

    // ── Trailing Alert (significant move from entry) ──
    if (_config.trailingAlert && pnlPct >= _config.trailingPct && !isOnCooldown(symbol, 'trailing')) {
      alerts.push({ type: 'trailing', symbol, data: alertData });
      markTriggered(symbol, 'trailing');
    }

    return alerts;
  }

  // ── Check All Positions ────────────────────────────────────
  async function checkAll() {
    if (!_config.enabled) return [];
    if (!isMarketOpen()) {
      console.log('[DhanAlerts] Market closed, skipping check');
      return [];
    }

    _lastCheck = Date.now();
    const openTrades = typeof TE !== 'undefined' ? TE.getOpenTrades() : [];
    
    if (!openTrades.length) return [];

    const allAlerts = [];

    for (const trade of openTrades) {
      const alerts = checkPosition(trade);
      for (const alert of alerts) {
        await sendAlert(alert.type, alert.symbol, alert.data);
        allAlerts.push(alert);
      }
    }

    if (allAlerts.length > 0) {
      console.log(`[DhanAlerts] Triggered ${allAlerts.length} alerts`);
    }

    return allAlerts;
  }

  // ── Start/Stop Monitoring ──────────────────────────────────
  function start() {
    loadConfig();
    if (!_config.enabled) {
      console.log('[DhanAlerts] Alerts disabled in config');
      return;
    }

    stop(); // Clear any existing timer

    // Initial check
    setTimeout(() => checkAll(), 2000);

    // Periodic checks
    _timer = setInterval(() => {
      if (isMarketOpen()) {
        checkAll();
      }
    }, _config.checkIntervalMs);

    console.log(`[DhanAlerts] Monitoring started (interval: ${_config.checkIntervalMs / 1000}s)`);
  }

  function stop() {
    if (_timer) {
      clearInterval(_timer);
      _timer = null;
      console.log('[DhanAlerts] Monitoring stopped');
    }
  }

  // ── Listen to DhanLive updates for instant checks ──────────
  if (typeof window !== 'undefined') {
    window.addEventListener('dhan-cmp-update', () => {
      // Only check if enough time has passed since last check
      if (_config.enabled && isMarketOpen() && (Date.now() - _lastCheck) > 30000) {
        checkAll();
      }
    });
  }

  // ── Public API ─────────────────────────────────────────────
  return {
    start,
    stop,
    checkNow: checkAll,
    
    getConfig: () => ({ ..._config }),
    setConfig: (cfg) => {
      _config = { ..._config, ...cfg };
      saveConfig();
      console.log('[DhanAlerts] Config updated:', _config);
    },
    
    getTriggered: () => ({ ..._triggered }),
    clearTriggered: (symbol) => {
      if (symbol) {
        Object.keys(_triggered).forEach(k => {
          if (k.startsWith(symbol + '_')) delete _triggered[k];
        });
      } else {
        _triggered = {};
      }
      saveTriggered();
      console.log('[DhanAlerts] Triggered alerts cleared');
    },
    
    isRunning: () => _timer !== null,
    isEnabled: () => _config.enabled,
    
    // Test alert (for debugging)
    testAlert: async (type = 'sl_hit') => {
      const testData = {
        cmp: 100, sl: 95, target: 120, entry: 100,
        side: 'Buy', pnl: -500, pnlPct: -5, openQty: 100
      };
      await sendAlert(type, 'TEST', testData);
    }
  };
})();

// Auto-start on load
if (typeof document !== 'undefined') {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      setTimeout(() => DhanAlerts.start(), 3000);
    });
  } else {
    setTimeout(() => DhanAlerts.start(), 3000);
  }
}

console.log('[DhanAlerts] Module loaded');
