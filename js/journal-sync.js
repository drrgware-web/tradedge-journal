// ═══════════════════════════════════════════════════════════════
// TradEdge Journal ↔ Edge Pilot Sync — js/journal-sync.js
// Bidirectional sync: localStorage cross-tab + Supabase cross-device
// ═══════════════════════════════════════════════════════════════

'use strict';

TE.sync = TE.sync || {};

// ── Cross-Tab Sync via StorageEvent ──────────────────────────
// When Edge Pilot writes to te_trades, Journal picks it up and vice versa
window.addEventListener('storage', function(e) {
  if (e.key !== 'te_trades' || !e.newValue) return;

  try {
    const incoming = JSON.parse(e.newValue);
    if (!Array.isArray(incoming)) return;

    // Merge strategy: union by trade ID, latest updatedAt wins
    const merged = TE.sync.mergeTradeSets(TE.trades, incoming);
    const changed = merged.length !== TE.trades.length ||
      JSON.stringify(merged.map(t => t.id).sort()) !== JSON.stringify(TE.trades.map(t => t.id).sort());

    if (changed) {
      TE.trades = merged;
      // Don't call TE.save() — that would loop (we're reacting to a storage event)
      // Just update in-memory state and fire render event
      window.dispatchEvent(new CustomEvent('te:trades-changed', {
        detail: { source: 'cross-tab', count: merged.length }
      }));
      console.log(`[Sync] Cross-tab update: ${incoming.length} incoming → ${merged.length} merged`);
    }
  } catch (err) {
    console.warn('[Sync] Cross-tab parse error:', err.message);
  }
});

// ── Merge Two Trade Arrays ───────────────────────────────────
// Union by ID. If same ID exists in both, keep the one with more exits
// (i.e., more recent activity), or the one with Edge Pilot metadata.
TE.sync.mergeTradeSets = function(local, remote) {
  const map = new Map();

  // Index local trades
  local.forEach(t => map.set(t.id, { ...t }));

  // Merge remote trades
  remote.forEach(rt => {
    const existing = map.get(rt.id);
    if (!existing) {
      // New trade from other tab — add it
      map.set(rt.id, { ...rt });
    } else {
      // Same ID — pick the more complete version
      const localExits  = (existing.exits || []).length;
      const remoteExits = (rt.exits || []).length;
      const localEntries  = (existing.entries || []).length;
      const remoteEntries = (rt.entries || []).length;

      // More exits = trade progressed further → prefer it
      if (remoteExits > localExits) {
        map.set(rt.id, { ...rt });
      }
      // More entries (pyramid) = prefer more complete
      else if (remoteExits === localExits && remoteEntries > localEntries) {
        map.set(rt.id, { ...rt });
      }
      // Edge Pilot fields present = prefer EP version (it adds metadata)
      else if (rt._epPyramid !== undefined && existing._epPyramid === undefined) {
        map.set(rt.id, { ...existing, ...TE.sync.extractEPFields(rt) });
      }
      // Otherwise merge EP fields onto existing without overwriting core data
      else if (rt._epPyramid !== undefined) {
        map.set(rt.id, { ...existing, ...TE.sync.extractEPFields(rt) });
      }
    }
  });

  return Array.from(map.values());
};

// ── Extract Edge Pilot metadata fields ───────────────────────
TE.sync.extractEPFields = function(trade) {
  const ep = {};
  Object.keys(trade).forEach(k => {
    if (k.startsWith('_ep')) ep[k] = trade[k];
  });
  return ep;
};

// ── Edge Pilot → Journal: Process new trades from EP ─────────
// Called when EP creates a trade via execution engine (Stage 6)
TE.sync.onEPTradeCreated = function(epTrade) {
  // Check if already exists
  const existing = TE.trades.find(t => t.id === epTrade.id);
  if (existing) {
    // Update with EP metadata
    Object.assign(existing, TE.sync.extractEPFields(epTrade));
    if (epTrade.sl) existing.sl = epTrade.sl;
    if (epTrade.entries?.length > existing.entries?.length) existing.entries = epTrade.entries;
    if (epTrade.exits?.length > existing.exits?.length) existing.exits = epTrade.exits;
  } else {
    TE.trades.push(epTrade);
  }
  TE.save();
  console.log(`[Sync] EP trade ${epTrade.symbol} synced to Journal`);
};

// ── Journal → Edge Pilot: Notify EP of SL/Target changes ────
// When user edits SL or target in Journal, EP needs to know
TE.sync.notifyEPUpdate = function(tradeId, field, value) {
  // This works via localStorage — EP watches for changes
  const trade = TE.trades.find(t => t.id === tradeId);
  if (!trade) return;

  trade[field] = value;
  trade._syncTs = Date.now(); // Timestamp for conflict resolution
  TE.save(); // This writes to localStorage, which fires StorageEvent on EP tab

  console.log(`[Sync] Journal → EP: ${trade.symbol} ${field}=${value}`);
};

// ── Supabase Polling (cross-device sync) ─────────────────────
// Polls every 60s during market hours for changes from other devices
let _syncPollTimer = null;

TE.sync.startPolling = function(intervalMs = 60000) {
  if (_syncPollTimer) clearInterval(_syncPollTimer);
  _syncPollTimer = setInterval(async () => {
    if (!TE.isMarketOpen()) return;
    if (TE._pushing || TE._pulling) return;

    try {
      const { url, key } = TE.sbGetCreds();
      if (!url || !key) return;

      // Quick check: has cloud been updated since our last save?
      const rows = await TE.sbApi('GET',
        'tradedge_trades?device_id=eq.tradedge_main&select=updated_at&limit=1'
      );
      if (!rows?.length) return;

      const cloudTime = new Date(rows[0].updated_at || 0).getTime();
      const localTime = +(localStorage.getItem('te_last_save') || 0);

      if (cloudTime > localTime + 5000) { // 5s buffer to avoid race
        console.log('[Sync] Cloud is newer — auto-pulling…');
        await TE.sbPull();
      }
    } catch (e) {
      // Silent fail — polling shouldn't spam errors
    }
  }, intervalMs);
};

TE.sync.stopPolling = function() {
  if (_syncPollTimer) {
    clearInterval(_syncPollTimer);
    _syncPollTimer = null;
  }
};

// ── Capital Sync ─────────────────────────────────────────────
// Edge Pilot reads te_current_capital from localStorage
// Journal's allocation.html writes to te_fund
// This bridges the two: compute current capital from fund data + trades P&L
TE.sync.computeCurrentCapital = function() {
  const fundData = TE.getFundData();
  const now = new Date();
  const year = now.getFullYear();
  const yearData = fundData[year] || {};

  // Sum all deposits and withdrawals
  let totalAdded = 0, totalWithdrawn = 0;
  Object.keys(yearData).forEach(k => {
    if (k.endsWith('_add')) totalAdded += (+yearData[k] || 0);
    if (k.endsWith('_wdr')) totalWithdrawn += (+yearData[k] || 0);
  });

  // Starting capital + deposits - withdrawals + realised P&L
  const startCap = TE.cfg.cap || 5000000;
  const all = TE.trades.map(t => TE.calc({ ...t }));
  const realisedPL = all.filter(t => t.status === 'Closed')
    .reduce((s, t) => s + t.realisedPL, 0);
  const unrealisedPL = all.filter(t => t.status !== 'Closed')
    .reduce((s, t) => s + t.unrealisedPL, 0);

  const currentCapital = startCap + totalAdded - totalWithdrawn + realisedPL;

  // Write to localStorage for Edge Pilot to read
  localStorage.setItem('te_current_capital', String(Math.round(currentCapital)));

  return {
    startCap,
    totalAdded,
    totalWithdrawn,
    realisedPL: Math.round(realisedPL),
    unrealisedPL: Math.round(unrealisedPL),
    currentCapital: Math.round(currentCapital),
    totalPL: Math.round(realisedPL + unrealisedPL)
  };
};

// ── Trade Event Logger (for Supabase trade_events table) ─────
TE.sync.logTradeEvent = async function(tradeId, eventType, data) {
  const { url, key } = TE.sbGetCreds();
  if (!url || !key) return;

  try {
    await TE.sbApi('POST', 'trade_events', {
      trade_id: tradeId,
      event_type: eventType,
      data: JSON.stringify(data),
      source: 'journal',
      created_at: new Date().toISOString()
    });
  } catch (e) {
    // Non-critical — log silently
    console.warn('[Sync] Event log failed:', e.message);
  }
};

// ── Boot ─────────────────────────────────────────────────────
(function syncBoot() {
  // Compute and set current capital on load
  TE.sync.computeCurrentCapital();

  // Start polling if auto-sync enabled
  if (localStorage.getItem('sb_auto')) {
    TE.sync.startPolling();
  }

  // Listen for trade changes to recompute capital
  window.addEventListener('te:trades-changed', () => {
    TE.sync.computeCurrentCapital();
  });
})();

console.log('[TE Sync] Journal ↔ Edge Pilot sync active');
