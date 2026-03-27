// ═══════════════════════════════════════════════════════════════
// EDGE PILOT ↔ JOURNAL SYNC BRIDGE
// Add as: <script src="js/ep-sync-bridge.js"></script> at bottom of autopilot.html
// Requires: js/core.js, js/supabase.js, js/journal-sync.js loaded first
//
// What this does:
// 1. Cross-tab sync: when Journal edits trades, EP picks up changes
// 2. When EP executes/sells/moves SL, Journal sees it instantly
// 3. Capital bridge: EP and Journal share the same capital computation
// 4. Supabase: EP's writes also trigger Journal's debounced cloud push
// ═══════════════════════════════════════════════════════════════

'use strict';

(function() {
  // ── Guard: only run if TE namespace exists (core.js loaded) ──
  if (typeof TE === 'undefined') {
    console.warn('[EP-Sync] TE namespace not found — core.js not loaded. Bridge disabled.');
    return;
  }

  console.log('[EP-Sync] Wiring Edge Pilot ↔ Journal bidirectional sync…');

  // ══════════════════════════════════════════════════
  // 1. CROSS-TAB SYNC: Journal → Edge Pilot
  // When Journal modifies te_trades (edit SL, close trade, etc.),
  // Edge Pilot detects it and refreshes its state.
  // ══════════════════════════════════════════════════

  // Listen for changes from Journal tabs (StorageEvent fires on OTHER tabs)
  window.addEventListener('storage', function(e) {
    if (e.key !== 'te_trades' || !e.newValue) return;
    try {
      const incoming = JSON.parse(e.newValue);
      if (!Array.isArray(incoming)) return;

      // Merge: use journal-sync's merge strategy
      if (typeof TE.sync?.mergeTradeSets === 'function') {
        const currentLocal = JSON.parse(localStorage.getItem('te_trades') || '[]');
        const merged = TE.sync.mergeTradeSets(currentLocal, incoming);
        // Only update if something changed
        if (merged.length !== currentLocal.length ||
            JSON.stringify(merged.map(t => t.id).sort()) !== JSON.stringify(currentLocal.map(t => t.id).sort())) {
          localStorage.setItem('te_trades', JSON.stringify(merged));
          TE.trades = merged;
          console.log(`[EP-Sync] Journal → EP: ${incoming.length} trades received, ${merged.length} after merge`);
          // Show toast if EP has showToast
          if (typeof showToast === 'function') {
            showToast('🔄', 'Journal sync', `${merged.length} trades updated from Journal`, 3000);
          }
        }
      }
    } catch (err) {
      console.warn('[EP-Sync] Cross-tab parse error:', err.message);
    }
  });

  // Also listen for same-tab CustomEvent (from journal-sync.js)
  window.addEventListener('te:trades-changed', function(e) {
    const source = e.detail?.source;
    if (source === 'cross-tab' || source === 'cloud-pull') {
      console.log(`[EP-Sync] Trades changed (${source}) — EP state refreshed`);
      // If EP had a render function we could call it, but EP's render()
      // re-reads from localStorage each time anyway
    }
  });

  // ══════════════════════════════════════════════════
  // 2. EDGE PILOT → JOURNAL: Hook executeOrder()
  // After EP writes a trade, also update TE.trades + call TE.save()
  // This triggers:
  //   - StorageEvent → Journal picks up the new trade
  //   - Debounced sbPush → Supabase gets updated
  //   - te:trades-changed event → Dashboard re-renders
  // ══════════════════════════════════════════════════

  // Monkey-patch: wrap the existing executeOrder to also call TE.save()
  const _origExecuteOrder = window.executeOrder;
  if (typeof _origExecuteOrder === 'function') {
    window.executeOrder = async function(symbol) {
      // Run original EP execution
      await _origExecuteOrder(symbol);

      // Sync TE.trades from localStorage (EP just wrote there)
      TE.trades = JSON.parse(localStorage.getItem('te_trades') || '[]');

      // Trigger Journal's save pipeline (debounced Supabase push + events)
      TE.save();

      // Recompute capital bridge
      if (typeof TE.sync?.computeCurrentCapital === 'function') {
        TE.sync.computeCurrentCapital();
      }

      console.log(`[EP-Sync] executeOrder(${symbol}) → TE.save() → Journal notified`);
    };
    console.log('[EP-Sync] ✓ executeOrder() hooked');
  }

  // ══════════════════════════════════════════════════
  // 3. Hook executePartialSell → TE.save()
  // ══════════════════════════════════════════════════
  const _origPartialSell = window.executePartialSell;
  if (typeof _origPartialSell === 'function') {
    window.executePartialSell = async function(tradeId, sellPct, sellPrice, reason, atrMultiple) {
      const result = await _origPartialSell(tradeId, sellPct, sellPrice, reason, atrMultiple);

      // Sync to TE.trades
      TE.trades = JSON.parse(localStorage.getItem('te_trades') || '[]');
      TE.save();

      if (typeof TE.sync?.computeCurrentCapital === 'function') {
        TE.sync.computeCurrentCapital();
      }

      console.log(`[EP-Sync] partialSell(${tradeId}, ${sellPct}%) → Journal notified`);
      return result;
    };
    console.log('[EP-Sync] ✓ executePartialSell() hooked');
  }

  // ══════════════════════════════════════════════════
  // 4. Hook logSLMove → TE.sync.notifyEPUpdate()
  // ══════════════════════════════════════════════════
  const _origLogSLMove = window.logSLMove;
  if (typeof _origLogSLMove === 'function') {
    window.logSLMove = async function(tradeId, newSL, reason) {
      await _origLogSLMove(tradeId, newSL, reason);

      // Sync to TE.trades
      TE.trades = JSON.parse(localStorage.getItem('te_trades') || '[]');
      TE.save();

      console.log(`[EP-Sync] SL move(${tradeId}, ₹${newSL}) → Journal notified`);
    };
    console.log('[EP-Sync] ✓ logSLMove() hooked');
  }

  // ══════════════════════════════════════════════════
  // 5. CAPITAL BRIDGE
  // EP's computeCurrentCapital() already writes te_current_capital.
  // But Journal's TE.sync.computeCurrentCapital() is more comprehensive
  // (includes fund data from te_fund). Let's use Journal's version
  // and keep EP's as fallback.
  // ══════════════════════════════════════════════════
  const _origComputeCapital = window.computeCurrentCapital;
  if (typeof _origComputeCapital === 'function' && typeof TE.sync?.computeCurrentCapital === 'function') {
    window.computeCurrentCapital = function() {
      // Use Journal's comprehensive version (includes te_fund deposits/withdrawals)
      const result = TE.sync.computeCurrentCapital();
      // Also run EP's version for its own cached state
      _origComputeCapital();
      return result.currentCapital;
    };
    console.log('[EP-Sync] ✓ computeCurrentCapital() bridged to Journal');
  }

  // ══════════════════════════════════════════════════
  // 6. SUPABASE BRIDGE
  // EP has its own supabaseUpsert/Fetch/Update functions.
  // Journal has TE.sbPush/TE.sbPull (fixed, debounced).
  // We don't replace EP's Supabase functions — they write to
  // different tables (trades, trade_events). But we make sure
  // TE.sbPush() also fires after EP writes, so the main
  // tradedge_trades row stays in sync.
  // ══════════════════════════════════════════════════

  // After EP's Supabase writes, also trigger Journal's cloud sync
  const _origSupabaseUpsert = window.supabaseUpsert;
  if (typeof _origSupabaseUpsert === 'function') {
    window.supabaseUpsert = async function(table, data) {
      const result = await _origSupabaseUpsert(table, data);

      // If EP wrote to 'trades' table, also queue a Journal cloud push
      if (table === 'trades' && typeof TE.sbPush === 'function' && !TE._pushing) {
        clearTimeout(TE._pushTimer);
        TE._pushTimer = setTimeout(() => TE.sbPush(), 5000); // 5s delay after EP write
      }

      return result;
    };
    console.log('[EP-Sync] ✓ supabaseUpsert() bridged → Journal cloud push queued');
  }

  // ══════════════════════════════════════════════════
  // 7. INITIAL SYNC ON BOOT
  // Make sure TE.trades matches localStorage on EP page load
  // ══════════════════════════════════════════════════
  setTimeout(() => {
    const lsTrades = JSON.parse(localStorage.getItem('te_trades') || '[]');
    if (lsTrades.length !== TE.trades.length) {
      TE.trades = lsTrades;
      console.log(`[EP-Sync] Boot sync: TE.trades updated to ${TE.trades.length} trades`);
    }

    // Compute capital on boot
    if (typeof TE.sync?.computeCurrentCapital === 'function') {
      const cap = TE.sync.computeCurrentCapital();
      console.log(`[EP-Sync] Capital: ₹${cap.currentCapital.toLocaleString('en-IN')}`);
    }
  }, 1000);

  console.log('[EP-Sync] ✓ Bridge active — Journal ↔ Edge Pilot bidirectional sync ready');
})();
