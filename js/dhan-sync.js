/**
 * TradEdge — Dhan Portfolio Sync Module
 * js/dhan-sync.js
 *
 * Fetches holdings and positions from Dhan API and syncs to te_trades.
 * 
 * Usage:
 *   await DhanSync.fetchPortfolio();     // Get holdings + positions
 *   await DhanSync.syncToTrades();       // Import to te_trades
 *   DhanSync.showSyncModal();            // Show sync UI
 *
 * localStorage keys: dhan_id, dhan_tk, zd_worker_url
 */

window.DhanSync = (() => {
  'use strict';

  // ── Helpers ────────────────────────────────────────────────
  function getCredentials() {
    return {
      url: localStorage.getItem('zd_worker_url') || '',
      token: localStorage.getItem('dhan_tk') || '',
      id: localStorage.getItem('dhan_id') || ''
    };
  }

  function isConfigured() {
    const { url, token, id } = getCredentials();
    return !!(url && token && id);
  }

  async function dhanFetch(action) {
    const { url, token, id } = getCredentials();
    if (!url || !token || !id) throw new Error('Dhan credentials not configured');

    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Kite-Action': action,
        'X-Dhan-Token': token,
        'X-Dhan-ID': id
      },
      body: '{}'
    });

    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }

  // ── Fetch Portfolio ────────────────────────────────────────
  async function fetchHoldings() {
    try {
      const data = await dhanFetch('dhan-holdings');
      if (data?.errorType) {
        console.log('[DhanSync] Holdings:', data.errorMessage);
        return [];
      }
      return Array.isArray(data) ? data : [];
    } catch (e) {
      console.warn('[DhanSync] Holdings fetch failed:', e.message);
      return [];
    }
  }

  async function fetchPositions() {
    try {
      const data = await dhanFetch('dhan-positions');
      if (data?.errorType) {
        console.log('[DhanSync] Positions:', data.errorMessage);
        return [];
      }
      return Array.isArray(data) ? data : [];
    } catch (e) {
      console.warn('[DhanSync] Positions fetch failed:', e.message);
      return [];
    }
  }

  async function fetchPortfolio() {
    const [holdings, positions] = await Promise.all([
      fetchHoldings(),
      fetchPositions()
    ]);
    return { holdings, positions };
  }

  // ── Parse Dhan data to TradEdge format ─────────────────────
  function parseHolding(h) {
    // Dhan holdings structure:
    // { tradingSymbol, exchange, securityId, isin, totalQty, dpQty, t1Qty, 
    //   availableQty, collateralQty, avgCostPrice, lastPrice, pnl }
    return {
      symbol: (h.tradingSymbol || '').replace(/-EQ$/, '').toUpperCase(),
      exchange: h.exchange || 'NSE',
      securityId: h.securityId,
      qty: h.totalQty || h.availableQty || 0,
      avgPrice: h.avgCostPrice || 0,
      cmp: h.lastPrice || 0,
      pnl: h.pnl || 0,
      type: 'holding',
      raw: h
    };
  }

  function parsePosition(p) {
    // Dhan positions structure:
    // { tradingSymbol, exchange, securityId, positionType, exchangeSegment,
    //   productType, buyQty, sellQty, netQty, buyAvg, sellAvg, costPrice,
    //   lastPrice, realizedProfit, unrealizedProfit, multiplier }
    const netQty = p.netQty ?? (p.buyQty - p.sellQty);
    return {
      symbol: (p.tradingSymbol || '').replace(/-EQ$/, '').toUpperCase(),
      exchange: p.exchange || 'NSE',
      securityId: p.securityId,
      qty: Math.abs(netQty),
      side: netQty >= 0 ? 'Buy' : 'Sell',
      avgPrice: p.costPrice || p.buyAvg || 0,
      cmp: p.lastPrice || 0,
      realizedPL: p.realizedProfit || 0,
      unrealizedPL: p.unrealizedProfit || 0,
      productType: p.productType, // CNC, INTRADAY, MARGIN
      type: 'position',
      raw: p
    };
  }

  function parsePortfolio(holdings, positions) {
    const parsed = [];
    
    holdings.forEach(h => {
      const p = parseHolding(h);
      if (p.symbol && p.qty > 0) parsed.push(p);
    });

    positions.forEach(pos => {
      const p = parsePosition(pos);
      if (p.symbol && p.qty > 0) parsed.push(p);
    });

    return parsed;
  }

  // ── Compare with te_trades ─────────────────────────────────
  function findMatchingTrade(symbol) {
    return TE.trades.find(t => 
      t.symbol?.toUpperCase() === symbol.toUpperCase() && 
      (t.status === 'Open' || t.status === 'Partial')
    );
  }

  function compareWithTrades(portfolio) {
    const results = {
      newTrades: [],      // In Dhan but not in te_trades
      matching: [],       // In both, can update CMP
      missingInDhan: [],  // In te_trades but not in Dhan
      conflicts: []       // Qty mismatch
    };

    const openTrades = TE.getOpenTrades();
    const dhanSymbols = new Set();

    portfolio.forEach(item => {
      dhanSymbols.add(item.symbol);
      const existing = findMatchingTrade(item.symbol);

      if (!existing) {
        results.newTrades.push(item);
      } else {
        const teQty = TE.calc(existing).openQty;
        if (teQty === item.qty) {
          results.matching.push({ dhan: item, trade: existing });
        } else {
          results.conflicts.push({ dhan: item, trade: existing, teQty, dhanQty: item.qty });
        }
      }
    });

    openTrades.forEach(t => {
      if (!dhanSymbols.has(t.symbol?.toUpperCase())) {
        results.missingInDhan.push(t);
      }
    });

    return results;
  }

  // ── Create trade from Dhan data ────────────────────────────
  function createTradeFromDhan(item) {
    const today = new Date().toISOString().slice(0, 10);
    return {
      id: Date.now() + '_' + Math.random().toString(36).slice(2, 8),
      symbol: item.symbol,
      setup: '',
      side: item.side || 'Buy',
      status: 'Open',
      sl: 0,
      target: 0,
      plan: '',
      exitTrigger: '',
      notes: `Imported from Dhan ${item.type}`,
      source: 'dhan',
      entries: [{
        date: today,
        price: item.avgPrice,
        qty: item.qty
      }],
      exits: [],
      cmp: item.cmp || 0
    };
  }

  // ── Import selected items ──────────────────────────────────
  function importTrades(items) {
    let imported = 0;
    items.forEach(item => {
      // Skip if already exists
      if (findMatchingTrade(item.symbol)) return;
      
      const trade = createTradeFromDhan(item);
      TE.trades.push(trade);
      imported++;
    });

    if (imported > 0) {
      TE.save();
      console.log(`[DhanSync] Imported ${imported} trades`);
    }
    return imported;
  }

  // ── Update CMP for matching trades ─────────────────────────
  function updateCMPs(matching) {
    let updated = 0;
    matching.forEach(({ dhan, trade }) => {
      if (dhan.cmp && dhan.cmp > 0) {
        trade.cmp = dhan.cmp;
        updated++;
      }
    });

    if (updated > 0) {
      TE.save();
      console.log(`[DhanSync] Updated CMP for ${updated} trades`);
    }
    return updated;
  }

  // ── Sync Modal UI ──────────────────────────────────────────
  function showSyncModal() {
    // Remove existing modal
    const existingModal = document.getElementById('dhan-sync-modal');
    if (existingModal) existingModal.remove();

    const modal = document.createElement('div');
    modal.id = 'dhan-sync-modal';
    modal.innerHTML = `
      <div class="dsm-overlay" onclick="DhanSync.closeModal()"></div>
      <div class="dsm-content">
        <div class="dsm-header">
          <span style="font-size:16px;font-weight:700">🔄 Dhan Portfolio Sync</span>
          <button class="dsm-close" onclick="DhanSync.closeModal()">✕</button>
        </div>
        <div class="dsm-body" id="dsm-body">
          <div style="text-align:center;padding:40px;color:var(--t3)">
            <div class="dsm-spinner"></div>
            <div style="margin-top:12px">Fetching from Dhan...</div>
          </div>
        </div>
        <div class="dsm-footer" id="dsm-footer" style="display:none">
          <button class="btn btn-ghost btn-sm" onclick="DhanSync.closeModal()">Cancel</button>
          <button class="btn btn-g btn-sm" id="dsm-import-btn" onclick="DhanSync.importSelected()">Import Selected</button>
        </div>
      </div>
    `;

    // Add styles
    const style = document.createElement('style');
    style.textContent = `
      #dhan-sync-modal { position:fixed;inset:0;z-index:9999;display:flex;align-items:center;justify-content:center }
      .dsm-overlay { position:absolute;inset:0;background:rgba(0,0,0,0.7) }
      .dsm-content { position:relative;background:var(--bg2);border:1px solid var(--b1);border-radius:12px;width:90%;max-width:600px;max-height:80vh;display:flex;flex-direction:column }
      .dsm-header { display:flex;justify-content:space-between;align-items:center;padding:16px 20px;border-bottom:1px solid var(--b1) }
      .dsm-close { background:none;border:none;color:var(--t3);font-size:18px;cursor:pointer }
      .dsm-close:hover { color:var(--t1) }
      .dsm-body { flex:1;overflow-y:auto;padding:16px 20px }
      .dsm-footer { display:flex;justify-content:flex-end;gap:10px;padding:16px 20px;border-top:1px solid var(--b1) }
      .dsm-spinner { width:32px;height:32px;border:3px solid var(--b2);border-top-color:var(--g);border-radius:50%;animation:dsm-spin 0.8s linear infinite;margin:0 auto }
      @keyframes dsm-spin { to { transform:rotate(360deg) } }
      .dsm-section { margin-bottom:20px }
      .dsm-section-title { font-size:12px;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:0.05em;margin-bottom:10px;display:flex;align-items:center;gap:8px }
      .dsm-count { background:var(--bg3);padding:2px 8px;border-radius:10px;font-size:11px }
      .dsm-item { display:flex;align-items:center;gap:12px;padding:10px 12px;background:var(--bg3);border:1px solid var(--b1);border-radius:8px;margin-bottom:8px }
      .dsm-item:hover { border-color:var(--b2) }
      .dsm-item input[type="checkbox"] { width:16px;height:16px;accent-color:var(--g) }
      .dsm-item-info { flex:1 }
      .dsm-item-symbol { font-weight:700;color:var(--t1) }
      .dsm-item-details { font-size:11px;color:var(--t3);margin-top:2px }
      .dsm-item-price { text-align:right;font-family:'IBM Plex Mono',monospace }
      .dsm-item-qty { font-size:13px;color:var(--t2) }
      .dsm-item-cmp { font-size:11px;color:var(--y) }
      .dsm-empty { text-align:center;padding:30px;color:var(--t3) }
      .dsm-stat { display:inline-flex;align-items:center;gap:6px;padding:6px 12px;background:var(--bg3);border-radius:8px;margin-right:8px;margin-bottom:8px;font-size:12px }
      .dsm-conflict { border-color:var(--y) !important; background:rgba(245,197,66,0.05) !important }
    `;
    document.head.appendChild(style);
    document.body.appendChild(modal);

    // Fetch and render
    loadSyncData();
  }

  let _syncData = null;
  let _portfolio = [];

  async function loadSyncData() {
    try {
      if (!isConfigured()) {
        renderError('Dhan credentials not configured. Go to Settings to add your Dhan API token.');
        return;
      }

      const { holdings, positions } = await fetchPortfolio();
      _portfolio = parsePortfolio(holdings, positions);
      _syncData = compareWithTrades(_portfolio);

      renderSyncData();
    } catch (e) {
      renderError('Failed to fetch: ' + e.message);
    }
  }

  function renderError(msg) {
    const body = document.getElementById('dsm-body');
    if (body) {
      body.innerHTML = `<div class="dsm-empty" style="color:var(--r)">❌ ${msg}</div>`;
    }
  }

  function renderSyncData() {
    const body = document.getElementById('dsm-body');
    const footer = document.getElementById('dsm-footer');
    if (!body || !_syncData) return;

    const { newTrades, matching, missingInDhan, conflicts } = _syncData;
    const totalDhan = _portfolio.length;
    const totalTE = TE.getOpenTrades().length;

    let html = `
      <div style="margin-bottom:16px">
        <span class="dsm-stat">📊 Dhan: <strong>${totalDhan}</strong></span>
        <span class="dsm-stat">📒 TradEdge: <strong>${totalTE}</strong> open</span>
        <span class="dsm-stat" style="color:var(--g)">✓ Matched: <strong>${matching.length}</strong></span>
      </div>
    `;

    // New trades (importable)
    if (newTrades.length > 0) {
      html += `
        <div class="dsm-section">
          <div class="dsm-section-title">
            <span>🆕 New in Dhan (not in TradEdge)</span>
            <span class="dsm-count">${newTrades.length}</span>
            <label style="margin-left:auto;font-size:11px;font-weight:400;cursor:pointer">
              <input type="checkbox" id="dsm-select-all" onchange="DhanSync.toggleSelectAll(this.checked)"> Select all
            </label>
          </div>
          ${newTrades.map((item, i) => `
            <div class="dsm-item">
              <input type="checkbox" class="dsm-import-cb" data-index="${i}">
              <div class="dsm-item-info">
                <div class="dsm-item-symbol">${item.symbol}</div>
                <div class="dsm-item-details">${item.type} · ${item.side || 'Buy'} · Avg: ₹${item.avgPrice.toFixed(2)}</div>
              </div>
              <div class="dsm-item-price">
                <div class="dsm-item-qty">${item.qty} qty</div>
                <div class="dsm-item-cmp">CMP: ₹${item.cmp?.toFixed(2) || '—'}</div>
              </div>
            </div>
          `).join('')}
        </div>
      `;
      footer.style.display = 'flex';
    }

    // Conflicts (qty mismatch)
    if (conflicts.length > 0) {
      html += `
        <div class="dsm-section">
          <div class="dsm-section-title">
            <span>⚠️ Quantity Mismatch</span>
            <span class="dsm-count">${conflicts.length}</span>
          </div>
          ${conflicts.map(c => `
            <div class="dsm-item dsm-conflict">
              <div class="dsm-item-info">
                <div class="dsm-item-symbol">${c.dhan.symbol}</div>
                <div class="dsm-item-details">Dhan: ${c.dhanQty} qty · TradEdge: ${c.teQty} qty</div>
              </div>
              <div class="dsm-item-price">
                <div class="dsm-item-cmp">Review manually</div>
              </div>
            </div>
          `).join('')}
        </div>
      `;
    }

    // Matching (already synced)
    if (matching.length > 0) {
      html += `
        <div class="dsm-section">
          <div class="dsm-section-title">
            <span>✅ Already Synced</span>
            <span class="dsm-count">${matching.length}</span>
          </div>
          ${matching.map(m => `
            <div class="dsm-item" style="opacity:0.7">
              <div class="dsm-item-info">
                <div class="dsm-item-symbol">${m.dhan.symbol}</div>
                <div class="dsm-item-details">${m.dhan.qty} qty · CMP: ₹${m.dhan.cmp?.toFixed(2) || '—'}</div>
              </div>
              <div class="dsm-item-price">
                <span style="color:var(--g);font-size:11px">✓ Matched</span>
              </div>
            </div>
          `).join('')}
        </div>
      `;
    }

    // Missing in Dhan (in TradEdge but not Dhan)
    if (missingInDhan.length > 0) {
      html += `
        <div class="dsm-section">
          <div class="dsm-section-title">
            <span>📤 Only in TradEdge (not in Dhan)</span>
            <span class="dsm-count">${missingInDhan.length}</span>
          </div>
          ${missingInDhan.map(t => {
            const ct = TE.calc(t);
            return `
              <div class="dsm-item" style="opacity:0.6">
                <div class="dsm-item-info">
                  <div class="dsm-item-symbol">${t.symbol}</div>
                  <div class="dsm-item-details">${ct.openQty} qty · Manual/Other broker</div>
                </div>
              </div>
            `;
          }).join('')}
        </div>
      `;
    }

    // Empty state
    if (totalDhan === 0) {
      html = `<div class="dsm-empty">No holdings or positions found in your Dhan account.</div>`;
      footer.style.display = 'none';
    }

    body.innerHTML = html;
  }

  function toggleSelectAll(checked) {
    document.querySelectorAll('.dsm-import-cb').forEach(cb => cb.checked = checked);
  }

  function importSelected() {
    const checkboxes = document.querySelectorAll('.dsm-import-cb:checked');
    const indices = Array.from(checkboxes).map(cb => parseInt(cb.dataset.index));
    const toImport = indices.map(i => _syncData.newTrades[i]).filter(Boolean);

    if (toImport.length === 0) {
      if (typeof toast === 'function') toast('Select items to import', '⚠️');
      return;
    }

    const imported = importTrades(toImport);

    // Also update CMPs for matching trades
    if (_syncData.matching.length > 0) {
      updateCMPs(_syncData.matching);
    }

    closeModal();

    if (typeof toast === 'function') {
      toast(`Imported ${imported} trades from Dhan`, '✅');
    }

    // Dispatch event to refresh UI
    window.dispatchEvent(new CustomEvent('te:trades-changed', { detail: { source: 'dhan-sync' } }));
  }

  function closeModal() {
    const modal = document.getElementById('dhan-sync-modal');
    if (modal) modal.remove();
    _syncData = null;
    _portfolio = [];
  }

  // ── Public API ─────────────────────────────────────────────
  return {
    isConfigured,
    fetchHoldings,
    fetchPositions,
    fetchPortfolio,
    parsePortfolio,
    compareWithTrades,
    importTrades,
    updateCMPs,
    showSyncModal,
    closeModal,
    toggleSelectAll,
    importSelected
  };
})();

console.log('[DhanSync] Module loaded');
