/**
 * TradEdge Watchlist Engine v1.0
 * ================================
 * localStorage + Supabase cloud sync for watchlists.
 * 
 * Data Model:
 *   watchlist = { id, name, stocks: [{symbol,name,sector,source,addedAt,notes,tags,trade}], createdAt, updatedAt }
 * 
 * Usage:
 *   const wl = new TEWatchlist();
 *   await wl.init();
 *   wl.createList("VCP Breakouts", [{symbol:"SBIN",name:"SBI",source:"chartink:VCP"}]);
 *   wl.addStock("list_id", {symbol:"RELIANCE", name:"Reliance"});
 *   const lists = wl.getLists();
 *   await wl.sync(); // push to Supabase
 */

(function() {
  'use strict';

  const LS_KEY = 'te_watchlists';
  const SB_URL = 'https://urnrdpyhncezljirpnmy.supabase.co';
  const SB_TABLE = 'tradedge_watchlists';

  function getSBKey() {
    // Try localStorage first, then fallback
    return localStorage.getItem('te_supabase_key') || '';
  }

  function genId() {
    return 'wl_' + Date.now().toString(36) + '_' + Math.random().toString(36).substr(2, 5);
  }

  class TEWatchlist {
    constructor() {
      this.lists = [];
      this.loaded = false;
      this.deviceId = localStorage.getItem('te_device_id') || (() => {
        const id = 'dev_' + Date.now().toString(36) + Math.random().toString(36).substr(2,4);
        localStorage.setItem('te_device_id', id);
        return id;
      })();
    }

    // ── Init: Load from localStorage ──
    init() {
      try {
        const raw = localStorage.getItem(LS_KEY);
        this.lists = raw ? JSON.parse(raw) : [];
      } catch(e) {
        this.lists = [];
      }
      this.loaded = true;
      return this;
    }

    // ── Save to localStorage ──
    _save() {
      try {
        localStorage.setItem(LS_KEY, JSON.stringify(this.lists));
      } catch(e) {
        console.error('[WL] Save failed:', e.message);
      }
    }

    // ── Get all lists ──
    getLists() {
      return this.lists;
    }

    // ── Get single list by ID ──
    getList(id) {
      return this.lists.find(l => l.id === id) || null;
    }

    // ── Create new watchlist ──
    createList(name, stocks = [], source = '') {
      const list = {
        id: genId(),
        name: name,
        stocks: stocks.map(s => ({
          symbol: s.symbol || '',
          name: s.name || '',
          sector: s.sector || '',
          source: s.source || source || 'manual',
          addedAt: new Date().toISOString(),
          notes: s.notes || '',
          tags: s.tags || [],
          trade: s.trade || { entry: null, target: null, sl: null, qty: 0, status: 'watching' },
        })),
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };
      this.lists.unshift(list);
      this._save();
      console.log(`[WL] Created "${name}" with ${stocks.length} stocks`);
      return list;
    }

    // ── Add stock to list ──
    addStock(listId, stock) {
      const list = this.getList(listId);
      if (!list) return false;
      // Check duplicate
      if (list.stocks.some(s => s.symbol === stock.symbol)) {
        console.log(`[WL] ${stock.symbol} already in "${list.name}"`);
        return false;
      }
      list.stocks.push({
        symbol: stock.symbol || '',
        name: stock.name || '',
        sector: stock.sector || '',
        source: stock.source || 'manual',
        addedAt: new Date().toISOString(),
        notes: stock.notes || '',
        tags: stock.tags || [],
        trade: stock.trade || { entry: null, target: null, sl: null, qty: 0, status: 'watching' },
      });
      list.updatedAt = new Date().toISOString();
      this._save();
      return true;
    }

    // ── Remove stock from list ──
    removeStock(listId, symbol) {
      const list = this.getList(listId);
      if (!list) return false;
      list.stocks = list.stocks.filter(s => s.symbol !== symbol);
      list.updatedAt = new Date().toISOString();
      this._save();
      return true;
    }

    // ── Update stock trade plan ──
    updateTrade(listId, symbol, trade) {
      const list = this.getList(listId);
      if (!list) return false;
      const stock = list.stocks.find(s => s.symbol === symbol);
      if (!stock) return false;
      stock.trade = { ...stock.trade, ...trade };
      list.updatedAt = new Date().toISOString();
      this._save();
      return true;
    }

    // ── Delete entire list ──
    deleteList(id) {
      this.lists = this.lists.filter(l => l.id !== id);
      this._save();
      return true;
    }

    // ── Rename list ──
    renameList(id, newName) {
      const list = this.getList(id);
      if (!list) return false;
      list.name = newName;
      list.updatedAt = new Date().toISOString();
      this._save();
      return true;
    }

    // ── Get all unique symbols across all lists ──
    getAllSymbols() {
      const syms = new Set();
      this.lists.forEach(l => l.stocks.forEach(s => syms.add(s.symbol)));
      return [...syms];
    }

    // ── Check if symbol is in any watchlist ──
    isWatched(symbol) {
      return this.lists.some(l => l.stocks.some(s => s.symbol === symbol));
    }

    // ── Get lists containing a symbol ──
    getListsForSymbol(symbol) {
      return this.lists.filter(l => l.stocks.some(s => s.symbol === symbol));
    }

    // ═══ SUPABASE SYNC ═══

    async sync() {
      const sbKey = getSBKey();
      if (!sbKey) {
        console.warn('[WL] No Supabase key — skipping sync');
        return { synced: false, reason: 'no_key' };
      }

      try {
        // Push local to cloud
        const payload = {
          device_id: this.deviceId,
          watchlists_json: JSON.stringify(this.lists),
          updated_at: new Date().toISOString(),
        };

        const pushRes = await fetch(`${SB_URL}/rest/v1/${SB_TABLE}?device_id=eq.${this.deviceId}`, {
          method: 'GET',
          headers: { 'apikey': sbKey, 'Authorization': `Bearer ${sbKey}` },
        });

        if (pushRes.ok) {
          const rows = await pushRes.json();
          const method = rows.length > 0 ? 'PATCH' : 'POST';
          const url = rows.length > 0 
            ? `${SB_URL}/rest/v1/${SB_TABLE}?device_id=eq.${this.deviceId}`
            : `${SB_URL}/rest/v1/${SB_TABLE}`;

          await fetch(url, {
            method,
            headers: {
              'apikey': sbKey,
              'Authorization': `Bearer ${sbKey}`,
              'Content-Type': 'application/json',
              'Prefer': 'return=minimal',
            },
            body: JSON.stringify(payload),
          });
        }

        console.log('[WL] Synced to Supabase');
        return { synced: true };
      } catch(e) {
        console.error('[WL] Sync failed:', e.message);
        return { synced: false, reason: e.message };
      }
    }

    async pullFromCloud() {
      const sbKey = getSBKey();
      if (!sbKey) return false;

      try {
        const res = await fetch(`${SB_URL}/rest/v1/${SB_TABLE}?select=watchlists_json,updated_at&order=updated_at.desc&limit=1`, {
          headers: { 'apikey': sbKey, 'Authorization': `Bearer ${sbKey}` },
        });
        if (!res.ok) return false;
        const rows = await res.json();
        if (!rows.length || !rows[0].watchlists_json) return false;
        
        const cloudLists = JSON.parse(rows[0].watchlists_json);
        // Merge: cloud lists that don't exist locally get added
        const localIds = new Set(this.lists.map(l => l.id));
        let added = 0;
        for (const cl of cloudLists) {
          if (!localIds.has(cl.id)) {
            this.lists.push(cl);
            added++;
          }
        }
        if (added > 0) {
          this._save();
          console.log(`[WL] Pulled ${added} new lists from cloud`);
        }
        return true;
      } catch(e) {
        console.error('[WL] Pull failed:', e.message);
        return false;
      }
    }
  }

  // ── Global Instance ──
  window.TEWatchlist = TEWatchlist;
  window.teWatchlist = new TEWatchlist().init();
  console.log('✅ TradEdge Watchlist Engine loaded |', window.teWatchlist.getLists().length, 'lists');
})();
