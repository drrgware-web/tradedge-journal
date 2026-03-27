// ═══════════════════════════════════════════════════════════════
// TradEdge Supabase Sync — js/supabase.js
// Cloud sync with debounce + re-entrancy guards
// FIXES: infinite loop, payload size, DOM corruption
// ═══════════════════════════════════════════════════════════════

'use strict';

// ── Supabase REST API helper ─────────────────────────────────
TE.sbGetCreds = function() {
  return {
    url: localStorage.getItem('sb_url') || '',
    key: localStorage.getItem('sb_key') || ''
  };
};

TE.sbApi = async function(method, path, body) {
  const { url, key } = TE.sbGetCreds();
  if (!url || !key) throw new Error('Supabase credentials not configured');

  const base    = url.replace(/\/$/, '');
  const fullUrl = base + '/rest/v1/' + path;

  const headers = {
    'apikey':        key,
    'Authorization': 'Bearer ' + key,
    'Content-Type':  'application/json',
    'Accept':        'application/json',
  };

  const opts = { method, headers };

  if (method === 'POST' || method === 'PATCH' || method === 'PUT') {
    headers['Prefer'] = 'resolution=merge-duplicates,return=minimal';
    opts.body = JSON.stringify(body);
  }

  let res;
  try {
    res = await fetch(fullUrl, opts);
  } catch (e) {
    throw new Error('Network error — check Supabase URL. (' + e.message + ')');
  }

  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch (e) {}

  if (!res.ok) {
    const msg = (json && (json.message || json.error_description || json.hint)) || text || ('HTTP ' + res.status);
    throw new Error('Supabase ' + res.status + ': ' + msg);
  }
  return json ?? [];
};

// ── Status UI helper (works if elements exist, fails silently otherwise) ──
TE.sbSetStatus = function(msg, state) {
  ['sb-dot', 'sb-sync-dot'].forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    el.className = 'sync-dot';
    if (state === 'ok') el.classList.add('connected');
    if (state === 'sync') el.classList.add('syncing');
  });
  ['sb-status-txt', 'sb-sync-status', 'sb-log'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.textContent = msg;
  });
};

// ── PUSH (with guard + stripped base64) ──────────────────────
TE.sbPush = async function() {
  if (TE._pushing) return;
  TE._pushing = true;

  const { url, key } = TE.sbGetCreds();
  if (!url || !key || !url.includes('.supabase.co')) {
    TE._pushing = false;
    return;
  }

  TE.sbSetStatus('Pushing…', 'sync');

  try {
    // Strip large base64 images to keep payload under 1MB
    const lightTrades = TE.trades.map(t => {
      const copy = { ...t };
      if (copy.entryImg && copy.entryImg.startsWith('data:')) delete copy.entryImg;
      if (copy.exitImg && copy.exitImg.startsWith('data:'))   delete copy.exitImg;
      return copy;
    });

    const ledger  = localStorage.getItem('te_ledger')  || '[]';
    const cfgStr  = localStorage.getItem('te_cfg')     || '{}';
    const fund    = localStorage.getItem('te_fund')    || '{}';
    const alerts  = localStorage.getItem('te_alerts')  || '[]';
    const gttJson = localStorage.getItem('te_gtt')     || '[]';

    const prefsObj = {
      rrmCache:  JSON.parse(localStorage.getItem('te_rrm_cache') || '{}'),
      mpsUrl:    localStorage.getItem('te_mps_url')    || '',
      theme:     localStorage.getItem('te_theme')      || 'dark',
      workerUrl: localStorage.getItem('zd_worker_url') || '',
    };

    const payload = {
      device_id:   'tradedge_main',
      trades_json: JSON.stringify(lightTrades),
      ledger_json: ledger,
      cfg_json:    cfgStr,
      fund_json:   fund,
      alerts_json: alerts,
      gtt_json:    gttJson,
      prefs_json:  JSON.stringify(prefsObj),
      updated_at:  new Date().toISOString()
    };

    const sizeKB = Math.round(JSON.stringify(payload).length / 1024);

    try {
      await TE.sbApi('POST', 'tradedge_trades?on_conflict=device_id', payload);
    } catch (pushErr) {
      // Fallback: remove gtt_json column if it doesn't exist yet
      if (pushErr.message?.includes('gtt_json')) {
        delete payload.gtt_json;
        await TE.sbApi('POST', 'tradedge_trades?on_conflict=device_id', payload);
      } else throw pushErr;
    }

    localStorage.setItem('te_last_save', String(Date.now()));
    const ts = new Date().toLocaleTimeString('en-IN');
    const lc = JSON.parse(ledger).length;
    TE.sbSetStatus(`Pushed ${lightTrades.length} trades · ${lc} ledger · ${sizeKB}KB ✅ · ${ts}`, 'ok');

  } catch (e) {
    TE.sbSetStatus('Push failed: ' + e.message, '');
    console.error('[sbPush]', e);
  } finally {
    TE._pushing = false;
  }
};

// ── PULL (with guard — suppresses auto-push during pull) ─────
TE.sbPull = async function(forcePull) {
  if (TE._pulling) return;
  TE._pulling = true;

  const { url, key } = TE.sbGetCreds();
  if (!url || !key || !url.includes('.supabase.co')) {
    TE._pulling = false;
    if (typeof TE.toast === 'function') TE.toast('Configure Supabase in Settings', '⚠️');
    return;
  }

  TE.sbSetStatus('Pulling…', 'sync');

  try {
    let rows;
    try {
      rows = await TE.sbApi('GET', 'tradedge_trades?device_id=eq.tradedge_main&select=trades_json,ledger_json,cfg_json,fund_json,alerts_json,gtt_json,prefs_json,updated_at&limit=1');
    } catch (e1) {
      rows = await TE.sbApi('GET', 'tradedge_trades?device_id=eq.tradedge_main&select=trades_json,ledger_json,cfg_json,fund_json,alerts_json,prefs_json,updated_at&limit=1');
    }

    if (!rows?.length) {
      TE.sbSetStatus('No cloud data — push first', '');
      TE._pulling = false;
      return;
    }

    const row = rows[0];
    const cloudTrades = JSON.parse(row.trades_json || '[]');
    const cloudTime   = new Date(row.updated_at || 0).getTime();
    const localTime   = +(localStorage.getItem('te_last_save') || 0);

    if (cloudTrades.length === 0 && TE.trades.length > 0) {
      TE.sbSetStatus('Cloud empty — keeping local', '');
      TE._pulling = false;
      return;
    }

    const shouldPull = forcePull || cloudTrades.length > TE.trades.length ||
                       cloudTime >= localTime || TE.trades.length === 0;

    if (shouldPull) {
      TE.trades = cloudTrades;
      localStorage.setItem('te_trades', JSON.stringify(TE.trades));
      if (row.ledger_json) localStorage.setItem('te_ledger', row.ledger_json);
      if (row.cfg_json)    localStorage.setItem('te_cfg',    row.cfg_json);
      if (row.fund_json)   localStorage.setItem('te_fund',   row.fund_json);
      if (row.alerts_json) localStorage.setItem('te_alerts', row.alerts_json);
      if (row.gtt_json)    localStorage.setItem('te_gtt',    row.gtt_json);

      if (row.prefs_json) {
        try {
          const prefs = JSON.parse(row.prefs_json);
          if (prefs.rrmCache)  localStorage.setItem('te_rrm_cache',  JSON.stringify(prefs.rrmCache));
          if (prefs.mpsUrl)    localStorage.setItem('te_mps_url',    prefs.mpsUrl);
          if (prefs.theme)     localStorage.setItem('te_theme',      prefs.theme);
          if (prefs.workerUrl) localStorage.setItem('zd_worker_url', prefs.workerUrl);
          // GTT fallback from prefs
          if (!row.gtt_json && prefs.gttOrders) {
            const gttStr = typeof prefs.gttOrders === 'string' ? prefs.gttOrders : JSON.stringify(prefs.gttOrders);
            localStorage.setItem('te_gtt', gttStr);
          }
        } catch (e2) {}
      }

      localStorage.setItem('te_last_save', String(cloudTime));

      // Reload config
      TE.cfg = JSON.parse(localStorage.getItem('te_cfg') || '{}');

      const ts = new Date().toLocaleTimeString('en-IN');
      TE.sbSetStatus(`Pulled ${cloudTrades.length} trades ✅ · ${ts}`, 'ok');

      // Fire event so pages can re-render
      window.dispatchEvent(new CustomEvent('te:trades-changed', { detail: { source: 'cloud-pull' } }));
    } else {
      TE.sbSetStatus(`Local (${TE.trades.length}) is newer — use Force Pull`, '');
    }

  } catch (e) {
    TE.sbSetStatus('Pull failed: ' + e.message, '');
    console.error('[sbPull]', e);
  } finally {
    TE._pulling = false;
  }
};

// ── Test Connection ──────────────────────────────────────────
TE.sbTestConnection = async function() {
  const { url, key } = TE.sbGetCreds();
  if (!url || !key) return { ok: false, msg: 'Missing credentials' };

  if (location.protocol === 'file:') {
    return { ok: false, msg: 'file:// protocol blocks requests — use a local server' };
  }

  try {
    const res = await fetch(url.replace(/\/$/, '') + '/rest/v1/', {
      headers: { 'apikey': key, 'Authorization': 'Bearer ' + key, 'Accept': 'application/json' }
    });
    if (res.ok || res.status === 200) return { ok: true, msg: 'Connection OK' };
    if (res.status === 401) return { ok: false, msg: 'Invalid API key (401)' };
    return { ok: false, msg: 'HTTP ' + res.status };
  } catch (e) {
    return { ok: false, msg: 'Failed to fetch — ' + e.message };
  }
};

// ── Auto-sync on page load ───────────────────────────────────
(function sbBoot() {
  // If auto-sync is enabled and we have zero local trades, pull from cloud
  if (localStorage.getItem('sb_auto') && TE.trades.length === 0) {
    setTimeout(() => TE.sbPull(), 2000);
  }
})();

console.log('[TE Supabase] Module loaded');
