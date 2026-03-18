// ══════════════════════════════════════════════════════════════════════════════
// TRADEDGE — GOOGLE DRIVE HYBRID ARCHIVE ENGINE
// ══════════════════════════════════════════════════════════════════════════════
// Drop this <script> BEFORE the closing </body> tag in index.html
// Also add the HTML card (see bottom of this file) into #page-settings
//
// WHAT THIS DOES:
// 1. Google OAuth sign-in via GIS (popup, no redirect needed)
// 2. Creates a TradEdge_Archive folder in user's Drive
// 3. Archives trades + ledger + charts older than N days as JSON files
// 4. Purge & Preserve: auto-trigger at 80% Supabase usage OR manual
// 5. Restore: browse archived months, pull back into active trades[]
// 6. Storage monitor: shows Supabase usage vs limits
//
// REQUIRES: Google Cloud OAuth Client ID (see setup guide PDF)
// ══════════════════════════════════════════════════════════════════════════════

// ── CONFIG ──
const GD_FOLDER_NAME  = 'TradEdge_Archive';
const GD_ARCHIVE_DAYS = 30;  // trades older than this get archived
const GD_AUTO_CHECK_INTERVAL = 60 * 60 * 1000; // check every 60 min
const GD_SB_WARN_PCT  = 80;  // warn at 80% Supabase usage
const GD_SB_MAX_KB    = 500 * 1024; // 500MB Supabase free tier (in KB)
const GD_SB_STORAGE_KB = 1024 * 1024; // 1GB file storage

// ── STATE ──
let _gdToken      = null;    // OAuth access token
let _gdFolderId   = null;    // TradEdge_Archive folder ID
let _gdClientId   = localStorage.getItem('gd_client_id') || '1086073222826-e0b21hrhdv8b28p6hrotj9mjt29f7sgv.apps.googleusercontent.com';
let _gdConnected  = false;
let _gdAutoTimer  = null;
let _gdArchiveLog = JSON.parse(localStorage.getItem('gd_archive_log') || '[]');

// ══════════════════════════════════════════════════
// 1. GOOGLE IDENTITY SERVICES (GIS) — OAuth Flow
// ══════════════════════════════════════════════════

// Load GIS library dynamically
function gdLoadGIS() {
  return new Promise((resolve, reject) => {
    if (window.google?.accounts?.oauth2) { resolve(); return; }
    const s = document.createElement('script');
    s.src = 'https://accounts.google.com/gsi/client';
    s.onload = () => {
      // Small delay for GIS to initialize
      setTimeout(resolve, 300);
    };
    s.onerror = () => reject(new Error('Failed to load Google Identity Services'));
    document.head.appendChild(s);
  });
}

// Initiate OAuth popup
async function gdSignIn() {
  const clientId = (document.getElementById('gd-client-id')?.value || _gdClientId || '').trim();
  if (!clientId || !clientId.includes('.apps.googleusercontent.com')) {
    toast('Enter a valid Google OAuth Client ID first', '⚠️');
    return;
  }
  localStorage.setItem('gd_client_id', clientId);
  _gdClientId = clientId;

  gdSetStatus('Connecting…', 'sync');

  try {
    await gdLoadGIS();

    const tokenClient = google.accounts.oauth2.initTokenClient({
      client_id: clientId,
      scope: 'https://www.googleapis.com/auth/drive.file',
      callback: (response) => {
        if (response.error) {
          gdSetStatus('Auth failed: ' + response.error, '');
          toast('Google sign-in failed: ' + response.error, '❌');
          return;
        }
        _gdToken = response.access_token;
        _gdConnected = true;
        localStorage.setItem('gd_token', _gdToken);
        localStorage.setItem('gd_token_ts', Date.now());
        gdSetStatus('Connected ✅', 'ok');
        toast('Google Drive connected! 🎉', '✅');
        gdEnsureFolder().then(() => {
          gdRefreshArchiveList();
          gdCheckStorageHealth();
        });
      },
    });

    tokenClient.requestAccessToken();
  } catch (e) {
    gdSetStatus('Error: ' + e.message, '');
    toast('Google auth error: ' + e.message, '❌');
  }
}

// Sign out
function gdSignOut() {
  _gdToken = null;
  _gdConnected = false;
  _gdFolderId = null;
  localStorage.removeItem('gd_token');
  localStorage.removeItem('gd_token_ts');
  localStorage.removeItem('gd_folder_id');
  gdSetStatus('Disconnected', '');
  toast('Google Drive disconnected', 'ℹ️');
  gdRenderArchiveList([]);
}

// Check if token is still valid (Google tokens last ~1 hour)
function gdIsTokenValid() {
  const ts = +(localStorage.getItem('gd_token_ts') || 0);
  return _gdToken && (Date.now() - ts) < 55 * 60 * 1000; // 55 min buffer
}

// Auto-restore token on page load
function gdRestoreSession() {
  const savedToken = localStorage.getItem('gd_token');
  const ts = +(localStorage.getItem('gd_token_ts') || 0);
  _gdClientId = localStorage.getItem('gd_client_id') || '';
  _gdFolderId = localStorage.getItem('gd_folder_id') || null;

  const clientEl = document.getElementById('gd-client-id');
  if (clientEl && _gdClientId) clientEl.value = _gdClientId;

  if (savedToken && (Date.now() - ts) < 55 * 60 * 1000) {
    _gdToken = savedToken;
    _gdConnected = true;
    gdSetStatus('Connected ✅', 'ok');
    // Silently refresh archive list
    setTimeout(() => gdRefreshArchiveList(), 2000);
  } else if (savedToken) {
    gdSetStatus('Token expired — sign in again', '');
    localStorage.removeItem('gd_token');
  } else if (_gdClientId) {
    gdSetStatus('Not signed in', '');
  } else {
    gdSetStatus('Enter Client ID to connect', '');
  }
}

// ══════════════════════════════════════════════════
// 2. GOOGLE DRIVE API HELPERS
// ══════════════════════════════════════════════════

async function gdApi(method, path, body, isUpload = false) {
  if (!_gdToken) throw new Error('Not signed in to Google Drive');

  const base = isUpload
    ? 'https://www.googleapis.com/upload/drive/v3/'
    : 'https://www.googleapis.com/drive/v3/';

  const headers = { 'Authorization': 'Bearer ' + _gdToken };
  const opts = { method, headers };

  if (body && !isUpload) {
    headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  } else if (body && isUpload) {
    opts.body = body; // FormData or raw body
    // Don't set Content-Type — browser will set it with boundary for multipart
  }

  const res = await fetch(base + path, opts);

  if (res.status === 401) {
    _gdConnected = false;
    gdSetStatus('Token expired — sign in again', '');
    throw new Error('Token expired');
  }

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error?.message || 'Drive API ' + res.status);
  }

  const text = await res.text();
  return text ? JSON.parse(text) : {};
}

// Ensure TradEdge_Archive folder exists
async function gdEnsureFolder() {
  if (_gdFolderId) {
    // Verify it still exists
    try {
      await gdApi('GET', `files/${_gdFolderId}?fields=id,name,trashed`);
      return _gdFolderId;
    } catch (e) {
      _gdFolderId = null;
    }
  }

  // Search for existing folder
  const q = `name='${GD_FOLDER_NAME}' and mimeType='application/vnd.google-apps.folder' and trashed=false`;
  const res = await gdApi('GET', `files?q=${encodeURIComponent(q)}&fields=files(id,name)&spaces=drive`);

  if (res.files && res.files.length > 0) {
    _gdFolderId = res.files[0].id;
  } else {
    // Create folder
    const folder = await gdApi('POST', 'files', {
      name: GD_FOLDER_NAME,
      mimeType: 'application/vnd.google-apps.folder',
      description: 'TradEdge Journal Archive — trades, ledger, and chart screenshots'
    });
    _gdFolderId = folder.id;
  }

  localStorage.setItem('gd_folder_id', _gdFolderId);
  return _gdFolderId;
}

// Upload a JSON file to Drive
async function gdUploadJSON(filename, data, description = '') {
  const folderId = await gdEnsureFolder();

  const metadata = {
    name: filename,
    parents: [folderId],
    mimeType: 'application/json',
    description: description || 'TradEdge archive — ' + new Date().toISOString()
  };

  const content = JSON.stringify(data, null, 2);

  // Check if file already exists (update instead of duplicate)
  const q = `name='${filename}' and '${folderId}' in parents and trashed=false`;
  const existing = await gdApi('GET', `files?q=${encodeURIComponent(q)}&fields=files(id)&spaces=drive`);

  if (existing.files && existing.files.length > 0) {
    // Update existing file
    const fileId = existing.files[0].id;
    const form = new FormData();
    form.append('metadata', new Blob([JSON.stringify({ description: metadata.description })], { type: 'application/json' }));
    form.append('file', new Blob([content], { type: 'application/json' }));

    return await gdApi('PATCH', `files/${fileId}?uploadType=multipart`, form, true);
  }

  // Create new file (multipart upload)
  const boundary = '---tradedge_boundary_' + Date.now();
  const delimiter = '\r\n--' + boundary + '\r\n';
  const closeDelimiter = '\r\n--' + boundary + '--';

  const body =
    delimiter +
    'Content-Type: application/json; charset=UTF-8\r\n\r\n' +
    JSON.stringify(metadata) +
    delimiter +
    'Content-Type: application/json\r\n\r\n' +
    content +
    closeDelimiter;

  const res = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart', {
    method: 'POST',
    headers: {
      'Authorization': 'Bearer ' + _gdToken,
      'Content-Type': 'multipart/related; boundary=' + boundary,
    },
    body: body
  });

  if (!res.ok) throw new Error('Upload failed: ' + res.status);
  return await res.json();
}

// List archive files in the folder
async function gdListArchives() {
  if (!_gdFolderId) await gdEnsureFolder();
  if (!_gdFolderId) return [];

  const q = `'${_gdFolderId}' in parents and trashed=false`;
  const res = await gdApi('GET',
    `files?q=${encodeURIComponent(q)}&fields=files(id,name,size,modifiedTime,description)&orderBy=name desc&pageSize=50&spaces=drive`
  );
  return res.files || [];
}

// Download a file's content
async function gdDownloadFile(fileId) {
  const res = await fetch(`https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`, {
    headers: { 'Authorization': 'Bearer ' + _gdToken }
  });
  if (!res.ok) throw new Error('Download failed');
  return await res.json();
}

// Delete a file
async function gdDeleteFile(fileId) {
  await gdApi('DELETE', `files/${fileId}`);
}

// ══════════════════════════════════════════════════
// 3. ARCHIVE ENGINE — Purge & Preserve
// ══════════════════════════════════════════════════

// Get trades older than N days
function gdGetOldTrades(days = GD_ARCHIVE_DAYS) {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - days);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  return trades.filter(t => {
    if (t.status === 'Open' || t.status === 'Partial') return false; // never archive open trades
    const lastDate = t.exits?.length
      ? t.exits.map(e => e.date).filter(Boolean).sort().pop()
      : t.entries?.[0]?.date;
    return lastDate && lastDate < cutoffStr;
  });
}

// Estimate current Supabase usage (rough)
function gdEstimateSupabaseKB() {
  const tradesJSON = localStorage.getItem('te_trades') || '[]';
  const ledgerJSON = localStorage.getItem('te_ledger') || '[]';
  const cfgJSON = localStorage.getItem('te_cfg') || '{}';
  const fundJSON = localStorage.getItem('te_fund') || '{}';
  const alertsJSON = localStorage.getItem('te_alerts') || '[]';
  const gttJSON = localStorage.getItem('te_gtt') || '[]';
  const prefsJSON = localStorage.getItem('te_rrm_cache') || '{}';

  const totalBytes = (tradesJSON + ledgerJSON + cfgJSON + fundJSON + alertsJSON + gttJSON + prefsJSON).length;
  return Math.round(totalBytes / 1024);
}

// Calculate storage health
function gdCheckStorageHealth() {
  const usageKB = gdEstimateSupabaseKB();
  const pct = Math.round(usageKB / GD_SB_MAX_KB * 100 * 100) / 100; // very small % for free tier

  // For realistic tracking: compare against 500MB as database limit
  // But our data is tiny (213KB for 248 trades), so track against a practical limit
  const practicalLimitKB = 1024; // 1MB practical warning threshold for single-row JSON
  const practicalPct = Math.min(100, Math.round(usageKB / practicalLimitKB * 100));

  const healthEl = document.getElementById('gd-storage-health');
  if (!healthEl) return;

  const oldTrades = gdGetOldTrades();
  const chartCount = trades.filter(t => t.entryImg || t.exitImg).length;
  const chartKB = trades.reduce((s, t) => {
    return s + ((t.entryImg || '').length + (t.exitImg || '').length) / 1024;
  }, 0);

  const barColor = practicalPct >= 80 ? 'var(--r)' : practicalPct >= 50 ? 'var(--y)' : 'var(--g)';

  healthEl.innerHTML = `
    <div style="margin-bottom:10px">
      <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:4px">
        <span style="color:var(--t3)">Supabase Usage</span>
        <span style="color:var(--t2);font-family:'IBM Plex Mono',monospace">${usageKB} KB / ~500 MB</span>
      </div>
      <div style="height:6px;background:var(--bg4);border-radius:3px;overflow:hidden">
        <div style="width:${Math.max(1, practicalPct)}%;height:100%;background:${barColor};border-radius:3px;transition:width .3s"></div>
      </div>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:11px">
      <div style="background:var(--bg3);padding:8px 10px;border-radius:6px">
        <div style="color:var(--t4);font-size:10px">Total Trades</div>
        <div style="color:var(--t1);font-weight:700">${trades.length}</div>
      </div>
      <div style="background:var(--bg3);padding:8px 10px;border-radius:6px">
        <div style="color:var(--t4);font-size:10px">Archivable (>${GD_ARCHIVE_DAYS}d)</div>
        <div style="color:${oldTrades.length ? 'var(--y)' : 'var(--t1)'};font-weight:700">${oldTrades.length}</div>
      </div>
      <div style="background:var(--bg3);padding:8px 10px;border-radius:6px">
        <div style="color:var(--t4);font-size:10px">Chart Screenshots</div>
        <div style="color:var(--t1);font-weight:700">${chartCount} (${Math.round(chartKB)} KB)</div>
      </div>
      <div style="background:var(--bg3);padding:8px 10px;border-radius:6px">
        <div style="color:var(--t4);font-size:10px">Drive Status</div>
        <div style="color:${_gdConnected ? 'var(--g)' : 'var(--t3)'};font-weight:700">${_gdConnected ? 'Connected' : 'Not connected'}</div>
      </div>
    </div>
    ${oldTrades.length >= 10 ? `
      <div style="margin-top:10px;padding:10px 12px;background:var(--yd);border:1px solid rgba(245,197,66,.3);border-radius:8px;font-size:11px;color:var(--y)">
        💡 <b>${oldTrades.length} closed trades</b> are older than ${GD_ARCHIVE_DAYS} days. Archive them to Google Drive to keep Supabase lean.
      </div>
    ` : ''}
  `;
}

// ── ARCHIVE: Move old trades to Drive ──
async function gdArchiveNow() {
  if (!_gdConnected || !_gdToken) { toast('Sign in to Google Drive first', '⚠️'); return; }
  if (!gdIsTokenValid()) { toast('Token expired — sign in again', '⚠️'); gdSetStatus('Token expired', ''); return; }

  const oldTrades = gdGetOldTrades();
  if (!oldTrades.length) { toast('No trades older than ' + GD_ARCHIVE_DAYS + ' days to archive', 'ℹ️'); return; }

  const statusEl = document.getElementById('gd-archive-status');
  if (statusEl) statusEl.innerHTML = '<span style="color:var(--y)">⏳ Archiving ' + oldTrades.length + ' trades…</span>';

  try {
    // Group by month
    const byMonth = {};
    oldTrades.forEach(t => {
      const d = t.exits?.length
        ? t.exits.map(e => e.date).filter(Boolean).sort().pop()
        : t.entries?.[0]?.date;
      if (!d) return;
      const key = d.slice(0, 7); // YYYY-MM
      if (!byMonth[key]) byMonth[key] = [];
      byMonth[key].push(t);
    });

    // Also archive matching ledger entries
    const ledger = JSON.parse(localStorage.getItem('te_ledger') || '[]');
    const oldestTradeDate = oldTrades.map(t => t.entries?.[0]?.date).filter(Boolean).sort()[0];

    let totalArchived = 0;
    let totalKB = 0;

    for (const [month, monthTrades] of Object.entries(byMonth)) {
      const filename = `trades_${month}.json`;
      const monthLedger = ledger.filter(e => e.date && e.date.startsWith(month));

      // Separate chart images to reduce JSON size
      const tradesNoCharts = monthTrades.map(t => {
        const clean = { ...t };
        delete clean.entryImg;
        delete clean.exitImg;
        return clean;
      });

      const archive = {
        version: 1,
        month: month,
        archivedAt: new Date().toISOString(),
        tradeCount: monthTrades.length,
        trades: tradesNoCharts,
        ledger: monthLedger,
        summary: {
          totalPL: monthTrades.reduce((s, t) => s + (calc({ ...t }).realisedPL || 0), 0),
          wins: monthTrades.filter(t => calc({ ...t }).realisedPL > 0).length,
          losses: monthTrades.filter(t => calc({ ...t }).realisedPL <= 0).length,
        }
      };

      await gdUploadJSON(filename, archive, `TradEdge archive: ${month} · ${monthTrades.length} trades`);
      totalArchived += monthTrades.length;
      totalKB += Math.round(JSON.stringify(archive).length / 1024);

      // Upload chart screenshots separately (if any)
      const chartsForMonth = monthTrades.filter(t => t.entryImg || t.exitImg);
      if (chartsForMonth.length) {
        const chartArchive = {
          version: 1,
          month: month,
          charts: chartsForMonth.map(t => ({
            tradeId: t.id,
            symbol: t.symbol,
            entryImg: t.entryImg || null,
            exitImg: t.exitImg || null,
          }))
        };
        await gdUploadJSON(`charts_${month}.json`, chartArchive, `Charts: ${month} · ${chartsForMonth.length} screenshots`);
      }

      if (statusEl) statusEl.innerHTML = `<span style="color:var(--y)">⏳ Archived ${month}… (${totalArchived}/${oldTrades.length})</span>`;
    }

    // ── PURGE: Remove archived trades from active storage ──
    const archivedIds = new Set(oldTrades.map(t => t.id));
    trades = trades.filter(t => !archivedIds.has(t.id));
    save(); // This also pushes to Supabase if auto-sync is on

    // Log the archive event
    _gdArchiveLog.unshift({
      ts: Date.now(),
      count: totalArchived,
      months: Object.keys(byMonth),
      sizeKB: totalKB,
    });
    if (_gdArchiveLog.length > 50) _gdArchiveLog = _gdArchiveLog.slice(0, 50);
    localStorage.setItem('gd_archive_log', JSON.stringify(_gdArchiveLog));

    // Refresh UI
    renderDash();
    renderTrades();
    gdCheckStorageHealth();
    await gdRefreshArchiveList();

    if (statusEl) statusEl.innerHTML = `<span style="color:var(--g)">✅ Archived ${totalArchived} trades (${totalKB} KB) to Google Drive</span>`;
    toast(`📦 ${totalArchived} trades archived to Drive · ${totalKB} KB saved`, '✅');

  } catch (e) {
    if (statusEl) statusEl.innerHTML = `<span style="color:var(--r)">❌ ${e.message}</span>`;
    toast('Archive failed: ' + e.message, '❌');
  }
}

// ── RESTORE: Pull archived trades back from Drive ──
async function gdRestoreMonth(fileId, filename) {
  if (!_gdConnected) { toast('Sign in to Google Drive first', '⚠️'); return; }

  const statusEl = document.getElementById('gd-archive-status');
  if (statusEl) statusEl.innerHTML = `<span style="color:var(--y)">⏳ Restoring ${filename}…</span>`;

  try {
    const data = await gdDownloadFile(fileId);
    if (!data.trades || !Array.isArray(data.trades)) {
      throw new Error('Invalid archive format');
    }

    // Merge restored trades (avoid duplicates)
    const existingIds = new Set(trades.map(t => t.id));
    let restored = 0;
    data.trades.forEach(t => {
      if (!existingIds.has(t.id)) {
        trades.push(t);
        restored++;
      }
    });

    // Restore ledger entries too
    if (data.ledger && Array.isArray(data.ledger)) {
      const ledger = JSON.parse(localStorage.getItem('te_ledger') || '[]');
      const existingLedgerIds = new Set(ledger.map(e => e.id));
      let ledgerRestored = 0;
      data.ledger.forEach(e => {
        if (!existingLedgerIds.has(e.id)) {
          ledger.push(e);
          ledgerRestored++;
        }
      });
      if (ledgerRestored) localStorage.setItem('te_ledger', JSON.stringify(ledger));
    }

    // Try to restore chart screenshots
    const month = data.month;
    if (month) {
      try {
        const chartFilename = `charts_${month}.json`;
        const q = `name='${chartFilename}' and '${_gdFolderId}' in parents and trashed=false`;
        const chartFiles = await gdApi('GET', `files?q=${encodeURIComponent(q)}&fields=files(id)&spaces=drive`);
        if (chartFiles.files?.length) {
          const chartData = await gdDownloadFile(chartFiles.files[0].id);
          if (chartData.charts) {
            chartData.charts.forEach(c => {
              const t = trades.find(x => x.id === c.tradeId);
              if (t) {
                if (c.entryImg) t.entryImg = c.entryImg;
                if (c.exitImg) t.exitImg = c.exitImg;
              }
            });
          }
        }
      } catch (e) { /* chart restore is best-effort */ }
    }

    save();
    renderDash();
    renderTrades();
    gdCheckStorageHealth();

    if (statusEl) statusEl.innerHTML = `<span style="color:var(--g)">✅ Restored ${restored} trades from ${filename}</span>`;
    toast(`📦 Restored ${restored} trades from ${filename}`, '✅');
  } catch (e) {
    if (statusEl) statusEl.innerHTML = `<span style="color:var(--r)">❌ ${e.message}</span>`;
    toast('Restore failed: ' + e.message, '❌');
  }
}

// ── Delete an archive from Drive ──
async function gdDeleteArchive(fileId, filename) {
  if (!confirm(`Delete ${filename} from Google Drive? This cannot be undone.`)) return;
  try {
    await gdDeleteFile(fileId);
    toast(`Deleted ${filename}`, '🗑');
    gdRefreshArchiveList();
  } catch (e) {
    toast('Delete failed: ' + e.message, '❌');
  }
}

// ══════════════════════════════════════════════════
// 4. ARCHIVE LIST UI
// ══════════════════════════════════════════════════

async function gdRefreshArchiveList() {
  const container = document.getElementById('gd-archive-list');
  if (!container) return;

  if (!_gdConnected) {
    container.innerHTML = '<div style="padding:20px;text-align:center;color:var(--t4);font-size:12px">Sign in to Google Drive to view archives</div>';
    return;
  }

  container.innerHTML = '<div style="padding:16px;text-align:center;color:var(--t4);font-size:12px">⏳ Loading archives…</div>';

  try {
    const files = await gdListArchives();
    gdRenderArchiveList(files);
  } catch (e) {
    container.innerHTML = `<div style="padding:16px;text-align:center;color:var(--r);font-size:12px">❌ ${e.message}</div>`;
  }
}

function gdRenderArchiveList(files) {
  const container = document.getElementById('gd-archive-list');
  if (!container) return;

  // Filter to only trade files (not chart files)
  const tradeFiles = (files || []).filter(f => f.name.startsWith('trades_'));
  const chartFiles = (files || []).filter(f => f.name.startsWith('charts_'));

  if (!tradeFiles.length) {
    container.innerHTML = `<div style="padding:20px;text-align:center;color:var(--t4);font-size:12px">
      <div style="font-size:24px;margin-bottom:6px">📦</div>
      No archives yet. Click 'Archive Now' to move older trades to Drive.
    </div>`;
    return;
  }

  // Total stats
  const totalFiles = tradeFiles.length;
  const totalSizeKB = tradeFiles.reduce((s, f) => s + (+f.size || 0) / 1024, 0);
  const chartSizeKB = chartFiles.reduce((s, f) => s + (+f.size || 0) / 1024, 0);

  let html = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;padding:0 4px">
    <span style="font-size:11px;color:var(--t3)">${totalFiles} archive${totalFiles !== 1 ? 's' : ''} · ${Math.round(totalSizeKB)} KB trades + ${Math.round(chartSizeKB)} KB charts</span>
  </div>`;

  html += tradeFiles.map(f => {
    const month = f.name.replace('trades_', '').replace('.json', '');
    const [y, m] = month.split('-');
    const label = new Date(+y, +m - 1).toLocaleDateString('en-IN', { month: 'long', year: 'numeric' });
    const sizeKB = Math.round((+f.size || 0) / 1024);
    const modified = f.modifiedTime ? new Date(f.modifiedTime).toLocaleDateString('en-IN', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' }) : '';
    const hasCharts = chartFiles.some(c => c.name === `charts_${month}.json`);
    const desc = f.description || '';
    const tradeCountMatch = desc.match(/(\d+)\s*trades/);
    const tradeCount = tradeCountMatch ? tradeCountMatch[1] : '?';

    return `<div style="display:flex;align-items:center;gap:10px;padding:10px 12px;background:var(--bg2);border:1px solid var(--b1);border-radius:8px;margin-bottom:4px">
      <div style="width:36px;height:36px;border-radius:8px;background:rgba(59,139,255,.12);display:flex;align-items:center;justify-content:center;font-size:16px;flex-shrink:0">📅</div>
      <div style="flex:1;min-width:0">
        <div style="font-size:13px;font-weight:700;color:var(--t1)">${label}</div>
        <div style="font-size:10px;color:var(--t3);margin-top:2px">
          ${tradeCount} trades · ${sizeKB} KB ${hasCharts ? '· 📸 charts' : ''} · ${modified}
        </div>
      </div>
      <div style="display:flex;gap:5px;flex-shrink:0">
        <button class="btn btn-ghost btn-xs" onclick="gdRestoreMonth('${f.id}','${f.name}')" title="Restore to active trades">📥 Restore</button>
        <button class="btn btn-ghost btn-xs" onclick="gdDeleteArchive('${f.id}','${f.name}')" title="Delete from Drive" style="color:var(--r)">🗑</button>
      </div>
    </div>`;
  }).join('');

  container.innerHTML = html;
}

// ══════════════════════════════════════════════════
// 5. AUTO-ARCHIVE (Purge & Preserve)
// ══════════════════════════════════════════════════

function gdStartAutoCheck() {
  if (_gdAutoTimer) clearInterval(_gdAutoTimer);
  if (!localStorage.getItem('gd_auto_archive')) return;

  _gdAutoTimer = setInterval(() => {
    if (!_gdConnected || !gdIsTokenValid()) return;

    const oldTrades = gdGetOldTrades();
    if (oldTrades.length >= 20) {
      // Auto-archive if 20+ old trades accumulate
      console.log('[GDrive] Auto-archive triggered: ' + oldTrades.length + ' old trades');
      gdArchiveNow();
    }
  }, GD_AUTO_CHECK_INTERVAL);
}

function gdToggleAutoArchive(on) {
  if (on) {
    localStorage.setItem('gd_auto_archive', '1');
    gdStartAutoCheck();
    toast('Auto-archive enabled — trades older than ' + GD_ARCHIVE_DAYS + ' days will be moved to Drive', '✅');
  } else {
    localStorage.removeItem('gd_auto_archive');
    if (_gdAutoTimer) clearInterval(_gdAutoTimer);
    _gdAutoTimer = null;
  }
}

// ══════════════════════════════════════════════════
// 6. UI HELPERS
// ══════════════════════════════════════════════════

function gdSetStatus(msg, state) {
  const dot = document.getElementById('gd-dot');
  const txt = document.getElementById('gd-status-txt');
  if (txt) txt.textContent = msg;
  if (dot) {
    dot.className = 'sync-dot';
    if (state === 'ok') dot.classList.add('connected');
    if (state === 'sync') dot.classList.add('syncing');
  }
}

function gdSaveClientId() {
  const val = (document.getElementById('gd-client-id')?.value || '').trim();
  if (!val) { toast('Enter your Google OAuth Client ID', '⚠️'); return; }
  localStorage.setItem('gd_client_id', val);
  _gdClientId = val;
  toast('Client ID saved ✅', '✅');
}

// ══════════════════════════════════════════════════
// 7. INIT ON LOAD
// ══════════════════════════════════════════════════

// Wait for DOM, then restore session
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => {
    setTimeout(gdRestoreSession, 1500);
    if (localStorage.getItem('gd_auto_archive')) gdStartAutoCheck();
  });
} else {
  setTimeout(gdRestoreSession, 1500);
  if (localStorage.getItem('gd_auto_archive')) gdStartAutoCheck();
}
