// ═══════════════════════════════════════════════════════════════
// TradEdge UI — js/ui.js
// Sidebar, topbar, toast, modals, theme, market clock
// ═══════════════════════════════════════════════════════════════

'use strict';

// ── Page Navigation ──────────────────────────────────────────
// In modular mode, each page is its own HTML file.
// nav() redirects to the appropriate page.
TE.PAGE_MAP = {
  dashboard:  'index.html',
  trades:     'trades.html',
  analytics:  'analytics.html',
  calendar:   'calendar.html',
  fund:       'fund.html',
  ai:         'ai.html',
  news:       'news.html',
  alerts:     'alerts.html',
  ledger:     'ledger.html',
  possize:    'possize.html',
  execute:    'execution.html',
  rrm:        'rrm-intel.html',
  mps:        'mps.html',
  edgepilot:  'autopilot.html',
  settings:   'settings.html',
};

TE.PAGE_NAMES = {
  dashboard:'Dashboard', trades:'Trade Log', analytics:'Analytics',
  calendar:'Calendar', fund:'Fund Management', ai:'AI Trade Review',
  edgepilot:'Edge Pilot',
  news:'News Intelligence', alerts:'Price Alerts', ledger:'Broker Ledger',
  possize:'Position Sizing', execute:'Trade Execution',
  rrm:'RRM Intelligence', mps:'MPS Dashboard', settings:'Settings & Sync'
};

// Detect current page from URL
TE.currentPage = (function() {
  const path = location.pathname.split('/').pop() || 'index.html';
  for (const [key, file] of Object.entries(TE.PAGE_MAP)) {
    if (path === file) return key;
  }
  return 'dashboard';
})();

TE.nav = function(page) {
  const file = TE.PAGE_MAP[page];
  if (!file) return;
  // If we're already on this page, don't navigate
  if (TE.currentPage === page) return;
  window.location.href = file;
};

// ── Toast ────────────────────────────────────────────────────
TE.toast = function(msg, icon = '✅') {
  const el = document.getElementById('toast');
  if (!el) {
    // Create toast element if it doesn't exist
    const t = document.createElement('div');
    t.id = 'toast';
    t.className = 'toast';
    t.innerHTML = '<span id="t-icon">✅</span><span id="t-msg">Saved</span>';
    document.body.appendChild(t);
  }
  const toastEl = document.getElementById('toast');
  document.getElementById('t-msg').textContent = msg;
  document.getElementById('t-icon').textContent = icon;
  toastEl.classList.add('show');
  clearTimeout(toastEl._timer);
  toastEl._timer = setTimeout(() => toastEl.classList.remove('show'), 3000);
};

// ── Modals ───────────────────────────────────────────────────
TE.openModal = function(id) {
  const el = document.getElementById(id);
  if (el) el.classList.add('open');
};

TE.closeModal = function(id) {
  const el = document.getElementById(id);
  if (el) el.classList.remove('open');
};

// Global alias for backward compatibility
window.openM  = TE.openModal;
window.closeM = TE.closeModal;
window.toast  = TE.toast;
window.nav    = TE.nav;

// ── Theme Toggle ─────────────────────────────────────────────
TE.applyTheme = function(t, save = true) {
  const btn = document.getElementById('theme-btn');
  if (t === 'light') {
    document.body.classList.add('light');
    if (btn) btn.textContent = '☀️';
  } else {
    document.body.classList.remove('light');
    if (btn) btn.textContent = '🌙';
  }
  if (save) localStorage.setItem('te_theme', t);
};

TE.toggleTheme = function() {
  const isLight = document.body.classList.contains('light');
  TE.applyTheme(isLight ? 'dark' : 'light');
};

window.toggleTheme = TE.toggleTheme;

// Init theme from localStorage
(function() {
  TE.applyTheme(localStorage.getItem('te_theme') || 'dark', false);
})();

// ── Market Clock ─────────────────────────────────────────────
TE.tickClock = function() {
  const el = document.getElementById('mkt-chip');
  if (!el) return;
  const ist = TE.getIST();
  const open = TE.isMarketOpen();
  el.textContent = `NSE • ${open ? '🟢 LIVE' : '🔴 CLOSED'} • ${ist.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' })}`;
};

// Start clock
TE.tickClock();
setInterval(TE.tickClock, 30000);

// ── Sidebar Active State ─────────────────────────────────────
(function highlightSidebar() {
  const activeNav = document.getElementById('nav-' + TE.currentPage);
  if (activeNav) activeNav.classList.add('active');
})();

// ── Info Tooltip System ──────────────────────────────────────
(function initInfoTips() {
  if (document.getElementById('info-tip')) return;
  const tip = document.createElement('div');
  tip.id = 'info-tip';
  tip.className = 'info-tip';
  tip.innerHTML = '<div class="info-tip-title" id="ititle"></div><div class="info-tip-body" id="ibody"></div><div class="info-tip-good" id="igood"></div>';
  document.body.appendChild(tip);

  window.showInfoTip = function(el, title, body, good) {
    document.getElementById('ititle').textContent = title;
    document.getElementById('ibody').textContent  = body;
    const g = document.getElementById('igood');
    g.textContent = good || '';
    g.style.display = good ? '' : 'none';
    const r = el.getBoundingClientRect();
    let left = r.left + r.width / 2 - 130;
    let top  = r.top - 90 - 10;
    if (top < 8) top = r.bottom + 8;
    if (left < 8) left = 8;
    if (left + 260 > window.innerWidth - 8) left = window.innerWidth - 268;
    tip.style.left = left + 'px';
    tip.style.top  = top + 'px';
    tip.style.width = '260px';
    tip.classList.add('show');
  };

  window.hideInfoTip = function() { tip.classList.remove('show'); };
  document.addEventListener('scroll', hideInfoTip, true);
  document.addEventListener('click', hideInfoTip, true);
})();

// ── KPI Info Map ─────────────────────────────────────────────
TE.kpiInfoMap = {
  'Win Rate':        ['Win Rate', 'Winners ÷ Total × 100', '> 50% is good'],
  'Profit Factor':   ['Profit Factor', 'Gross profit ÷ gross loss', '> 1.5 good, > 2.0 excellent'],
  'Expectancy':      ['Expectancy', '(WR × Avg Win) + (LR × Avg Loss)', 'Positive = edge'],
  'Avg R-Multiple':  ['Avg R-Multiple', 'Avg return in risk units', '> 0.5R is good'],
  'Max Drawdown':    ['Max Drawdown', 'Largest peak-to-trough drop', 'Keep below 20%'],
  'Sharpe Ratio':    ['Sharpe Ratio', 'Avg return ÷ std dev × √252', '> 1.0 good, > 2.0 excellent'],
  'Total P&L':       ['Total P&L', 'Realised + unrealised', 'Positive = profitable'],
  'Unrealised P&L':  ['Unrealised P&L', 'Open positions mark-to-market', 'Changes with CMP'],
  'Capital':         ['Trading Capital', 'Total allocated capital', 'Set in Settings'],
};

console.log(`[TE UI] Page: ${TE.currentPage}`);
