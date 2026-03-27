// ═══════════════════════════════════════════════════════════════
// TradEdge Sidebar + Topbar — js/sidebar.js
// Injects the sidebar and topbar into any page
// ═══════════════════════════════════════════════════════════════

'use strict';

TE.injectShell = function() {
  // ── SIDEBAR ──
  const sidebarHTML = `
  <nav class="sidebar">
    <div class="sb-item" id="nav-dashboard" data-tip="Dashboard" onclick="nav('dashboard')">
      <svg viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>
    </div>
    <div class="sb-item" id="nav-trades" data-tip="Trade Log" onclick="nav('trades')">
      <svg viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="9" y1="15" x2="15" y2="15"/></svg>
    </div>
    <div class="sb-item" id="nav-analytics" data-tip="Analytics" onclick="nav('analytics')">
      <svg viewBox="0 0 24 24"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>
    </div>
    <div class="sb-item" id="nav-calendar" data-tip="Calendar" onclick="nav('calendar')">
      <svg viewBox="0 0 24 24"><rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>
    </div>
    <div class="sb-item" id="nav-fund" data-tip="Fund Mgmt" onclick="nav('fund')">
      <svg viewBox="0 0 24 24"><text x="12" y="17" text-anchor="middle" font-size="16" font-weight="700" fill="currentColor" stroke="none">₹</text></svg>
    </div>
    <div class="sb-item" id="nav-execute" data-tip="Execution" onclick="nav('execute')">
      <svg viewBox="0 0 24 24"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
    </div>
    <div class="sb-item" id="nav-rrm" data-tip="RRM Intel" onclick="nav('rrm')">
      <svg viewBox="0 0 24 24"><path d="M3 3v18h18"/><path d="M7 16l4-4 4 4 4-6"/></svg>
    </div>
    <div class="sb-item" id="nav-ai" data-tip="AI Review" onclick="nav('ai')">
      <svg viewBox="0 0 24 24"><path d="M12 2a10 10 0 110 20A10 10 0 0112 2z"/><path d="M8 12h.01M12 12h.01M16 12h.01" stroke-width="2.5"/></svg>
    </div>
    <div class="sb-item" id="nav-news" data-tip="News" onclick="nav('news')">
      <svg viewBox="0 0 24 24"><path d="M19 20H5a2 2 0 01-2-2V6a2 2 0 012-2h10a2 2 0 012 2v1m2 13a2 2 0 01-2-2V7m2 13a2 2 0 002-2V9a2 2 0 00-2-2h-2"/></svg>
    </div>
    <div class="sb-item" id="nav-alerts" data-tip="Alerts" onclick="nav('alerts')">
      <svg viewBox="0 0 24 24"><path d="M18 8A6 6 0 006 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 01-3.46 0"/></svg>
    </div>
    <div class="sb-item" id="nav-possize" data-tip="Pos Size" onclick="nav('possize')">
      <svg viewBox="0 0 24 24"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>
    </div>
    <div class="sb-item" id="nav-ledger" data-tip="Ledger" onclick="nav('ledger')">
      <svg viewBox="0 0 24 24"><rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/></svg>
    </div>
    <div class="sb-spacer"></div>
    <div class="sb-item" id="nav-settings" data-tip="Settings" onclick="nav('settings')">
      <svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z"/></svg>
    </div>
  </nav>`;

  // ── TOPBAR ──
  const pageName = TE.PAGE_NAMES[TE.currentPage] || 'TradEdge';
  const topbarHTML = `
  <div class="topbar">
    <div class="topbar-title">${pageName}</div>
    <div style="flex:1"></div>
    <div class="market-chip" id="mkt-chip">NSE • —</div>
    <div class="theme-toggle" id="theme-btn" onclick="toggleTheme()" title="Toggle theme">🌙</div>
    <button class="btn btn-y btn-sm" onclick="TE.openAddTrade ? TE.openAddTrade() : nav('trades')">
      <svg viewBox="0 0 24 24"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
      Add Trade
    </button>
  </div>`;

  // ── INJECT ──
  const body = document.body;

  // Create wrapper structure: sidebar + main(topbar + content)
  const existingContent = body.innerHTML;

  body.innerHTML = `
    ${sidebarHTML}
    <div class="main">
      ${topbarHTML}
      <div class="content" id="content">
        ${existingContent}
      </div>
    </div>
    <div class="toast" id="toast"><span id="t-icon">✅</span><span id="t-msg">Saved</span></div>
  `;

  // Highlight active sidebar item
  const activeNav = document.getElementById('nav-' + TE.currentPage);
  if (activeNav) activeNav.classList.add('active');

  // Start market clock
  TE.tickClock();
};

// Auto-inject on DOMContentLoaded
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', TE.injectShell);
} else {
  TE.injectShell();
}
