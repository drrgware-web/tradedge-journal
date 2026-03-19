/**
 * TradEdge — Global Stock Click Handler v2.0 (TradingView Pro/Premium)
 * =====================================================================
 * Drop into ANY TradEdge page for instant click-to-chart on stock symbols.
 * 
 * SUPPORTS TRADINGVIEW PRO/PRO+/PREMIUM:
 *   - Opens charts in your logged-in TradingView session
 *   - Full access to premium indicators, multi-chart, alerts, replay
 *   - Custom chart layouts preserved via your TV account
 * 
 * THREE CHART MODES (user picks default, can switch anytime):
 *   1. POPUP  — Floating embedded TV widget (quick glance, basic features)
 *   2. TV TAB — Opens tradingview.com/chart in new tab (FULL Pro features)
 *   3. FULL   — Opens stock.html drilldown (O'Neil, P&F, Renko, fundamentals)
 *
 * INTERACTIONS:
 *   Click        → Default mode (configurable)
 *   Shift+Click  → TV Tab (full Pro features)
 *   Ctrl+Click   → Stock drilldown (stock.html)
 *   Double-Click  → TV Tab (full Pro features)
 *   Escape       → Close popup
 *   Right-Click  → Context menu with all 3 options
 *
 * USAGE:
 *   <script src="scripts/tradedge-stock-popup.js"></script>
 *
 *   <!-- Optional: configure default mode -->
 *   <script>
 *     window.TRADEDGE_CHART_CONFIG = {
 *       defaultMode: 'tvtab',       // 'popup' | 'tvtab' | 'full'
 *       tvChartUrl: 'https://www.tradingview.com/chart/',
 *       tvInterval: 'D',            // Default timeframe
 *       tvStudies: ['RSI@tv-basicstudies','MACD@tv-basicstudies','MAExp@tv-basicstudies'],
 *       tvLayout: '',               // Your saved layout ID (from TV URL)
 *       stockPageUrl: 'stock.html',
 *       showContextMenu: true,
 *       popupWidth: 700,
 *       popupHeight: 500,
 *     };
 *   </script>
 *
 * MAKE ELEMENTS CLICKABLE:
 *   <span data-symbol="RELIANCE">RELIANCE</span>
 *   <td class="stock-link" data-symbol="TCS">TCS</td>
 *   <div class="te-auto-detect">INFY reported strong Q4...</div>
 */

(function() {
  'use strict';

  // ── Config (merge user overrides) ──
  const CFG = Object.assign({
    defaultMode: 'popup',             // 'popup' | 'tvtab' | 'full'
    tvChartUrl: 'https://in.tradingview.com/chart/',
    tvInterval: 'D',
    tvStudies: ['RSI@tv-basicstudies', 'MACD@tv-basicstudies'],
    tvLayout: '8RFOgQZr',            // Rahul's saved Pro layout
    stockPageUrl: 'stock.html',
    definedgeUrl: 'https://zone.definedge.com',
    showContextMenu: true,
    popupWidth: 700,
    popupHeight: 500,
    theme: 'dark',
  }, window.TRADEDGE_CHART_CONFIG || {});

  // Try to load saved preference
  try {
    const saved = localStorage.getItem('te_chart_mode');
    if (saved && ['popup', 'tvtab', 'full'].includes(saved)) {
      CFG.defaultMode = saved;
    }
  } catch(e) {}

  // ── State ──
  let activePanel = null;
  let contextMenu = null;
  let dragState = null;
  let lastPos = { x: null, y: null };

  // ── Inject Styles ──
  const STYLES = `
    .te-chart-panel {
      position: fixed; z-index: 10000;
      background: #0c1018;
      border: 1px solid rgba(0,255,136,0.25);
      border-radius: 10px;
      box-shadow: 0 20px 60px rgba(0,0,0,0.8), 0 0 30px rgba(0,255,136,0.05);
      overflow: hidden; resize: both;
      min-width: 420px; min-height: 320px;
      animation: te-in 0.2s ease-out;
    }
    .te-chart-panel.closing { animation: te-out 0.15s ease-in forwards; }
    @keyframes te-in { from { opacity:0; transform:scale(0.95) translateY(10px); } to { opacity:1; transform:scale(1); } }
    @keyframes te-out { from { opacity:1; } to { opacity:0; transform:scale(0.95) translateY(10px); } }

    .te-hdr {
      display:flex; align-items:center; justify-content:space-between;
      padding:6px 12px; background:#080c12;
      border-bottom:1px solid rgba(0,255,136,0.1);
      cursor:move; user-select:none;
    }
    .te-hdr-sym {
      font-family:'Outfit','JetBrains Mono',monospace;
      font-weight:800; font-size:14px; color:#fff; letter-spacing:1px;
    }
    .te-hdr-tag {
      font-family:'JetBrains Mono',monospace;
      font-size:9px; color:#556; margin-left:6px;
    }
    .te-hdr-actions { display:flex; gap:3px; align-items:center; }

    .te-btn {
      padding:3px 8px; border-radius:3px; font-size:9px; font-weight:600;
      font-family:'JetBrains Mono',monospace; cursor:pointer;
      border:1px solid rgba(255,255,255,0.08); background:transparent;
      color:#889; transition:all 0.12s; white-space:nowrap;
    }
    .te-btn:hover { border-color:rgba(0,255,136,0.3); color:#00ff88; background:rgba(0,255,136,0.05); }
    .te-btn-active { border-color:#00ff88; color:#00ff88; background:rgba(0,255,136,0.08); }
    .te-btn-tv { border-color:rgba(0,200,255,0.2); color:#00c8ff; }
    .te-btn-tv:hover { border-color:#00c8ff; background:rgba(0,200,255,0.08); }
    .te-btn-full { border-color:rgba(136,68,255,0.2); color:#8844ff; }
    .te-btn-full:hover { border-color:#8844ff; background:rgba(136,68,255,0.08); }

    .te-close {
      width:24px; height:24px; border-radius:3px;
      border:1px solid rgba(255,255,255,0.06); background:transparent;
      color:#445; font-size:13px; cursor:pointer;
      display:flex; align-items:center; justify-content:center; transition:all 0.12s;
    }
    .te-close:hover { background:rgba(255,68,85,0.12); border-color:rgba(255,68,85,0.25); color:#ff4455; }

    .te-body { width:100%; height:calc(100% - 38px); }
    .te-body iframe { width:100%; height:100%; border:none; }

    /* Context menu */
    .te-ctx {
      position:fixed; z-index:10001;
      background:#0c1018; border:1px solid rgba(0,255,136,0.2);
      border-radius:6px; padding:4px 0; min-width:200px;
      box-shadow:0 10px 40px rgba(0,0,0,0.7);
      animation:te-in 0.12s ease-out;
    }
    .te-ctx-item {
      padding:7px 14px; font-size:11px; font-family:'JetBrains Mono',monospace;
      color:#ccc; cursor:pointer; display:flex; align-items:center; gap:8px;
      transition:background 0.1s;
    }
    .te-ctx-item:hover { background:rgba(0,255,136,0.06); color:#00ff88; }
    .te-ctx-item .te-ctx-key {
      margin-left:auto; font-size:9px; color:#445;
      background:rgba(255,255,255,0.04); padding:1px 5px; border-radius:2px;
    }
    .te-ctx-sep { height:1px; background:rgba(0,255,136,0.06); margin:3px 8px; }

    .te-mode-bar {
      display:flex; gap:2px; padding:4px 12px; background:#080c12;
      border-bottom:1px solid rgba(0,255,136,0.06);
    }
    .te-mode-btn {
      padding:2px 8px; border-radius:3px; font-size:8px; font-weight:700;
      font-family:'JetBrains Mono',monospace; cursor:pointer;
      border:none; background:transparent; color:#445; transition:all 0.12s;
      letter-spacing:0.5px;
    }
    .te-mode-btn:hover { color:#889; }
    .te-mode-btn.active { color:#00ff88; background:rgba(0,255,136,0.08); }

    /* Clickable symbols */
    [data-symbol], .stock-link { cursor:pointer; transition:color 0.12s; }
    [data-symbol]:hover, .stock-link:hover {
      color:#00ff88 !important;
      text-decoration:underline;
      text-decoration-color:rgba(0,255,136,0.3);
      text-underline-offset:2px;
    }
  `;
  document.head.appendChild(Object.assign(document.createElement('style'), { textContent: STYLES }));


  // ═══════════════════════════════════════════════════════
  // CHART OPENERS
  // ═══════════════════════════════════════════════════════

  // Keep a reference to the TV tab so we can reuse it
  let tvTabRef = null;

  function openTVTab(symbol) {
    /**
     * Opens TradingView with the correct symbol. Two strategies:
     * 
     * Strategy 1 (tvMode: 'layout'): Opens your saved layout URL with ?symbol= param.
     *   - TV *sometimes* respects ?symbol= on saved layouts
     *   - Your indicators/drawings are preserved
     *   - Reuses the same tab (so you can keep switching symbols from TradEdge)
     * 
     * Strategy 2 (tvMode: 'fresh'): Opens a fresh chart URL (no layout ID).
     *   - Symbol is GUARANTEED to be correct
     *   - Uses your default TV indicators (from your account settings)
     *   - Each click opens a new tab
     * 
     * Recommended: Use 'layout' mode. If TV doesn't switch symbol, 
     * just type the symbol in TV's search bar — your layout stays.
     */
    const mode = CFG.tvMode || 'layout';  // 'layout' | 'fresh'

    if (mode === 'layout' && CFG.tvLayout) {
      // Reuse same tab — TV loads your layout and *should* switch symbol
      const url = `${CFG.tvChartUrl}${CFG.tvLayout}/?symbol=NSE%3A${symbol}&interval=${CFG.tvInterval}`;
      
      // Reuse tab if still open, otherwise open new
      if (tvTabRef && !tvTabRef.closed) {
        tvTabRef.location.href = url;
        tvTabRef.focus();
      } else {
        tvTabRef = window.open(url, 'tradedge_tv');
      }
    } else {
      // Fresh chart — guaranteed correct symbol, default TV layout
      const url = `${CFG.tvChartUrl}?symbol=NSE%3A${symbol}&interval=${CFG.tvInterval}`;
      window.open(url, '_blank');
    }
  }

  function openTVFresh(symbol, interval) {
    /**
     * Always opens a NEW fresh TV chart with guaranteed correct symbol.
     * No layout ID — uses your TV account defaults.
     */
    const tf = interval || CFG.tvInterval;
    window.open(`${CFG.tvChartUrl}?symbol=NSE%3A${symbol}&interval=${tf}`, '_blank');
  }

  function openDefinedge(symbol, chartType) {
    /**
     * Opens Definedge Zone/RZone chart — P&F, Renko, RS charts etc.
     * Since you're logged into Definedge in your browser,
     * it opens with your full RZone subscription features.
     * 
     * chartType: 'pf', 'renko', 'candle', 'rspf', 'heikinashi', 'matrix'
     */
    const base = CFG.definedgeUrl || 'https://zone.definedge.com';
    const typeMap = {
      'candle': `${base}/chart/NSE/${symbol}`,
      'pf':     `${base}/chart/NSE/${symbol}/pf`,
      'renko':  `${base}/chart/NSE/${symbol}/renko`,
      'rspf':   `${base}/chart/NSE/${symbol}/rspf`,
      'ha':     `${base}/chart/NSE/${symbol}/ha`,
      'matrix': `${base}/rzone/matrix`,
    };
    window.open(typeMap[chartType || 'pf'] || typeMap['pf'], '_blank');
  }

  function openStockPage(symbol) {
    window.open(`${CFG.stockPageUrl}?s=${symbol}`, '_blank');
  }

  function openPopup(symbol) {
    closePanel();

    const panel = document.createElement('div');
    panel.className = 'te-chart-panel';

    const x = lastPos.x ?? (window.innerWidth - CFG.popupWidth) / 2;
    const y = lastPos.y ?? Math.max(50, (window.innerHeight - CFG.popupHeight) / 2 - 40);
    Object.assign(panel.style, {
      left: x + 'px', top: y + 'px',
      width: CFG.popupWidth + 'px', height: CFG.popupHeight + 'px',
    });

    // Header
    const hdr = document.createElement('div');
    hdr.className = 'te-hdr';
    hdr.innerHTML = `
      <div>
        <span class="te-hdr-sym">◈ ${symbol}</span>
        <span class="te-hdr-tag">NSE</span>
      </div>
      <div class="te-hdr-actions">
        <button class="te-btn te-btn-tv" data-action="tvtab" title="Open in TradingView (Pro features)">
          ⭐ TV Pro →
        </button>
        <button class="te-btn te-btn-full" data-action="full" title="Full stock analysis">
          Analysis →
        </button>
        <button class="te-close" data-action="close" title="Close (Esc)">✕</button>
      </div>
    `;
    panel.appendChild(hdr);

    // Mode switcher bar
    const modeBar = document.createElement('div');
    modeBar.className = 'te-mode-bar';
    modeBar.innerHTML = `
      <button class="te-mode-btn active" data-mode="embed">EMBEDDED CHART</button>
      <button class="te-mode-btn" data-mode="tvfull">↗ OPEN TV PRO (FULL FEATURES)</button>
      <button class="te-mode-btn" data-mode="drilldown">📊 STOCK DRILLDOWN</button>
      <span style="margin-left:auto;font-size:8px;color:#334;font-family:'JetBrains Mono',monospace">
        DEFAULT: <select id="te-default-mode" style="background:#111;border:1px solid #222;color:#00ff88;font-size:8px;font-family:inherit;border-radius:2px;padding:0 4px">
          <option value="popup" ${CFG.defaultMode==='popup'?'selected':''}>Popup</option>
          <option value="tvtab" ${CFG.defaultMode==='tvtab'?'selected':''}>TV Tab</option>
          <option value="full" ${CFG.defaultMode==='full'?'selected':''}>Full Analysis</option>
        </select>
      </span>
    `;
    panel.appendChild(modeBar);

    // Chart body
    const body = document.createElement('div');
    body.className = 'te-body';
    body.style.height = 'calc(100% - 62px)';

    const iframe = document.createElement('iframe');
    iframe.srcdoc = `<!DOCTYPE html><html><head><style>
      body{margin:0;background:#0c1018;overflow:hidden}
    </style></head><body>
      <div class="tradingview-widget-container" style="height:100%;width:100%">
        <div class="tradingview-widget-container__widget" style="height:100%;width:100%"></div>
        <script src="https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js">
        {
          "autosize": true,
          "symbol": "NSE:${symbol}",
          "interval": "${CFG.tvInterval}",
          "timezone": "Asia/Kolkata",
          "theme": "${CFG.theme}",
          "style": "1",
          "locale": "en",
          "backgroundColor": "rgba(12,16,24,1)",
          "gridColor": "rgba(0,255,136,0.03)",
          "allow_symbol_change": true,
          "hide_top_toolbar": false,
          "hide_legend": false,
          "save_image": true,
          "calendar": false,
          "studies": ${JSON.stringify(CFG.tvStudies)},
          "support_host": "https://www.tradingview.com"
        }
        <\/script>
      </div>
    </body></html>`;
    body.appendChild(iframe);
    panel.appendChild(body);

    // Event handlers
    panel.addEventListener('click', (e) => {
      const action = e.target.closest('[data-action]')?.dataset.action;
      const mode = e.target.closest('[data-mode]')?.dataset.mode;

      if (action === 'tvtab') openTVTab(symbol);
      else if (action === 'full') openStockPage(symbol);
      else if (action === 'close') closePanel();

      if (mode === 'tvfull') openTVTab(symbol);
      else if (mode === 'drilldown') openStockPage(symbol);
    });

    // Default mode save
    panel.addEventListener('change', (e) => {
      if (e.target.id === 'te-default-mode') {
        CFG.defaultMode = e.target.value;
        try { localStorage.setItem('te_chart_mode', e.target.value); } catch(ex) {}
      }
    });

    // Drag
    hdr.addEventListener('mousedown', (e) => {
      if (e.target.tagName === 'BUTTON' || e.target.tagName === 'SELECT') return;
      dragState = { sx: e.clientX - panel.offsetLeft, sy: e.clientY - panel.offsetTop };
      const onMove = (ev) => {
        const nx = Math.max(0, ev.clientX - dragState.sx);
        const ny = Math.max(0, ev.clientY - dragState.sy);
        panel.style.left = nx + 'px'; panel.style.top = ny + 'px';
        lastPos = { x: nx, y: ny };
      };
      const onUp = () => {
        dragState = null;
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
      };
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });

    document.body.appendChild(panel);
    activePanel = panel;
  }


  // ═══════════════════════════════════════════════════════
  // CONTEXT MENU
  // ═══════════════════════════════════════════════════════

  function showContextMenu(symbol, x, y) {
    closeContextMenu();

    const menu = document.createElement('div');
    menu.className = 'te-ctx';
    menu.style.left = Math.min(x, window.innerWidth - 220) + 'px';
    menu.style.top = Math.min(y, window.innerHeight - 200) + 'px';

    menu.innerHTML = `
      <div class="te-ctx-item" data-action="popup">
        📊 Quick Chart (Popup)
        <span class="te-ctx-key">Click</span>
      </div>
      <div class="te-ctx-item" data-action="tvtab">
        ⭐ TradingView Pro (Full Features)
        <span class="te-ctx-key">Shift+Click</span>
      </div>
      <div class="te-ctx-item" data-action="full">
        📋 Stock Analysis (O'Neil, P&F, Renko)
        <span class="te-ctx-key">Ctrl+Click</span>
      </div>
      <div class="te-ctx-sep"></div>
      <div class="te-ctx-item" data-action="de-pf">🔷 Definedge — Point & Figure</div>
      <div class="te-ctx-item" data-action="de-renko">🔷 Definedge — Renko</div>
      <div class="te-ctx-item" data-action="de-rspf">🔷 Definedge — RS P&F (Relative Strength)</div>
      <div class="te-ctx-item" data-action="de-candle">🔷 Definedge — Candlestick</div>
      <div class="te-ctx-sep"></div>
      <div class="te-ctx-item" data-action="tv-1h">📈 TV Fresh — 1 Hour</div>
      <div class="te-ctx-item" data-action="tv-15m">📈 TV Fresh — 15 Min</div>
      <div class="te-ctx-item" data-action="tv-w">📈 TV Fresh — Weekly</div>
      <div class="te-ctx-sep"></div>
      <div class="te-ctx-item" data-action="scanner">🔍 Find in Scanner</div>
    `;

    menu.addEventListener('click', (e) => {
      const action = e.target.closest('.te-ctx-item')?.dataset.action;
      if (!action) return;

      closeContextMenu();

      if (action === 'popup') openPopup(symbol);
      else if (action === 'tvtab') openTVTab(symbol);
      else if (action === 'full') openStockPage(symbol);
      else if (action === 'de-pf') openDefinedge(symbol, 'pf');
      else if (action === 'de-renko') openDefinedge(symbol, 'renko');
      else if (action === 'de-rspf') openDefinedge(symbol, 'rspf');
      else if (action === 'de-candle') openDefinedge(symbol, 'candle');
      else if (action === 'tv-1h') openTVFresh(symbol, '60');
      else if (action === 'tv-15m') openTVFresh(symbol, '15');
      else if (action === 'tv-w') openTVFresh(symbol, 'W');
      else if (action === 'scanner') {
        window.open(`scanner.html?search=${symbol}`, '_blank');
      }
    });

    document.body.appendChild(menu);
    contextMenu = menu;

    // Close on outside click
    setTimeout(() => {
      document.addEventListener('click', closeContextMenu, { once: true });
    }, 10);
  }

  function closeContextMenu() {
    if (contextMenu) { contextMenu.remove(); contextMenu = null; }
  }

  function closePanel() {
    if (activePanel) {
      activePanel.classList.add('closing');
      const p = activePanel;
      setTimeout(() => p.remove(), 150);
      activePanel = null;
    }
  }

  window._teClosePanel = closePanel;


  // ═══════════════════════════════════════════════════════
  // EVENT HANDLERS
  // ═══════════════════════════════════════════════════════

  function getSymbol(target) {
    const el = target.closest('[data-symbol]') || target.closest('.stock-link');
    if (!el) return null;
    const sym = el.getAttribute('data-symbol') ||
                el.textContent.trim().split(/\s/)[0].replace(/[^A-Z0-9&-]/gi, '');
    return (sym && sym.length <= 20) ? sym.toUpperCase() : null;
  }

  // Click — default mode (or shift/ctrl override)
  document.addEventListener('click', (e) => {
    const sym = getSymbol(e.target);
    if (!sym) return;
    e.preventDefault();
    e.stopPropagation();

    if (e.shiftKey) {
      openTVTab(sym);                    // Shift+Click → TV Pro tab
    } else if (e.ctrlKey || e.metaKey) {
      openStockPage(sym);               // Ctrl+Click → stock.html
    } else {
      // Default mode
      if (CFG.defaultMode === 'tvtab') openTVTab(sym);
      else if (CFG.defaultMode === 'full') openStockPage(sym);
      else openPopup(sym);              // Default: popup
    }
  });

  // Double-click → always TV Pro tab
  document.addEventListener('dblclick', (e) => {
    const sym = getSymbol(e.target);
    if (!sym) return;
    e.preventDefault();
    openTVTab(sym);
  });

  // Right-click context menu
  if (CFG.showContextMenu) {
    document.addEventListener('contextmenu', (e) => {
      const sym = getSymbol(e.target);
      if (!sym) return;
      e.preventDefault();
      showContextMenu(sym, e.clientX, e.clientY);
    });
  }

  // Escape → close
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') { closePanel(); closeContextMenu(); }
  });


  // ═══════════════════════════════════════════════════════
  // AUTO-DETECT SYMBOLS
  // ═══════════════════════════════════════════════════════

  function autoDetectSymbols() {
    document.querySelectorAll('.te-auto-detect').forEach(container => {
      const walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT);
      const nodes = [];
      while (walker.nextNode()) nodes.push(walker.currentNode);

      const re = /\b([A-Z][A-Z0-9&-]{1,19})\b/g;
      nodes.forEach(node => {
        const text = node.textContent;
        if (!re.test(text)) return;
        re.lastIndex = 0;

        const frag = document.createDocumentFragment();
        let last = 0, match;
        while ((match = re.exec(text)) !== null) {
          if (match.index > last) frag.appendChild(document.createTextNode(text.slice(last, match.index)));
          const span = document.createElement('span');
          span.setAttribute('data-symbol', match[1]);
          span.textContent = match[1];
          frag.appendChild(span);
          last = match.index + match[0].length;
        }
        if (last < text.length) frag.appendChild(document.createTextNode(text.slice(last)));
        if (last > 0) node.parentNode.replaceChild(frag, node);
      });
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', autoDetectSymbols);
  else autoDetectSymbols();

  console.log('✅ TradEdge Stock Handler v2.0 loaded | Default:', CFG.defaultMode, '| TV Pro: enabled');
})();
