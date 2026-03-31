/**
 * TradEdge MATRIX — Stale Data Banner v1.1
 * ──────────────────────────────────────────
 * Drop-in script for matrix.html
 * 
 * How it works:
 *   1. Fetches rrm_data.json and reads metadata.date / metadata.generated_at
 *   2. If data is older than 20 hours → shows yellow warning bar
 *   3. If data is older than 48 hours → shows red critical bar
 *   4. Provides direct "Run Workflow" link to GitHub Actions
 *   5. Auto-dismisses after 60 seconds (or click ✕)
 *
 * Install:
 *   Add before </body> in matrix.html:
 *   <script src="scripts/matrix-stale-banner.js"></script>
 */

(function () {
  'use strict';

  var REPO = 'https://github.com/drrgware-web/tradedge-journal';
  var DATA_URL = './data/rrm_data.json';
  var STALE_HOURS = 20;   // Yellow warning threshold
  var CRITICAL_HOURS = 48; // Red critical threshold
  var AUTO_DISMISS_MS = 60000; // 60 seconds

  // Inject CSS
  var style = document.createElement('style');
  style.id = 'stale-banner-css';
  style.textContent = [
    '@keyframes stale-pulse{0%,100%{opacity:1}50%{opacity:.65}}',
    '@keyframes stale-slide{from{transform:translateY(-100%);opacity:0}to{transform:translateY(0);opacity:1}}',
    '#stale-banner{position:fixed;top:0;left:0;right:0;z-index:99999;',
    'font-family:"JetBrains Mono",monospace;font-size:11px;font-weight:600;',
    'display:flex;align-items:center;justify-content:center;gap:12px;',
    'padding:8px 16px;animation:stale-slide .3s ease-out;transition:opacity .4s}',
    '#stale-banner.warn{background:rgba(255,208,0,.95);color:#412402}',
    '#stale-banner.crit{background:rgba(255,51,102,.93);color:#fff;animation:stale-slide .3s ease-out,stale-pulse 2.5s ease-in-out infinite}',
    '#stale-banner a{color:inherit;text-decoration:underline;font-weight:700}',
    '#stale-banner button{background:none;border:none;color:inherit;cursor:pointer;font-size:16px;opacity:.6;padding:0 4px;font-family:inherit}',
    '#stale-banner button:hover{opacity:1}',
  ].join('\n');
  document.head.appendChild(style);

  function checkAndShow() {
    fetch(DATA_URL + '?_t=' + Date.now())
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var meta = data && data.metadata;
        if (!meta || !meta.date) return;

        // Determine data timestamp
        var dataTime;
        if (meta.generated_at) {
          dataTime = new Date(meta.generated_at);
        } else {
          // Fallback: assume 6 PM IST (12:30 UTC) on the metadata date
          dataTime = new Date(meta.date + 'T12:30:00Z');
        }

        var now = new Date();
        var diffMs = now - dataTime;
        var diffHours = diffMs / (1000 * 60 * 60);

        if (diffHours < STALE_HOURS) return; // Data is fresh

        var diffDays = Math.floor(diffHours / 24);
        var isCritical = diffHours >= CRITICAL_HOURS;

        // Build ago text
        var agoText;
        if (diffDays > 0) {
          agoText = diffDays + ' day' + (diffDays > 1 ? 's' : '') + ' ago';
        } else {
          var h = Math.floor(diffHours);
          agoText = h + ' hour' + (h !== 1 ? 's' : '') + ' ago';
        }

        // Determine cause
        var cause = '';
        var dayOfWeek = now.getDay(); // 0=Sun, 6=Sat
        if (diffDays <= 2 && (dayOfWeek === 0 || dayOfWeek === 6)) {
          cause = ' (weekend gap)';
        } else if (diffDays >= 3) {
          cause = ' — workflow may have stopped';
        }

        // Build banner
        var banner = document.createElement('div');
        banner.id = 'stale-banner';
        banner.className = isCritical ? 'crit' : 'warn';

        var icon = isCritical ? '🔴' : '⚠️';
        banner.innerHTML =
          '<span>' + icon + ' STALE DATA — ' + meta.date + ' (' + agoText + ')' + cause + '</span>' +
          '<a href="' + REPO + '/actions/workflows/rrm-daily.yml" target="_blank">Run Workflow</a>' +
          '<button onclick="this.parentElement.remove()" title="Dismiss">✕</button>';

        document.body.prepend(banner);

        // Adjust body padding so banner doesn't overlap content
        var bannerHeight = banner.offsetHeight;
        document.body.style.paddingTop = bannerHeight + 'px';

        // Auto-dismiss
        setTimeout(function () {
          var el = document.getElementById('stale-banner');
          if (el) {
            el.style.opacity = '0';
            setTimeout(function () {
              if (el.parentElement) el.remove();
              document.body.style.paddingTop = '';
            }, 400);
          }
        }, AUTO_DISMISS_MS);
      })
      .catch(function () {
        // Silently fail — don't break the dashboard
      });
  }

  // Run after page loads
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      setTimeout(checkAndShow, 1500);
    });
  } else {
    setTimeout(checkAndShow, 1500);
  }
})();
