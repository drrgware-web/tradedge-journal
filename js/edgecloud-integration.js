// ═══════════════════════════════════════════════════════════════════════════
// EdgeCloud Integration Patch for execution.html
// ═══════════════════════════════════════════════════════════════════════════
//
// This file documents the exact changes needed to wire edgecloud.js into
// execution.html. Apply these changes in order.
//
// STEP 1: Add script tag (after js/execution.js, before js/ui.js)
// STEP 2: Add EdgeCloud fetch to buildMergedPositions (tech data section)
// STEP 3: Add EdgeCloud card section to renderPositions
// STEP 4: Add EdgeCloud alerts to refreshAll cycle
// STEP 5: Add EdgeCloud TSL option to auto-trail system
// ═══════════════════════════════════════════════════════════════════════════


// ═══ STEP 1: Script Tag ═══
// In execution.html, find these lines (~line 770-777):
//
//   <script src="js/execution.js"></script>
//   <script src="js/ui.js"></script>
//
// Add between them:
//
//   <script src="js/edgecloud.js"></script>


// ═══ STEP 2: EdgeCloud Fetch in buildMergedPositions ═══
// After the ATR/EMA/DMA hourly fetch block (around line 2700),
// add EdgeCloud computation. This runs on the same hourly cycle.
//
// Find this line:
//   // Re-read trades after updates
//   const updatedTrades = JSON.parse(localStorage.getItem('te_trades') || '[]');
//
// BEFORE that line, insert:

/* --- BEGIN EDGECLOUD FETCH PATCH --- */

// EdgeCloud computation — runs alongside ATR/EMA fetch
if (typeof EdgeCloud !== 'undefined') {
  const ecMultiplier = +localStorage.getItem('ec_st_multiplier') || 2;
  console.log(`EdgeCloud: computing for ${symsNeedATR.length} symbols (ST mult=${ecMultiplier})`);

  for (let i = 0; i < symsNeedATR.length; i += 2) {
    const batch = symsNeedATR.slice(i, i + 2);
    await Promise.allSettled(batch.map(async sym => {
      try {
        const ec = await EdgeCloud.fetchAndCompute(sym, { stMultiplier: ecMultiplier });
        if (ec && ec.current) {
          const allTrades2 = JSON.parse(localStorage.getItem('te_trades') || '[]');
          const tIdx2 = allTrades2.findIndex(t => t.symbol === sym && t.status === 'Open');
          if (tIdx2 >= 0) {
            EdgeCloud.storeOnTrade(allTrades2[tIdx2], ec);
            localStorage.setItem('te_trades', JSON.stringify(allTrades2));
            console.log(`EdgeCloud ${sym}: ${ec.current.state} | Walk ₹${ec.current.walkingLine} | Run ₹${ec.current.runningLine} | CW ₹${ec.current.cloudWidth} | ${ec.current.actionLabel}`);
          }
        }
      } catch (e) {
        console.log('EdgeCloud error for', sym, e.message);
      }
    }));
  }
}

/* --- END EDGECLOUD FETCH PATCH --- */


// ═══ STEP 3: EdgeCloud Card Section in renderPositions ═══
// In the renderPositions() function, find the ATR Meter section (around line 2893):
//
//   <!-- ATR Meter -->
//   ${trade?._epAtr14 || trade ? `<div class="ep-pos-section">${renderATRMeter(trade, ltp, p._atrSource)}</div>` : ''}
//
// AFTER that, add:

/* --- BEGIN EDGECLOUD CARD PATCH --- */

//   <!-- EdgeCloud -->
//   ${trade?._ecState ? `<div class="ep-pos-section">
//     <div class="ep-section-label">EdgeCloud</div>
//     ${typeof EdgeCloud !== 'undefined' ? EdgeCloud.renderCardSection(trade, ltp) : ''}
//   </div>` : ''}

/* --- END EDGECLOUD CARD PATCH --- */


// ═══ STEP 4: EdgeCloud Alert Check in refreshAll ═══
// In refreshAll(), after checkSLBreachAlerts() (around line 2267), add:

/* --- BEGIN EDGECLOUD ALERT PATCH --- */

// 6. EdgeCloud signal alerts
if (typeof EdgeCloud !== 'undefined') {
  checkEdgeCloudAlerts();
}

// Add this function after checkSLBreachAlerts():
function checkEdgeCloudAlerts() {
  _mergedPositions.forEach(p => {
    const trade = p._trade;
    if (!trade || !trade._ecState) return;
    const cmp = p.last_price || 0;
    if (!cmp || p._cmpSource === 'entry') return;

    // Pullback re-entry alert
    if (trade._ecLastPullback) {
      const alertKey = `ec_pullback_${trade.id}_${trade._ecLastPullback.bar}`;
      if (!wasAlertSent(alertKey)) {
        markAlertSent(alertKey);
        playAlertSound();
        toast(`🟢 ${trade.symbol}: EdgeCloud Pullback (${trade._ecLastPullback.score}pts) — Add opportunity`, '🟢');
        sendTelegramAlert(EdgeCloud.telegramAlert(trade.symbol, trade._ecLastPullback, 'pullback'));
      }
    }

    // Pyramid breakout alert
    if (trade._ecLastPyramid) {
      const alertKey = `ec_pyramid_${trade.id}_${trade._ecLastPyramid.bar}`;
      if (!wasAlertSent(alertKey)) {
        markAlertSent(alertKey);
        playAlertSound();
        toast(`🔺 ${trade.symbol}: EdgeCloud Pyramid — Donchian breakout`, '🔺');
        sendTelegramAlert(EdgeCloud.telegramAlert(trade.symbol, trade._ecLastPyramid, 'pyramid'));
      }
    }

    // Exhaustion star alert
    if (trade._ecLastStar && trade._ecLastStar.level >= 2) {
      const alertKey = `ec_star_${trade.id}_${trade._ecLastStar.bar}`;
      if (!wasAlertSent(alertKey)) {
        markAlertSent(alertKey);
        playAlertSound();
        playAlertSound(); // Double alert for exhaustion
        toast(`⭐ ${trade.symbol}: EdgeCloud Exhaustion L${trade._ecLastStar.level} — ${trade._ecLastStar.levelLabel}`, '⭐');
        sendTelegramAlert(EdgeCloud.telegramAlert(trade.symbol, trade._ecLastStar, 'star'));
      }
    }

    // State change alert (entered cloud or dropped below)
    const stateKey = `ec_state_${trade.id}_${trade._ecState}`;
    if ((trade._ecState === 'in_cloud' || trade._ecState === 'weakening') && !wasAlertSent(stateKey)) {
      markAlertSent(stateKey);
      toast(`⚠️ ${trade.symbol}: Entered EdgeCloud — caution`, '⚠️');
      sendTelegramAlert(`⚠️ EdgeCloud — ${trade.symbol}\nState: ${trade._ecState.toUpperCase()}\nWalking TSL: ₹${trade._ecWalking}\nAction: ${trade._ecAction || 'Monitor'}`);
    }
  });
}

/* --- END EDGECLOUD ALERT PATCH --- */


// ═══ STEP 5: EdgeCloud TSL in autoTrailSL ═══
// In the autoTrailSL() function, enhance the trail logic with EdgeCloud's
// Walking Line. Find the section that computes trailValue (around line 2382-2401).
//
// Add this block at the START of the trail computation (before the exitPct checks):

/* --- BEGIN EDGECLOUD TSL PATCH --- */

// EdgeCloud TSL override — if available and higher than current trail
if (trade._ecTsl && +trade._ecTsl > 0) {
  const ecTSL = +trade._ecTsl;
  // Use EdgeCloud TSL if it's higher (only raises, never lowers)
  if (ecTSL > trailValue && ecTSL > currentSL) {
    trailValue = ecTSL;
    trailSource = `EdgeCloud ${trade._ecTslSource || 'SuperTrend'}`;
  }
}

/* --- END EDGECLOUD TSL PATCH --- */


// ═══ STEP 6: Settings — ST Multiplier Toggle ═══
// In the Settings panel HTML (around line 490), add a new settings block:

/* --- BEGIN EDGECLOUD SETTINGS PATCH --- */

// Add this HTML block inside settings-content div:
//
// <!-- EdgeCloud Settings -->
// <div style="background:var(--bg3);border-radius:8px;padding:12px">
//   <div style="display:flex;align-items:center;gap:6px;margin-bottom:8px">
//     <span style="font-size:14px">☁️</span>
//     <span style="font-size:11px;font-weight:600;color:var(--t1)">EdgeCloud</span>
//     <span id="ec-status" style="font-size:9px;padding:2px 6px;border-radius:4px;background:rgba(0,229,160,0.15);color:#00E5A0">Active</span>
//   </div>
//   <div style="display:flex;gap:8px;margin-bottom:8px">
//     <label style="font-size:10px;color:var(--t2);display:flex;align-items:center;gap:4px">
//       SuperTrend Multiplier:
//     </label>
//     <button class="ep-act-btn" id="ec-st2-btn" onclick="setECMultiplier(2)" style="font-size:10px;padding:4px 10px">10, 2</button>
//     <button class="ep-act-btn" id="ec-st3-btn" onclick="setECMultiplier(3)" style="font-size:10px;padding:4px 10px">10, 3</button>
//   </div>
//   <div style="font-size:9px;color:var(--t4)">
//     10,2 = tighter TSL (more responsive) · 10,3 = wider TSL (fewer whipsaws)
//   </div>
// </div>

// Add this JS function:
function setECMultiplier(m) {
  localStorage.setItem('ec_st_multiplier', m);
  const b2 = document.getElementById('ec-st2-btn');
  const b3 = document.getElementById('ec-st3-btn');
  if (b2 && b3) {
    b2.style.borderColor = m === 2 ? 'var(--c)' : '';
    b2.style.color = m === 2 ? 'var(--c)' : '';
    b3.style.borderColor = m === 3 ? 'var(--c)' : '';
    b3.style.color = m === 3 ? 'var(--c)' : '';
  }
  toast(`EdgeCloud: SuperTrend(10,${m})`, '☁️');
  // Force re-fetch on next refresh
  _atrFetchTime = 0;
}

// Initialize button state on settings load:
function loadECSettings() {
  const m = +localStorage.getItem('ec_st_multiplier') || 2;
  setECMultiplier(m);
}
// Call loadECSettings() inside loadSettingsValues()

/* --- END EDGECLOUD SETTINGS PATCH --- */


// ═══ STEP 7: SL Move Modal — EdgeCloud Preset ═══
// In openSLMove(), add EdgeCloud Walking Line as an SL preset option.
// Find the slButtons array (around line 1532) and add:

/* --- BEGIN EDGECLOUD SL PRESET PATCH --- */

// After the existing presets (B/E, Chandelier, CMP-ATR, EMA, DMA), add:
if (trade._ecWalking && +trade._ecWalking > 0) {
  slButtons.push({
    label: 'EC Walk ₹' + (+trade._ecWalking).toFixed(0),
    value: +trade._ecWalking,
    color: 'var(--c)'
  });
}
if (trade._ecRunning && +trade._ecRunning > 0) {
  slButtons.push({
    label: 'EC Run ₹' + (+trade._ecRunning).toFixed(0),
    value: +trade._ecRunning,
    color: '#ff9800'
  });
}

/* --- END EDGECLOUD SL PRESET PATCH --- */
