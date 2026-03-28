// ═══════════════════════════════════════════════════════════════════════════
// EdgeCloud v1.0 — Adaptive Trend Cloud Indicator for TradEdge
// ═══════════════════════════════════════════════════════════════════════════
//
// Architecture:
//   Walking Line = SuperTrend(10, multiplier) → TSL / hard stop
//   Running Line = EMA(21) → aggressive trend tracker
//   Cloud = zone between Walking & Running lines
//
// Signals:
//   P (Pullback)  — price dips into cloud, re-emerges above Running Line
//   Arrow (Pyramid) — Donchian(20) new high breakout above cloud
//   Star (Exhaustion) — disparity > N× cloud width + reversal bar
//
// Depends on: OHLCV bars array [{o,h,l,c,v?}] from Yahoo via Worker
// Integrates with: execution.html fetchTechDataForSymbol() pipeline
// ═══════════════════════════════════════════════════════════════════════════

'use strict';

window.EdgeCloud = (function () {

  // ─── Defaults ───────────────────────────────────────────────────────────
  const DEFAULTS = {
    stPeriod: 10,
    stMultiplier: 2,        // User toggle: 2 (tight) or 3 (wide)
    emaPeriod: 21,           // Running Line
    donchianPeriod: 20,      // For pyramid breakout detection
    rsiPeriod: 14,           // For pullback qualification

    // Pullback (P) qualification filters
    pullback: {
      minBarsInCloud: 2,     // Too short = noise
      maxBarsInCloud: 7,     // Too long = trend breaking
      volContractionRatio: 0.70,  // Cloud-bar avg vol < 70% of pre-cloud avg
      reentryBarUpperPct: 0.30,   // Close in upper 30% of bar range
      rsiFloor: 40,               // RSI must stay above 40 during pullback
      cloudWidthExpanding: false,  // Reject if cloud is expanding
    },

    // Arrow (Pyramid) qualification filters
    pyramid: {
      volSurgeRatio: 1.5,    // Breakout bar vol >= 1.5× 20-bar avg
      minBarsFromLastP: 5,   // Minimum distance from last P signal
      minMoveFromEntry: 3.0, // Must be +3% from entry already
    },

    // Star (Exhaustion) levels — maps to tranche exits
    exhaustion: {
      mildMultiple: 2,       // Disparity > 2× cloud width → awareness
      t1Multiple: 3,         // Disparity > 3× cloud width → sell 25%
      t2Multiple: 4,         // Disparity > 4× cloud width → sell 50-75%
      minTrendBars: 10,      // Min bars above cloud before star is valid
      reversalBarPct: 0.40,  // Close in lower 40% of range = reversal quality
    },
  };


  // ═══════════════════════════════════════════════════════════════════════
  // CORE CALCULATIONS
  // ═══════════════════════════════════════════════════════════════════════

  /**
   * ATR — Wilder's Smoothing (matches execution.html calcATR14)
   * @param {Array} bars [{o,h,l,c}]
   * @param {number} period
   * @returns {Array} ATR values per bar (first `period` bars use SMA seed)
   */
  function calcATR(bars, period) {
    if (!bars || bars.length < period + 1) return [];
    const trs = [bars[0].h - bars[0].l];
    for (let i = 1; i < bars.length; i++) {
      trs.push(Math.max(
        bars[i].h - bars[i].l,
        Math.abs(bars[i].h - bars[i - 1].c),
        Math.abs(bars[i].l - bars[i - 1].c)
      ));
    }
    // SMA seed
    let atr = 0;
    for (let i = 0; i < period; i++) atr += trs[i];
    atr /= period;
    const result = [];
    for (let i = 0; i < period; i++) result.push(atr);
    // Wilder's smoothing
    for (let i = period; i < trs.length; i++) {
      atr = (atr * (period - 1) + trs[i]) / period;
      result.push(atr);
    }
    return result;
  }


  /**
   * EMA — Exponential Moving Average (series)
   * @param {Array} values — close prices
   * @param {number} period
   * @returns {Array} EMA values per bar
   */
  function calcEMASeries(values, period) {
    if (!values || values.length < period) return [];
    const k = 2 / (period + 1);
    let ema = 0;
    for (let i = 0; i < period; i++) ema += values[i];
    ema /= period;
    const result = [];
    for (let i = 0; i < period; i++) result.push(ema); // seed
    for (let i = period; i < values.length; i++) {
      ema = values[i] * k + ema * (1 - k);
      result.push(ema);
    }
    return result;
  }


  /**
   * RSI — Wilder's RSI
   * @param {Array} closes
   * @param {number} period
   * @returns {Array} RSI values per bar
   */
  function calcRSI(closes, period) {
    if (!closes || closes.length < period + 1) return [];
    const result = new Array(closes.length).fill(50);
    let avgGain = 0, avgLoss = 0;
    for (let i = 1; i <= period; i++) {
      const d = closes[i] - closes[i - 1];
      if (d > 0) avgGain += d; else avgLoss -= d;
    }
    avgGain /= period;
    avgLoss /= period;
    result[period] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
    for (let i = period + 1; i < closes.length; i++) {
      const d = closes[i] - closes[i - 1];
      avgGain = (avgGain * (period - 1) + (d > 0 ? d : 0)) / period;
      avgLoss = (avgLoss * (period - 1) + (d < 0 ? -d : 0)) / period;
      result[i] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
    }
    return result;
  }


  /**
   * SuperTrend — Walking Line
   * @param {Array} bars [{o,h,l,c}]
   * @param {number} period — ATR lookback
   * @param {number} mult — ATR multiplier
   * @returns {Object} { st: Array, dir: Array(1=bull, -1=bear) }
   */
  function calcSuperTrend(bars, period, mult) {
    const atr = calcATR(bars, period);
    if (!atr.length) return { st: [], dir: [] };
    const st = [];
    const dir = [];
    let prevUp = 0, prevDn = 0, prevDir = 1;

    for (let i = 0; i < bars.length; i++) {
      const hl2 = (bars[i].h + bars[i].l) / 2;
      let up = hl2 - mult * atr[i];
      let dn = hl2 + mult * atr[i];

      // Ratchet: upper band only rises, lower band only falls
      if (i > 0 && bars[i - 1].c > prevUp) up = Math.max(up, prevUp);
      if (i > 0 && bars[i - 1].c < prevDn) dn = Math.min(dn, prevDn);

      // Direction flip
      let d = prevDir;
      if (i === 0) d = 1;
      else if (prevDir === 1 && bars[i].c < prevUp) d = -1;
      else if (prevDir === -1 && bars[i].c > prevDn) d = 1;

      st.push(d === 1 ? up : dn);
      dir.push(d);
      prevUp = up;
      prevDn = dn;
      prevDir = d;
    }
    return { st, dir };
  }


  /**
   * Donchian Channel
   * @param {Array} bars [{o,h,l,c}]
   * @param {number} period
   * @returns {Object} { upper: Array, lower: Array }
   */
  function calcDonchian(bars, period) {
    const upper = [], lower = [];
    for (let i = 0; i < bars.length; i++) {
      const s = Math.max(0, i - period + 1);
      let hi = -Infinity, lo = Infinity;
      for (let j = s; j <= i; j++) {
        hi = Math.max(hi, bars[j].h);
        lo = Math.min(lo, bars[j].l);
      }
      upper.push(hi);
      lower.push(lo);
    }
    return { upper, lower };
  }


  // ═══════════════════════════════════════════════════════════════════════
  // CLOUD STATE & SIGNAL DETECTION
  // ═══════════════════════════════════════════════════════════════════════

  /**
   * Compute full EdgeCloud analysis from OHLCV bars
   *
   * @param {Array} bars — [{o,h,l,c,v?}] minimum 30 bars, ideally 60-90
   * @param {Object} opts — override DEFAULTS
   * @returns {Object} Full analysis result
   */
  function compute(bars, opts) {
    const cfg = { ...DEFAULTS, ...opts };
    if (!bars || bars.length < 30) {
      return { error: 'Need at least 30 bars', signals: null, state: null };
    }

    const closes = bars.map(b => b.c);
    const highs = bars.map(b => b.h);
    const lows = bars.map(b => b.l);
    const vols = bars.map(b => b.v || 0);
    const hasVolume = vols.some(v => v > 0);

    // Core lines
    const walkST = calcSuperTrend(bars, cfg.stPeriod, cfg.stMultiplier);
    const runEMA = calcEMASeries(closes, cfg.emaPeriod);
    const don = calcDonchian(bars, cfg.donchianPeriod);
    const rsi = calcRSI(closes, cfg.rsiPeriod);
    const atr = calcATR(bars, cfg.stPeriod);

    const n = bars.length;

    // ── Per-bar cloud state ──────────────────────────────────────────
    const states = [];    // 'strong_bull' | 'in_cloud' | 'strong_bear' | 'below_cloud'
    const cloudTop = [];  // upper edge of cloud
    const cloudBot = [];  // lower edge of cloud
    const cloudWidth = [];
    const cloudBullish = []; // is cloud bullish?

    for (let i = 0; i < n; i++) {
      const w = walkST.st[i] || closes[i];
      const r = runEMA[i] || closes[i];
      const bullish = walkST.dir[i] === 1;
      const top = Math.max(w, r);
      const bot = Math.min(w, r);

      cloudTop.push(top);
      cloudBot.push(bot);
      cloudWidth.push(top - bot);
      cloudBullish.push(bullish);

      if (closes[i] > top) {
        states.push(bullish ? 'strong_bull' : 'recovering');
      } else if (closes[i] < bot) {
        states.push(bullish ? 'weakening' : 'strong_bear');
      } else {
        states.push('in_cloud');
      }
    }


    // ── Pullback (P) signals — qualified ─────────────────────────────
    const pullbacks = [];
    let cloudEntryBar = -1;
    let wasAboveCloud = false;
    let rsiMinDuringCloud = 100;
    let cloudWidthAtEntry = 0;

    for (let i = 1; i < n; i++) {
      const aboveCloud = closes[i] > cloudTop[i];
      const inCloud = states[i] === 'in_cloud';
      const bullish = cloudBullish[i];

      if (!bullish) {
        // Only track bullish pullbacks for now
        wasAboveCloud = false;
        cloudEntryBar = -1;
        continue;
      }

      if (aboveCloud) {
        // Check if this is a pullback exit (was in cloud, now above)
        if (cloudEntryBar >= 0 && wasAboveCloud) {
          const barsInCloud = i - cloudEntryBar;
          const pCfg = cfg.pullback;

          // ── Qualification filters ──
          let qualified = true;
          let score = 0;
          const reasons = [];

          // Filter 1: Cloud residence time (2-7 bars)
          if (barsInCloud >= pCfg.minBarsInCloud && barsInCloud <= pCfg.maxBarsInCloud) {
            score += 15;
            reasons.push(`cloud ${barsInCloud} bars`);
          } else {
            qualified = false;
            reasons.push(`cloud ${barsInCloud} bars (need ${pCfg.minBarsInCloud}-${pCfg.maxBarsInCloud})`);
          }

          // Filter 2: Volume contraction during pullback
          if (hasVolume && qualified) {
            const preCloudVols = vols.slice(Math.max(0, cloudEntryBar - 10), cloudEntryBar);
            const cloudVols = vols.slice(cloudEntryBar, i);
            if (preCloudVols.length && cloudVols.length) {
              const preAvg = preCloudVols.reduce((s, v) => s + v, 0) / preCloudVols.length;
              const cloudAvg = cloudVols.reduce((s, v) => s + v, 0) / cloudVols.length;
              if (preAvg > 0 && cloudAvg / preAvg <= pCfg.volContractionRatio) {
                score += 20;
                reasons.push('vol contracted');
              } else if (preAvg > 0 && cloudAvg / preAvg > 1.0) {
                qualified = false;
                reasons.push('vol expanded in cloud');
              } else {
                score += 10; // Mild contraction
                reasons.push('vol neutral');
              }
            }
          } else if (!hasVolume) {
            score += 10; // No vol data — give partial credit
          }

          // Filter 3: Re-entry bar quality (close in upper 30% of range)
          if (qualified) {
            const barRange = bars[i].h - bars[i].l;
            const closePos = barRange > 0 ? (closes[i] - bars[i].l) / barRange : 0.5;
            if (closePos >= (1 - pCfg.reentryBarUpperPct)) {
              score += 15;
              reasons.push('strong re-entry bar');
            } else if (closePos >= 0.5) {
              score += 8;
              reasons.push('decent re-entry bar');
            } else {
              qualified = false;
              reasons.push('weak re-entry bar');
            }
          }

          // Filter 4: RSI floor during pullback
          if (qualified) {
            if (rsiMinDuringCloud >= pCfg.rsiFloor) {
              score += 15;
              reasons.push(`RSI held ${Math.round(rsiMinDuringCloud)}`);
            } else {
              qualified = false;
              reasons.push(`RSI dropped to ${Math.round(rsiMinDuringCloud)}`);
            }
          }

          // Filter 5: Cloud width not expanding
          if (qualified && cloudEntryBar > 0) {
            const cwAtEntry = cloudWidth[cloudEntryBar];
            const cwNow = cloudWidth[i];
            if (cwNow <= cwAtEntry * 1.2) {
              score += 10;
              reasons.push('cloud stable');
            } else {
              score += 0;
              reasons.push('cloud expanding');
            }
          }

          // Filter 6: Price closes above Running Line (EMA)
          if (qualified && closes[i] > runEMA[i]) {
            score += 15;
            reasons.push('above EMA');
          } else if (qualified) {
            qualified = false;
            reasons.push('below EMA on re-entry');
          }

          if (qualified && score >= 40) {
            pullbacks.push({
              bar: i,
              price: closes[i],
              walkingLine: walkST.st[i],
              runningLine: runEMA[i],
              barsInCloud: barsInCloud,
              rsiAtReentry: Math.round(rsi[i]),
              score: Math.min(100, score),
              reasons: reasons,
              tsl: walkST.st[i], // Walking Line = stop loss
            });
          }
        }

        wasAboveCloud = true;
        cloudEntryBar = -1;
        rsiMinDuringCloud = 100;
      } else if (inCloud) {
        if (wasAboveCloud && cloudEntryBar < 0) {
          cloudEntryBar = i;
          cloudWidthAtEntry = cloudWidth[i];
        }
        if (rsi[i] !== undefined) rsiMinDuringCloud = Math.min(rsiMinDuringCloud, rsi[i]);
      } else {
        // Below cloud — reset
        wasAboveCloud = false;
        cloudEntryBar = -1;
      }
    }


    // ── Arrow (Pyramid) signals — qualified ──────────────────────────
    const pyramids = [];
    let prevDonUpper = 0;

    for (let i = 1; i < n; i++) {
      const bullish = cloudBullish[i];
      const aboveCloud = closes[i] > cloudTop[i];

      // Donchian new high breakout
      const isDonBreakout = bars[i].h >= don.upper[i] && (i < 2 || bars[i - 1].h < don.upper[i - 1]);

      if (!bullish || !aboveCloud || !isDonBreakout) continue;

      const pCfg = cfg.pyramid;
      let score = 0;
      const reasons = [];

      // Qualification 1: Volume surge on breakout
      if (hasVolume) {
        const avgVol20 = vols.slice(Math.max(0, i - 20), i).reduce((s, v) => s + v, 0) / 20;
        if (avgVol20 > 0 && vols[i] >= avgVol20 * pCfg.volSurgeRatio) {
          score += 25;
          reasons.push(`vol ${(vols[i] / avgVol20).toFixed(1)}× avg`);
        } else if (avgVol20 > 0 && vols[i] >= avgVol20) {
          score += 10;
          reasons.push('vol adequate');
        } else {
          reasons.push('low vol breakout');
        }
      } else {
        score += 10;
      }

      // Qualification 2: Minimum distance from last P signal
      const lastP = pullbacks.length ? pullbacks[pullbacks.length - 1].bar : -999;
      if (i - lastP >= pCfg.minBarsFromLastP) {
        score += 15;
        reasons.push(`${i - lastP} bars from last P`);
      } else {
        reasons.push('too close to P signal');
      }

      // Qualification 3: Price above both lines (confirmed trend)
      if (closes[i] > walkST.st[i] && closes[i] > runEMA[i]) {
        score += 20;
        reasons.push('above both lines');
      }

      // Qualification 4: Donchian is rising (not just touching old high)
      if (don.upper[i] > don.upper[Math.max(0, i - 5)]) {
        score += 15;
        reasons.push('channel expanding');
      } else {
        score += 5;
        reasons.push('channel flat');
      }

      // Qualification 5: RSI strength
      if (rsi[i] >= 55 && rsi[i] <= 80) {
        score += 15;
        reasons.push(`RSI ${Math.round(rsi[i])}`);
      } else if (rsi[i] > 80) {
        score += 5;
        reasons.push(`RSI overbought ${Math.round(rsi[i])}`);
      }

      if (score >= 35) {
        pyramids.push({
          bar: i,
          price: closes[i],
          donchianHigh: don.upper[i],
          walkingLine: walkST.st[i],
          score: Math.min(100, score),
          reasons: reasons,
        });
      }
    }


    // ── Star (Exhaustion) signals — graduated ────────────────────────
    const stars = [];
    let barsAboveCloud = 0;

    for (let i = 2; i < n; i++) {
      const aboveCloud = closes[i] > cloudTop[i] || closes[i - 1] > cloudTop[i - 1];
      if (aboveCloud && cloudBullish[i]) {
        barsAboveCloud++;
      } else if (closes[i] < cloudBot[i]) {
        barsAboveCloud = 0;
        continue;
      }

      // Disparity from Running Line (EMA)
      const disparity = Math.abs(closes[i] - runEMA[i]);
      const cw = cloudWidth[i] || 1;
      const disparityMultiple = disparity / cw;

      // Need minimum trend duration
      if (barsAboveCloud < cfg.exhaustion.minTrendBars) continue;

      // Need a reversal bar pattern
      const barRange = bars[i].h - bars[i].l;
      const closePos = barRange > 0 ? (closes[i] - bars[i].l) / barRange : 0.5;
      const isReversalBar = closePos <= cfg.exhaustion.reversalBarPct;
      const isDirectionChange = (closes[i - 1] > closes[i - 2] && closes[i] < closes[i - 1]);

      if (!isReversalBar && !isDirectionChange) continue;

      // Determine exhaustion level
      let level = 0;
      let levelLabel = '';
      if (disparityMultiple >= cfg.exhaustion.t2Multiple) {
        level = 3;
        levelLabel = 'T2/T3 — sell 50-75%';
      } else if (disparityMultiple >= cfg.exhaustion.t1Multiple) {
        level = 2;
        levelLabel = 'T1 — sell 25%';
      } else if (disparityMultiple >= cfg.exhaustion.mildMultiple) {
        level = 1;
        levelLabel = 'Mild extension — tighten TSL';
      } else {
        continue;
      }

      let score = 0;
      const reasons = [];

      // Score by disparity level
      score += level * 20;
      reasons.push(`disparity ${disparityMultiple.toFixed(1)}× cloud`);

      // Score by trend duration
      if (barsAboveCloud >= 20) { score += 15; reasons.push(`${barsAboveCloud} bars in trend`); }
      else if (barsAboveCloud >= 15) { score += 10; reasons.push(`${barsAboveCloud} bars in trend`); }

      // Score by reversal bar quality
      if (isReversalBar) { score += 15; reasons.push(`reversal bar (${Math.round(closePos * 100)}%)`); }
      if (isDirectionChange) { score += 10; reasons.push('direction change'); }

      // Volume spike on exhaustion candle
      if (hasVolume) {
        const avgVol10 = vols.slice(Math.max(0, i - 10), i).reduce((s, v) => s + v, 0) / 10;
        if (avgVol10 > 0 && vols[i] >= avgVol10 * 1.5) {
          score += 15;
          reasons.push('climax volume');
        }
      }

      stars.push({
        bar: i,
        price: closes[i],
        level: level,
        levelLabel: levelLabel,
        disparityMultiple: Math.round(disparityMultiple * 10) / 10,
        barsInTrend: barsAboveCloud,
        score: Math.min(100, score),
        reasons: reasons,
      });
    }


    // ── Current state summary (last bar) ─────────────────────────────
    const last = n - 1;
    const currentWalk = walkST.st[last];
    const currentRun = runEMA[last];
    const currentCW = cloudWidth[last];
    const currentState = states[last];
    const currentDisparity = Math.abs(closes[last] - currentRun);
    const currentDisparityMult = currentCW > 0 ? currentDisparity / currentCW : 0;

    // Determine TSL recommendation
    let tslValue = currentWalk;
    let tslSource = `SuperTrend(${cfg.stPeriod},${cfg.stMultiplier})`;

    // Cloud width vs ATR ratio — adjust TSL buffer
    const currentATR = atr[last] || 0;
    const cwAtrRatio = currentATR > 0 ? currentCW / currentATR : 1;
    if (cwAtrRatio < 0.5 && currentState !== 'strong_bull') {
      // Cloud converging, trend may be weakening — add buffer
      tslValue = currentWalk - 0.5 * currentATR;
      tslSource += ' - 0.5×ATR buffer';
    }

    // Recent signal lookback (last 5 bars)
    const recentP = pullbacks.filter(p => p.bar >= last - 5);
    const recentPyr = pyramids.filter(p => p.bar >= last - 5);
    const recentStar = stars.filter(s => s.bar >= last - 5);

    // Signal summary for the position card
    let actionLabel = '';
    let actionColor = '';
    if (recentStar.length && recentStar[0].level >= 2) {
      actionLabel = recentStar[0].levelLabel;
      actionColor = 'var(--r)';
    } else if (recentP.length) {
      actionLabel = 'P — Add / re-enter';
      actionColor = 'var(--g)';
    } else if (recentPyr.length) {
      actionLabel = 'Pyramid — Donchian breakout';
      actionColor = 'var(--y)';
    } else if (currentState === 'strong_bull') {
      actionLabel = 'Hold — trend intact';
      actionColor = 'var(--g)';
    } else if (currentState === 'in_cloud') {
      actionLabel = 'Caution — in cloud';
      actionColor = 'var(--y)';
    } else if (currentState === 'strong_bear' || currentState === 'weakening') {
      actionLabel = 'Exit — below cloud';
      actionColor = 'var(--r)';
    }

    return {
      // Full series (for charting)
      series: {
        walkingLine: walkST.st,
        walkingDir: walkST.dir,
        runningLine: runEMA,
        cloudTop: cloudTop,
        cloudBot: cloudBot,
        cloudWidth: cloudWidth,
        cloudBullish: cloudBullish,
        donchianUpper: don.upper,
        donchianLower: don.lower,
        rsi: rsi,
        states: states,
      },

      // Discrete signals
      signals: {
        pullbacks: pullbacks,
        pyramids: pyramids,
        stars: stars,
      },

      // Current bar state (for position card)
      current: {
        state: currentState,
        walkingLine: Math.round(currentWalk * 100) / 100,
        runningLine: Math.round(currentRun * 100) / 100,
        cloudWidth: Math.round(currentCW * 100) / 100,
        cloudBullish: cloudBullish[last],
        disparityMultiple: Math.round(currentDisparityMult * 10) / 10,
        rsi: Math.round(rsi[last]),
        tslValue: Math.round(tslValue * 100) / 100,
        tslSource: tslSource,
        actionLabel: actionLabel,
        actionColor: actionColor,
        atr: Math.round((atr[last] || 0) * 100) / 100,
      },

      // Recent signals for alerts
      recentSignals: {
        pullback: recentP.length ? recentP[0] : null,
        pyramid: recentPyr.length ? recentPyr[0] : null,
        star: recentStar.length ? recentStar[0] : null,
      },

      // Config used
      config: {
        stPeriod: cfg.stPeriod,
        stMultiplier: cfg.stMultiplier,
        emaPeriod: cfg.emaPeriod,
      },
    };
  }


  // ═══════════════════════════════════════════════════════════════════════
  // INTEGRATION HELPERS — for execution.html
  // ═══════════════════════════════════════════════════════════════════════

  /**
   * Fetch OHLCV and compute EdgeCloud for a symbol
   * Uses the same Worker yahoo-proxy as fetchTechDataForSymbol
   *
   * @param {string} sym — stock symbol (e.g. 'TDPOWERSYS')
   * @param {Object} opts — override defaults (e.g. { stMultiplier: 3 })
   * @returns {Object|null} EdgeCloud analysis result
   */
  async function fetchAndCompute(sym, opts) {
    const wk = localStorage.getItem('zd_worker_url') || '';
    if (!wk) return null;

    try {
      const ticker = sym.includes('.') ? sym : sym + '.NS';
      const r = await fetch(wk, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Kite-Action': 'yahoo-proxy' },
        body: JSON.stringify({ ticker, range: '6mo', interval: '1d' }),
        signal: AbortSignal.timeout(12000),
      });
      if (!r.ok) return null;
      const d = await r.json();
      const rs = d?.chart?.result?.[0];
      if (!rs) return null;

      const ts = rs.timestamp || [];
      const q = rs.indicators?.quote?.[0] || {};
      const bars = [];
      for (let i = 0; i < ts.length; i++) {
        const o = q.open?.[i], h = q.high?.[i], l = q.low?.[i], c = q.close?.[i], v = q.volume?.[i];
        if (o && h && l && c) bars.push({ o, h, l, c, v: v || 0 });
      }

      if (bars.length < 30) return null;
      return compute(bars, opts);
    } catch (e) {
      console.error('EdgeCloud fetch error for', sym, e.message);
      return null;
    }
  }


  /**
   * Store EdgeCloud data on a trade object (for persistence in te_trades)
   *
   * @param {Object} trade — journal trade object
   * @param {Object} ec — EdgeCloud compute() result
   */
  function storeOnTrade(trade, ec) {
    if (!trade || !ec || !ec.current) return;
    trade._ecState = ec.current.state;
    trade._ecWalking = ec.current.walkingLine;
    trade._ecRunning = ec.current.runningLine;
    trade._ecCloudWidth = ec.current.cloudWidth;
    trade._ecCloudBullish = ec.current.cloudBullish;
    trade._ecDisparity = ec.current.disparityMultiple;
    trade._ecRsi = ec.current.rsi;
    trade._ecTsl = ec.current.tslValue;
    trade._ecTslSource = ec.current.tslSource;
    trade._ecAction = ec.current.actionLabel;
    trade._ecActionColor = ec.current.actionColor;
    trade._ecAtr = ec.current.atr;
    trade._ecConfig = ec.config;
    trade._ecUpdated = new Date().toISOString();

    // Store recent signals
    if (ec.recentSignals.pullback) trade._ecLastPullback = ec.recentSignals.pullback;
    if (ec.recentSignals.pyramid) trade._ecLastPyramid = ec.recentSignals.pyramid;
    if (ec.recentSignals.star) trade._ecLastStar = ec.recentSignals.star;

    // Signal counts (lifetime)
    trade._ecPullbackCount = ec.signals.pullbacks.length;
    trade._ecPyramidCount = ec.signals.pyramids.length;
    trade._ecStarCount = ec.signals.stars.length;
  }


  /**
   * Render EdgeCloud summary for a position card
   *
   * @param {Object} trade — journal trade with _ec* fields
   * @param {number} cmp — current market price
   * @returns {string} HTML string
   */
  function renderCardSection(trade, cmp) {
    if (!trade || !trade._ecState) {
      return '<div style="font-size:9px;color:var(--t4);padding:4px 0">EdgeCloud: loading...</div>';
    }

    const state = trade._ecState;
    const walk = trade._ecWalking || 0;
    const run = trade._ecRunning || 0;
    const cw = trade._ecCloudWidth || 0;
    const disp = trade._ecDisparity || 0;
    const action = trade._ecAction || '';
    const actionColor = trade._ecActionColor || 'var(--t2)';
    const tsl = trade._ecTsl || walk;
    const rsiVal = trade._ecRsi || 0;
    const cfg = trade._ecConfig || {};

    // State badge
    const stateColors = {
      strong_bull: { bg: 'rgba(0,229,160,.12)', color: 'var(--g)', label: 'STRONG BULL' },
      in_cloud: { bg: 'rgba(245,158,11,.12)', color: 'var(--y)', label: 'IN CLOUD' },
      recovering: { bg: 'rgba(0,187,255,.12)', color: 'var(--c)', label: 'RECOVERING' },
      weakening: { bg: 'rgba(255,152,0,.12)', color: '#ff9800', label: 'WEAKENING' },
      strong_bear: { bg: 'rgba(255,69,96,.12)', color: 'var(--r)', label: 'BEAR' },
    };
    const sc = stateColors[state] || stateColors.in_cloud;

    // Cloud visualization — mini bar
    const total = Math.max(cmp, walk, run) - Math.min(cmp, walk, run) || 1;
    const minVal = Math.min(walk, run, cmp) * 0.998;
    const maxVal = Math.max(walk, run, cmp) * 1.002;
    const range = maxVal - minVal || 1;
    const walkPct = ((walk - minVal) / range * 100).toFixed(1);
    const runPct = ((run - minVal) / range * 100).toFixed(1);
    const cmpPct = ((cmp - minVal) / range * 100).toFixed(1);
    const cloudL = Math.min(+walkPct, +runPct);
    const cloudR = Math.max(+walkPct, +runPct);
    const cloudColor = trade._ecCloudBullish ? 'rgba(0,229,160,.15)' : 'rgba(255,69,96,.12)';

    // Recent signal badges
    let signalBadges = '';
    if (trade._ecLastPullback) {
      signalBadges += `<span style="font-size:8px;font-weight:700;padding:2px 6px;border-radius:4px;background:rgba(0,229,160,.12);color:var(--g)">P ${trade._ecLastPullback.score}pts</span>`;
    }
    if (trade._ecLastPyramid) {
      signalBadges += `<span style="font-size:8px;font-weight:700;padding:2px 6px;border-radius:4px;background:rgba(245,158,11,.12);color:var(--y)">▲ PYR</span>`;
    }
    if (trade._ecLastStar) {
      const starColors = { 1: 'var(--y)', 2: '#ff9800', 3: 'var(--r)' };
      const sc2 = starColors[trade._ecLastStar.level] || 'var(--y)';
      signalBadges += `<span style="font-size:8px;font-weight:700;padding:2px 6px;border-radius:4px;background:rgba(255,69,96,.12);color:${sc2}">★ ${trade._ecLastStar.levelLabel}</span>`;
    }

    return `<div style="margin:0">
      <!-- State + Action -->
      <div style="display:flex;align-items:center;justify-content:space-between;gap:6px;flex-wrap:wrap;margin-bottom:6px">
        <div style="display:flex;align-items:center;gap:6px">
          <span style="font-size:8.5px;font-weight:700;padding:2px 7px;border-radius:10px;background:${sc.bg};color:${sc.color}">${sc.label}</span>
          <span style="font-size:8px;color:var(--t4)">ST(${cfg.stPeriod || 10},${cfg.stMultiplier || 2})</span>
        </div>
        ${action ? `<span style="font-size:9px;font-weight:700;color:${actionColor}">${action}</span>` : ''}
      </div>

      <!-- Mini cloud bar -->
      <div style="position:relative;height:18px;border-radius:6px;background:var(--bg3);margin-bottom:4px;overflow:hidden">
        <div style="position:absolute;top:0;left:${cloudL}%;width:${cloudR - cloudL}%;height:100%;background:${cloudColor}"></div>
        <div style="position:absolute;top:0;left:${walkPct}%;width:2px;height:100%;background:var(--c);z-index:2" title="Walking ₹${walk}"></div>
        <div style="position:absolute;top:0;left:${runPct}%;width:2px;height:100%;background:#ff9800;z-index:2" title="Running ₹${run}"></div>
        <div style="position:absolute;top:0;left:${cmpPct}%;width:3px;height:100%;background:var(--t1);z-index:3" title="CMP ₹${cmp}"></div>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:8px;color:var(--t4);margin-bottom:6px">
        <span style="color:var(--c)">Walk ₹${walk.toFixed(0)}</span>
        <span style="color:#ff9800">Run ₹${run.toFixed(0)}</span>
        <span>Cloud ₹${cw.toFixed(0)}</span>
        <span>Disp ${disp}×</span>
        <span>RSI ${rsiVal}</span>
      </div>

      <!-- Signal badges -->
      ${signalBadges ? `<div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:4px">${signalBadges}</div>` : ''}

      <!-- TSL recommendation -->
      <div style="display:flex;align-items:center;gap:6px;font-size:9px">
        <span style="color:var(--t3)">TSL:</span>
        <span style="font-weight:700;color:var(--c);font-family:'IBM Plex Mono',monospace">₹${tsl.toFixed(0)}</span>
        <span style="color:var(--t4);font-size:8px">${trade._ecTslSource || ''}</span>
      </div>
    </div>`;
  }


  /**
   * Generate Telegram alert text for an EdgeCloud signal
   *
   * @param {string} sym — symbol
   * @param {Object} signal — pullback/pyramid/star signal object
   * @param {string} type — 'pullback' | 'pyramid' | 'star'
   * @returns {string} Telegram message
   */
  function telegramAlert(sym, signal, type) {
    if (!signal) return '';
    const score = signal.score || 0;
    const reasons = (signal.reasons || []).join(' · ');

    if (type === 'pullback') {
      return `🟢 EdgeCloud P — ${sym}\n` +
        `Price: ₹${signal.price.toFixed(2)}\n` +
        `TSL: ₹${signal.tsl?.toFixed(2) || signal.walkingLine?.toFixed(2)}\n` +
        `Score: ${score}/100 | ${signal.barsInCloud} bars in cloud\n` +
        `RSI: ${signal.rsiAtReentry}\n` +
        `${reasons}`;
    }
    if (type === 'pyramid') {
      return `🔺 EdgeCloud PYRAMID — ${sym}\n` +
        `Donchian(20) breakout: ₹${signal.donchianHigh?.toFixed(2)}\n` +
        `Price: ₹${signal.price.toFixed(2)}\n` +
        `Score: ${score}/100\n` +
        `${reasons}`;
    }
    if (type === 'star') {
      return `⭐ EdgeCloud EXHAUSTION — ${sym}\n` +
        `Level: ${signal.levelLabel}\n` +
        `Disparity: ${signal.disparityMultiple}× cloud width\n` +
        `Score: ${score}/100 | ${signal.barsInTrend} bars in trend\n` +
        `${reasons}`;
    }
    return '';
  }


  // ═══════════════════════════════════════════════════════════════════════
  // PUBLIC API
  // ═══════════════════════════════════════════════════════════════════════

  return {
    // Core
    compute: compute,
    fetchAndCompute: fetchAndCompute,

    // Integration
    storeOnTrade: storeOnTrade,
    renderCardSection: renderCardSection,
    telegramAlert: telegramAlert,

    // Calculation primitives (for testing / reuse)
    calcATR: calcATR,
    calcEMASeries: calcEMASeries,
    calcSuperTrend: calcSuperTrend,
    calcDonchian: calcDonchian,
    calcRSI: calcRSI,

    // Version
    VERSION: '1.0.0',
    NAME: 'EdgeCloud',
  };

})();
