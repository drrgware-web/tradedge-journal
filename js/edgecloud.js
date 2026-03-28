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
      // Phase 2: regime overlay applied via applyRegime()
      regime: null,
    };
  }


  // ═══════════════════════════════════════════════════════════════════════
  // PHASE 2: REGIME OVERLAY — MPS + Sector RS + Sweet Zone
  // ═══════════════════════════════════════════════════════════════════════

  const MPS_ZONES = {
    'Extreme Bull Zone':   { regime: 'bull',    signalGate: ['P','Arrow','Star'], sizeMulti: 1.5 },
    'Bull Zone':           { regime: 'bull',    signalGate: ['P','Arrow','Star'], sizeMulti: 1.2 },
    'Accumulation Zone':   { regime: 'bull',    signalGate: ['P','Arrow','Star'], sizeMulti: 1.0 },
    'Caution Zone':        { regime: 'neutral', signalGate: ['P','Star'],         sizeMulti: 0.7 },
    'Distribution Zone':   { regime: 'neutral', signalGate: ['Star'],             sizeMulti: 0.5 },
    'No Money Zone':       { regime: 'bear',    signalGate: ['Star'],             sizeMulti: 0.0 },
  };

  const SECTOR_RS_MODIFIERS = {
    'Leading':   { scoreMod: 20, sizeMulti: 1.0, color: 'var(--g)' },
    'Improving': { scoreMod: 10, sizeMulti: 0.8, color: 'var(--c)' },
    'Weakening': { scoreMod: -5, sizeMulti: 0.5, color: 'var(--y)' },
    'Lagging':   { scoreMod:-15, sizeMulti: 0.3, color: 'var(--r)' },
  };

  const SECTOR_PROXY = {
    'TCS':'IT','INFY':'IT','WIPRO':'IT','HCLTECH':'IT','TECHM':'IT','LTIM':'IT','MPHASIS':'IT',
    'COFORGE':'IT','PERSISTENT':'IT','LTTS':'IT','TATAELXSI':'IT','DATAPATTNS':'IT',
    'HDFCBANK':'Bank','ICICIBANK':'Bank','KOTAKBANK':'Bank','AXISBANK':'Bank','SBIN':'Bank',
    'BANKBARODA':'Bank','PNB':'Bank','INDUSINDBK':'Bank','IDFCFIRSTB':'Bank','UNIONBANK':'Bank',
    'FEDERALBNK':'Bank','BANDHANBNK':'Bank','CANBK':'Bank','INDIANB':'Bank',
    'BAJFINANCE':'Financial Services','BAJAJFINSV':'Financial Services','CHOLAFIN':'Financial Services',
    'SHRIRAMFIN':'Financial Services','MUTHOOTFIN':'Financial Services','MANAPPURAM':'Financial Services',
    'TATAMOTORS':'Auto','MARUTI':'Auto','M&M':'Auto','BAJAJ-AUTO':'Auto','HEROMOTOCO':'Auto',
    'EICHERMOT':'Auto','ASHOKLEY':'Auto','TVSMOTOR':'Auto','MOTHERSON':'Auto',
    'SUNPHARMA':'Pharma','DRREDDY':'Pharma','CIPLA':'Pharma','DIVISLAB':'Pharma',
    'LUPIN':'Pharma','AUROPHARMA':'Pharma','TORNTPHARM':'Pharma','ALKEM':'Pharma',
    'BIOCON':'Pharma','IPCALAB':'Pharma','PHARMABEES':'Pharma',
    'TATASTEEL':'Metal','JSWSTEEL':'Metal','HINDALCO':'Metal','VEDL':'Metal',
    'COALINDIA':'Metal','NMDC':'Metal','SAIL':'Metal','NATIONALUM':'Metal',
    'RELIANCE':'Oil & Gas','ONGC':'Oil & Gas','BPCL':'Oil & Gas','IOC':'Oil & Gas',
    'GAIL':'Oil & Gas','OIL':'Oil & Gas','PETRONET':'Oil & Gas',
    'NTPC':'Power','POWERGRID':'Power','TATAPOWER':'Power','ADANIGREEN':'Power',
    'NHPC':'Power','SJVN':'Power','IREDA':'Power','TDPOWERSYS':'Power',
    'BEL':'Power','QPOWER':'Power',
    'HINDUNILVR':'FMCG','ITC':'FMCG','NESTLEIND':'FMCG','BRITANNIA':'FMCG',
    'DABUR':'FMCG','MARICO':'FMCG','GODREJCP':'FMCG','COLPAL':'FMCG',
    'LARSENTOUB':'Infrastructure','ADANIENT':'Infrastructure','MAHLOG':'Infrastructure',
    'DLF':'Realty','GODREJPROP':'Realty','OBEROIRLTY':'Realty','PRESTIGE':'Realty','BRIGADE':'Realty',
    'HAVELLS':'Consumer Durables','VOLTAS':'Consumer Durables','CROMPTON':'Consumer Durables',
    'ASTRAMICRO':'Consumer Durables',
    'BHARTIARTL':'Telecom','IDEA':'Telecom','TATACOMM':'Telecom',
    'APOLLOHOSP':'Healthcare','FORTIS':'Healthcare','MAXHEALTH':'Healthcare',
    'STARHEALTH':'Healthcare','METROPOLIS':'Healthcare','LALPATHLAB':'Healthcare',
    'GOLDBEES':'Commodities','SILVERBEES':'Commodities',
  };

  const SECTOR_INDEX_MAP = {
    'IT':'NIFTY_IT.NS', 'Bank':'NIFTY_BANK.NS', 'Financial Services':'NIFTY_FIN_SERVICE.NS',
    'Auto':'NIFTY_AUTO.NS', 'Pharma':'NIFTY_PHARMA.NS', 'Metal':'NIFTY_METAL.NS',
    'Oil & Gas':'NIFTY_OIL_AND_GAS.NS', 'Power':'NIFTY_ENERGY.NS', 'FMCG':'NIFTY_FMCG.NS',
    'Infrastructure':'NIFTY_INFRA.NS', 'Realty':'NIFTY_REALTY.NS', 'PSU Bank':'NIFTY_PSU_BANK.NS',
    'Consumer Durables':'NIFTY_CONSR_DURBL.NS', 'Telecom':'NIFTY_MEDIA.NS', 'Media':'NIFTY_MEDIA.NS',
    'Healthcare':'NIFTY_HEALTHCARE.NS', 'Commodities':'NIFTY_COMMODITIES.NS',
  };

  // Session caches
  let _mpsCache = null;
  let _mpsFetched = false;
  let _sectorRSCache = {};

  async function fetchMPS() {
    try {
      const urls = ['mps_latest.json', 'data/mps_latest.json', '/mps_latest.json'];
      for (const url of urls) {
        try {
          const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
          if (!res.ok) continue;
          const data = await res.json();
          const cur = data?.current || data;
          if (!cur?.zone) continue;
          const z = MPS_ZONES[cur.zone] || MPS_ZONES['Caution Zone'];
          return {
            zone: cur.zone, finalScore: cur.final_score || 0, state: cur.state || '',
            regime: z.regime, sizeMulti: z.sizeMulti, signalGate: z.signalGate,
            riskPerTrade: cur.risk_per_trade || '', zoneAction: cur.zone_action || '',
            atrRegime: cur.atr_regime || '', burstLabel: cur.burst_label || '',
            macro: cur.macro_summary || '',
            modifiers: (cur.modifiers || []).filter(m => m.triggered).map(m => m.name),
          };
        } catch (e) { continue; }
      }
      // Fallback: localStorage
      try {
        const c = localStorage.getItem('te_mps_current');
        if (c) { const d = JSON.parse(c); if (d?.zone) { const z = MPS_ZONES[d.zone] || MPS_ZONES['Caution Zone']; return { zone: d.zone, finalScore: d.final_score || 0, state: d.state || '', regime: z.regime, sizeMulti: z.sizeMulti, signalGate: z.signalGate, riskPerTrade:'',zoneAction:'',atrRegime:'',burstLabel:'',macro:'',modifiers:[] }; } }
      } catch (e) {}
      return null;
    } catch (e) { return null; }
  }

  function calcJdK(stockCloses, benchCloses, window) {
    window = window || 10;
    const n = Math.min(stockCloses.length, benchCloses.length);
    if (n < window * 3) return null;
    const sec = stockCloses.slice(-n), ben = benchCloses.slice(-n);
    const rsRaw = sec.map((s, i) => ben[i] > 0 ? s / ben[i] : 1);
    const rsNorm = new Array(rsRaw.length).fill(NaN);
    for (let i = window - 1; i < rsRaw.length; i++) { let sum = 0; for (let j = i - window + 1; j <= i; j++) sum += rsRaw[j]; rsNorm[i] = sum / window > 0 ? (rsRaw[i] / (sum / window) * 100) : 100; }
    const alpha = 2.0 / (window + 1);
    const rsRatio = new Array(rsNorm.length).fill(NaN);
    rsRatio[window - 1] = rsNorm[window - 1];
    for (let i = window; i < rsNorm.length; i++) { if (!isNaN(rsNorm[i]) && !isNaN(rsRatio[i - 1])) rsRatio[i] = alpha * rsNorm[i] + (1 - alpha) * rsRatio[i - 1]; }
    const rsMom = new Array(rsRatio.length).fill(NaN);
    for (let i = window; i < rsRatio.length; i++) { if (!isNaN(rsRatio[i]) && !isNaN(rsRatio[i - window])) { const prev = rsRatio[i - window]; rsMom[i] = prev > 0 ? (rsRatio[i] / prev * 100) : 100; } }
    let lastR = NaN, lastM = NaN;
    for (let i = rsRatio.length - 1; i >= 0; i--) { if (!isNaN(rsRatio[i]) && isNaN(lastR)) lastR = rsRatio[i]; if (!isNaN(rsMom[i]) && isNaN(lastM)) lastM = rsMom[i]; if (!isNaN(lastR) && !isNaN(lastM)) break; }
    if (isNaN(lastR) || isNaN(lastM)) return null;
    return { ratio: Math.round(lastR * 100) / 100, mom: Math.round(lastM * 100) / 100 };
  }

  async function fetchSectorRS(sectorTicker) {
    const wk = localStorage.getItem('zd_worker_url') || '';
    if (!wk || !sectorTicker) return null;
    try {
      const [sRes, nRes] = await Promise.all([
        fetch(wk, { method:'POST', headers:{'Content-Type':'application/json','X-Kite-Action':'yahoo-proxy'}, body:JSON.stringify({ticker:sectorTicker,range:'1y',interval:'1d'}), signal:AbortSignal.timeout(8000) }),
        fetch(wk, { method:'POST', headers:{'Content-Type':'application/json','X-Kite-Action':'yahoo-proxy'}, body:JSON.stringify({ticker:'^NSEI',range:'1y',interval:'1d'}), signal:AbortSignal.timeout(8000) }),
      ]);
      if (!sRes.ok || !nRes.ok) return null;
      const sData = await sRes.json(); const nData = await nRes.json();
      const sCloses = sData?.chart?.result?.[0]?.indicators?.quote?.[0]?.close?.filter(c => c != null);
      const nCloses = nData?.chart?.result?.[0]?.indicators?.quote?.[0]?.close?.filter(c => c != null);
      if (!sCloses?.length || !nCloses?.length) return null;
      const jdk = calcJdK(sCloses, nCloses, 10);
      if (!jdk) return null;
      let quadrant = 'Lagging';
      if (jdk.ratio >= 100 && jdk.mom >= 100) quadrant = 'Leading';
      else if (jdk.ratio >= 100 && jdk.mom < 100) quadrant = 'Weakening';
      else if (jdk.ratio < 100 && jdk.mom >= 100) quadrant = 'Improving';
      return { quadrant, ratio: jdk.ratio, momentum: jdk.mom };
    } catch (e) { return null; }
  }

  function applyRegime(ecResult, regime) {
    if (!ecResult || !regime) return ecResult;
    const mps = regime.mps || null;
    const sectorRS = regime.sectorRS || null;
    const sectorName = regime.sectorName || '';

    let mpsGate = ['P','Arrow','Star'];
    let mpsSizeMulti = 1.0;
    let mpsRegime = 'unknown';
    if (mps) { mpsGate = mps.signalGate || mpsGate; mpsSizeMulti = mps.sizeMulti ?? 1.0; mpsRegime = mps.regime || 'unknown'; }

    let sectorScoreMod = 0, sectorSizeMulti = 1.0, sectorLabel = '', sectorColor = 'var(--t4)';
    if (sectorRS) {
      const mod = SECTOR_RS_MODIFIERS[sectorRS.quadrant];
      if (mod) { sectorScoreMod = mod.scoreMod; sectorSizeMulti = mod.sizeMulti; sectorLabel = `${sectorName} ${sectorRS.quadrant}`; sectorColor = mod.color; }
    }

    const combinedSizeMulti = Math.round(mpsSizeMulti * sectorSizeMulti * 100) / 100;

    const adjustSignal = (sig, type) => {
      if (!sig) return sig;
      const adj = { ...sig };
      adj.adjustedScore = Math.max(0, Math.min(100, (sig.score || 0) + sectorScoreMod));
      const typeKey = type === 'pullback' ? 'P' : type === 'pyramid' ? 'Arrow' : 'Star';
      adj.gatedByMPS = !mpsGate.includes(typeKey);
      adj.reasons = [...(adj.reasons || [])];
      if (adj.gatedByMPS) adj.reasons.push(`BLOCKED by MPS ${mps?.zone || ''}`);
      else if (sectorScoreMod !== 0) adj.reasons.push(`${sectorLabel} ${sectorScoreMod > 0 ? '+' : ''}${sectorScoreMod}`);
      adj.sizeMultiplier = combinedSizeMulti;
      return adj;
    };

    const adjP = adjustSignal(ecResult.recentSignals.pullback, 'pullback');
    const adjPyr = adjustSignal(ecResult.recentSignals.pyramid, 'pyramid');
    const adjStar = adjustSignal(ecResult.recentSignals.star, 'star');

    // Override action label
    let actionLabel = ecResult.current.actionLabel;
    let actionColor = ecResult.current.actionColor;

    if (mpsRegime === 'bear') {
      if (adjP?.gatedByMPS) { actionLabel = 'P blocked — No Money Zone'; actionColor = 'var(--r)'; }
      if (adjPyr?.gatedByMPS) { actionLabel = 'Pyramid blocked — No Money Zone'; actionColor = 'var(--r)'; }
      if (adjStar && !adjStar.gatedByMPS) { actionLabel = (adjStar.levelLabel || 'Sell') + ' (MPS confirms)'; actionColor = 'var(--r)'; }
      if (!adjP && !adjPyr && !adjStar && ecResult.current.state === 'strong_bull') { actionLabel = 'Hold cautious — MPS bear'; actionColor = 'var(--y)'; }
    } else if (mpsRegime === 'neutral') {
      if (adjPyr?.gatedByMPS) { actionLabel = 'Pyramid blocked — MPS caution'; actionColor = 'var(--y)'; }
    }
    if (sectorRS?.quadrant === 'Lagging' && actionLabel.includes('Add')) { actionLabel = 'P — weak sector, half size'; actionColor = 'var(--y)'; }

    ecResult.regime = {
      mps: mps ? { zone: mps.zone, score: mps.finalScore, regime: mpsRegime, sizeMulti: mpsSizeMulti, gate: mpsGate, state: mps.state, atrRegime: mps.atrRegime, macro: mps.macro } : null,
      sector: sectorRS ? { name: sectorName, quadrant: sectorRS.quadrant, ratio: sectorRS.ratio, momentum: sectorRS.momentum, scoreMod: sectorScoreMod, sizeMulti: sectorSizeMulti, color: sectorColor } : null,
      combined: { sizeMultiplier: combinedSizeMulti, signalGate: mpsGate },
    };
    ecResult.recentSignals = { pullback: adjP, pyramid: adjPyr, star: adjStar };
    ecResult.current.actionLabel = actionLabel;
    ecResult.current.actionColor = actionColor;
    ecResult.current.regimeSizeMulti = combinedSizeMulti;
    return ecResult;
  }

  async function fetchComputeWithRegime(sym, opts, sector) {
    const ec = await fetchAndCompute(sym, opts);
    if (!ec) return null;
    // MPS: fetch once per session
    let mps = _mpsCache;
    if (!mps && !_mpsFetched) {
      _mpsFetched = true;
      mps = await fetchMPS();
      _mpsCache = mps;
      if (mps) { console.log(`MPS: ${mps.zone} (score ${mps.finalScore}) — ${mps.regime}`); try { localStorage.setItem('te_mps_current', JSON.stringify({ zone: mps.zone, final_score: mps.finalScore, state: mps.state })); } catch(e){} }
    }
    // Sector RS: resolve and cache hourly
    const sectorName = sector || SECTOR_PROXY[sym.toUpperCase()] || '';
    let sectorRS = null;
    if (sectorName) {
      const sectorTicker = SECTOR_INDEX_MAP[sectorName];
      if (sectorTicker) {
        const ck = `ec_sRS_${sectorName}`;
        const cached = _sectorRSCache[ck];
        if (cached && (Date.now() - cached.time < 3600000)) { sectorRS = cached.data; }
        else { sectorRS = await fetchSectorRS(sectorTicker); if (sectorRS) { _sectorRSCache[ck] = { data: sectorRS, time: Date.now() }; console.log(`Sector RS ${sectorName}: ${sectorRS.quadrant} (R:${sectorRS.ratio} M:${sectorRS.momentum})`); } }
      }
    }
    return applyRegime(ec, { mps, sectorRS, sectorName });
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

    // Phase 2: Regime overlay data
    if (ec.regime) {
      if (ec.regime.mps) {
        trade._ecMpsZone = ec.regime.mps.zone;
        trade._ecMpsRegime = ec.regime.mps.regime;
        trade._ecMpsSizeMulti = ec.regime.mps.sizeMulti;
        trade._ecMpsGate = ec.regime.mps.gate;
      }
      if (ec.regime.sector) {
        trade._ecSectorName = ec.regime.sector.name;
        trade._ecSectorQuadrant = ec.regime.sector.quadrant;
        trade._ecSectorRatio = ec.regime.sector.ratio;
        trade._ecSectorMomentum = ec.regime.sector.momentum;
        trade._ecSectorScoreMod = ec.regime.sector.scoreMod;
      }
      if (ec.regime.combined) {
        trade._ecSizeMulti = ec.regime.combined.sizeMultiplier;
      }
    }
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
      const gated = trade._ecLastPullback.gatedByMPS;
      signalBadges += `<span style="font-size:8px;font-weight:700;padding:2px 6px;border-radius:4px;background:${gated ? 'rgba(255,69,96,.12)' : 'rgba(0,229,160,.12)'};color:${gated ? 'var(--r)' : 'var(--g)'}">${gated ? 'P ✕' : 'P'} ${(trade._ecLastPullback.adjustedScore || trade._ecLastPullback.score)}pts</span>`;
    }
    if (trade._ecLastPyramid) {
      const gated = trade._ecLastPyramid.gatedByMPS;
      signalBadges += `<span style="font-size:8px;font-weight:700;padding:2px 6px;border-radius:4px;background:${gated ? 'rgba(255,69,96,.12)' : 'rgba(245,158,11,.12)'};color:${gated ? 'var(--r)' : 'var(--y)'}">${gated ? '▲ ✕' : '▲ PYR'}</span>`;
    }
    if (trade._ecLastStar) {
      const starColors = { 1: 'var(--y)', 2: '#ff9800', 3: 'var(--r)' };
      const sc2 = starColors[trade._ecLastStar.level] || 'var(--y)';
      signalBadges += `<span style="font-size:8px;font-weight:700;padding:2px 6px;border-radius:4px;background:rgba(255,69,96,.12);color:${sc2}">★ ${trade._ecLastStar.levelLabel}</span>`;
    }

    // Phase 2: Regime badges
    let regimeBadges = '';
    if (trade._ecMpsZone) {
      const mpsColors = { 'bull':'rgba(0,229,160,.12)', 'neutral':'rgba(245,158,11,.12)', 'bear':'rgba(255,69,96,.12)' };
      const mpsTextColors = { 'bull':'var(--g)', 'neutral':'var(--y)', 'bear':'var(--r)' };
      const reg = trade._ecMpsRegime || 'neutral';
      regimeBadges += `<span style="font-size:7.5px;font-weight:700;padding:2px 5px;border-radius:4px;background:${mpsColors[reg]||mpsColors.neutral};color:${mpsTextColors[reg]||'var(--t3)'}">${trade._ecMpsZone}</span>`;
    }
    if (trade._ecSectorQuadrant) {
      const sqColors = { Leading:'var(--g)', Improving:'var(--c)', Weakening:'var(--y)', Lagging:'var(--r)' };
      regimeBadges += `<span style="font-size:7.5px;font-weight:700;padding:2px 5px;border-radius:4px;background:rgba(0,187,255,.08);color:${sqColors[trade._ecSectorQuadrant]||'var(--t3)'}">${trade._ecSectorName||''} ${trade._ecSectorQuadrant}</span>`;
    }
    if (trade._ecSizeMulti !== undefined && trade._ecSizeMulti !== 1) {
      const smColor = trade._ecSizeMulti >= 1 ? 'var(--g)' : trade._ecSizeMulti >= 0.5 ? 'var(--y)' : 'var(--r)';
      regimeBadges += `<span style="font-size:7.5px;font-weight:700;padding:2px 5px;border-radius:4px;background:var(--bg3);color:${smColor}">${trade._ecSizeMulti}× size</span>`;
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

      <!-- Regime badges -->
      ${regimeBadges ? `<div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:4px">${regimeBadges}</div>` : ''}

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
    const score = signal.adjustedScore || signal.score || 0;
    const reasons = (signal.reasons || []).join(' · ');
    const gated = signal.gatedByMPS ? '\n🚫 BLOCKED by MPS' : '';
    const sizeLine = signal.sizeMultiplier && signal.sizeMultiplier !== 1 ? `\nSize: ${signal.sizeMultiplier}×` : '';

    if (type === 'pullback') {
      return `🟢 EdgeCloud P — ${sym}\n` +
        `Price: ₹${signal.price.toFixed(2)}\n` +
        `TSL: ₹${signal.tsl?.toFixed(2) || signal.walkingLine?.toFixed(2)}\n` +
        `Score: ${score}/100 | ${signal.barsInCloud} bars in cloud\n` +
        `RSI: ${signal.rsiAtReentry}${sizeLine}${gated}\n` +
        `${reasons}`;
    }
    if (type === 'pyramid') {
      return `🔺 EdgeCloud PYRAMID — ${sym}\n` +
        `Donchian(20) breakout: ₹${signal.donchianHigh?.toFixed(2)}\n` +
        `Price: ₹${signal.price.toFixed(2)}\n` +
        `Score: ${score}/100${sizeLine}${gated}\n` +
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
  // TIER 3: SIGNAL SCORING ENGINE — Learns from Trade Outcomes
  // ═══════════════════════════════════════════════════════════════════════
  //
  // Analyzes closed trades that had EdgeCloud signals at entry/during hold,
  // correlates signal factors with actual R-multiple outcomes, and produces
  // adjusted weights that improve future signal scoring.
  //
  // Data flow:
  //   1. analyzeTrades() scans all closed trades in te_trades
  //   2. For each trade, extracts signal context at entry (stored _ec* fields)
  //   3. Computes actual outcome (R-multiple, P&L%, holding days, max drawdown)
  //   4. Correlates factors → outcomes to find which factors predict winners
  //   5. Stores learned weights in localStorage (te_ec_learned_weights)
  //   6. applyLearnedWeights() adjusts live signal scores using these weights
  // ═══════════════════════════════════════════════════════════════════════

  const WEIGHT_STORAGE_KEY = 'te_ec_learned_weights';
  const ANALYSIS_STORAGE_KEY = 'te_ec_signal_analysis';

  // Default factor weights (before any learning)
  const DEFAULT_WEIGHTS = {
    // Pullback factors
    p_cloudResidence: 15,    // 2-7 bars in cloud
    p_volumeContraction: 20, // Vol drops during pullback
    p_reentryBarQuality: 15, // Close in upper 30%
    p_rsiFloor: 15,          // RSI stays above 40
    p_cloudStable: 10,       // Cloud not expanding
    p_aboveEMA: 15,          // Closes above Running Line
    p_sectorLeading: 10,     // Sector in Leading/Improving
    // Pyramid factors
    a_volumeSurge: 25,       // Breakout vol > 1.5× avg
    a_distFromP: 15,         // Min bars from last P
    a_aboveBothLines: 20,    // Above Walking + Running
    a_channelExpanding: 15,  // Donchian rising
    a_rsiRange: 15,          // RSI 55-80
    a_sectorStrength: 10,    // Sector RS score
    // Star factors
    s_disparityLevel: 20,    // Disparity × cloud width
    s_trendDuration: 15,     // Bars above cloud
    s_reversalBar: 15,       // Close in lower 40%
    s_directionChange: 10,   // Prior bar was up, this is down
    s_climaxVolume: 15,      // Vol spike on exhaustion
    // Regime factors
    r_mpsRegime: 15,         // MPS zone alignment
    r_sectorQuadrant: 15,    // Sector RS quadrant
  };

  /**
   * Extract outcome metrics from a closed/partial trade
   */
  function extractOutcome(trade) {
    if (!trade) return null;
    const entries = trade.entries || [];
    const exits = trade.exits || [];
    if (!entries.length) return null;

    const totalEntryQty = entries.reduce((s, e) => s + (+e.qty || 0), 0);
    const totalExitQty = exits.reduce((s, e) => s + (+e.qty || 0), 0);
    const avgEntry = totalEntryQty > 0
      ? entries.reduce((s, e) => s + (+e.price || 0) * (+e.qty || 0), 0) / totalEntryQty : 0;
    const avgExit = totalExitQty > 0
      ? exits.reduce((s, e) => s + (+e.price || 0) * (+e.qty || 0), 0) / totalExitQty : 0;

    if (!avgEntry) return null;

    const pnlPct = avgExit > 0 ? ((avgExit - avgEntry) / avgEntry * 100) : 0;
    const riskPerShare = trade.sl ? Math.abs(avgEntry - (+trade.sl)) : avgEntry * 0.05;
    const rMultiple = riskPerShare > 0 && avgExit > 0 ? (avgExit - avgEntry) / riskPerShare : 0;

    const entryDate = new Date(entries[0].date);
    const exitDate = exits.length ? new Date(exits[exits.length - 1].date) : new Date();
    const holdingDays = Math.max(1, Math.floor((exitDate - entryDate) / 86400000));

    // Outcome classification
    let outcome = 'neutral';
    if (rMultiple >= 2) outcome = 'big_win';
    else if (rMultiple >= 1) outcome = 'win';
    else if (rMultiple >= 0) outcome = 'scratch';
    else if (rMultiple >= -0.5) outcome = 'small_loss';
    else outcome = 'loss';

    return {
      symbol: trade.symbol,
      tradeId: trade.id,
      avgEntry, avgExit, pnlPct: Math.round(pnlPct * 100) / 100,
      rMultiple: Math.round(rMultiple * 100) / 100,
      holdingDays, outcome,
      isWin: rMultiple > 0,
      isBigWin: rMultiple >= 2,
      sector: trade.sector || trade._ecSectorName || '',
    };
  }

  /**
   * Extract signal context that was active at trade entry/hold
   */
  function extractSignalContext(trade) {
    return {
      // EdgeCloud state at last update
      ecState: trade._ecState || null,
      cloudBullish: trade._ecCloudBullish,
      disparity: trade._ecDisparity || 0,
      rsi: trade._ecRsi || 0,
      cloudWidth: trade._ecCloudWidth || 0,

      // Signal counts over trade lifetime
      pullbackCount: trade._ecPullbackCount || 0,
      pyramidCount: trade._ecPyramidCount || 0,
      starCount: trade._ecStarCount || 0,

      // Last signal details (if any)
      lastPullback: trade._ecLastPullback || null,
      lastPyramid: trade._ecLastPyramid || null,
      lastStar: trade._ecLastStar || null,

      // Regime at last update
      mpsZone: trade._ecMpsZone || null,
      mpsRegime: trade._ecMpsRegime || null,
      sectorName: trade._ecSectorName || null,
      sectorQuadrant: trade._ecSectorQuadrant || null,
      sizeMulti: trade._ecSizeMulti || 1,

      // Trade metadata
      pyramidLevel: trade._epPyramid || 'p1',
      entryCount: (trade.entries || []).length,
      slMoveCount: (trade._epSlMoves || []).length,
      grade: trade._epGrade || null,
      source: trade.source || 'manual',
    };
  }

  /**
   * Analyze all trades and compute factor → outcome correlations
   * Returns analysis object with win rates per factor and learned weights
   */
  function analyzeTrades(trades) {
    if (!trades || !trades.length) return null;

    // Only analyze trades that have EdgeCloud data AND some exit data
    const analyzable = trades.filter(t =>
      (t.status === 'Closed' || (t.exits && t.exits.length > 0)) &&
      t._ecState // Has EdgeCloud data
    );

    if (analyzable.length < 5) {
      return { error: 'Need at least 5 closed trades with EdgeCloud data', count: analyzable.length };
    }

    const results = [];

    analyzable.forEach(trade => {
      const outcome = extractOutcome(trade);
      const context = extractSignalContext(trade);
      if (!outcome) return;

      results.push({ ...outcome, ...context });
    });

    if (results.length < 5) {
      return { error: 'Not enough analyzable trades', count: results.length };
    }

    // ── Compute factor win rates ──────────────────────────────────────

    const factors = {};

    function trackFactor(name, condition, result) {
      if (!factors[name]) factors[name] = { present: { wins: 0, total: 0, rSum: 0 }, absent: { wins: 0, total: 0, rSum: 0 } };
      const bucket = condition ? factors[name].present : factors[name].absent;
      bucket.total++;
      bucket.rSum += result.rMultiple;
      if (result.isWin) bucket.wins++;
    }

    results.forEach(r => {
      // Cloud state factors
      trackFactor('entered_strong_bull', r.ecState === 'strong_bull', r);
      trackFactor('entered_in_cloud', r.ecState === 'in_cloud', r);
      trackFactor('cloud_bullish', r.cloudBullish === true, r);

      // Signal presence factors
      trackFactor('had_pullback_signal', r.pullbackCount > 0, r);
      trackFactor('had_pyramid_signal', r.pyramidCount > 0, r);
      trackFactor('had_star_signal', r.starCount > 0, r);
      trackFactor('multiple_pullbacks', r.pullbackCount >= 2, r);

      // Pullback quality (if last pullback exists)
      if (r.lastPullback) {
        trackFactor('p_high_score', (r.lastPullback.adjustedScore || r.lastPullback.score || 0) >= 60, r);
        trackFactor('p_strong_reentry', r.lastPullback.rsiAtReentry >= 50, r);
        trackFactor('p_short_cloud', (r.lastPullback.barsInCloud || 0) <= 4, r);
      }

      // RSI context
      trackFactor('rsi_above_50', r.rsi > 50, r);
      trackFactor('rsi_above_60', r.rsi > 60, r);
      trackFactor('rsi_overbought', r.rsi > 70, r);

      // Disparity
      trackFactor('low_disparity', r.disparity <= 1.5, r);
      trackFactor('high_disparity', r.disparity >= 3, r);

      // Regime factors
      trackFactor('mps_bull', r.mpsRegime === 'bull', r);
      trackFactor('mps_bear', r.mpsRegime === 'bear', r);
      trackFactor('sector_leading', r.sectorQuadrant === 'Leading', r);
      trackFactor('sector_improving', r.sectorQuadrant === 'Improving', r);
      trackFactor('sector_lagging', r.sectorQuadrant === 'Lagging', r);

      // Trade management
      trackFactor('had_sl_moves', r.slMoveCount > 0, r);
      trackFactor('pyramided', r.entryCount >= 2, r);
      trackFactor('systematic_entry', r.source === 'edgepilot' || r.source === 'autopilot', r);

      // Size multiplier
      trackFactor('full_size', r.sizeMulti >= 1.0, r);
      trackFactor('reduced_size', r.sizeMulti < 0.7, r);
    });

    // ── Compute win rates and edge ──────────────────────────────────

    const factorAnalysis = {};
    Object.entries(factors).forEach(([name, data]) => {
      const pWinRate = data.present.total > 0 ? (data.present.wins / data.present.total * 100) : 0;
      const aWinRate = data.absent.total > 0 ? (data.absent.wins / data.absent.total * 100) : 0;
      const pAvgR = data.present.total > 0 ? (data.present.rSum / data.present.total) : 0;
      const aAvgR = data.absent.total > 0 ? (data.absent.rSum / data.absent.total) : 0;
      const edge = pWinRate - aWinRate;
      const rEdge = pAvgR - aAvgR;

      factorAnalysis[name] = {
        present: { count: data.present.total, winRate: Math.round(pWinRate), avgR: Math.round(pAvgR * 100) / 100 },
        absent: { count: data.absent.total, winRate: Math.round(aWinRate), avgR: Math.round(aAvgR * 100) / 100 },
        edge: Math.round(edge),       // Win rate edge (pp)
        rEdge: Math.round(rEdge * 100) / 100,  // R-multiple edge
        significant: data.present.total >= 3 && data.absent.total >= 3, // Minimum sample
      };
    });

    // ── Derive learned weight adjustments ────────────────────────────
    // Factors with positive edge → boost weight. Negative edge → reduce.
    // Only adjust factors with statistical significance (3+ samples each side)

    const learnedWeights = { ...DEFAULT_WEIGHTS };
    const adjustments = {};

    // Map factor analysis back to weight keys
    const factorToWeight = {
      'had_pullback_signal': ['p_cloudResidence', 'p_volumeContraction', 'p_reentryBarQuality'],
      'p_high_score': ['p_cloudResidence', 'p_reentryBarQuality'],
      'p_strong_reentry': ['p_rsiFloor'],
      'p_short_cloud': ['p_cloudResidence'],
      'rsi_above_50': ['p_rsiFloor', 'a_rsiRange'],
      'had_pyramid_signal': ['a_volumeSurge', 'a_aboveBothLines'],
      'sector_leading': ['p_sectorLeading', 'a_sectorStrength', 'r_sectorQuadrant'],
      'sector_lagging': ['p_sectorLeading', 'a_sectorStrength', 'r_sectorQuadrant'],
      'mps_bull': ['r_mpsRegime'],
      'mps_bear': ['r_mpsRegime'],
      'high_disparity': ['s_disparityLevel'],
      'had_star_signal': ['s_disparityLevel', 's_reversalBar'],
    };

    Object.entries(factorToWeight).forEach(([factor, weightKeys]) => {
      const fa = factorAnalysis[factor];
      if (!fa || !fa.significant) return;

      // Scale adjustment: +/- up to 30% of original weight
      const adjustPct = Math.max(-0.3, Math.min(0.3, fa.rEdge * 0.15));

      weightKeys.forEach(wk => {
        if (learnedWeights[wk] === undefined) return;
        const adj = Math.round(learnedWeights[wk] * adjustPct);
        learnedWeights[wk] = Math.max(0, learnedWeights[wk] + adj);
        if (adj !== 0) {
          adjustments[wk] = (adjustments[wk] || 0) + adj;
        }
      });
    });

    // ── Summary stats ────────────────────────────────────────────────

    const totalTrades = results.length;
    const wins = results.filter(r => r.isWin).length;
    const bigWins = results.filter(r => r.isBigWin).length;
    const avgR = results.reduce((s, r) => s + r.rMultiple, 0) / totalTrades;
    const winRate = Math.round(wins / totalTrades * 100);
    const avgHoldDays = Math.round(results.reduce((s, r) => s + r.holdingDays, 0) / totalTrades);

    // Expectancy = (winRate × avgWin) - (lossRate × avgLoss)
    const winRs = results.filter(r => r.isWin).map(r => r.rMultiple);
    const lossRs = results.filter(r => !r.isWin).map(r => Math.abs(r.rMultiple));
    const avgWinR = winRs.length ? winRs.reduce((s, v) => s + v, 0) / winRs.length : 0;
    const avgLossR = lossRs.length ? lossRs.reduce((s, v) => s + v, 0) / lossRs.length : 0;
    const expectancy = (winRate / 100 * avgWinR) - ((100 - winRate) / 100 * avgLossR);

    const analysis = {
      totalTrades, wins, bigWins, winRate,
      avgR: Math.round(avgR * 100) / 100,
      avgWinR: Math.round(avgWinR * 100) / 100,
      avgLossR: Math.round(avgLossR * 100) / 100,
      expectancy: Math.round(expectancy * 100) / 100,
      avgHoldDays,
      factorAnalysis,
      learnedWeights,
      adjustments,
      tradesAnalyzed: results,
      generatedAt: new Date().toISOString(),
    };

    // Persist
    try {
      localStorage.setItem(WEIGHT_STORAGE_KEY, JSON.stringify(learnedWeights));
      localStorage.setItem(ANALYSIS_STORAGE_KEY, JSON.stringify(analysis));
    } catch (e) { console.error('Failed to save learned weights:', e); }

    return analysis;
  }

  /**
   * Get stored learned weights (or defaults if none learned yet)
   */
  function getLearnedWeights() {
    try {
      const stored = localStorage.getItem(WEIGHT_STORAGE_KEY);
      if (stored) return JSON.parse(stored);
    } catch (e) {}
    return DEFAULT_WEIGHTS;
  }

  /**
   * Get last analysis results
   */
  function getLastAnalysis() {
    try {
      const stored = localStorage.getItem(ANALYSIS_STORAGE_KEY);
      if (stored) return JSON.parse(stored);
    } catch (e) {}
    return null;
  }

  /**
   * Run analysis on current te_trades and return results
   * Call this from console or settings panel
   */
  function runAnalysis() {
    const trades = JSON.parse(localStorage.getItem('te_trades') || '[]');
    const result = analyzeTrades(trades);
    if (result && !result.error) {
      console.log('EdgeCloud Tier 3 Analysis Complete:');
      console.log(`  Trades: ${result.totalTrades} | Win Rate: ${result.winRate}% | Avg R: ${result.avgR} | Expectancy: ${result.expectancy}R`);
      console.log(`  Big Wins (2R+): ${result.bigWins} | Avg Hold: ${result.avgHoldDays} days`);
      console.log('  Top factors by edge:');
      Object.entries(result.factorAnalysis)
        .filter(([, v]) => v.significant)
        .sort((a, b) => Math.abs(b[1].rEdge) - Math.abs(a[1].rEdge))
        .slice(0, 10)
        .forEach(([name, v]) => {
          const dir = v.rEdge >= 0 ? '+' : '';
          console.log(`    ${name}: ${dir}${v.rEdge}R edge (${v.present.count} present, ${v.absent.count} absent)`);
        });
      console.log('  Weight adjustments:', result.adjustments);
    }
    return result;
  }

  /**
   * Render Tier 3 analysis summary as HTML for a dashboard or modal
   */
  function renderAnalysisSummary() {
    const analysis = getLastAnalysis();
    if (!analysis || analysis.error) {
      return `<div style="font-size:10px;color:var(--t4);padding:8px">
        ${analysis?.error || 'No analysis yet. Close some trades with EdgeCloud data, then run EdgeCloud.runAnalysis()'}
        ${analysis?.count !== undefined ? ` (${analysis.count} trades found)` : ''}
      </div>`;
    }

    const expColor = analysis.expectancy >= 0.5 ? 'var(--g)' : analysis.expectancy >= 0 ? 'var(--y)' : 'var(--r)';
    const wrColor = analysis.winRate >= 55 ? 'var(--g)' : analysis.winRate >= 45 ? 'var(--y)' : 'var(--r)';

    // Top 5 factors sorted by absolute R-edge
    const topFactors = Object.entries(analysis.factorAnalysis)
      .filter(([, v]) => v.significant)
      .sort((a, b) => Math.abs(b[1].rEdge) - Math.abs(a[1].rEdge))
      .slice(0, 6);

    const factorRows = topFactors.map(([name, v]) => {
      const edgeColor = v.rEdge >= 0.3 ? 'var(--g)' : v.rEdge <= -0.3 ? 'var(--r)' : 'var(--t2)';
      const label = name.replace(/_/g, ' ').replace(/^(p|a|s|r) /, '');
      return `<div style="display:flex;justify-content:space-between;align-items:center;padding:3px 0;border-bottom:1px solid rgba(255,255,255,.04)">
        <span style="font-size:9px;color:var(--t2)">${label}</span>
        <div style="display:flex;gap:8px;font-family:'IBM Plex Mono',monospace;font-size:9px">
          <span style="color:var(--t3)">${v.present.winRate}% (${v.present.count})</span>
          <span style="color:${edgeColor};font-weight:700">${v.rEdge >= 0 ? '+' : ''}${v.rEdge}R</span>
        </div>
      </div>`;
    }).join('');

    // Weight changes
    const adjEntries = Object.entries(analysis.adjustments || {}).filter(([, v]) => v !== 0);
    const adjRows = adjEntries.length ? adjEntries.map(([name, adj]) => {
      const adjColor = adj > 0 ? 'var(--g)' : 'var(--r)';
      return `<span style="font-size:8px;padding:2px 5px;border-radius:4px;background:var(--bg3);color:${adjColor}">${name.replace(/_/g,' ')} ${adj > 0 ? '+' : ''}${adj}</span>`;
    }).join(' ') : '<span style="font-size:9px;color:var(--t4)">No adjustments (need more data)</span>';

    return `<div style="margin:0">
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:8px">
        <div style="text-align:center"><div style="font-size:8px;color:var(--t3)">Win Rate</div><div style="font-size:14px;font-weight:700;color:${wrColor};font-family:'IBM Plex Mono',monospace">${analysis.winRate}%</div></div>
        <div style="text-align:center"><div style="font-size:8px;color:var(--t3)">Avg R</div><div style="font-size:14px;font-weight:700;color:var(--c);font-family:'IBM Plex Mono',monospace">${analysis.avgR}R</div></div>
        <div style="text-align:center"><div style="font-size:8px;color:var(--t3)">Expectancy</div><div style="font-size:14px;font-weight:700;color:${expColor};font-family:'IBM Plex Mono',monospace">${analysis.expectancy}R</div></div>
        <div style="text-align:center"><div style="font-size:8px;color:var(--t3)">Trades</div><div style="font-size:14px;font-weight:700;color:var(--t1);font-family:'IBM Plex Mono',monospace">${analysis.totalTrades}</div></div>
      </div>
      <div style="font-size:9px;font-weight:700;color:var(--t2);margin-bottom:4px">Factor edge (R-multiple)</div>
      ${factorRows}
      <div style="font-size:9px;font-weight:700;color:var(--t2);margin:8px 0 4px">Weight adjustments</div>
      <div style="display:flex;flex-wrap:wrap;gap:4px">${adjRows}</div>
      <div style="font-size:8px;color:var(--t4);margin-top:6px">Analyzed ${analysis.generatedAt?.slice(0,10) || '—'} · ${analysis.totalTrades} trades · ${analysis.bigWins} big wins (2R+)</div>
    </div>`;
  }

  return {
    // Core
    compute: compute,
    fetchAndCompute: fetchAndCompute,

    // Phase 2: Regime overlay
    fetchComputeWithRegime: fetchComputeWithRegime,
    applyRegime: applyRegime,
    fetchMPS: fetchMPS,
    fetchSectorRS: fetchSectorRS,

    // Tier 3: Signal scoring engine
    runAnalysis: runAnalysis,
    analyzeTrades: analyzeTrades,
    getLearnedWeights: getLearnedWeights,
    getLastAnalysis: getLastAnalysis,
    renderAnalysisSummary: renderAnalysisSummary,
    DEFAULT_WEIGHTS: DEFAULT_WEIGHTS,

    // Integration
    storeOnTrade: storeOnTrade,
    renderCardSection: renderCardSection,
    telegramAlert: telegramAlert,

    // Lookups
    SECTOR_PROXY: SECTOR_PROXY,
    SECTOR_INDEX_MAP: SECTOR_INDEX_MAP,
    MPS_ZONES: MPS_ZONES,

    // Calculation primitives
    calcATR: calcATR,
    calcEMASeries: calcEMASeries,
    calcSuperTrend: calcSuperTrend,
    calcDonchian: calcDonchian,
    calcRSI: calcRSI,
    calcJdK: calcJdK,

    // Version
    VERSION: '3.0.0',
    NAME: 'EdgeCloud',
  };

})();
