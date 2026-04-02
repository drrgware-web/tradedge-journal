/**
 * EdgeCloud Mini Chart — Reusable chart component for TradEdge
 * Usage: createEdgeCloudChart(container, symbol, options)
 *
 * Requires: lightweight-charts@4.1.0 loaded globally
 */

(function(){
'use strict';

// ── Indicators ──
function ema(data,p){const k=2/(p+1);const o=[];let prev=null;for(const v of data){if(v==null){o.push(prev);continue}prev=prev==null?v:v*k+prev*(1-k);o.push(prev)}return o}

function atr(ohlc,p){const tr=[];for(let i=0;i<ohlc.length;i++){const d=ohlc[i];const h=d.high||d.h,l=d.low||d.l,pc=i>0?(ohlc[i-1].close||ohlc[i-1].c):l;tr.push(Math.max(h-l,Math.abs(h-pc),Math.abs(l-pc)))}const o=[];let s=0;for(let i=0;i<tr.length;i++){s+=tr[i];if(i>=p){s-=tr[i-p];o.push(s/p)}else if(i===p-1){o.push(s/p)}else{o.push(null)}}return o}

function superTrend(ohlc,period,mult){
  const len=ohlc.length;const st=new Array(len).fill(null);const dir=new Array(len).fill(1);const a=atr(ohlc,period);
  let pU=0,pD=0,pDir=1;
  for(let i=0;i<len;i++){if(a[i]==null)continue;const d=ohlc[i];const h=d.high||d.h,l=d.low||d.l,c=d.close||d.c;const mid=(h+l)/2;let up=mid-mult*a[i],dn=mid+mult*a[i];if(pU>0)up=Math.max(up,pU);if(pD>0)dn=Math.min(dn,pD);const d2=c>pD?1:c<pU?-1:pDir;st[i]=d2===1?up:dn;dir[i]=d2;pU=up;pD=dn;pDir=d2}
  return{values:st,direction:dir};
}

function edgeCloud(ohlc,cfg){
  const c=cfg||{};
  const st1=superTrend(ohlc,c.stPeriod||10,c.stMult||3);
  const walking=st1.values.slice();const walkDir=st1.direction.slice();
  const closes=ohlc.map(d=>d.close||d.c);
  const running=ema(closes,c.emaPeriod||21);
  return{walking,walkDir,running};
}

// ── Fetch OHLCV ──
async function fetchOHLCV(symbol,range){
  const wUrl=localStorage.getItem('zd_worker_url');
  if(!wUrl)throw new Error('Set zd_worker_url');
  const res=await fetch(wUrl,{method:'POST',headers:{'Content-Type':'application/json','X-Kite-Action':'yahoo-proxy'},body:JSON.stringify({ticker:`${symbol}.NS`,range:range||'6mo',interval:'1d'}),signal:AbortSignal.timeout(12000)});
  if(!res.ok)throw new Error('HTTP '+res.status);
  const data=await res.json();
  const result=data?.chart?.result?.[0];if(!result)throw new Error('No data');
  const ts=result.timestamp||[];const q=result.indicators?.quote?.[0]||{};
  const ohlc=[];
  for(let i=0;i<ts.length;i++){
    if(q.close[i]==null)continue;
    ohlc.push({time:Math.floor(ts[i]),open:q.open[i]||q.close[i],high:q.high[i]||q.close[i],low:q.low[i]||q.close[i],close:q.close[i],volume:q.volume?.[i]||0});
  }
  return ohlc;
}

// ── Main: Create EdgeCloud Chart ──
window.createEdgeCloudChart = async function(container, symbol, options) {
  const opts = Object.assign({
    height: 400,
    compact: false,
    showVolume: true,
    showIndicators: true,
    range: '6mo',
    stPeriod: 10,
    stMult: 3,
    emaPeriod: 21,
    darkTheme: true,
    onClick: null,  // callback(symbol) when chart is clicked
  }, options || {});

  const h = opts.compact ? Math.min(opts.height, 250) : opts.height;

  // Loading indicator
  container.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:${h}px;background:${opts.darkTheme?'#0d1117':'#fff'}"><div style="width:20px;height:20px;border:2px solid ${opts.darkTheme?'rgba(66,165,245,.15)':'rgba(66,165,245,.3)'};border-top:2px solid #42A5F5;border-radius:50%;animation:spin .8s linear infinite"></div></div>`;

  try {
    const ohlc = await fetchOHLCV(symbol, opts.range);
    if (!ohlc.length) throw new Error('Empty');

    container.innerHTML = '';
    const dk = opts.darkTheme;
    const bgCol = dk ? '#0d1117' : '#ffffff';

    const chart = LightweightCharts.createChart(container, {
      width: container.clientWidth || 600,
      height: h,
      layout: { background: { color: bgCol }, textColor: dk ? '#888' : '#333', fontFamily: 'IBM Plex Mono, monospace' },
      grid: { vertLines: { color: dk ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.06)' }, horzLines: { color: dk ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.06)' } },
      rightPriceScale: { borderColor: dk ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.08)' },
      timeScale: { borderColor: dk ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.08)', timeVisible: false },
      crosshair: { mode: opts.compact ? 1 : 0 },
    });

    // HLC bars
    const mainSeries = chart.addBarSeries({ upColor: '#26A69A', downColor: '#EF5350', thinBars: false, openVisible: false });
    mainSeries.setData(ohlc.map((d, i) => ({ time: d.time, open: i > 0 ? ohlc[i - 1].close : d.open, high: d.high, low: d.low, close: d.close })));

    // Volume
    if (opts.showVolume) {
      const vs = chart.addHistogramSeries({ priceFormat: { type: 'volume' }, priceScaleId: 'vol', color: '#26A69A' });
      vs.priceScale().applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } });
      vs.setData(ohlc.map(d => ({ time: d.time, value: d.volume, color: d.close >= d.open ? (dk ? 'rgba(38,166,154,0.3)' : 'rgba(38,166,154,0.45)') : (dk ? 'rgba(239,83,80,0.3)' : 'rgba(239,83,80,0.45)') })));
    }

    // EdgeCloud overlay
    if (opts.showIndicators) {
      const { walking, walkDir, running } = edgeCloud(ohlc, opts);

      // Walking Line (SuperTrend)
      const wkSeries = chart.addLineSeries({ lineWidth: opts.compact ? 1 : 2, lineStyle: LightweightCharts.LineStyle.Dashed, lastValueVisible: !opts.compact, priceLineVisible: false, color: '#FFA000' });
      const wkD = []; for (let i = 0; i < ohlc.length; i++) { if (walking[i] != null) wkD.push({ time: ohlc[i].time, value: walking[i] }) }
      wkSeries.setData(wkD);

      // Running Line (EMA)
      const rnSeries = chart.addLineSeries({ color: '#42A5F5', lineWidth: opts.compact ? 1 : 2, lineStyle: LightweightCharts.LineStyle.Solid, lastValueVisible: !opts.compact, priceLineVisible: false });
      const rnD = []; for (let i = 0; i < ohlc.length; i++) { if (running[i] != null) rnD.push({ time: ohlc[i].time, value: running[i] }) }
      rnSeries.setData(rnD);

      // Cloud fill between WL and RL
      const bullTop = [], bullBot = [], bearTop = [], bearBot = [];
      for (let i = 0; i < ohlc.length; i++) {
        if (walking[i] == null || running[i] == null) continue;
        const t = ohlc[i].time; const hi = Math.max(walking[i], running[i]); const lo = Math.min(walking[i], running[i]);
        if (walking[i] >= running[i]) { bullTop.push({ time: t, value: hi }); bullBot.push({ time: t, value: lo }); bearTop.push({ time: t, value: lo }); bearBot.push({ time: t, value: lo }); }
        else { bearTop.push({ time: t, value: hi }); bearBot.push({ time: t, value: lo }); bullTop.push({ time: t, value: lo }); bullBot.push({ time: t, value: lo }); }
      }
      const aOpts = { lineColor: 'transparent', lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false };
      chart.addAreaSeries({ ...aOpts, topColor: 'rgba(0,230,118,0.12)', bottomColor: 'rgba(0,230,118,0.12)' }).setData(bullTop);
      chart.addAreaSeries({ ...aOpts, topColor: bgCol, bottomColor: bgCol }).setData(bullBot);
      chart.addAreaSeries({ ...aOpts, topColor: 'rgba(239,83,80,0.12)', bottomColor: 'rgba(239,83,80,0.12)' }).setData(bearTop);
      chart.addAreaSeries({ ...aOpts, topColor: bgCol, bottomColor: bgCol }).setData(bearBot);
    }

    chart.timeScale().fitContent();

    // Click handler — open in techart.html
    if (opts.onClick) {
      container.style.cursor = 'pointer';
      container.addEventListener('click', () => opts.onClick(symbol));
    }

    // Resize observer
    const ro = new ResizeObserver(() => { if (container.clientWidth > 0) chart.resize(container.clientWidth, h) });
    ro.observe(container);

    return { chart, destroy: () => { ro.disconnect(); chart.remove() } };
  } catch (e) {
    container.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:${h}px;color:${opts.darkTheme?'#ff3d5a':'#d32f2f'};font:500 11px 'IBM Plex Mono',monospace">${e.message}</div>`;
    return null;
  }
};

})();
