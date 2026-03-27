/**
 * TradEdge — Corporate Actions Module
 * js/corporate-actions.js
 *
 * Fetches corporate actions/announcements for portfolio stocks
 * Uses Groq AI for sentiment analysis and trading signal generation
 *
 * Usage:
 *   CorpActions.fetchForSymbol('RELIANCE')      // Single stock
 *   CorpActions.fetchForPortfolio()              // All open positions
 *   CorpActions.renderPanel(containerId)         // Render UI panel
 *   CorpActions.analyzeWithAI(announcements)     // AI sentiment analysis
 */

window.CorpActions = (() => {
  'use strict';

  // ── Config ─────────────────────────────────────────────────
  const GROQ_MODEL = 'llama-3.3-70b-versatile';
  const CACHE_KEY = 'te_corp_actions_cache';
  const CACHE_TTL = 4 * 60 * 60 * 1000; // 4 hours

  // ── Get API Keys ───────────────────────────────────────────
  function getGroqKey() {
    return localStorage.getItem('groq_key') || '';
  }

  function getWorkerUrl() {
    return localStorage.getItem('zd_worker_url') || 'https://spring-fire-41a0.drrgware.workers.dev';
  }

  // ── Cache Management ───────────────────────────────────────
  function getCache() {
    try {
      const raw = localStorage.getItem(CACHE_KEY);
      if (!raw) return {};
      const cache = JSON.parse(raw);
      // Clean expired entries
      const now = Date.now();
      Object.keys(cache).forEach(k => {
        if (cache[k].expires < now) delete cache[k];
      });
      return cache;
    } catch { return {}; }
  }

  function setCache(symbol, data) {
    const cache = getCache();
    cache[symbol] = {
      data,
      expires: Date.now() + CACHE_TTL,
      fetched: new Date().toISOString()
    };
    localStorage.setItem(CACHE_KEY, JSON.stringify(cache));
  }

  function getCached(symbol) {
    const cache = getCache();
    return cache[symbol]?.data || null;
  }

  // ── Fetch from Screener.in via Worker ──────────────────────
  async function fetchFromScreener(symbol) {
    const workerUrl = getWorkerUrl();
    
    try {
      const res = await fetch(workerUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Kite-Action': 'screener'
        },
        body: JSON.stringify({ symbol })
      });

      if (!res.ok) {
        console.warn(`[CorpActions] Screener fetch failed for ${symbol}: ${res.status}`);
        return null;
      }

      const data = await res.json();
      return data;
    } catch (err) {
      console.error(`[CorpActions] Error fetching ${symbol}:`, err);
      return null;
    }
  }

  // ── Parse Announcements from Screener Data ─────────────────
  function parseAnnouncements(screenerData, symbol) {
    if (!screenerData) return [];
    
    const announcements = [];
    
    // Check if we have announcements in the data
    // Screener returns HTML, so we need to extract from it
    // For now, we'll use a simplified structure based on known patterns
    
    // If the data contains structured announcements
    if (screenerData.announcements && Array.isArray(screenerData.announcements)) {
      return screenerData.announcements.map(a => ({
        symbol,
        date: a.date,
        title: a.title,
        summary: a.summary || a.title,
        type: classifyAnnouncement(a.title),
        url: a.url,
        raw: a
      }));
    }
    
    // If data is HTML string, try to parse
    if (typeof screenerData === 'string' || screenerData.html) {
      const html = screenerData.html || screenerData;
      return parseAnnouncementsFromHTML(html, symbol);
    }
    
    return announcements;
  }

  // ── Parse HTML for Announcements ───────────────────────────
  function parseAnnouncementsFromHTML(html, symbol) {
    const announcements = [];
    
    // Simple regex patterns to extract announcements
    // Looking for patterns like: "1d - Summary text" or "18 Mar - Summary text"
    const patterns = [
      /(\d+[dh]|\d+\s+\w{3})\s*[-–]\s*([^<\n]+)/gi,
      /class="[^"]*announcement[^"]*"[^>]*>([^<]+)<\/[^>]+>\s*[-–]\s*([^<]+)/gi
    ];
    
    patterns.forEach(pattern => {
      let match;
      while ((match = pattern.exec(html)) !== null) {
        const date = match[1]?.trim();
        const text = match[2]?.trim();
        if (date && text && text.length > 10) {
          announcements.push({
            symbol,
            date,
            title: text.slice(0, 100),
            summary: text,
            type: classifyAnnouncement(text),
            raw: { date, text }
          });
        }
      }
    });
    
    return announcements.slice(0, 10); // Limit to recent 10
  }

  // ── Classify Announcement Type ─────────────────────────────
  function classifyAnnouncement(text) {
    const t = (text || '').toLowerCase();
    
    if (t.includes('dividend')) return 'dividend';
    if (t.includes('bonus') || t.includes('split')) return 'bonus';
    if (t.includes('buyback')) return 'buyback';
    if (t.includes('acquisition') || t.includes('acquire') || t.includes('merger')) return 'acquisition';
    if (t.includes('board meeting') || t.includes('result')) return 'results';
    if (t.includes('order') || t.includes('contract') || t.includes('agreement')) return 'order';
    if (t.includes('expansion') || t.includes('capex') || t.includes('capacity')) return 'expansion';
    if (t.includes('rating') || t.includes('upgrade') || t.includes('downgrade')) return 'rating';
    if (t.includes('fraud') || t.includes('penalty') || t.includes('violation')) return 'negative';
    if (t.includes('press release') || t.includes('media')) return 'press';
    
    return 'general';
  }

  // ── Get Type Badge Color ───────────────────────────────────
  function getTypeBadge(type) {
    const badges = {
      dividend: { bg: 'rgba(0,229,160,0.15)', color: '#00E5A0', label: '💰 Dividend' },
      bonus: { bg: 'rgba(0,229,160,0.15)', color: '#00E5A0', label: '🎁 Bonus/Split' },
      buyback: { bg: 'rgba(0,229,160,0.15)', color: '#00E5A0', label: '🔄 Buyback' },
      acquisition: { bg: 'rgba(59,139,255,0.15)', color: '#3B8BFF', label: '🤝 Acquisition' },
      results: { bg: 'rgba(245,197,66,0.15)', color: '#F5C542', label: '📊 Results' },
      order: { bg: 'rgba(59,139,255,0.15)', color: '#3B8BFF', label: '📝 Order/Contract' },
      expansion: { bg: 'rgba(0,229,160,0.15)', color: '#00E5A0', label: '🏭 Expansion' },
      rating: { bg: 'rgba(245,197,66,0.15)', color: '#F5C542', label: '⭐ Rating' },
      negative: { bg: 'rgba(255,69,96,0.15)', color: '#FF4560', label: '⚠️ Alert' },
      press: { bg: 'rgba(139,160,184,0.15)', color: '#8BA0B8', label: '📰 Press' },
      general: { bg: 'rgba(139,160,184,0.15)', color: '#8BA0B8', label: '📋 Update' }
    };
    return badges[type] || badges.general;
  }

  // ── AI Sentiment Analysis with Groq ────────────────────────
  async function analyzeWithAI(announcements, symbol) {
    const groqKey = getGroqKey();
    if (!groqKey) {
      console.warn('[CorpActions] No Groq API key set');
      return null;
    }

    if (!announcements.length) return null;

    const announcementText = announcements.slice(0, 5).map((a, i) => 
      `${i + 1}. [${a.date}] ${a.summary}`
    ).join('\n');

    const prompt = `Analyze these corporate announcements for ${symbol} (Indian stock).

Announcements:
${announcementText}

Respond ONLY in this exact JSON format (no markdown, no explanation):
{
  "signal": "BUY" or "SELL" or "HOLD" or "WATCH",
  "sentiment": "BULLISH" or "BEARISH" or "NEUTRAL",
  "confidence": 60-95,
  "summary": "One line summary under 80 chars",
  "risk": "One risk factor if any, else empty string"
}

Rules:
- BUY: Strong positive news (expansion, orders, good results, dividend)
- SELL: Negative news (fraud, loss, downgrade, penalty)
- HOLD: Mixed or routine announcements
- WATCH: Uncertain, needs monitoring
- Keep summary SHORT and actionable
- Be decisive, traders need clear signals`;

    try {
      const res = await fetch('https://api.groq.com/openai/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${groqKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          model: GROQ_MODEL,
          messages: [
            { role: 'system', content: 'You are a trading analyst. Give SHORT, decisive signals. Output ONLY valid JSON, no markdown.' },
            { role: 'user', content: prompt }
          ],
          temperature: 0.2,
          max_tokens: 200
        })
      });

      if (!res.ok) {
        console.error('[CorpActions] Groq API error:', res.status);
        return null;
      }

      const data = await res.json();
      const content = data.choices?.[0]?.message?.content || '';
      
      // Parse JSON from response
      const jsonMatch = content.match(/\{[\s\S]*\}/);
      if (jsonMatch) {
        const parsed = JSON.parse(jsonMatch[0]);
        // Normalize values
        return {
          signal: (parsed.signal || 'HOLD').toUpperCase(),
          sentiment: (parsed.sentiment || 'NEUTRAL').toUpperCase(),
          confidence: parseInt(parsed.confidence) || 70,
          summary: parsed.summary || '',
          risks: parsed.risk ? [parsed.risk] : []
        };
      }
      
      return null;
    } catch (err) {
      console.error('[CorpActions] AI analysis error:', err);
      return null;
    }
  }

  // ── Fetch for Single Symbol ────────────────────────────────
  async function fetchForSymbol(symbol, forceRefresh = false) {
    if (!forceRefresh) {
      const cached = getCached(symbol);
      if (cached) {
        console.log(`[CorpActions] Using cached data for ${symbol}`);
        return cached;
      }
    }

    console.log(`[CorpActions] Fetching announcements for ${symbol}`);
    const screenerData = await fetchFromScreener(symbol);
    
    if (!screenerData) {
      return { symbol, announcements: [], aiAnalysis: null, error: 'Fetch failed' };
    }

    const announcements = parseAnnouncements(screenerData, symbol);
    
    // Run AI analysis if we have announcements
    let aiAnalysis = null;
    if (announcements.length > 0 && getGroqKey()) {
      aiAnalysis = await analyzeWithAI(announcements, symbol);
    }

    const result = {
      symbol,
      announcements,
      aiAnalysis,
      fetchedAt: new Date().toISOString()
    };

    setCache(symbol, result);
    return result;
  }

  // ── Fetch for All Portfolio Stocks ─────────────────────────
  async function fetchForPortfolio() {
    const openTrades = typeof TE !== 'undefined' ? TE.getOpenTrades() : [];
    const symbols = [...new Set(openTrades.map(t => t.symbol))];
    
    if (!symbols.length) {
      console.log('[CorpActions] No open positions');
      return [];
    }

    console.log(`[CorpActions] Fetching for ${symbols.length} symbols:`, symbols);
    
    const results = [];
    for (const symbol of symbols) {
      const data = await fetchForSymbol(symbol);
      results.push(data);
      // Small delay to avoid rate limiting
      await new Promise(r => setTimeout(r, 500));
    }

    return results;
  }

  // ── Get Signal Color ───────────────────────────────────────
  function getSignalStyle(signal, sentiment) {
    const styles = {
      BUY: { bg: 'rgba(0,229,160,0.2)', border: '#00E5A0', color: '#00E5A0', icon: '🟢' },
      SELL: { bg: 'rgba(255,69,96,0.2)', border: '#FF4560', color: '#FF4560', icon: '🔴' },
      HOLD: { bg: 'rgba(245,197,66,0.2)', border: '#F5C542', color: '#F5C542', icon: '🟡' },
      WATCH: { bg: 'rgba(59,139,255,0.2)', border: '#3B8BFF', color: '#3B8BFF', icon: '👁️' }
    };
    return styles[signal] || styles.HOLD;
  }

  // ── Render Panel ───────────────────────────────────────────
  function renderPanel(containerId, data) {
    const container = document.getElementById(containerId);
    if (!container) return;

    if (!data || !data.length) {
      container.innerHTML = `
        <div style="text-align:center;padding:20px;color:var(--t3)">
          <div style="font-size:24px;margin-bottom:8px">📰</div>
          <div>No corporate actions data</div>
          <button class="btn btn-ghost btn-xs" onclick="CorpActions.refresh()" style="margin-top:12px">Refresh</button>
        </div>
      `;
      return;
    }

    let html = '';
    
    data.forEach(item => {
      const { symbol, announcements, aiAnalysis } = item;
      
      if (!announcements.length && !aiAnalysis) return;

      // AI Analysis Card
      let aiCard = '';
      if (aiAnalysis) {
        const signalStyle = getSignalStyle(aiAnalysis.signal, aiAnalysis.sentiment);
        aiCard = `
          <div style="background:${signalStyle.bg};border:1px solid ${signalStyle.border};border-radius:8px;padding:12px;margin-bottom:12px">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
              <div style="display:flex;align-items:center;gap:8px">
                <span style="font-size:18px">${signalStyle.icon}</span>
                <span style="font-weight:700;color:${signalStyle.color}">${aiAnalysis.signal}</span>
                <span style="font-size:10px;color:var(--t3);background:var(--bg3);padding:2px 6px;border-radius:4px">${aiAnalysis.sentiment}</span>
              </div>
              <div style="font-size:10px;color:var(--t3)">Confidence: ${aiAnalysis.confidence || '—'}%</div>
            </div>
            <div style="font-size:11px;color:var(--t2);margin-bottom:8px">${aiAnalysis.summary || aiAnalysis.impact || ''}</div>
            ${aiAnalysis.highlights?.length ? `
              <div style="font-size:10px;color:var(--t3)">
                ${aiAnalysis.highlights.map(h => `<div style="margin-bottom:2px">• ${h}</div>`).join('')}
              </div>
            ` : ''}
            ${aiAnalysis.risks?.length ? `
              <div style="font-size:10px;color:var(--r);margin-top:6px">
                ⚠️ ${aiAnalysis.risks.join(' | ')}
              </div>
            ` : ''}
          </div>
        `;
      }

      // Announcements List
      const announcementsList = announcements.slice(0, 5).map(a => {
        const badge = getTypeBadge(a.type);
        return `
          <div style="display:flex;gap:10px;padding:8px 0;border-bottom:1px solid var(--b1)">
            <div style="font-size:10px;color:var(--t4);min-width:50px">${a.date}</div>
            <div style="flex:1">
              <span style="font-size:9px;padding:2px 6px;border-radius:4px;background:${badge.bg};color:${badge.color};margin-right:6px">${badge.label}</span>
              <span style="font-size:11px;color:var(--t2)">${a.summary?.slice(0, 150)}${a.summary?.length > 150 ? '...' : ''}</span>
            </div>
          </div>
        `;
      }).join('');

      html += `
        <div style="background:var(--bg2);border:1px solid var(--b1);border-radius:10px;padding:14px;margin-bottom:12px">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">
            <div style="width:36px;height:36px;border-radius:8px;background:var(--bg3);display:flex;align-items:center;justify-content:center;font-weight:700;color:var(--t1);font-size:11px">${symbol.slice(0,3)}</div>
            <div style="flex:1">
              <div style="font-weight:700;color:var(--t1)">${symbol}</div>
              <div style="font-size:10px;color:var(--t4)">${announcements.length} recent announcement${announcements.length !== 1 ? 's' : ''}</div>
            </div>
            <button class="btn btn-ghost btn-xs" onclick="CorpActions.fetchForSymbol('${symbol}', true).then(() => CorpActions.refreshUI())" title="Refresh">↻</button>
          </div>
          ${aiCard}
          <div style="max-height:200px;overflow-y:auto">
            ${announcementsList || '<div style="font-size:11px;color:var(--t4);padding:10px 0">No recent announcements</div>'}
          </div>
        </div>
      `;
    });

    container.innerHTML = html || '<div style="text-align:center;padding:20px;color:var(--t3)">No announcements found</div>';
  }

  // ── Refresh UI ─────────────────────────────────────────────
  let _panelContainer = null;
  let _panelData = [];

  async function refresh() {
    _panelData = await fetchForPortfolio();
    if (_panelContainer) {
      renderPanel(_panelContainer, _panelData);
    }
    return _panelData;
  }

  function refreshUI() {
    if (_panelContainer && _panelData.length) {
      // Re-read from cache
      const symbols = _panelData.map(d => d.symbol);
      _panelData = symbols.map(s => getCached(s)).filter(Boolean);
      renderPanel(_panelContainer, _panelData);
    }
  }

  function init(containerId) {
    _panelContainer = containerId;
    refresh();
  }

  // ── Public API ─────────────────────────────────────────────
  return {
    fetchForSymbol,
    fetchForPortfolio,
    analyzeWithAI,
    renderPanel,
    refresh,
    refreshUI,
    init,
    getCache,
    clearCache: () => localStorage.removeItem(CACHE_KEY),
    setGroqKey: (key) => localStorage.setItem('groq_key', key)
  };
})();

console.log('[CorpActions] Module loaded');
