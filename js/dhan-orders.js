/**
 * TradEdge — Dhan Orders Module v2
 * js/dhan-orders.js
 *
 * Order placement via Dhan API through Cloudflare Worker.
 *
 * Worker actions used:
 *   X-Kite-Action: dhan-place   → POST order
 *   X-Kite-Action: dhan-orders  → GET  order list
 *   X-Kite-Action: dhan-cancel  → POST { orderId }
 *   X-Kite-Action: dhan-positions → GET
 *   X-Kite-Action: dhan-holdings  → GET
 *   X-Kite-Action: dhan-margins   → GET
 *   X-Kite-Action: dhan-forever-place/list/modify/delete
 *
 * Auth: X-Dhan-Token, X-Dhan-ID headers
 * Requires: DhanLive (for symbol → securityId mapping)
 */

window.DhanOrders = (() => {

  function getCreds() {
    const url = (localStorage.getItem('zd_worker_url') || '').replace(/\/$/, '');
    const tk = localStorage.getItem('dhan_tk') || '';
    const id = localStorage.getItem('dhan_id') || '';
    if (!url || !tk || !id) throw new Error('Dhan credentials not configured. Set dhan_id + dhan_tk in Settings.');
    return { url, tk, id };
  }

  function getSecId(symbol) {
    if (!window.DhanLive) throw new Error('DhanLive module not loaded');
    const info = DhanLive.getSymbolInfo(symbol);
    if (!info) throw new Error(`Unknown symbol: ${symbol}. Ensure DhanLive is initialized.`);
    return info; // { secId, exch }
  }

  async function workerCall(action, body = null) {
    const { url, tk, id } = getCreds();
    const headers = {
      'Content-Type': 'application/json',
      'X-Kite-Action': action,
      'X-Dhan-Token': tk,
      'X-Dhan-ID': id
    };
    const opts = { method: 'POST', headers };
    if (body) opts.body = JSON.stringify(body);

    const res = await fetch(url, opts);
    const data = await res.json();

    if (data?.error || data?.status === 'error') {
      throw new Error(data.remarks || data.error || data.message || `Worker ${action} failed`);
    }
    return data;
  }

  // --- Order Placement ---

  async function placeOrder({ symbol, qty, price = 0, side = 'BUY', product = 'CNC', orderType = null }) {
    const { secId, exch } = getSecId(symbol);
    const oType = orderType || (price > 0 ? 'LIMIT' : 'MARKET');

    const payload = {
      symbol,
      securityId: String(secId),
      exchange: exch || 'NSE_EQ',
      transaction_type: side.toUpperCase(),
      order_type: oType,
      product,
      quantity: qty,
      price: oType === 'MARKET' ? 0 : price,
      trigger_price: 0,
      validity: 'DAY'
    };

    console.log('[DhanOrders] Placing:', payload);
    const result = await workerCall('dhan-place', payload);

    window.dispatchEvent(new CustomEvent('dhan-order-placed', {
      detail: { symbol, side, qty, price, result }
    }));

    return {
      success: true,
      orderId: result.orderId,
      orderStatus: result.orderStatus || 'PENDING',
      raw: result
    };
  }

  async function placeSLOrder({ symbol, qty, triggerPrice, side = 'SELL', product = 'CNC' }) {
    const { secId, exch } = getSecId(symbol);

    return await workerCall('dhan-place', {
      symbol,
      securityId: String(secId),
      exchange: exch || 'NSE_EQ',
      transaction_type: side.toUpperCase(),
      order_type: 'SL-M',
      product,
      quantity: qty,
      price: 0,
      trigger_price: triggerPrice,
      validity: 'DAY'
    });
  }

  async function placeBracketOrder({ symbol, qty, price = 0, sl, target = null, product = 'CNC', side = 'BUY' }) {
    if (!sl) throw new Error('Stop-loss required');

    const results = { entry: null, stopLoss: null, target: null, errors: [] };

    // Entry
    try {
      results.entry = await placeOrder({ symbol, qty, price, side, product });
    } catch (err) {
      results.errors.push(`Entry: ${err.message}`);
      return results;
    }

    // SL-M (opposite side)
    const slSide = side === 'BUY' ? 'SELL' : 'BUY';
    try {
      const slResult = await placeSLOrder({ symbol, qty, triggerPrice: sl, side: slSide, product });
      results.stopLoss = { success: true, orderId: slResult.orderId, trigger: sl, raw: slResult };
    } catch (err) {
      results.errors.push(`SL: ${err.message}`);
    }

    // Target (LIMIT, opposite side)
    if (target) {
      try {
        results.target = await placeOrder({ symbol, qty, price: target, side: slSide, product });
      } catch (err) {
        results.errors.push(`Target: ${err.message}`);
      }
    }

    window.dispatchEvent(new CustomEvent('dhan-bracket-placed', { detail: { symbol, results } }));
    return results;
  }

  async function placePyramidOrder({ symbol, qty, price = 0, sl, pyramidLevel = 'P2', product = 'CNC' }) {
    const result = await placeOrder({ symbol, qty, price, product });

    // Update te_trades pyramid metadata
    try {
      const trades = JSON.parse(localStorage.getItem('te_trades') || '[]');
      const trade = trades.find(t => t.symbol === symbol && t.status === 'Open');
      if (trade) {
        if (!trade._epPyramid) trade._epPyramid = [];
        trade._epPyramid.push({
          level: pyramidLevel,
          date: new Date().toISOString().slice(0, 10),
          price: price || DhanLive?.getCMP(symbol) || 0,
          qty,
          orderId: result.orderId
        });
        localStorage.setItem('te_trades', JSON.stringify(trades));
      }
    } catch {}

    return result;
  }

  // --- Dhan Forever Orders (GTT equivalent) ---

  async function placeForeverSL({ symbol, qty, triggerPrice, price = 0, product = 'CNC', side = 'SELL' }) {
    const { secId, exch } = getSecId(symbol);
    return await workerCall('dhan-forever-place', {
      securityId: String(secId),
      exchange: exch || 'NSE_EQ',
      transaction_type: side,
      order_type: price > 0 ? 'LIMIT' : 'SL-M',
      product,
      quantity: qty,
      price,
      trigger_price: triggerPrice,
      type: 'SINGLE'
    });
  }

  async function placeForeverOCO({ symbol, qty, slTrigger, slPrice = 0, targetPrice, product = 'CNC' }) {
    const { secId, exch } = getSecId(symbol);
    return await workerCall('dhan-forever-place', {
      securityId: String(secId),
      exchange: exch || 'NSE_EQ',
      transaction_type: 'SELL',
      order_type: 'LIMIT',
      product,
      quantity: qty,
      price: targetPrice,
      trigger_price: 0,
      type: 'OCO',
      slTrigger,
      slPrice: slPrice || 0,
      qty1: qty
    });
  }

  // --- Queries ---

  async function getOrders() { return await workerCall('dhan-orders', {}); }
  async function getPositions() { return await workerCall('dhan-positions', {}); }
  async function getHoldings() { return await workerCall('dhan-holdings', {}); }
  async function getMargins() { return await workerCall('dhan-margins', {}); }
  async function getForeverOrders() { return await workerCall('dhan-forever-list', {}); }

  async function cancelOrder(orderId) {
    return await workerCall('dhan-cancel', { orderId });
  }

  async function cancelForever(orderId) {
    return await workerCall('dhan-forever-delete', { orderId });
  }

  // --- Telegram ---
  async function notifyTelegram(msg) {
    try {
      await fetch(`https://api.telegram.org/bot8659936599:AAGoa-eJV35BxhLUogzel_0JcLp6d8yZz2U/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: '183752078', text: msg, parse_mode: 'HTML' })
      });
    } catch (err) {
      console.warn('[DhanOrders] Telegram failed:', err.message);
    }
  }

  // --- Public API ---
  return {
    placeOrder,
    placeSLOrder,
    placeBracketOrder,
    placePyramidOrder,
    placeForeverSL,
    placeForeverOCO,
    getOrders,
    getPositions,
    getHoldings,
    getMargins,
    getForeverOrders,
    cancelOrder,
    cancelForever,
    notifyTelegram,

    /**
     * Full Edge Pilot execution flow:
     * Place order + SL + Telegram alert
     */
    async executeFromEdgePilot({ symbol, qty, price, sl, target, product = 'CNC', pyramidLevel = null }) {
      const label = pyramidLevel ? `${pyramidLevel} ADD` : 'ENTRY';

      try {
        let result;
        if (pyramidLevel) {
          const r = await placePyramidOrder({ symbol, qty, price, sl, pyramidLevel, product });
          result = { entry: r, stopLoss: null, target: null, errors: [] };
        } else {
          result = await placeBracketOrder({ symbol, qty, price, sl, target, product });
        }

        const entryId = result.entry?.orderId || 'N/A';
        const slId = result.stopLoss?.orderId || 'N/A';
        const errs = result.errors.length ? `\n⚠️ ${result.errors.join(', ')}` : '';

        await notifyTelegram(
          `🟢 <b>EDGE PILOT — ${label}</b>\n` +
          `📊 ${symbol}\n` +
          `💰 Qty: ${qty} @ ₹${price || 'MKT'}\n` +
          `🛡️ SL: ₹${sl}\n` +
          (target ? `🎯 Target: ₹${target}\n` : '') +
          `📋 Entry: #${entryId}\n` +
          `📋 SL: #${slId}${errs}`
        );

        return result;
      } catch (err) {
        await notifyTelegram(`🔴 <b>ORDER FAILED</b>\n📊 ${symbol}\n❌ ${err.message}`);
        throw err;
      }
    },

    isReady() {
      try { getCreds(); return true; } catch { return false; }
    }
  };
})();
