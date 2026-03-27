/**
 * TradEdge — Dhan Orders Module
 * js/dhan-orders.js
 *
 * Place and manage orders via Dhan API through Cloudflare Worker.
 * Integrates with Edge Pilot's executeOrder() flow.
 *
 * Usage:
 *   const result = await DhanOrders.placeBracketOrder({
 *     symbol: 'RELIANCE',
 *     qty: 10,
 *     price: 1234.50,      // limit price (0 = market)
 *     sl: 1200,            // stop-loss trigger
 *     target: 1300,        // optional target
 *     product: 'CNC'       // CNC (delivery) | MIS (intraday)
 *   });
 *
 *   const orders = await DhanOrders.getOrders();
 *   const positions = await DhanOrders.getPositions();
 *   const holdings = await DhanOrders.getHoldings();
 *
 * Requires: DhanLive to be initialized (for symbol → securityId mapping)
 * localStorage: dhan_id, dhan_tk, zd_worker_url
 */

window.DhanOrders = (() => {
  // --- Config ---
  const ORDER_TYPES = {
    MARKET: 'MARKET',
    LIMIT: 'LIMIT',
    SL: 'STOP_LOSS',
    SLM: 'STOP_LOSS_MARKET'
  };

  const PRODUCT_TYPES = {
    CNC: 'CNC',       // Delivery
    MIS: 'INTRADAY',  // Intraday (Dhan uses INTRADAY not MIS)
    BO: 'BO',         // Bracket Order
    CO: 'CO'          // Cover Order
  };

  // --- Helpers ---
  function getCreds() {
    const workerUrl = (localStorage.getItem('zd_worker_url') || '').replace(/\/$/, '');
    const token = localStorage.getItem('dhan_tk') || '';
    const clientId = localStorage.getItem('dhan_id') || '';
    if (!workerUrl || !token || !clientId) {
      throw new Error('Dhan credentials not configured. Set dhan_id, dhan_tk, zd_worker_url in Settings.');
    }
    return { workerUrl, token, clientId };
  }

  function getSecurityId(symbol) {
    if (!window.DhanLive) throw new Error('DhanLive module not loaded');
    const info = DhanLive.getSymbolInfo(symbol);
    if (!info) throw new Error(`Unknown symbol: ${symbol}. Symbol map may not be loaded.`);
    return info;
  }

  async function dhanRequest(endpoint, method = 'GET', body = null) {
    const { workerUrl, token, clientId } = getCreds();
    const url = `${workerUrl}${endpoint}?token=${encodeURIComponent(token)}&client_id=${encodeURIComponent(clientId)}`;

    const opts = {
      method,
      headers: { 'Content-Type': 'application/json' }
    };
    if (body) opts.body = JSON.stringify(body);

    const resp = await fetch(url, opts);
    const data = await resp.json();

    if (!resp.ok) {
      const msg = data?.remarks || data?.error || data?.message || `HTTP ${resp.status}`;
      throw new Error(`Dhan API error: ${msg}`);
    }

    return data;
  }

  // --- Order Placement ---

  /**
   * Place a simple order (LIMIT or MARKET).
   */
  async function placeOrder({
    symbol,
    qty,
    price = 0,
    side = 'BUY',
    product = 'CNC',
    orderType = null
  }) {
    const { secId, exchange } = getSecurityId(symbol);

    const oType = orderType || (price > 0 ? ORDER_TYPES.LIMIT : ORDER_TYPES.MARKET);
    const dhanProduct = PRODUCT_TYPES[product] || product;

    const payload = {
      dhanClientId: getCreds().clientId,
      transactionType: side.toUpperCase(),
      exchangeSegment: exchange,
      productType: dhanProduct,
      orderType: oType,
      validity: 'DAY',
      securityId: String(secId),
      quantity: qty,
      price: oType === ORDER_TYPES.MARKET ? 0 : price,
      triggerPrice: 0,
      disclosedQuantity: 0,
      afterMarketOrder: false
    };

    console.log('[DhanOrders] Placing order:', payload);
    const result = await dhanRequest('/dhan-orders', 'POST', payload);

    // Log to console and dispatch event
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

  /**
   * Place a bracket order: Entry + SL + optional Target.
   * For CNC (delivery), this places separate orders since Dhan BO is intraday-only.
   */
  async function placeBracketOrder({
    symbol,
    qty,
    price = 0,
    sl,
    target = null,
    product = 'CNC',
    side = 'BUY'
  }) {
    if (!sl) throw new Error('Stop-loss is required for bracket orders');

    const results = {
      entry: null,
      stopLoss: null,
      target: null,
      errors: []
    };

    // --- Entry Order ---
    try {
      results.entry = await placeOrder({ symbol, qty, price, side, product });
    } catch (err) {
      results.errors.push(`Entry failed: ${err.message}`);
      return results;
    }

    // --- Stop-Loss Order (SL-M) ---
    // For delivery (CNC), place a separate SL-M sell order
    // The SL order uses the opposite side
    const slSide = side === 'BUY' ? 'SELL' : 'BUY';
    try {
      const { secId, exchange } = getSecurityId(symbol);
      const slPayload = {
        dhanClientId: getCreds().clientId,
        transactionType: slSide,
        exchangeSegment: exchange,
        productType: PRODUCT_TYPES[product] || product,
        orderType: ORDER_TYPES.SLM,
        validity: 'DAY',
        securityId: String(secId),
        quantity: qty,
        price: 0,
        triggerPrice: sl,
        disclosedQuantity: 0,
        afterMarketOrder: false
      };

      const slResult = await dhanRequest('/dhan-orders', 'POST', slPayload);
      results.stopLoss = {
        success: true,
        orderId: slResult.orderId,
        triggerPrice: sl,
        raw: slResult
      };
    } catch (err) {
      results.errors.push(`SL order failed: ${err.message}`);
    }

    // --- Target Order (LIMIT) ---
    if (target) {
      try {
        results.target = await placeOrder({
          symbol,
          qty,
          price: target,
          side: slSide,
          product
        });
      } catch (err) {
        results.errors.push(`Target order failed: ${err.message}`);
      }
    }

    // Dispatch event
    window.dispatchEvent(new CustomEvent('dhan-bracket-placed', {
      detail: { symbol, results }
    }));

    return results;
  }

  // --- Pyramid Add (additional entry at higher price) ---
  async function placePyramidOrder({
    symbol,
    qty,
    price = 0,
    sl,
    pyramidLevel = 'P2',
    product = 'CNC'
  }) {
    const result = await placeOrder({ symbol, qty, price, product });

    // Update te_trades with pyramid metadata
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

  // --- Order/Position/Holdings Queries ---

  async function getOrders() {
    return await dhanRequest('/dhan-orders', 'GET');
  }

  async function getPositions() {
    return await dhanRequest('/dhan-positions', 'GET');
  }

  async function getHoldings() {
    return await dhanRequest('/dhan-holdings', 'GET');
  }

  /**
   * Cancel an order by orderId.
   */
  async function cancelOrder(orderId) {
    const { workerUrl, token, clientId } = getCreds();
    // Dhan cancel is DELETE /v2/orders/{orderId}
    const url = `${workerUrl}/dhan-orders?token=${encodeURIComponent(token)}&client_id=${encodeURIComponent(clientId)}&order_id=${orderId}`;
    const resp = await fetch(url, { method: 'DELETE' });
    return await resp.json();
  }

  // --- Telegram Notification ---
  async function notifyTelegram(message) {
    try {
      const botToken = '8659936599';
      const chatId = '183752078';
      // Use worker to avoid CORS, or direct if allowed
      const url = `https://api.telegram.org/bot${botToken}/sendMessage`;
      await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: chatId,
          text: message,
          parse_mode: 'HTML'
        })
      });
    } catch (err) {
      console.warn('[DhanOrders] Telegram notification failed:', err.message);
    }
  }

  // --- Public API ---
  return {
    placeOrder,
    placeBracketOrder,
    placePyramidOrder,
    getOrders,
    getPositions,
    getHoldings,
    cancelOrder,
    notifyTelegram,

    /**
     * Full execution flow for Edge Pilot.
     * Places order + SL + optional target, logs to te_trades, sends Telegram alert.
     */
    async executeFromEdgePilot({
      symbol, qty, price, sl, target, product = 'CNC', pyramidLevel = null
    }) {
      const actionLabel = pyramidLevel ? `${pyramidLevel} ADD` : 'ENTRY';

      try {
        let result;
        if (pyramidLevel) {
          result = await placePyramidOrder({ symbol, qty, price, sl, pyramidLevel, product });
          result = { entry: result, stopLoss: null, target: null, errors: [] };
        } else {
          result = await placeBracketOrder({ symbol, qty, price, sl, target, product });
        }

        // Build Telegram message
        const entryId = result.entry?.orderId || 'N/A';
        const slId = result.stopLoss?.orderId || 'N/A';
        const errors = result.errors.length ? `\n⚠️ ${result.errors.join(', ')}` : '';

        const msg = `🟢 <b>EDGE PILOT — ${actionLabel}</b>\n` +
          `📊 ${symbol}\n` +
          `💰 Qty: ${qty} @ ₹${price || 'MKT'}\n` +
          `🛡️ SL: ₹${sl}\n` +
          (target ? `🎯 Target: ₹${target}\n` : '') +
          `📋 Entry: #${entryId}\n` +
          `📋 SL: #${slId}${errors}`;

        await notifyTelegram(msg);

        return result;
      } catch (err) {
        // Notify failure
        await notifyTelegram(
          `🔴 <b>ORDER FAILED</b>\n📊 ${symbol}\n❌ ${err.message}`
        );
        throw err;
      }
    },

    /**
     * Check if Dhan order placement is available.
     */
    isReady() {
      try {
        getCreds();
        return true;
      } catch {
        return false;
      }
    }
  };
})();
