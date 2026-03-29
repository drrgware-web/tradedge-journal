// ═══════════════════════════════════════════════════════════════════════════════
// TradEdge × OpenAlgo Integration Module v1.0
// ═══════════════════════════════════════════════════════════════════════════════
// Replaces: js/dhan-live.js (LTP polling + Yahoo fallback)
//           js/dhan-orders.js (bracket/pyramid/Forever orders)
//
// Provides: Unified broker-agnostic order management, real-time LTP via
//           WebSocket + REST fallback, position/funds/holdings queries,
//           and Telegram alerts — all through OpenAlgo's unified API.
//
// Architecture:
//   Browser → Cloudflare Worker (CORS proxy, action=openalgo) → OpenAlgo Server
//   Browser → WebSocket (direct to OpenAlgo WSS for real-time LTP)
//
// Dependencies: None (vanilla JS, browser-native fetch + WebSocket)
// ═══════════════════════════════════════════════════════════════════════════════

(function (window) {
    'use strict';

    // ─────────────────────────────────────────────────────────────────────────
    // Configuration — pulled from localStorage, overridable per-session
    // ─────────────────────────────────────────────────────────────────────────
    const CONFIG_KEY = 'te_openalgo_config';

    const DEFAULT_CONFIG = {
        // OpenAlgo host (your self-hosted instance)
        host: 'http://127.0.0.1:5000',
        // OpenAlgo API key (from OpenAlgo dashboard after login)
        apiKey: '',
        // WebSocket URL for real-time LTP streaming
        // NOTE: Dhan Data API not subscribed, so WS LTP won't work.
        // Market data stays on existing dhan-live.js pipeline.
        wsUrl: 'ws://127.0.0.1:8765',
        // Cloudflare Worker URL (existing TradEdge worker)
        workerUrl: 'https://spring-fire-41a0.drrgware.workers.dev',
        // Default strategy name tagged on all orders
        strategy: 'TradEdge',
        // Default product type: CNC (delivery) or MIS (intraday)
        defaultProduct: 'CNC',
        // Use Worker as CORS proxy (true) or call OpenAlgo directly (false)
        // Set false when running locally (MacBook/Dell) on same machine
        useWorkerProxy: false,
        // Telegram alerts via OpenAlgo's built-in Telegram integration
        telegramEnabled: true,
        // OpenAlgo login username (needed for Telegram API)
        openalgoUsername: '',
        // Fallback: use REST polling if WebSocket unavailable
        restPollIntervalMs: 5000,
        // Skip market data APIs (Dhan Data API not subscribed)
        // When true, all LTP/quote calls fall through to dhan-live.js
        skipMarketData: true,
    };

    function loadConfig() {
        try {
            const saved = JSON.parse(localStorage.getItem(CONFIG_KEY) || '{}');
            return { ...DEFAULT_CONFIG, ...saved };
        } catch {
            return { ...DEFAULT_CONFIG };
        }
    }

    function saveConfig(cfg) {
        localStorage.setItem(CONFIG_KEY, JSON.stringify(cfg));
    }

    let _config = loadConfig();

    // ─────────────────────────────────────────────────────────────────────────
    // HTTP Transport — routes through Worker proxy or direct to OpenAlgo
    // ─────────────────────────────────────────────────────────────────────────

    async function apiCall(endpoint, body = {}, method = 'POST') {
        // Inject API key into every request body
        const payload = { apikey: _config.apiKey, ...body };

        let url, fetchOpts;

        if (_config.useWorkerProxy) {
            // Route through Cloudflare Worker with action=openalgo
            url = _config.workerUrl;
            fetchOpts = {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Kite-Action': 'openalgo',
                },
                body: JSON.stringify({
                    endpoint: `/api/v1/${endpoint}`,
                    method,
                    payload,
                }),
            };
        } else {
            // Direct call to OpenAlgo (requires CORS or same-origin)
            url = `${_config.host}/api/v1/${endpoint}`;
            fetchOpts = {
                method,
                headers: { 'Content-Type': 'application/json' },
                body: method !== 'GET' ? JSON.stringify(payload) : undefined,
            };
        }

        try {
            const resp = await fetch(url, fetchOpts);
            if (!resp.ok) {
                const errText = await resp.text().catch(() => resp.statusText);
                throw new Error(`OpenAlgo API error ${resp.status}: ${errText}`);
            }
            return await resp.json();
        } catch (err) {
            console.error(`[OpenAlgo] ${endpoint} failed:`, err);
            throw err;
        }
    }

    // GET variant (for endpoints that accept query params)
    async function apiGet(endpoint, params = {}) {
        const qs = new URLSearchParams({ apikey: _config.apiKey, ...params }).toString();

        if (_config.useWorkerProxy) {
            return apiCall(endpoint, params, 'GET');
        }

        const url = `${_config.host}/api/v1/${endpoint}?${qs}`;
        try {
            const resp = await fetch(url);
            if (!resp.ok) throw new Error(`OpenAlgo GET error ${resp.status}`);
            return await resp.json();
        } catch (err) {
            console.error(`[OpenAlgo] GET ${endpoint} failed:`, err);
            throw err;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ORDERS API — replaces dhan-orders.js
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Place a standard order.
     * @param {Object} opts
     * @param {string} opts.symbol    - NSE symbol (e.g. "SAIL", "RELIANCE")
     * @param {string} opts.action    - "BUY" or "SELL"
     * @param {number} opts.quantity  - Number of shares
     * @param {string} [opts.exchange="NSE"]
     * @param {string} [opts.product]     - "CNC", "MIS", "NRML"
     * @param {string} [opts.priceType="MARKET"] - "MARKET", "LIMIT", "SL", "SL-M"
     * @param {number} [opts.price=0]
     * @param {number} [opts.triggerPrice=0]
     * @param {string} [opts.strategy]
     * @returns {Promise<{orderid: string, status: string}>}
     */
    async function placeOrder(opts) {
        const body = {
            strategy: opts.strategy || _config.strategy,
            symbol: opts.symbol,
            action: opts.action,
            exchange: opts.exchange || 'NSE',
            pricetype: opts.priceType || 'MARKET',
            product: opts.product || _config.defaultProduct,
            quantity: String(opts.quantity),
            price: String(opts.price || 0),
            trigger_price: String(opts.triggerPrice || 0),
            disclosed_quantity: String(opts.disclosedQuantity || 0),
        };
        const result = await apiCall('placeorder', body);
        _emitEvent('orderPlaced', result);
        return result;
    }

    /**
     * Place a Smart Order — auto-matches position size for pyramids.
     * Replaces the manual P1/P2/P3 delta calculation in dhan-orders.js.
     *
     * Example: You want total position of 100 shares. You currently hold 50.
     *   placeSmartOrder({ symbol: "SAIL", action: "BUY", quantity: 50, positionSize: 100 })
     *   → OpenAlgo calculates: need to BUY 50 more to reach positionSize=100.
     *
     * @param {Object} opts
     * @param {string} opts.symbol
     * @param {string} opts.action     - "BUY" or "SELL"
     * @param {number} opts.quantity   - Order quantity
     * @param {number} opts.positionSize - Target net position
     * @param {string} [opts.exchange="NSE"]
     * @param {string} [opts.product]
     * @param {string} [opts.priceType="MARKET"]
     * @param {number} [opts.price=0]
     * @param {number} [opts.triggerPrice=0]
     * @param {string} [opts.strategy]
     * @returns {Promise<{orderid: string, status: string}>}
     */
    async function placeSmartOrder(opts) {
        const body = {
            strategy: opts.strategy || _config.strategy,
            symbol: opts.symbol,
            action: opts.action,
            exchange: opts.exchange || 'NSE',
            pricetype: opts.priceType || 'MARKET',
            product: opts.product || _config.defaultProduct,
            quantity: String(opts.quantity),
            position_size: String(opts.positionSize),
            price: String(opts.price || 0),
            trigger_price: String(opts.triggerPrice || 0),
            disclosed_quantity: String(opts.disclosedQuantity || 0),
        };
        const result = await apiCall('placesmartorder', body);
        _emitEvent('smartOrderPlaced', result);
        return result;
    }

    /**
     * Place a basket of orders (multiple symbols at once).
     * Replaces manual sequential order placement in Edge Pilot.
     * @param {Array<Object>} orders - Array of order objects
     * @returns {Promise<{status: string, results: Array}>}
     */
    async function placeBasketOrder(orders) {
        const body = {
            orders: orders.map(o => ({
                symbol: o.symbol,
                exchange: o.exchange || 'NSE',
                action: o.action,
                quantity: String(o.quantity),
                pricetype: o.priceType || 'MARKET',
                product: o.product || _config.defaultProduct,
                price: String(o.price || 0),
                trigger_price: String(o.triggerPrice || 0),
            })),
        };
        return apiCall('basketorder', body);
    }

    /**
     * Place a split order — breaks large orders into smaller chunks.
     * Useful for avoiding impact cost on large positions.
     * @param {Object} opts
     * @param {number} opts.splitSize - Max quantity per child order
     */
    async function placeSplitOrder(opts) {
        const body = {
            symbol: opts.symbol,
            exchange: opts.exchange || 'NSE',
            action: opts.action,
            quantity: String(opts.quantity),
            split_size: String(opts.splitSize),
            pricetype: opts.priceType || 'MARKET',
            product: opts.product || _config.defaultProduct,
        };
        return apiCall('splitorder', body);
    }

    /**
     * Modify an existing order (change price, quantity, type).
     */
    async function modifyOrder(opts) {
        const body = {
            strategy: opts.strategy || _config.strategy,
            orderId: opts.orderId,
            symbol: opts.symbol,
            action: opts.action,
            exchange: opts.exchange || 'NSE',
            pricetype: opts.priceType || 'LIMIT',
            product: opts.product || _config.defaultProduct,
            quantity: String(opts.quantity),
            price: String(opts.price || 0),
            trigger_price: String(opts.triggerPrice || 0),
        };
        return apiCall('modifyorder', body);
    }

    /**
     * Cancel a specific order by ID.
     */
    async function cancelOrder(orderId) {
        return apiCall('cancelorder', {
            strategy: _config.strategy,
            orderId,
        });
    }

    /**
     * Cancel all open & trigger-pending orders for the strategy.
     */
    async function cancelAllOrders() {
        return apiCall('cancelallorder', {
            strategy: _config.strategy,
        });
    }

    /**
     * Close all open positions (square off everything).
     */
    async function closeAllPositions() {
        return apiCall('closeposition', {
            strategy: _config.strategy,
        });
    }

    /**
     * Get order status by order ID.
     */
    async function getOrderStatus(orderId) {
        return apiCall('orderstatus', {
            strategy: _config.strategy,
            orderId,
        });
    }

    /**
     * Get current open position for a specific symbol.
     * Useful for Edge Pilot's pyramid detection.
     */
    async function getOpenPosition(symbol, exchange = 'NSE', product) {
        return apiCall('openposition', {
            strategy: _config.strategy,
            symbol,
            exchange,
            product: product || _config.defaultProduct,
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DATA API — replaces dhan-live.js CMP cascade
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Get real-time quote for a single symbol (REST, snapshot).
     * Returns: { ltp, open, high, low, bid, ask, prev_close, volume }
     */
    async function getQuote(symbol, exchange = 'NSE') {
        return apiCall('quotes', { symbol, exchange });
    }

    /**
     * Get real-time quotes for multiple symbols in one call.
     * Replaces the sequential Dhan LTP polling loop.
     * @param {Array<{symbol: string, exchange?: string}>} symbols
     */
    async function getMultiQuotes(symbols) {
        const formatted = symbols.map(s => ({
            symbol: s.symbol,
            exchange: s.exchange || 'NSE',
        }));
        return apiCall('multiquotes', { symbols: formatted });
    }

    /**
     * Get Level 5 market depth for a symbol.
     */
    async function getDepth(symbol, exchange = 'NSE') {
        return apiCall('depth', { symbol, exchange });
    }

    /**
     * Get historical OHLCV data.
     * @param {string} interval - "1m", "3m", "5m", "10m", "15m", "30m", "1h", "D"
     */
    async function getHistory(symbol, exchange, interval, startDate, endDate) {
        return apiCall('history', {
            symbol,
            exchange: exchange || 'NSE',
            interval,
            start_date: startDate,
            end_date: endDate,
        });
    }

    /**
     * Search for a symbol by keyword.
     */
    async function searchSymbol(query, exchange = 'NSE') {
        return apiCall('search', { query, exchange });
    }

    /**
     * Get symbol metadata (lot size, tick size, freeze qty, etc.)
     */
    async function getSymbolInfo(symbol, exchange = 'NSE') {
        return apiCall('symbol', { symbol, exchange });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ACCOUNTS API — replaces Dhan-specific fund/position calls
    // ─────────────────────────────────────────────────────────────────────────

    /** Get available funds / margin. */
    async function getFunds() {
        return apiCall('funds', {});
    }

    /** Get margin requirement for proposed positions. */
    async function getMargin(positions) {
        return apiCall('margin', { positions });
    }

    /** Get today's order book. */
    async function getOrderbook() {
        return apiCall('orderbook', {});
    }

    /** Get today's trade book. */
    async function getTradebook() {
        return apiCall('tradebook', {});
    }

    /** Get current positions with P&L. */
    async function getPositionbook() {
        return apiCall('positionbook', {});
    }

    /** Get CNC/delivery holdings. */
    async function getHoldings() {
        return apiCall('holdings', {});
    }

    // ─────────────────────────────────────────────────────────────────────────
    // WEBSOCKET LTP STREAMING — replaces dhan-live.js polling
    // ─────────────────────────────────────────────────────────────────────────

    let _ws = null;
    let _wsReconnectTimer = null;
    let _wsSubscriptions = new Map(); // "SYMBOL.EXCHANGE" → { mode, callback }
    let _wsAuthenticated = false;
    let _wsReconnectAttempts = 0;
    const MAX_RECONNECT_ATTEMPTS = 10;
    const RECONNECT_BASE_DELAY = 2000;

    /**
     * Connect to OpenAlgo WebSocket for real-time market data.
     * Auto-reconnects on disconnect with exponential backoff.
     */
    function wsConnect() {
        if (_ws && (_ws.readyState === WebSocket.OPEN || _ws.readyState === WebSocket.CONNECTING)) {
            return;
        }

        const wsUrl = _config.wsUrl;
        if (!wsUrl || !_config.apiKey) {
            console.warn('[OpenAlgo WS] No wsUrl or apiKey configured, falling back to REST polling');
            return;
        }

        try {
            _ws = new WebSocket(wsUrl);
        } catch (err) {
            console.error('[OpenAlgo WS] Connection failed:', err);
            _scheduleReconnect();
            return;
        }

        _ws.onopen = () => {
            console.log('[OpenAlgo WS] Connected, authenticating...');
            _wsReconnectAttempts = 0;
            _ws.send(JSON.stringify({
                action: 'authenticate',
                api_key: _config.apiKey,
            }));
        };

        _ws.onmessage = (evt) => {
            try {
                const msg = JSON.parse(evt.data);
                _handleWsMessage(msg);
            } catch (err) {
                console.warn('[OpenAlgo WS] Parse error:', err);
            }
        };

        _ws.onclose = (evt) => {
            console.log(`[OpenAlgo WS] Closed (code=${evt.code})`);
            _wsAuthenticated = false;
            _ws = null;
            _scheduleReconnect();
        };

        _ws.onerror = (err) => {
            console.error('[OpenAlgo WS] Error:', err);
        };
    }

    function _handleWsMessage(msg) {
        // Authentication response
        if (msg.type === 'auth' || msg.status === 'authenticated') {
            _wsAuthenticated = true;
            console.log('[OpenAlgo WS] Authenticated');
            // Re-subscribe all active subscriptions
            _wsSubscriptions.forEach((sub, topic) => {
                const [symbol, exchange] = topic.split('.');
                _wsSend({
                    action: 'subscribe',
                    symbol,
                    exchange,
                    mode: sub.mode,
                });
            });
            _emitEvent('wsConnected');
            return;
        }

        // Market data update
        if (msg.type === 'market_data' && msg.data) {
            const topic = `${msg.data.symbol}.${msg.data.exchange}`;
            const sub = _wsSubscriptions.get(topic);
            if (sub && sub.callback) {
                sub.callback(msg.data, msg.mode);
            }
            // Also emit a global event for any listener
            _emitEvent('ltp', msg.data);
            return;
        }

        // Error
        if (msg.type === 'error') {
            console.warn('[OpenAlgo WS] Server error:', msg.message);
            _emitEvent('wsError', msg);
        }
    }

    function _wsSend(data) {
        if (_ws && _ws.readyState === WebSocket.OPEN) {
            _ws.send(JSON.stringify(data));
        }
    }

    function _scheduleReconnect() {
        if (_wsReconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
            console.error('[OpenAlgo WS] Max reconnect attempts reached, falling back to REST');
            _emitEvent('wsFallback');
            return;
        }
        const delay = RECONNECT_BASE_DELAY * Math.pow(1.5, _wsReconnectAttempts);
        _wsReconnectAttempts++;
        console.log(`[OpenAlgo WS] Reconnecting in ${Math.round(delay)}ms (attempt ${_wsReconnectAttempts})`);
        clearTimeout(_wsReconnectTimer);
        _wsReconnectTimer = setTimeout(wsConnect, delay);
    }

    /**
     * Subscribe to real-time LTP for a symbol.
     * @param {string} symbol   - e.g. "RELIANCE"
     * @param {string} exchange - e.g. "NSE"
     * @param {Function} callback - Called with (data, mode) on each tick
     * @param {number} [mode=1] - 1=LTP, 2=Quote, 3=Depth
     */
    function subscribeLTP(symbol, exchange = 'NSE', callback, mode = 1) {
        const topic = `${symbol}.${exchange}`;
        _wsSubscriptions.set(topic, { mode, callback });

        if (_wsAuthenticated && _ws) {
            _wsSend({ action: 'subscribe', symbol, exchange, mode });
        } else {
            // Attempt connection if not connected
            wsConnect();
        }
    }

    /**
     * Subscribe to multiple symbols at once.
     * @param {Array<{symbol: string, exchange?: string}>} instruments
     * @param {Function} callback - Called with (data, mode) per tick
     * @param {number} [mode=1]
     */
    function subscribeMulti(instruments, callback, mode = 1) {
        instruments.forEach(inst => {
            subscribeLTP(inst.symbol, inst.exchange || 'NSE', callback, mode);
        });
    }

    /**
     * Unsubscribe from a symbol's real-time feed.
     */
    function unsubscribeLTP(symbol, exchange = 'NSE') {
        const topic = `${symbol}.${exchange}`;
        const sub = _wsSubscriptions.get(topic);
        if (sub) {
            _wsSend({ action: 'unsubscribe', symbol, exchange, mode: sub.mode });
            _wsSubscriptions.delete(topic);
        }
    }

    /** Unsubscribe from all and close WebSocket. */
    function wsDisconnect() {
        clearTimeout(_wsReconnectTimer);
        _wsSubscriptions.clear();
        _wsReconnectAttempts = MAX_RECONNECT_ATTEMPTS; // prevent reconnect
        if (_ws) {
            _ws.close();
            _ws = null;
        }
        _wsAuthenticated = false;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // REST POLLING FALLBACK — for when WebSocket is unavailable
    // Used automatically when WS fails after MAX_RECONNECT_ATTEMPTS
    // ─────────────────────────────────────────────────────────────────────────

    let _pollTimer = null;
    let _pollCallbacks = new Map(); // "SYMBOL.EXCHANGE" → callback

    /**
     * Start REST polling for LTP (fallback when WebSocket unavailable).
     * Uses MultiQuotes to batch all subscriptions into one request.
     */
    function startPolling() {
        if (_pollTimer) return;

        const poll = async () => {
            if (_pollCallbacks.size === 0) return;

            const symbols = [];
            _pollCallbacks.forEach((cb, topic) => {
                const [symbol, exchange] = topic.split('.');
                symbols.push({ symbol, exchange });
            });

            try {
                const result = await getMultiQuotes(symbols);
                if (result.status === 'success' && result.results) {
                    result.results.forEach(r => {
                        const topic = `${r.symbol}.${r.exchange}`;
                        const cb = _pollCallbacks.get(topic);
                        if (cb && r.data) {
                            cb({
                                symbol: r.symbol,
                                exchange: r.exchange,
                                ltp: r.data.ltp,
                                open: r.data.open,
                                high: r.data.high,
                                low: r.data.low,
                                volume: r.data.volume,
                                prev_close: r.data.prev_close,
                            }, 2);
                        }
                    });
                }
            } catch (err) {
                console.warn('[OpenAlgo Poll] Error:', err);
            }
        };

        poll(); // immediate first poll
        _pollTimer = setInterval(poll, _config.restPollIntervalMs);
    }

    function stopPolling() {
        clearInterval(_pollTimer);
        _pollTimer = null;
    }

    /**
     * Subscribe to LTP with automatic WS → REST fallback.
     * This is the primary API that Edge Pilot and execution.html should use.
     */
    function subscribeCMP(symbol, exchange = 'NSE', callback) {
        // Try WebSocket first
        subscribeLTP(symbol, exchange, callback, 1);

        // Also register for polling fallback
        const topic = `${symbol}.${exchange}`;
        _pollCallbacks.set(topic, callback);
    }

    function unsubscribeCMP(symbol, exchange = 'NSE') {
        unsubscribeLTP(symbol, exchange);
        _pollCallbacks.delete(`${symbol}.${exchange}`);
    }

    // Auto-start polling when WS falls back
    function _onWsFallback() {
        console.log('[OpenAlgo] WebSocket unavailable, switching to REST polling');
        startPolling();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // BULK CMP ENRICHMENT — replaces dhan-live.js enrichTrades()
    // Batch-fetches LTP for all open trades in one MultiQuotes call
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Enrich an array of trades with current market price.
     * Replaces the 3-tier Dhan LTP → Yahoo → entry price cascade.
     *
     * @param {Array} trades - TradEdge trade objects from te_trades
     * @returns {Array} trades with `cmp` field updated
     */
    async function enrichTradesWithCMP(trades) {
        const openTrades = trades.filter(t => t.status === 'Open' && t.symbol);
        if (openTrades.length === 0) return trades;

        // Dedupe symbols
        const symbolSet = new Map();
        openTrades.forEach(t => {
            const key = t.symbol;
            if (!symbolSet.has(key)) {
                symbolSet.set(key, { symbol: t.symbol, exchange: 'NSE' });
            }
        });

        try {
            const result = await getMultiQuotes([...symbolSet.values()]);
            if (result.status === 'success' && result.results) {
                const priceMap = {};
                result.results.forEach(r => {
                    if (r.data && r.data.ltp) {
                        priceMap[r.symbol] = r.data.ltp;
                    }
                });

                // Update CMP on matching trades
                trades.forEach(t => {
                    if (t.status === 'Open' && priceMap[t.symbol]) {
                        t.cmp = priceMap[t.symbol];
                    }
                });
            }
        } catch (err) {
            console.warn('[OpenAlgo] CMP enrichment failed, keeping existing prices:', err);
            // Graceful degradation: trades keep their last known cmp or entry price
        }

        return trades;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // TELEGRAM ALERTS — replaces custom Telegram bot pipeline
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Send a Telegram alert via OpenAlgo's built-in integration.
     * @param {string} message - Alert text
     */
    async function sendTelegramAlert(message) {
        if (!_config.telegramEnabled || !_config.openalgoUsername) {
            console.warn('[OpenAlgo] Telegram not configured');
            return { status: 'skipped', reason: 'not_configured' };
        }
        return apiCall('telegram', {
            username: _config.openalgoUsername,
            message,
        });
    }

    /**
     * Format and send a trade alert (convenience wrapper).
     */
    async function alertTradeExecution(trade, action, details = '') {
        const msg = [
            `🔔 TradEdge ${action}`,
            `Symbol: ${trade.symbol}`,
            `Action: ${trade.side || trade.action}`,
            `Qty: ${trade.quantity || trade.qty}`,
            trade.price ? `Price: ₹${trade.price}` : '',
            trade.sl ? `SL: ₹${trade.sl}` : '',
            details,
            `Strategy: ${_config.strategy}`,
            `Time: ${new Date().toLocaleString('en-IN')}`,
        ].filter(Boolean).join('\n');

        return sendTelegramAlert(msg);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EDGE PILOT HELPERS — specialized methods for autopilot.html
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Execute a pyramid entry (P1/P2/P3) using SmartOrder.
     * SmartOrder auto-calculates delta needed to reach target position.
     *
     * @param {string} symbol
     * @param {number} trancheQty    - Quantity for this tranche
     * @param {number} totalPosition - Target total position after this tranche
     * @param {Object} [opts]        - Optional overrides
     */
    async function executePyramidEntry(symbol, trancheQty, totalPosition, opts = {}) {
        const result = await placeSmartOrder({
            symbol,
            action: 'BUY',
            quantity: trancheQty,
            positionSize: totalPosition,
            exchange: opts.exchange || 'NSE',
            product: opts.product || _config.defaultProduct,
            priceType: opts.priceType || 'MARKET',
            price: opts.price || 0,
            strategy: opts.strategy || `${_config.strategy}_EP`,
        });

        // Send Telegram alert
        if (result.status === 'success') {
            await alertTradeExecution(
                { symbol, side: 'BUY', quantity: trancheQty, price: opts.price },
                `Pyramid Entry (target: ${totalPosition})`,
            );
        }

        return result;
    }

    /**
     * Execute a trailing stop-loss modification.
     * Called by Edge Pilot's TSL Monitor.
     */
    async function updateTrailingSL(orderId, symbol, newSLPrice) {
        return modifyOrder({
            orderId,
            symbol,
            action: 'SELL',
            priceType: 'SL-M',
            quantity: 0, // keep same qty
            triggerPrice: newSLPrice,
        });
    }

    /**
     * Execute a tranche exit (partial position close).
     * Uses SplitOrder if quantity is large enough to warrant splitting.
     */
    async function executeTrancheExit(symbol, exitQty, opts = {}) {
        const splitThreshold = opts.splitThreshold || 500;

        let result;
        if (exitQty > splitThreshold) {
            result = await placeSplitOrder({
                symbol,
                action: 'SELL',
                quantity: exitQty,
                splitSize: opts.splitSize || Math.ceil(exitQty / 5),
                exchange: opts.exchange || 'NSE',
                product: opts.product || _config.defaultProduct,
                priceType: opts.priceType || 'MARKET',
            });
        } else {
            result = await placeOrder({
                symbol,
                action: 'SELL',
                quantity: exitQty,
                exchange: opts.exchange || 'NSE',
                product: opts.product || _config.defaultProduct,
                priceType: opts.priceType || 'MARKET',
                strategy: `${_config.strategy}_EP`,
            });
        }

        if (result.status === 'success') {
            await alertTradeExecution(
                { symbol, side: 'SELL', quantity: exitQty },
                `Tranche Exit`,
            );
        }

        return result;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ANALYZER / SANDBOX MODE
    // ─────────────────────────────────────────────────────────────────────────

    /** Check if OpenAlgo is in analyzer (sandbox) mode. */
    async function getAnalyzerStatus() {
        return apiCall('analyzerstatus', {});
    }

    /**
     * Toggle analyzer mode on/off.
     * @param {boolean} enable - true = sandbox mode, false = live
     */
    async function setAnalyzerMode(enable) {
        return apiCall('analyzertoggle', { mode: enable });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CONNECTION TEST / HEALTH CHECK
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Ping the OpenAlgo server to verify connectivity.
     * Call this on page load to show connection status in TradEdge UI.
     */
    async function ping() {
        try {
            const result = await apiCall('ping', {});
            return { connected: true, ...result };
        } catch {
            return { connected: false, error: 'OpenAlgo unreachable' };
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EVENT SYSTEM — for UI integration
    // Replaces the scattered event dispatching in dhan-orders.js
    // ─────────────────────────────────────────────────────────────────────────

    const _listeners = {};

    function on(event, callback) {
        if (!_listeners[event]) _listeners[event] = [];
        _listeners[event].push(callback);
        return () => off(event, callback);
    }

    function off(event, callback) {
        if (!_listeners[event]) return;
        _listeners[event] = _listeners[event].filter(cb => cb !== callback);
    }

    function _emitEvent(event, data) {
        if (_listeners[event]) {
            _listeners[event].forEach(cb => {
                try { cb(data); } catch (err) { console.error(`[OpenAlgo event:${event}]`, err); }
            });
        }
    }

    // Listen for WS fallback to auto-start polling
    on('wsFallback', _onWsFallback);

    // ─────────────────────────────────────────────────────────────────────────
    // COMPATIBILITY LAYER — maps old dhan-orders.js function signatures
    // Drop-in replacements so existing pages work without code changes
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Compatibility: matches dhan-orders.js placeDhanOrder() signature.
     * Maps Dhan-specific fields to OpenAlgo format.
     */
    async function placeDhanOrder(opts) {
        console.warn('[OpenAlgo] placeDhanOrder() is deprecated, use placeOrder() instead');
        return placeOrder({
            symbol: opts.symbol || opts.tradingSymbol,
            action: opts.transactionType || opts.action,
            quantity: opts.quantity,
            exchange: 'NSE',
            priceType: opts.orderType === 'MARKET' ? 'MARKET' : 'LIMIT',
            product: opts.productType === 'INTRADAY' ? 'MIS' : 'CNC',
            price: opts.price || 0,
            triggerPrice: opts.triggerPrice || 0,
        });
    }

    /**
     * Compatibility: matches dhan-live.js fetchLTP() signature.
     * Returns just the LTP number for a symbol.
     */
    async function fetchLTP(symbol) {
        try {
            const result = await getQuote(symbol, 'NSE');
            return result?.data?.ltp || null;
        } catch {
            return null;
        }
    }

    /**
     * Compatibility: matches dhan-live.js fetchMultiLTP() signature.
     * Returns a { symbol: ltp } map.
     */
    async function fetchMultiLTP(symbols) {
        const priceMap = {};
        try {
            const formatted = symbols.map(s => ({
                symbol: typeof s === 'string' ? s : s.symbol,
                exchange: 'NSE',
            }));
            const result = await getMultiQuotes(formatted);
            if (result.status === 'success' && result.results) {
                result.results.forEach(r => {
                    if (r.data) priceMap[r.symbol] = r.data.ltp;
                });
            }
        } catch (err) {
            console.warn('[OpenAlgo] fetchMultiLTP fallback:', err);
        }
        return priceMap;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CONFIGURATION UI HELPERS
    // ─────────────────────────────────────────────────────────────────────────

    /** Get current config (read-only copy). */
    function getConfig() {
        return { ..._config };
    }

    /** Update config and persist. */
    function updateConfig(updates) {
        _config = { ..._config, ...updates };
        saveConfig(_config);
        // Reconnect WS if URL changed
        if (updates.wsUrl || updates.apiKey) {
            wsDisconnect();
            _wsReconnectAttempts = 0;
            wsConnect();
        }
        _emitEvent('configUpdated', _config);
    }

    /** Check if OpenAlgo is configured (has API key). */
    function isConfigured() {
        return !!_config.apiKey;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PUBLIC API
    // ─────────────────────────────────────────────────────────────────────────

    window.OpenAlgo = {
        // Configuration
        getConfig,
        updateConfig,
        isConfigured,
        ping,

        // Orders (replaces dhan-orders.js)
        placeOrder,
        placeSmartOrder,
        placeBasketOrder,
        placeSplitOrder,
        modifyOrder,
        cancelOrder,
        cancelAllOrders,
        closeAllPositions,
        getOrderStatus,
        getOpenPosition,

        // Market Data (replaces dhan-live.js)
        getQuote,
        getMultiQuotes,
        getDepth,
        getHistory,
        searchSymbol,
        getSymbolInfo,
        enrichTradesWithCMP,

        // Real-time streaming
        wsConnect,
        wsDisconnect,
        subscribeLTP,
        subscribeMulti,
        unsubscribeLTP,
        subscribeCMP,      // primary API with WS→REST fallback
        unsubscribeCMP,
        startPolling,
        stopPolling,

        // Accounts
        getFunds,
        getMargin,
        getOrderbook,
        getTradebook,
        getPositionbook,
        getHoldings,

        // Sandbox / Analyzer
        getAnalyzerStatus,
        setAnalyzerMode,

        // Telegram
        sendTelegramAlert,
        alertTradeExecution,

        // Edge Pilot helpers
        executePyramidEntry,
        updateTrailingSL,
        executeTrancheExit,

        // Compatibility (deprecated, use new API)
        placeDhanOrder,
        fetchLTP,
        fetchMultiLTP,

        // Events
        on,
        off,
    };

})(window);
