import asyncio
import json
import zmq
import zmq.asyncio
from loguru import logger
from ib_async import IB, util, Event, Stock, Future, Forex, Crypto, MarketOrder, LimitOrder, StartupFetch

class IBProxy:
    def __init__(self, ib_host='127.0.0.1', ib_port=4001, client_id=1, zmq_port=5555, zmq_rep_port=5556):
        self.ib_host = ib_host
        self.ib_port = ib_port
        self.client_id = client_id
        self.zmq_port = zmq_port
        self.zmq_rep_port = zmq_rep_port
        
        self.ib = IB()
        # Manually add missing events from ib_async
        self.ib.positionEndEvent = Event('positionEndEvent')
        self._patch_ib_wrapper()

        self.context = zmq.asyncio.Context()
        self.pub_socket = self.context.socket(zmq.PUB)
        self.rep_socket = self.context.socket(zmq.REP)
        
        # Internal state
        self._is_running = False

    def _patch_ib_wrapper(self):
        """Patch the IB wrapper to emit custom events for sync completion."""
        orig_position_end = self.ib.wrapper.positionEnd
        def position_end_hook():
            orig_position_end()
            self.ib.positionEndEvent.emit()
        self.ib.wrapper.positionEnd = position_end_hook

    async def connect(self):
        """Connect to IB Gateway and bind ZeroMQ socket."""
        logger.info(f"Connecting to IB Gateway at {self.ib_host}:{self.ib_port}...")
        try:
            await self.ib.connectAsync(
                self.ib_host, self.ib_port, 
                clientId=self.client_id, 
                timeout=15
            )
            logger.info("Successfully connected to IB Gateway.")
        except Exception as e:
            logger.error(f"Failed to connect to IB Gateway: {e}")
            raise

        logger.info(f"Binding ZeroMQ PUB socket to tcp://*:{self.zmq_port}...")
        self.pub_socket.bind(f"tcp://*:{self.zmq_port}")
        
        logger.info(f"Binding ZeroMQ REP socket to tcp://*:{self.zmq_rep_port}...")
        self.rep_socket.bind(f"tcp://*:{self.zmq_rep_port}")
        
        # Register event handlers
        self.setup_event_handlers()

    def setup_event_handlers(self):
        """Setup IB event handlers for various data types."""
        self.ib.pendingTickersEvent += self.on_pending_tickers
        self.ib.accountValueEvent += self.on_account_value
        self.ib.positionEvent += self.on_position
        self.ib.positionEndEvent += self.on_position_end
        self.ib.openOrderEvent += self.on_open_order
        self.ib.orderStatusEvent += self.on_order_status
        self.ib.execDetailsEvent += self.on_exec_details
        self.ib.pnlEvent += self.on_pnl
        logger.info("Event handlers configured.")

    def on_position_end(self):
        """当 IBKR 报告当前账户持仓快照推送完毕时触发"""
        topic = "portfolio_sync_done.all"
        data = {"status": "completed"}
        asyncio.create_task(self.publish(topic, data))

    def on_pending_tickers(self, tickers):
        for t in tickers:
            topic = f"marketdata.{t.contract.secType}.{t.contract.symbol}.{t.contract.currency}.{t.contract.exchange}"
            data = {
                'symbol': t.contract.symbol,
                'currency': t.contract.currency,
                'secType': t.contract.secType,
                'exchange': t.contract.exchange,
                'bid': t.bid,
                'bidSize': t.bidSize,
                'ask': t.ask,
                'askSize': t.askSize,
                'last': t.last,
                'lastSize': t.lastSize,
                'volume': t.volume,
                'time': t.time.isoformat() if t.time else None
            }
            asyncio.create_task(self.publish(topic, data))

    def on_account_value(self, value):
        topic = f"account.{value.account}"
        data = {
            'account': value.account,
            'tag': value.tag,
            'value': value.value,
            'currency': value.currency
        }
        asyncio.create_task(self.publish(topic, data))

    def _contract_data(self, contract):
        return {
            'symbol': contract.symbol,
            'currency': getattr(contract, 'currency', ''),
            'secType': getattr(contract, 'secType', ''),
            'exchange': getattr(contract, 'exchange', ''),
            'conId': getattr(contract, 'conId', 0),
            'localSymbol': getattr(contract, 'localSymbol', ''),
            'lastTradeDateOrContractMonth': getattr(contract, 'lastTradeDateOrContractMonth', '')
        }

    def _order_update_data(self, trade):
        order = trade.order
        status = trade.orderStatus
        data = {
            **self._contract_data(trade.contract),
            'account': getattr(order, 'account', ''),
            'order_id': getattr(order, 'orderId', 0),
            'permId': getattr(order, 'permId', 0),
            'clientId': getattr(order, 'clientId', 0),
            'orderRef': getattr(order, 'orderRef', ''),
            'action': getattr(order, 'action', ''),
            'orderType': getattr(order, 'orderType', ''),
            'lmtPrice': getattr(order, 'lmtPrice', 0.0),
            'totalQuantity': getattr(order, 'totalQuantity', 0),
            'status': getattr(status, 'status', ''),
            'filled': getattr(status, 'filled', 0),
            'remaining': getattr(status, 'remaining', 0),
            'avgFillPrice': getattr(status, 'avgFillPrice', 0.0),
            'lastFillPrice': getattr(status, 'lastFillPrice', 0.0),
            'whyHeld': getattr(status, 'whyHeld', '')
        }
        data['price'] = data['lmtPrice'] or data['avgFillPrice'] or data['lastFillPrice'] or 0.0
        return data

    def on_open_order(self, trade):
        data = self._order_update_data(trade)
        topic = f"orders.{data['account'] or 'all'}"
        asyncio.create_task(self.publish(topic, data))

    def on_order_status(self, trade):
        data = self._order_update_data(trade)
        topic = f"orders.{data['account'] or 'all'}"
        asyncio.create_task(self.publish(topic, data))

    def on_position(self, position):
        topic = f"portfolio.{position.account}"
        data = {
            'account': position.account,
            **self._contract_data(position.contract),
            'position': position.position,
            'avgCost': position.avgCost
        }
        asyncio.create_task(self.publish(topic, data))

    def on_exec_details(self, trade, fill):
        topic = f"executions.{fill.execution.acctNumber}"
        data = {
            'account': fill.execution.acctNumber,
            'symbol': fill.contract.symbol,
            'execution': util.asDict(fill.execution),
            'commission': util.asDict(fill.commissionReport) if fill.commissionReport else None
        }
        asyncio.create_task(self.publish(topic, data))

    def on_pnl(self, pnl):
        # pnl is a Pnl object or PnlSingle object
        # Topic depends on if it's account-wide or contract-specific
        if pnl.conId == 0:
            topic = f"pnl.{pnl.account}.account"
        else:
            topic = f"pnl.{pnl.account}.{pnl.conId}"
            
        data = util.asDict(pnl)
        asyncio.create_task(self.publish(topic, data))

    def subscribe_market_data(self, contracts):
        """Subscribe to real-time market data for a list of contracts."""
        for contract in contracts:
            logger.info(f"Subscribing to market data for {contract.symbol}...")
            self.ib.reqMktData(contract)

    def _contract_from_request(self, req):
        contract_type = req.get('sec_type', 'STK')
        symbol = req.get('symbol')
        exchange = req.get('exchange', 'SMART')
        currency = req.get('currency', 'USD')

        if contract_type == 'STK':
            return Stock(symbol, exchange, currency)
        if contract_type == 'FUT':
            return Future(symbol, req.get('expiry'), exchange, currency)
        if contract_type == 'CASH':
            return Forex(symbol, exchange)
        if contract_type == 'CRYPTO':
            return Crypto(symbol, exchange, currency)
        return None

    def _contract_from_symbol(self, symbol):
        parts = symbol.split('.')
        if len(parts) >= 4:
            sec_type = parts[0].upper()
            if sec_type in {'STK', 'CRYPTO'} and len(parts) == 4:
                req = {
                    'sec_type': sec_type,
                    'symbol': parts[1],
                    'currency': parts[2],
                    'exchange': parts[3]
                }
                return self._contract_from_request(req)
            if sec_type == 'CASH' and len(parts) == 4:
                return Forex(f"{parts[1]}{parts[2]}", parts[3])
            if sec_type == 'FUT' and len(parts) == 5:
                req = {
                    'sec_type': 'FUT',
                    'symbol': parts[1],
                    'currency': parts[2],
                    'exchange': parts[3],
                    'expiry': parts[4]
                }
                return self._contract_from_request(req)

        if symbol.startswith('FX:'):
            return Forex(symbol[3:])
        if symbol.startswith('CRYPTO:'):
            return Crypto(symbol[7:], 'PAXOS', 'USD')
        if symbol.startswith('FUT:'):
            fut_parts = symbol.split(':')
            if len(fut_parts) == 4:
                return Future(fut_parts[1], fut_parts[2], fut_parts[3])
            raise ValueError(f"Invalid futures format: {symbol}. Expected FUT:SYMBOL:YYYYMM:EXCHANGE")

        colon_parts = symbol.split(':')
        if len(colon_parts) == 3:
            return Future(colon_parts[0], colon_parts[1], colon_parts[2], currency='USD')

        return Stock(symbol, 'SMART', 'USD')

    def subscribe_pnl(self, account, contracts=None):
        """Subscribe to PnL updates."""
        self.ib.reqPnL(account)
        if contracts:
            for contract in contracts:
                self.ib.reqPnLSingle(account, "", contract.conId)

    async def publish(self, topic, data):
        """Publish data to ZeroMQ."""
        message = json.dumps(data, default=str)
        await self.pub_socket.send_string(f"{topic} {message}")
        logger.debug(f"Published to topic {topic}: {message}")
        # Using debug to avoid flooding the console, but info could be used if preferred.
        # Given the user's request, I will use info for visibility.
        logger.info(f"Successfully published to ZMQ: {topic}")

    async def _command_loop(self):
        """Listen for and process ZMQ REP commands."""
        logger.info("Command loop started.")
        while self._is_running:
            try:
                # Use a timeout to allow checking self._is_running
                if await self.rep_socket.poll(1000):
                    req = await self.rep_socket.recv_json()
                    action = req.get('action')
                    logger.info(f"Received command: {action}")
                    
                    response = {"status": "error", "message": "Unknown action"}
                    
                    if action == 'place_order':
                        # Expects: symbol, sec_type, exchange, currency, qty, action_type (BUY/SELL), order_type (MKT/LMT), [lmt_price]
                        contract = self._contract_from_request(req)
                            
                        if contract:
                            qty = float(req.get('qty', 0))
                            action_type = req.get('action_type', 'BUY')
                            order_type = req.get('order_type', 'MKT')
                            
                            if order_type == 'MKT':
                                order = MarketOrder(action_type, qty)
                            elif order_type == 'LMT':
                                order = LimitOrder(action_type, qty, req.get('lmt_price'))
                            else:
                                order = None
                                
                            if order:
                                trade = self.ib.placeOrder(contract, order)
                                response = {"status": "success", "order_id": trade.order.orderId}
                            else:
                                response = {"status": "error", "message": "Invalid order type"}
                        else:
                            response = {"status": "error", "message": "Invalid contract type"}
                            
                    elif action == 'cancel_order':
                        order_id = req.get('order_id')
                        # Find the trade in current trades
                        trades = [t for t in self.ib.trades() if str(t.order.orderId) == str(order_id)]
                        if trades:
                            self.ib.cancelOrder(trades[0].order)
                            response = {"status": "success", "message": f"Order {order_id} cancellation requested"}
                        else:
                            response = {"status": "error", "message": "Order not found"}
                            
                    elif action == 'get_positions':
                        positions = []
                        for p in self.ib.positions():
                            positions.append({
                                'account': p.account,
                                **self._contract_data(p.contract),
                                'position': p.position,
                                'avgCost': p.avgCost
                            })
                        response = {"status": "success", "data": positions}
                        
                    elif action == 'get_account':
                        account_data = []
                        for v in self.ib.accountValues():
                            account_data.append({
                                'account': v.account,
                                'tag': v.tag,
                                'value': v.value,
                                'currency': v.currency
                            })
                        response = {"status": "success", "data": account_data}

                    elif action == 'get_orders':
                        orders = [self._order_update_data(t) for t in self.ib.trades()]
                        response = {"status": "success", "data": orders}

                    elif action == 'subscribe_market_data':
                        symbols = req.get('symbols', [])
                        if not isinstance(symbols, list):
                            response = {"status": "error", "message": "symbols must be a list"}
                        else:
                            contracts = [self._contract_from_symbol(s) for s in symbols]
                            qualified_contracts = await self.ib.qualifyContractsAsync(*contracts)
                            self.subscribe_market_data(qualified_contracts)
                            response = {
                                "status": "success",
                                "data": [self._contract_data(c) for c in qualified_contracts]
                            }
                        
                    await self.rep_socket.send_json(response)
            except Exception as e:
                logger.error(f"Error in command loop: {e}")
                try:
                    await self.rep_socket.send_json({"status": "error", "message": str(e)})
                except:
                    pass

    async def run_forever(self):
        """Main loop to keep the proxy alive and handle reconnections."""
        self._is_running = True
        
        # Run connection monitor and command loop concurrently
        await asyncio.gather(
            self._connection_monitor(),
            self._command_loop()
        )

    async def _connection_monitor(self):
        """Monitor IB connection and attempt reconnection."""
        while self._is_running:
            if not self.ib.isConnected():
                logger.warning("IB connection lost. Attempting to reconnect...")
                try:
                    await self.ib.connectAsync(
                        self.ib_host, self.ib_port, 
                        clientId=self.client_id, 
                        timeout=15
                    )
                    logger.info("Reconnected to IB Gateway.")
                except Exception as e:
                    logger.error(f"Reconnection failed: {e}. Retrying in 5 seconds...")
                    await asyncio.sleep(5)
                    continue
            
            await asyncio.sleep(1)

    def stop(self):
        """Stop the proxy."""
        self._is_running = False
        self.ib.disconnect()
        self.pub_socket.close()
        self.rep_socket.close()
        self.context.term()
        logger.info("Proxy stopped.")
