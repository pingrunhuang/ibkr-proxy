import asyncio
import copy
import hashlib
import json
import re
from dataclasses import asdict, is_dataclass
from datetime import datetime
import zmq
import zmq.asyncio
from loguru import logger
from ib_async import IB, Event, Stock, Future, Forex, Crypto, MarketOrder, LimitOrder, StartupFetch
from order_ownership import OrderOwnershipStore


def _text(value):
    return "" if value is None else str(value).strip()


def _trading_day_from_timestamp(value):
    text = _text(value)
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10].replace("-", "")
    if (
        len(text) >= 8
        and text[:8].isdigit()
        and 1900 <= int(text[:4]) <= 2199
    ):
        return text[:8]
    if len(text) >= 6 and text[:6].isdigit():
        try:
            return datetime.strptime(text[:6], "%y%m%d").strftime("%Y%m%d")
        except ValueError:
            return ""
    return ""


def _trade_event_id(
    *,
    gateway_name,
    account_id,
    trading_day,
    exchange,
    trade_id,
    fallback,
):
    """Return a stable identifier for one native fill."""
    identity = {
        "gateway_name": gateway_name,
        "account_id": account_id,
        "trading_day": trading_day,
        "exchange": exchange,
        "trade_id": trade_id,
    }
    if not trade_id:
        identity["fallback"] = fallback
    encoded = json.dumps(
        identity,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    ).encode("utf-8")
    return f"trade:{gateway_name.lower()}:{hashlib.sha256(encoded).hexdigest()}"


class IBProxy:
    def __init__(
        self,
        ib_host='127.0.0.1',
        ib_port=4001,
        client_id=1,
        zmq_port=5555,
        zmq_rep_port=5556,
        ownership_db_path=":memory:",
    ):
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
        self.contracts_by_con_id = {}
        self.contracts_by_symbol_key = {}
        self.request_symbol_to_con_id = {}
        self.order_ownership = OrderOwnershipStore(ownership_db_path)
        self._unresolved_order_ids_logged = set()
        logger.info(
            f"IB order ownership store initialized path={ownership_db_path}"
        )

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
            try:
                metadata = self._contract_metadata(t.contract)
                topic = metadata['topic']
            except ValueError as e:
                logger.error(f"Skipping market data tick without a unique topic: {e}")
                continue

            data = {
                **metadata,
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
            'lastTradeDateOrContractMonth': getattr(contract, 'lastTradeDateOrContractMonth', ''),
            'multiplier': getattr(contract, 'multiplier', ''),
            'tradingClass': getattr(contract, 'tradingClass', '')
        }

    @staticmethod
    def _object_data(value):
        if is_dataclass(value):
            return asdict(value)
        return dict(vars(value))

    def _symbol_key(self, contract):
        data = self._contract_data(contract)
        sec_type = data['secType']
        parts = [sec_type, data['symbol'], data['currency'], data['exchange']]
        if sec_type == 'FUT':
            parts.extend([
                data['lastTradeDateOrContractMonth'],
                data['multiplier'],
                data['tradingClass'],
            ])
        return '.'.join(str(part) for part in parts if part not in (None, ''))

    def _market_data_topic(self, contract):
        con_id = getattr(contract, 'conId', 0)
        if not con_id:
            raise ValueError(f"contract has no conId after qualification: {contract}")
        return f"marketdata.IB.{con_id}"

    def _normalize_order_expiry(self, expiry):
        if expiry in (None, ''):
            return expiry

        value = str(expiry)
        match = re.match(r'^(\d{8}) \d{2}:\d{2}:\d{2} ', value)
        if match:
            return match.group(1)
        return value

    def _contract_for_order(self, contract):
        if getattr(contract, 'secType', '') != 'FUT':
            return contract

        expiry = getattr(contract, 'lastTradeDateOrContractMonth', '')
        normalized = self._normalize_order_expiry(expiry)
        if normalized == expiry:
            return contract

        order_contract = copy.copy(contract)
        order_contract.lastTradeDateOrContractMonth = normalized
        logger.info(
            "Using normalized futures expiry for IB order "
            f"conId={getattr(contract, 'conId', '')} "
            f"from={expiry} to={normalized}"
        )
        return order_contract

    def _contract_metadata(self, contract, request_symbol=None):
        data = self._contract_data(contract)
        con_id = data['conId']
        if not request_symbol and con_id:
            stored = self.contracts_by_con_id.get(str(con_id), {})
            request_symbol = stored.get('request_symbol')

        metadata = {
            **data,
            'symbol_key': self._symbol_key(contract),
            'topic': self._market_data_topic(contract),
        }
        if request_symbol:
            metadata['request_symbol'] = request_symbol
        return metadata

    def _register_contract(self, contract, request_symbol=None):
        metadata = self._contract_metadata(contract, request_symbol)
        con_id = str(metadata['conId'])
        self.contracts_by_con_id[con_id] = {
            **metadata,
            'contract': contract,
        }
        self.contracts_by_symbol_key[metadata['symbol_key']] = con_id
        if request_symbol:
            self.request_symbol_to_con_id[request_symbol] = con_id

        logger.info(
            "Qualified IB contract "
            f"request_symbol={request_symbol or ''} "
            f"symbol_key={metadata['symbol_key']} "
            f"conId={metadata['conId']} "
            f"topic={metadata['topic']} "
            f"localSymbol={metadata['localSymbol']} "
            f"expiry={metadata['lastTradeDateOrContractMonth']} "
            f"multiplier={metadata['multiplier']} "
            f"tradingClass={metadata['tradingClass']}"
        )
        return metadata

    def _order_update_data(self, trade):
        order = trade.order
        status = trade.orderStatus
        client_order_id = str(getattr(order, 'orderRef', '') or '')
        broker_order_id = str(getattr(order, 'orderId', '') or '')
        owner = self.order_ownership.find(
            client_order_id=client_order_id,
            broker_order_id=broker_order_id,
        ) or {}
        if not owner:
            unresolved_key = client_order_id or broker_order_id
            if unresolved_key and unresolved_key not in self._unresolved_order_ids_logged:
                self._unresolved_order_ids_logged.add(unresolved_key)
                logger.warning(
                    "IB order ownership unresolved "
                    f"client_order_id={client_order_id or '<empty>'} "
                    f"broker_order_id={broker_order_id or '<empty>'}"
                )
        data = {
            **self._contract_data(trade.contract),
            'account': getattr(order, 'account', ''),
            'order_id': getattr(order, 'orderId', 0),
            'permId': getattr(order, 'permId', 0),
            'clientId': getattr(order, 'clientId', 0),
            'orderRef': getattr(order, 'orderRef', ''),
            'client_order_id': owner.get('client_order_id', client_order_id),
            'broker_order_id': broker_order_id,
            'account_id': owner.get('account_id') or getattr(order, 'account', ''),
            'client_id': owner.get('client_id', ''),
            'strategy_id': owner.get('strategy_id', ''),
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
        try:
            execution = self._object_data(fill.execution)
            contract = self._contract_data(fill.contract)
            gateway_name = "IB_PROXY"
            account_id = _text(execution.get("acctNumber"))
            trading_day = _trading_day_from_timestamp(execution.get("time"))
            exchange = _text(execution.get("exchange") or contract.get("exchange"))
            trade_id = _text(execution.get("execId"))
            order_id = _text(execution.get("orderId"))
            client_order_id = _text(execution.get("orderRef"))
            owner = self.order_ownership.find(
                client_order_id=client_order_id,
                broker_order_id=order_id,
            ) or {}
            client_order_id = owner.get("client_order_id", client_order_id)
            if owner:
                logger.debug(
                    "Recovered IB execution ownership "
                    f"strategy_id={owner['strategy_id']} "
                    f"client_id={owner['client_id']} "
                    f"client_order_id={client_order_id} "
                    f"broker_order_id={order_id} trade_id={trade_id}"
                )
            else:
                logger.error(
                    "IB execution ownership unresolved; Engine will quarantine "
                    f"client_order_id={client_order_id or '<empty>'} "
                    f"broker_order_id={order_id or '<empty>'} "
                    f"trade_id={trade_id} account_id={account_id}"
                )
            topic = f"executions.{account_id}"
            data = {
                "event_id": _trade_event_id(
                    gateway_name=gateway_name,
                    account_id=account_id,
                    trading_day=trading_day,
                    exchange=exchange,
                    trade_id=trade_id,
                    fallback={
                        "order_id": order_id,
                        "client_order_id": client_order_id,
                        "con_id": contract.get("conId"),
                        "side": execution.get("side"),
                        "shares": execution.get("shares"),
                        "price": execution.get("price"),
                        "trade_time": execution.get("time"),
                    },
                ),
                "gateway_name": gateway_name,
                "account_id": account_id,
                "client_id": owner.get("client_id", ""),
                "strategy_id": owner.get("strategy_id", ""),
                "client_order_id": client_order_id,
                "trade_id": trade_id,
                "order_id": order_id,
                "trading_day": trading_day,
                "exchange": exchange,
                'account': account_id,
                **contract,
                'execution': execution,
                'commission': (
                    self._object_data(fill.commissionReport)
                    if fill.commissionReport
                    else None
                )
            }
            trade_cursor = self.order_ownership.record_trade(data)
            logger.debug(
                "Persisted IB trade event_id={} strategy_id={} trade_cursor={}",
                data["event_id"],
                data["strategy_id"],
                trade_cursor,
            )
            if not trade_cursor:
                logger.debug("Skip duplicate IB trade event_id={}", data["event_id"])
                return
            data["trade_cursor"] = int(trade_cursor)
            asyncio.create_task(self.publish(topic, data))
        except Exception:
            logger.exception("Failed to publish IB execution details")

    def on_pnl(self, pnl):
        # pnl is a Pnl object or PnlSingle object
        # Topic depends on if it's account-wide or contract-specific
        if pnl.conId == 0:
            topic = f"pnl.{pnl.account}.account"
        else:
            topic = f"pnl.{pnl.account}.{pnl.conId}"
            
        data = self._object_data(pnl)
        asyncio.create_task(self.publish(topic, data))

    def subscribe_market_data(self, contracts):
        """Subscribe to real-time market data for a list of contracts."""
        for contract in contracts:
            metadata = self._contract_metadata(contract)
            logger.info(
                "Subscribing IB market data "
                f"topic={metadata['topic']} "
                f"symbol_key={metadata['symbol_key']} "
                f"conId={metadata['conId']} "
                f"localSymbol={metadata['localSymbol']}"
            )
            self.ib.reqMktData(contract)

    def _apply_contract_overrides(self, contract, values):
        for attr, key in (
            ('conId', 'con_id'),
            ('multiplier', 'multiplier'),
            ('tradingClass', 'trading_class'),
            ('localSymbol', 'local_symbol'),
        ):
            value = values.get(key)
            if value in (None, ''):
                value = values.get(attr)
            if value not in (None, ''):
                if attr == 'conId':
                    value = int(value)
                setattr(contract, attr, value)
        return contract

    def _contract_from_request(self, req):
        contract_type = req.get('sec_type', 'STK')
        symbol = req.get('symbol')
        exchange = req.get('exchange', 'SMART')
        currency = req.get('currency', 'USD')

        if not symbol:
            return None

        if contract_type == 'STK':
            return Stock(symbol, exchange, currency)
        if contract_type == 'FUT':
            contract = Future(
                symbol=symbol,
                lastTradeDateOrContractMonth=req.get('expiry'),
                exchange=exchange,
                currency=currency,
            )
            return self._apply_contract_overrides(contract, req)
        if contract_type == 'CASH':
            return Forex(symbol, exchange)
        if contract_type == 'CRYPTO':
            return Crypto(symbol, exchange, currency)
        return None

    def _parse_optional_contract_values(self, values):
        parsed = {}
        positional = []
        for value in values:
            if '=' in value:
                key, raw_value = value.split('=', 1)
                key = key.strip()
                if key == 'tradingClass':
                    key = 'trading_class'
                elif key == 'localSymbol':
                    key = 'local_symbol'
                elif key == 'conId':
                    key = 'con_id'
                parsed[key] = raw_value.strip()
            else:
                positional.append(value)

        if positional:
            parsed['multiplier'] = positional[0]
        if len(positional) > 1:
            parsed['trading_class'] = positional[1]
        if len(positional) > 2:
            parsed['local_symbol'] = positional[2]
        return parsed

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
            if sec_type == 'FUT' and len(parts) >= 5:
                req = {
                    'sec_type': 'FUT',
                    'symbol': parts[1],
                    'currency': parts[2],
                    'exchange': parts[3],
                    'expiry': parts[4]
                }
                req.update(self._parse_optional_contract_values(parts[5:]))
                return self._contract_from_request(req)

        if symbol.startswith('FX:'):
            return Forex(symbol[3:])
        if symbol.startswith('CRYPTO:'):
            return Crypto(symbol[7:], 'PAXOS', 'USD')
        if symbol.startswith('FUT:'):
            fut_parts = symbol.split(':')
            if len(fut_parts) >= 4:
                req = {
                    'sec_type': 'FUT',
                    'symbol': fut_parts[1],
                    'expiry': fut_parts[2],
                    'exchange': fut_parts[3],
                    'currency': 'USD',
                }
                req.update(self._parse_optional_contract_values(fut_parts[4:]))
                return self._contract_from_request(req)
            raise ValueError(f"Invalid futures format: {symbol}. Expected FUT:SYMBOL:YYYYMM:EXCHANGE[:MULTIPLIER[:TRADING_CLASS]]")

        colon_parts = symbol.split(':')
        if len(colon_parts) >= 3:
            req = {
                'sec_type': 'FUT',
                'symbol': colon_parts[0],
                'expiry': colon_parts[1],
                'exchange': colon_parts[2],
                'currency': 'USD',
            }
            req.update(self._parse_optional_contract_values(colon_parts[3:]))
            return self._contract_from_request(req)

        return Stock(symbol, 'SMART', 'USD')

    def _contract_request_records(self, symbols=None, contract_requests=None):
        records = []
        for symbol in symbols or []:
            if not isinstance(symbol, str) or not symbol.strip():
                raise ValueError(f"Invalid symbol request: {symbol}")
            request_symbol = symbol.strip()
            records.append({
                'request_symbol': request_symbol,
                'contract': self._contract_from_symbol(request_symbol),
            })

        for req in contract_requests or []:
            if not isinstance(req, dict):
                raise ValueError(f"Invalid contract request: {req}")
            contract = self._contract_from_request(req)
            if not contract:
                raise ValueError(f"Invalid contract request: {req}")
            request_symbol = req.get('request_symbol') or req.get('symbol_key') or self._symbol_key(contract)
            records.append({
                'request_symbol': request_symbol,
                'contract': contract,
            })
        return records

    async def qualify_contracts(self, symbols=None, contract_requests=None):
        records = self._contract_request_records(symbols, contract_requests)
        if not records:
            logger.info("No IB contracts requested for qualification.")
            return []

        logger.info(
            "Qualifying IB contracts "
            f"count={len(records)} "
            f"requests={[r['request_symbol'] for r in records]}"
        )
        try:
            qualified_contracts = await self.ib.qualifyContractsAsync(*(r['contract'] for r in records))
        except Exception:
            logger.exception(
                "IB contract qualification failed "
                f"requests={[r['request_symbol'] for r in records]}"
            )
            raise

        if len(qualified_contracts) != len(records):
            missing_requests = [r['request_symbol'] for r in records[len(qualified_contracts):]]
            logger.warning(
                "IB qualification returned a different number of contracts "
                f"requested={len(records)} returned={len(qualified_contracts)} "
                f"missing_requests={missing_requests}"
            )

        results = []
        for record, contract in zip(records, qualified_contracts):
            metadata = self._register_contract(contract, record['request_symbol'])
            results.append({
                'contract': contract,
                'metadata': metadata,
            })
        return results

    def _contract_for_order_request(self, req):
        con_id = req.get('con_id') or req.get('conId')
        if con_id not in (None, ''):
            contract_record = self.contracts_by_con_id.get(str(con_id))
            if contract_record:
                logger.info(f"Using registered IB contract for order conId={con_id}")
                return self._contract_for_order(contract_record['contract'])
            logger.warning(
                "Order requested conId that is not in the local registry; "
                f"falling back to request contract fields conId={con_id}"
            )

        contract = self._contract_from_request(req)
        if contract:
            return contract
        return None

    def _order_from_request(self, req):
        qty = float(req.get('qty', 0))
        action_type = req.get('action_type', 'BUY')
        order_type = req.get('order_type', 'MKT')

        if order_type == 'MKT':
            return MarketOrder(action_type, qty, tif='DAY')
        if order_type == 'LMT':
            return LimitOrder(action_type, qty, req.get('lmt_price'), tif='DAY')
        return None

    def _place_order_from_request(self, req):
        client_id = _text(req.get("client_id"))
        strategy_id = _text(req.get("strategy_id"))
        client_order_id = _text(req.get("client_order_id"))
        if not client_id or not strategy_id or not client_order_id:
            logger.error(
                "Rejected IB place_order with missing Engine identity "
                f"client_id={client_id or '<empty>'} "
                f"strategy_id={strategy_id or '<empty>'} "
                f"client_order_id={client_order_id or '<empty>'}"
            )
            return {
                "status": "error",
                "message": (
                    "place_order requires client_id, strategy_id, and "
                    "client_order_id"
                ),
            }
        existing_owner = self.order_ownership.find(
            client_order_id=client_order_id
        )
        if existing_owner:
            if (
                existing_owner["client_id"] != client_id
                or existing_owner["strategy_id"] != strategy_id
            ):
                logger.error(
                    "Rejected duplicate IB client_order_id ownership mismatch "
                    f"client_order_id={client_order_id} "
                    f"stored_client_id={existing_owner['client_id']} "
                    f"incoming_client_id={client_id} "
                    f"stored_strategy_id={existing_owner['strategy_id']} "
                    f"incoming_strategy_id={strategy_id}"
                )
                return {
                    "status": "error",
                    "message": "client_order_id ownership mismatch",
                }
            if existing_owner["broker_order_id"]:
                logger.warning(
                    "Returning existing IB order for duplicate request "
                    f"strategy_id={strategy_id} "
                    f"client_order_id={client_order_id} "
                    f"broker_order_id={existing_owner['broker_order_id']}"
                )
                return {
                    "status": "success",
                    "duplicate": True,
                    "order_id": existing_owner["broker_order_id"],
                    "broker_order_id": existing_owner["broker_order_id"],
                    "client_order_id": client_order_id,
                }
            logger.error(
                "IB order submission requires reconciliation "
                f"strategy_id={strategy_id} client_order_id={client_order_id}"
            )
            return {
                "status": "error",
                "recovery_required": True,
                "message": "client_order_id has a pending broker submission",
            }
        contract = self._contract_for_order_request(req)
        if not contract:
            return {"status": "error", "message": "Invalid contract type"}

        order = self._order_from_request(req)
        if not order:
            return {"status": "error", "message": "Invalid order type"}
        order.orderRef = client_order_id
        self.order_ownership.reserve(
            client_order_id=client_order_id,
            client_id=client_id,
            strategy_id=strategy_id,
            account_id=_text(req.get("account_id")),
        )
        logger.debug(
            "Reserved IB order ownership before broker submission "
            f"strategy_id={strategy_id} client_id={client_id} "
            f"client_order_id={client_order_id}"
        )

        metadata = self._contract_metadata(contract) if getattr(contract, 'conId', 0) else self._contract_data(contract)
        logger.info(
            "Placing IB order "
            f"conId={metadata.get('conId', '')} "
            f"symbol_key={metadata.get('symbol_key', '')} "
            f"action={req.get('action_type', 'BUY')} "
            f"order_type={req.get('order_type', 'MKT')} "
            f"qty={req.get('qty', 0)} "
            f"lmt_price={req.get('lmt_price', '')}"
        )
        trade = self.ib.placeOrder(contract, order)
        broker_order_id = str(trade.order.orderId)
        self.order_ownership.upsert(
            client_order_id=client_order_id,
            broker_order_id=broker_order_id,
            client_id=client_id,
            strategy_id=strategy_id,
            account_id=_text(req.get("account_id")),
        )
        logger.info(
            "Persisted IB order ownership after broker acceptance "
            f"strategy_id={strategy_id} client_id={client_id} "
            f"client_order_id={client_order_id} "
            f"broker_order_id={broker_order_id}"
        )
        return {
            "status": "success",
            "order_id": trade.order.orderId,
            "broker_order_id": broker_order_id,
            "client_order_id": client_order_id,
        }

    def _cancel_order_from_request(self, req):
        client_order_id = _text(req.get("client_order_id"))
        broker_order_id = _text(req.get("broker_order_id") or req.get("order_id"))
        owner = self.order_ownership.find(
            client_order_id=client_order_id,
            broker_order_id=broker_order_id,
        )
        if not owner:
            logger.error(
                "Rejected IB cancel_order: ownership not found "
                f"client_order_id={client_order_id or '<empty>'} "
                f"broker_order_id={broker_order_id or '<empty>'}"
            )
            return {"status": "error", "message": "Order ownership not found"}
        broker_order_id = owner["broker_order_id"] or broker_order_id
        if _text(req.get("client_id")) != owner["client_id"]:
            logger.error(
                "Rejected IB cancel_order: client_id mismatch "
                f"client_order_id={owner['client_order_id']} "
                f"stored_client_id={owner['client_id']} "
                f"incoming_client_id={_text(req.get('client_id')) or '<empty>'}"
            )
            return {"status": "error", "message": "Order client_id mismatch"}
        if _text(req.get("strategy_id")) != owner["strategy_id"]:
            logger.error(
                "Rejected IB cancel_order: strategy_id mismatch "
                f"client_order_id={owner['client_order_id']} "
                f"stored_strategy_id={owner['strategy_id']} "
                f"incoming_strategy_id={_text(req.get('strategy_id')) or '<empty>'}"
            )
            return {"status": "error", "message": "Order strategy_id mismatch"}
        if not broker_order_id:
            logger.error(
                "Rejected IB cancel_order: broker order id unavailable "
                f"client_order_id={owner['client_order_id']}"
            )
            return {"status": "error", "message": "Broker order id not available"}

        trades = [
            trade
            for trade in self.ib.trades()
            if str(trade.order.orderId) == broker_order_id
        ]
        if not trades:
            logger.error(
                "Rejected IB cancel_order: active broker order not found "
                f"client_order_id={owner['client_order_id']} "
                f"broker_order_id={broker_order_id}"
            )
            return {"status": "error", "message": "Order not found"}
        self.ib.cancelOrder(trades[0].order)
        logger.info(
            "Submitted IB cancel_order "
            f"strategy_id={owner['strategy_id']} "
            f"client_order_id={owner['client_order_id']} "
            f"broker_order_id={broker_order_id}"
        )
        return {
            "status": "success",
            "message": f"Order {broker_order_id} cancellation requested",
        }

    def subscribe_pnl(self, account, contracts=None):
        """Subscribe to PnL updates."""
        self.ib.reqPnL(account)
        if contracts:
            for contract in contracts:
                self.ib.reqPnLSingle(account, "", contract.conId)

    async def publish(self, topic, data):
        """Publish data to ZeroMQ."""
        message = json.dumps(data, default=str)
        logger.debug(f"Publishing ZMQ topic={topic} payload={message}")
        await self.pub_socket.send_string(f"{topic} {message}")
        logger.debug(f"Published ZMQ topic={topic}")

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

                    if action == 'ping':
                        response = {
                            "status": "success",
                            "message": "pong",
                            "ib_connected": bool(self.ib.isConnected()),
                            "database_ready": self.order_ownership.is_healthy(),
                        }

                    elif action == 'place_order':
                        # Expects: symbol, sec_type, exchange, currency, qty, action_type (BUY/SELL), order_type (MKT/LMT), [lmt_price]
                        response = self._place_order_from_request(req)
                            
                    elif action == 'cancel_order':
                        response = self._cancel_order_from_request(req)
                            
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

                    elif action == 'get_trades':
                        client_id = _text(req.get("client_id"))
                        strategy_id = _text(req.get("strategy_id"))
                        if not client_id or not strategy_id:
                            response = {
                                "status": "error",
                                "message": "get_trades requires client_id and strategy_id",
                            }
                        else:
                            try:
                                after_id = int(req.get("after_id") or 0)
                                limit = int(req.get("limit") or 500)
                                if after_id < 0 or limit <= 0 or limit > 1000:
                                    raise ValueError
                            except (TypeError, ValueError):
                                response = {
                                    "status": "error",
                                    "message": "invalid get_trades pagination",
                                }
                            else:
                                response = {
                                    "status": "success",
                                    "data": self.order_ownership.list_trades(
                                        client_id,
                                        strategy_id,
                                        after_id=after_id,
                                        limit=limit,
                                    ),
                                }

                    elif action == 'get_trade_cursor':
                        client_id = _text(req.get("client_id"))
                        strategy_id = _text(req.get("strategy_id"))
                        if not client_id or not strategy_id:
                            response = {
                                "status": "error",
                                "message": "get_trade_cursor requires client_id and strategy_id",
                            }
                        else:
                            response = {
                                "status": "success",
                                "data": {
                                    "cursor": self.order_ownership.latest_trade_cursor(
                                        client_id,
                                        strategy_id,
                                    )
                                },
                            }

                    elif action == 'qualify_contracts':
                        symbols = req.get('symbols', [])
                        contract_requests = req.get('contracts', [])
                        if not isinstance(symbols, list) or not isinstance(contract_requests, list):
                            response = {"status": "error", "message": "symbols and contracts must be lists"}
                        else:
                            qualified = await self.qualify_contracts(symbols, contract_requests)
                            response = {
                                "status": "success",
                                "data": [r['metadata'] for r in qualified]
                            }

                    elif action == 'subscribe_market_data':
                        symbols = req.get('symbols', [])
                        contract_requests = req.get('contracts', [])
                        if not isinstance(symbols, list) or not isinstance(contract_requests, list):
                            response = {"status": "error", "message": "symbols and contracts must be lists"}
                        else:
                            qualified = await self.qualify_contracts(symbols, contract_requests)
                            qualified_contracts = [r['contract'] for r in qualified]
                            self.subscribe_market_data(qualified_contracts)
                            response = {
                                "status": "success",
                                "data": [r['metadata'] for r in qualified]
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
        self.order_ownership.close()
        logger.info("Proxy stopped.")
