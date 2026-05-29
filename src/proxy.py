import asyncio
import json
import logging
import zmq
import zmq.asyncio
from ib_async import IB, util

logger = logging.getLogger(__name__)

class IBProxy:
    def __init__(self, ib_host='127.0.0.1', ib_port=4001, client_id=1, zmq_port=5555):
        self.ib_host = ib_host
        self.ib_port = ib_port
        self.client_id = client_id
        self.zmq_port = zmq_port
        
        self.ib = IB()
        self.context = zmq.asyncio.Context()
        self.pub_socket = self.context.socket(zmq.PUB)
        
        # Internal state
        self._is_running = False

    async def connect(self):
        """Connect to IB Gateway and bind ZeroMQ socket."""
        logger.info(f"Connecting to IB Gateway at {self.ib_host}:{self.ib_port}...")
        try:
            await self.ib.connectAsync(self.ib_host, self.ib_port, clientId=self.client_id)
            logger.info("Successfully connected to IB Gateway.")
        except Exception as e:
            logger.error(f"Failed to connect to IB Gateway: {e}")
            raise

        logger.info(f"Binding ZeroMQ PUB socket to tcp://*:{self.zmq_port}...")
        self.pub_socket.bind(f"tcp://*:{self.zmq_port}")
        
        # Register event handlers
        self.setup_event_handlers()

    def setup_event_handlers(self):
        """Setup IB event handlers for various data types."""
        self.ib.pendingTickersEvent += self.on_pending_tickers
        self.ib.accountValueEvent += self.on_account_value
        self.ib.positionEvent += self.on_position
        self.ib.execDetailsEvent += self.on_exec_details
        self.ib.pnlEvent += self.on_pnl
        logger.info("Event handlers configured.")

    def on_pending_tickers(self, tickers):
        for t in tickers:
            topic = f"marketdata.{t.contract.symbol}"
            data = {
                'symbol': t.contract.symbol,
                'currency': t.contract.currency,
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

    def on_position(self, position):
        topic = f"portfolio.{position.account}"
        data = {
            'account': position.account,
            'symbol': position.contract.symbol,
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

    async def run_forever(self):
        """Main loop to keep the proxy alive and handle reconnections."""
        self._is_running = True
        while self._is_running:
            if not self.ib.isConnected():
                logger.warning("IB connection lost. Attempting to reconnect...")
                try:
                    await self.ib.connectAsync(self.ib_host, self.ib_port, clientId=self.client_id)
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
        self.context.term()
        logger.info("Proxy stopped.")
