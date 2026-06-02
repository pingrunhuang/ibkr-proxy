import asyncio
import os
import argparse
import logging
import sys
from dotenv import load_dotenv
from loguru import logger
from ib_async import Stock, Forex, Crypto
from proxy import IBProxy

# Intercept standard logging
class InterceptHandler(logging.Handler):
    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())

logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

# Configure loguru
logger.remove()  # Remove default handler
logger.add(sys.stderr, level="INFO")  # Add back console logging
logger.add("logs/ibkr_proxy.log", rotation="4 hours", level="INFO", enqueue=True)

async def main():
    load_dotenv()
    
    parser = argparse.ArgumentParser(description='IBKR ZeroMQ Proxy')
    parser.add_argument('--ib-host', default=os.getenv('IB_HOST', '127.0.0.1'), help='IB Gateway host')
    parser.add_argument('--ib-port', type=int, default=int(os.getenv('IB_API_PORT', 4002)), help='IB Gateway port')
    parser.add_argument('--client-id', type=int, default=int(os.getenv('IB_CLIENT_ID', 1)), help='IB API client ID')
    parser.add_argument('--zmq-port', type=int, default=int(os.getenv('ZMQ_PUB_PORT', 5555)), help='ZeroMQ PUB port')
    parser.add_argument('--symbols', default=os.getenv('IB_SYMBOLS', 'AAPL,TSLA,SPY'), help='Comma-separated symbols to subscribe to')
    
    args = parser.parse_args()
    
    proxy = IBProxy(
        ib_host=args.ib_host,
        ib_port=args.ib_port,
        client_id=args.client_id,
        zmq_port=args.zmq_port
    )
    
    try:
        await proxy.connect()
        
        # Subscribe to market data for symbols
        symbols = [s.strip() for s in args.symbols.split(',')]
        contracts = []
        for s in symbols:
            if s.startswith('FX:'):
                pair = s[3:]
                contracts.append(Forex(pair))
            elif s.startswith('CRYPTO:'):
                coin = s[7:]
                contracts.append(Crypto(coin, 'PAXOS', 'USD'))
            else:
                contracts.append(Stock(s, 'SMART', 'USD'))
        
        # Qualify contracts (resolves conId, exchange, etc.)
        qualified_contracts = await proxy.ib.qualifyContractsAsync(*contracts)
        proxy.subscribe_market_data(qualified_contracts)
        
        # Subscribe to account updates
        accounts = proxy.ib.managedAccounts()
        if accounts:
            logger.info(f"Subscribing to updates for accounts: {accounts}")
            for acc in accounts:
                proxy.subscribe_pnl(acc, qualified_contracts)
        
        # Keep running
        await proxy.run_forever()
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as e:
        logger.exception(f"Unhandled exception: {e}")
    finally:
        proxy.stop()

if __name__ == "__main__":
    asyncio.run(main())
