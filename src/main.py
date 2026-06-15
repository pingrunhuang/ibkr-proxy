import asyncio
import os
import argparse
from dotenv import load_dotenv
from loguru import logger
from logger import configure_logger
from proxy import IBProxy

load_dotenv()
configure_logger()

async def main():
    parser = argparse.ArgumentParser(description='IBKR ZeroMQ Proxy')
    parser.add_argument('--ib-host', default=os.getenv('IB_HOST', '127.0.0.1'), help='IB Gateway host')
    parser.add_argument('--ib-port', type=int, default=int(os.getenv('IB_API_PORT', 4002)), help='IB Gateway port')
    parser.add_argument('--client-id', type=int, default=int(os.getenv('IB_CLIENT_ID', 1)), help='IB API client ID')
    parser.add_argument('--zmq-port', type=int, default=int(os.getenv('ZMQ_PUB_PORT', 5555)), help='ZeroMQ PUB port')
    parser.add_argument('--zmq-rep-port', type=int, default=int(os.getenv('ZMQ_REP_PORT', 5556)), help='ZeroMQ REP port for commands')
    parser.add_argument('--symbols', default=os.getenv('IB_SYMBOLS', 'AAPL,TSLA,SPY'), help='Comma-separated symbols to subscribe to')
    
    args = parser.parse_args()
    
    proxy = IBProxy(
        ib_host=args.ib_host,
        ib_port=args.ib_port,
        client_id=args.client_id,
        zmq_port=args.zmq_port,
        zmq_rep_port=args.zmq_rep_port
    )
    
    try:
        await proxy.connect()
        
        # Subscribe to market data for symbols.
        symbols = [s.strip() for s in args.symbols.split(',') if s.strip()]
        qualified = await proxy.qualify_contracts(symbols=symbols)
        qualified_contracts = [r['contract'] for r in qualified]
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
