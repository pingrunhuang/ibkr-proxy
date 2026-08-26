import asyncio
import os
import argparse
from dotenv import load_dotenv
from loguru import logger
from logger import configure_logger
from proxy import IBProxy

load_dotenv()
configure_logger()


TRUE_VALUES = frozenset({'1', 'true', 'yes', 'on', 'y'})


def bool_env(name, default=True):
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in TRUE_VALUES


async def main():
    parser = argparse.ArgumentParser(description='IBKR ZeroMQ Proxy')
    parser.add_argument('--ib-host', default=os.getenv('IB_HOST', '127.0.0.1'), help='IB Gateway host')
    parser.add_argument('--ib-port', type=int, default=int(os.getenv('IB_API_PORT', 4002)), help='IB Gateway port')
    parser.add_argument('--client-id', type=int, default=int(os.getenv('IB_CLIENT_ID', 1)), help='IB API client ID')
    parser.add_argument('--zmq-port', type=int, default=int(os.getenv('ZMQ_PUB_PORT', 5555)), help='ZeroMQ PUB port')
    parser.add_argument('--zmq-rep-port', type=int, default=int(os.getenv('ZMQ_REP_PORT', 5556)), help='ZeroMQ REP port for commands')
    parser.add_argument('--symbols', default=os.getenv('IB_SYMBOLS', 'AAPL,TSLA,SPY'), help='Comma-separated symbols to subscribe to')
    parser.add_argument(
        '--enable-md',
        action=argparse.BooleanOptionalAction,
        default=bool_env('IB_ENABLE_MD', True),
        help='Enable IB market data subscriptions',
    )
    parser.add_argument(
        '--ownership-db-path',
        default=os.getenv(
            'IB_ORDER_OWNERSHIP_DB_PATH',
            'data/ib_order_ownership.sqlite3',
        ),
        help='SQLite path for durable Engine order ownership',
    )
    
    args = parser.parse_args()
    
    proxy = IBProxy(
        ib_host=args.ib_host,
        ib_port=args.ib_port,
        client_id=args.client_id,
        zmq_port=args.zmq_port,
        zmq_rep_port=args.zmq_rep_port,
        ownership_db_path=args.ownership_db_path,
        enable_md=args.enable_md,
    )
    
    try:
        await proxy.connect()
        
        qualified_contracts = []
        if args.enable_md:
            # Subscribe to market data for symbols.
            symbols = [s.strip() for s in args.symbols.split(',') if s.strip()]
            qualified = await proxy.qualify_contracts(symbols=symbols)
            qualified_contracts = [r['contract'] for r in qualified]
            proxy.subscribe_market_data(qualified_contracts)
        else:
            logger.info('IB market data disabled by IB_ENABLE_MD=false')
        
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
        raise
    finally:
        proxy.stop()

if __name__ == "__main__":
    asyncio.run(main())
