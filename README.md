# IBKR ZeroMQ Proxy

A high-performance, asynchronous proxy that bridges the Interactive Brokers (IBKR) API to ZeroMQ. It allows multiple downstream services to consume real-time market data, account updates, and trade executions without maintaining individual connections to the IB API.

## Features
- **Headless IB Gateway**: Runs in Docker with automated login (IBC).
- **ZeroMQ PUB/SUB**: Broadcasts data to any number of consumers.
- **ZeroMQ REQ/REP commands**: Account, position, order, and runtime market-data subscription commands.
- **Asynchronous**: Built with `ib_async` and `asyncio` for high throughput.
- **Auto-reconnect**: Automatically reconnects to IB Gateway if the connection is lost.

## Architecture
```
IBKR Server <-> IB Gateway (Docker) <-> Python Proxy <-> ZeroMQ PUB/SUB + REQ/REP
```

## Setup

### 1. Prerequisites
- Docker and Docker Compose
- Python 3.10+

### 2. Configure IBKR Credentials
Copy the `.env.example` file to `.env` and fill in your IBKR paper trading credentials:
```bash
cp .env.example .env
```
Edit `.env`:
- `TWS_USERID`: Your IBKR username
- `TWS_PASSWORD`: Your IBKR password
- `TRADING_MODE`: `paper` (highly recommended for testing)
- `IB_SYMBOLS`: optional comma-separated startup subscriptions.
- `ZMQ_PUB_PORT`: market-data/event publish port, default `5555`.
- `ZMQ_REP_PORT`: command port, default `5556`.

### 3. Start IB Gateway and Proxy
```bash
docker compose up --build -d
```
Check logs to ensure it logs in successfully:
```bash
docker compose logs -f
```

The compose stack starts:
- `ib-gateway`: IB Gateway/IBC.
- `ib-proxy`: Python proxy connected to `ib-gateway:4004`.

From the trading engine host, use:
```env
ZMQ_HOST=127.0.0.1
ZMQ_PORT=5555
ZMQ_REP_PORT=5556
```

### 4. Install Proxy Dependencies for Local Development
```bash
# Install uv if you haven't: https://astral.sh/uv/install.sh
uv sync
```

### 5. Run the Proxy Locally
```bash
# Example: Subscribe to stocks, forex, and crypto
uv run src/main.py --symbols "AAPL,FX:USDCNH,CRYPTO:BTC"
```

### 6. Test with a Consumer
In another terminal:
```bash
uv run src/consumer_example.py
```

## ZeroMQ Topics
- `marketdata.<secType>.<symbol>.<currency>.<exchange>`: Streaming tick data.
  - Stocks: `marketdata.STK.AAPL.USD.SMART`
  - Forex: `marketdata.CASH.USD.CNH.IDEALPRO`
  - Crypto: `marketdata.CRYPTO.BTC.USD.PAXOS`
- `account.<account_id>`: Account balance and margin.
- `portfolio.<account_id>`: Position updates.
- `executions.<account_id>`: Order fills and trade details.
- `pnl.<account_id>.account`: Daily PnL for the account.
- `pnl.<account_id>.<conId>`: Daily PnL for a specific contract.

## ZeroMQ Commands
Send JSON requests to `tcp://<host>:5556`:

- `{"action": "get_account"}`
- `{"action": "get_positions"}`
- `{"action": "get_orders"}`
- `{"action": "subscribe_market_data", "symbols": ["CASH.USD.CNH.IDEALPRO"]}`
- `{"action": "place_order", "sec_type": "STK", "symbol": "AAPL", "exchange": "SMART", "currency": "USD", "qty": 1, "action_type": "BUY", "order_type": "LMT", "lmt_price": 100.0}`
- `{"action": "cancel_order", "order_id": 123}`

Supported symbol formats for `subscribe_market_data` include:
- `AAPL`
- `STK.AAPL.USD.SMART`
- `CASH.USD.CNH.IDEALPRO`
- `FUT.ES.USD.CME.202609`
- `FX:USDCNH`
- `CRYPTO:BTC`
- `FUT:ES:202609:CME`
- `ES:202609:CME`

## Tests
Proxy-side unit tests mock IBKR objects and do not require a live IB Gateway:
```bash
uv run --with pytest pytest tests -q
```

These tests validate contract parsing, order payload normalization, and subscription calls. Live IBKR integration checks should be run from the trading engine test suite with the explicit integration flags documented there.
