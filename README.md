# IBKR ZeroMQ Proxy

A high-performance, asynchronous proxy that bridges the Interactive Brokers (IBKR) API to ZeroMQ. It allows multiple downstream services to consume real-time market data, account updates, and trade executions without maintaining individual connections to the IB API.

## Features
- **Headless IB Gateway**: Runs in Docker with automated login (IBC).
- **ZeroMQ PUB/SUB**: Broadcasts data to any number of consumers.
- **Asynchronous**: Built with `ib_async` and `asyncio` for high throughput.
- **Auto-reconnect**: Automatically reconnects to IB Gateway if the connection is lost.

## Architecture
```
IBKR Server <-> IB Gateway (Docker) <-> Python Proxy <-> ZeroMQ (PUB/SUB)
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

### 3. Start IB Gateway
```bash
docker compose up -d
```
Check logs to ensure it logs in successfully:
```bash
docker compose logs -f
```

### 4. Install Proxy Dependencies
```bash
# Install uv if you haven't: https://astral.sh/uv/install.sh
uv sync
```

### 5. Run the Proxy
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
