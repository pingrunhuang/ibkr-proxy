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

Complete `.env` template:

```env
# IB Gateway / IBC login
TWS_USERID=your_ibkr_username
TWS_PASSWORD=your_ibkr_password
TRADING_MODE=paper
TWS_ACCEPT_EULA=yes
READ_ONLY_API=no

# IB Gateway VNC password used by docker-compose.yml.
# docker-compose maps this into the container as VNC_SERVER_PASSWORD.
VNC_PASSWORD=ibgateway

# Logging
LOG_LEVEL=INFO
LOG_BACKUP_COUNT=30

# Proxy connection to IB Gateway.
# docker-compose overrides these to IB_HOST=ib-gateway and IB_API_PORT=4004.
IB_HOST=127.0.0.1
IB_API_PORT=4002
IB_CLIENT_ID=99

# Initial market data subscriptions for proxy startup.
# Runtime subscriptions can also be requested via the subscribe_market_data command.
# Ambiguous futures such as COMEX silver need a multiplier or trading class.
# Example standard silver: FUT:SI:202608:COMEX:5000:SI
# Example mini silver: FUT:SI:202608:COMEX:1000:SIL
IB_SYMBOLS=AAPL,FX:USDCNH,CRYPTO:BTC

# ZeroMQ ports exposed by the proxy.
ZMQ_PUB_PORT=5555
ZMQ_REP_PORT=5556
```

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

The systemd installer supports these one-off override variables when running
`scripts/install_systemd.sh`; they are shell environment overrides, not values
that the service reads from `.env` at runtime:

```env
SYSTEMD_UNIT_DIR=/etc/systemd/system
SKIP_SYSTEMD_RELOAD=false
```

The installer only installs `ibkr-proxy.service`. Scheduling is handled by
Prefect, not by systemd timers.

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
- `marketdata.IB.<conId>`: Streaming tick data keyed by IBKR's unique contract ID.
  - Example: `marketdata.IB.760200615`
  - The payload includes `request_symbol`, `symbol_key`, `conId`, `localSymbol`, `lastTradeDateOrContractMonth`, `multiplier`, and `tradingClass`.
- `account.<account_id>`: Account balance and margin.
- `portfolio.<account_id>`: Position updates.
- `executions.<account_id>`: Order fills and trade details.
- `pnl.<account_id>.account`: Daily PnL for the account.
- `pnl.<account_id>.<conId>`: Daily PnL for a specific contract.

## ZeroMQ Commands
Send JSON requests to `tcp://<host>:5556`:

- `{"action": "get_account"}`
- `{"action": "ping"}`
- `{"action": "get_positions"}`
- `{"action": "get_orders"}`
- `{"action": "get_trades", "client_id": "multi-market-engine", "strategy_id": "GcStrategy", "after_id": 0, "limit": 500}`
- `{"action": "qualify_contracts", "symbols": ["FUT:SI:202608:COMEX:5000:SI"]}`
- `{"action": "subscribe_market_data", "symbols": ["CASH.USD.CNH.IDEALPRO"]}`
- `{"action": "subscribe_market_data", "symbols": ["FUT:SI:202608:COMEX:5000:SI"]}`
- `{"action": "subscribe_market_data", "contracts": [{"sec_type": "FUT", "symbol": "SI", "exchange": "COMEX", "currency": "USD", "expiry": "202608", "multiplier": "5000", "trading_class": "SI"}]}`
- `{"action": "place_order", "con_id": 760200615, "qty": 1, "action_type": "BUY", "order_type": "LMT", "lmt_price": 38.0}`
- `{"action": "place_order", "sec_type": "STK", "symbol": "AAPL", "exchange": "SMART", "currency": "USD", "qty": 1, "action_type": "BUY", "order_type": "LMT", "lmt_price": 100.0}`
- `{"action": "cancel_order", "order_id": 123}`

Execution payloads are written to the ownership SQLite database before PUB delivery.
`get_trades` is owner-scoped and cursor-paginated so the Engine can replay fills after
startup or reconnect; Engine-side `event_id` processing remains idempotent.

`qualify_contracts` resolves readable symbols without subscribing. `subscribe_market_data` resolves and subscribes. Both return contract metadata with the ZMQ topic:
```json
{
  "request_symbol": "FUT:SI:202608:COMEX:5000:SI",
  "symbol_key": "FUT.SI.USD.COMEX.20260827.5000.SI",
  "topic": "marketdata.IB.760200615",
  "conId": 760200615,
  "secType": "FUT",
  "symbol": "SI",
  "currency": "USD",
  "exchange": "COMEX",
  "localSymbol": "SIQ6",
  "lastTradeDateOrContractMonth": "20260827",
  "multiplier": "5000",
  "tradingClass": "SI"
}
```

If `place_order` includes `con_id`, the proxy first looks up the qualified contract in its local registry and places the order with that exact contract. This avoids ambiguous futures orders after the engine has qualified and subscribed the contract.

Supported symbol formats for `subscribe_market_data` include:
- `AAPL`
- `STK.AAPL.USD.SMART`
- `CASH.USD.CNH.IDEALPRO`
- `FUT.ES.USD.CME.202609`
- `FX:USDCNH`
- `CRYPTO:BTC`
- `FUT:ES:202609:CME`
- `ES:202609:CME`
- `FUT:SI:202608:COMEX:5000:SI`
- `FUT.SI.USD.COMEX.202608.multiplier=1000.tradingClass=SIL`

Some futures share the same symbol, month, and exchange. COMEX silver is one example: `SI` can resolve to standard silver (`multiplier=5000`, `tradingClass=SI`) or mini silver (`multiplier=1000`, `tradingClass=SIL`). Use an explicit multiplier or trading class when IBKR reports an ambiguous contract.

## Tests
Proxy-side unit tests mock IBKR objects and do not require a live IB Gateway:
```bash
uv run --with pytest pytest tests -q
```

These tests validate contract parsing, order payload normalization, and subscription calls. Live IBKR integration checks should be run from the trading engine test suite with the explicit integration flags documented there.
