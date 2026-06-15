import asyncio
import importlib
import sys
import types
from pathlib import Path

import pytest


class FakeContract:
    def __init__(self, symbol="", exchange="", currency="", secType="", lastTradeDateOrContractMonth=""):
        self.symbol = symbol
        self.exchange = exchange
        self.currency = currency
        self.secType = secType
        self.lastTradeDateOrContractMonth = lastTradeDateOrContractMonth
        self.conId = 0
        self.localSymbol = ""
        self.multiplier = ""
        self.tradingClass = ""


class FakeStock(FakeContract):
    def __init__(self, symbol, exchange, currency):
        super().__init__(symbol, exchange, currency, "STK")


class FakeFuture(FakeContract):
    def __init__(
        self,
        symbol="",
        lastTradeDateOrContractMonth="",
        exchange="",
        localSymbol="",
        multiplier="",
        currency="",
        **kwargs,
    ):
        super().__init__(symbol, exchange, currency, "FUT", lastTradeDateOrContractMonth)
        self.localSymbol = localSymbol
        self.multiplier = multiplier


class FakeForex(FakeContract):
    def __init__(self, pair, exchange="IDEALPRO"):
        super().__init__(pair[:3], exchange, pair[3:], "CASH")


class FakeCrypto(FakeContract):
    def __init__(self, symbol, exchange, currency):
        super().__init__(symbol, exchange, currency, "CRYPTO")


class FakeEvent:
    def __init__(self, name):
        self.name = name


class FakeIB:
    def __init__(self):
        self.wrapper = types.SimpleNamespace(positionEnd=lambda: None)


class FakeOrder:
    account = "DU12345"
    orderId = 321
    permId = 654
    clientId = 99
    orderRef = "strategy-ref"
    action = "BUY"
    orderType = "LMT"
    lmtPrice = 150.0
    totalQuantity = 10


class FakeOrderStatus:
    status = "Submitted"
    filled = 4
    remaining = 6
    avgFillPrice = 0.0
    lastFillPrice = 0.0
    whyHeld = ""


class FakeExecution:
    def __init__(self):
        self.execId = "0001.01"
        self.acctNumber = "DU12345"
        self.exchange = "COMEX"
        self.side = "BOT"
        self.shares = 1.0
        self.price = 4216.4
        self.orderId = 321
        self.time = "2026-06-12T10:15:02+00:00"


class FakeCommissionReport:
    def __init__(self):
        self.execId = "0001.01"
        self.commission = 2.52
        self.currency = "USD"


class FakeIBForSubscriptions:
    def __init__(self):
        self.market_data_requests = []

    def reqMktData(self, contract):
        self.market_data_requests.append(contract)


class FakeIBForProxyCommands:
    def __init__(self, qualified_contracts=None):
        self.qualified_contracts = qualified_contracts or []
        self.qualification_requests = []
        self.market_data_requests = []
        self.placed_orders = []

    async def qualifyContractsAsync(self, *contracts):
        self.qualification_requests.append(contracts)
        return self.qualified_contracts

    def reqMktData(self, contract):
        self.market_data_requests.append(contract)

    def placeOrder(self, contract, order):
        self.placed_orders.append((contract, order))
        return types.SimpleNamespace(order=types.SimpleNamespace(orderId=987))


class FakeMarketOrder:
    def __init__(self, action, qty):
        self.action = action
        self.totalQuantity = qty
        self.orderType = "MKT"


class FakeLimitOrder:
    def __init__(self, action, qty, lmt_price):
        self.action = action
        self.totalQuantity = qty
        self.lmtPrice = lmt_price
        self.orderType = "LMT"


def install_fake_ib_async():
    fake_ib_async = types.ModuleType("ib_async")
    fake_ib_async.IB = FakeIB
    fake_ib_async.Event = FakeEvent
    fake_ib_async.Stock = FakeStock
    fake_ib_async.Future = FakeFuture
    fake_ib_async.Forex = FakeForex
    fake_ib_async.Crypto = FakeCrypto
    fake_ib_async.MarketOrder = FakeMarketOrder
    fake_ib_async.LimitOrder = FakeLimitOrder
    fake_ib_async.StartupFetch = object
    fake_ib_async.util = types.SimpleNamespace(asDict=lambda value: value.__dict__)
    sys.modules["ib_async"] = fake_ib_async


install_fake_ib_async()
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
proxy_module = importlib.import_module("proxy")


@pytest.fixture
def proxy():
    instance = proxy_module.IBProxy()
    try:
        yield instance
    finally:
        instance.pub_socket.close(0)
        instance.rep_socket.close(0)
        instance.context.term()


def test_contract_from_symbol_supports_plain_and_dotted_formats(proxy):
    stock = proxy._contract_from_symbol("STK.AAPL.USD.SMART")
    assert stock.secType == "STK"
    assert stock.symbol == "AAPL"
    assert stock.currency == "USD"
    assert stock.exchange == "SMART"

    forex = proxy._contract_from_symbol("CASH.USD.CNH.IDEALPRO")
    assert forex.secType == "CASH"
    assert forex.symbol == "USD"
    assert forex.currency == "CNH"
    assert forex.exchange == "IDEALPRO"

    future = proxy._contract_from_symbol("FUT.ES.USD.CME.202609")
    assert future.secType == "FUT"
    assert future.symbol == "ES"
    assert future.currency == "USD"
    assert future.exchange == "CME"
    assert future.lastTradeDateOrContractMonth == "202609"
    assert future.localSymbol == ""

    crypto = proxy._contract_from_symbol("CRYPTO.BTC.USD.PAXOS")
    assert crypto.secType == "CRYPTO"
    assert crypto.symbol == "BTC"
    assert crypto.currency == "USD"
    assert crypto.exchange == "PAXOS"


def test_contract_from_symbol_supports_startup_and_legacy_formats(proxy):
    assert proxy._contract_from_symbol("AAPL").secType == "STK"
    assert proxy._contract_from_symbol("FX:USDCNH").secType == "CASH"
    assert proxy._contract_from_symbol("CRYPTO:BTC").exchange == "PAXOS"

    startup_future = proxy._contract_from_symbol("FUT:ES:202609:CME")
    assert startup_future.secType == "FUT"
    assert startup_future.symbol == "ES"
    assert startup_future.exchange == "CME"
    assert startup_future.lastTradeDateOrContractMonth == "202609"

    legacy_future = proxy._contract_from_symbol("ES:202609:CME")
    assert legacy_future.secType == "FUT"
    assert legacy_future.symbol == "ES"
    assert legacy_future.exchange == "CME"
    assert legacy_future.lastTradeDateOrContractMonth == "202609"


def test_contract_from_symbol_supports_future_disambiguators(proxy):
    standard_silver = proxy._contract_from_symbol("FUT:SI:202608:COMEX:5000:SI")
    assert standard_silver.secType == "FUT"
    assert standard_silver.symbol == "SI"
    assert standard_silver.exchange == "COMEX"
    assert standard_silver.lastTradeDateOrContractMonth == "202608"
    assert standard_silver.multiplier == "5000"
    assert standard_silver.tradingClass == "SI"
    assert standard_silver.currency == "USD"
    assert standard_silver.localSymbol == ""

    mini_silver = proxy._contract_from_symbol("FUT.SI.USD.COMEX.202608.multiplier=1000.tradingClass=SIL")
    assert mini_silver.secType == "FUT"
    assert mini_silver.currency == "USD"
    assert mini_silver.multiplier == "1000"
    assert mini_silver.tradingClass == "SIL"


def silver_contract(month, con_id, local_symbol):
    contract = FakeFuture(
        symbol="SI",
        lastTradeDateOrContractMonth=month,
        exchange="COMEX",
        currency="USD",
    )
    contract.conId = con_id
    contract.localSymbol = local_symbol
    contract.multiplier = "5000"
    contract.tradingClass = "SI"
    return contract


def test_qualify_contracts_returns_distinct_conid_topics_for_si_futures(proxy):
    aug_silver = silver_contract("20260827", 760200615, "SIQ6")
    oct_silver = silver_contract("20261028", 760200616, "SIV6")
    fake_ib = FakeIBForProxyCommands([aug_silver, oct_silver])
    proxy.ib = fake_ib

    result = asyncio.run(proxy.qualify_contracts(symbols=[
        "FUT:SI:202608:COMEX:5000:SI",
        "FUT:SI:202610:COMEX:5000:SI",
    ]))

    assert len(result) == 2
    assert result[0]["metadata"]["request_symbol"] == "FUT:SI:202608:COMEX:5000:SI"
    assert result[0]["metadata"]["topic"] == "marketdata.IB.760200615"
    assert result[0]["metadata"]["symbol_key"] == "FUT.SI.USD.COMEX.20260827.5000.SI"
    assert result[1]["metadata"]["request_symbol"] == "FUT:SI:202610:COMEX:5000:SI"
    assert result[1]["metadata"]["topic"] == "marketdata.IB.760200616"
    assert result[0]["metadata"]["topic"] != result[1]["metadata"]["topic"]
    assert proxy.contracts_by_con_id["760200615"]["contract"] is aug_silver
    assert proxy.request_symbol_to_con_id["FUT:SI:202608:COMEX:5000:SI"] == "760200615"


def test_on_pending_tickers_publishes_conid_topic_and_contract_metadata(proxy):
    contract = silver_contract("20260827", 760200615, "SIQ6")
    proxy._register_contract(contract, "FUT:SI:202608:COMEX:5000:SI")
    published = []

    async def fake_publish(topic, data):
        published.append((topic, data))

    async def run_tick():
        proxy.publish = fake_publish
        ticker = types.SimpleNamespace(
            contract=contract,
            bid=38.1,
            bidSize=2,
            ask=38.2,
            askSize=3,
            last=38.15,
            lastSize=1,
            volume=100,
            time=None,
        )
        proxy.on_pending_tickers([ticker])
        await asyncio.sleep(0)

    asyncio.run(run_tick())

    assert len(published) == 1
    topic, data = published[0]
    assert topic == "marketdata.IB.760200615"
    assert data["topic"] == "marketdata.IB.760200615"
    assert data["request_symbol"] == "FUT:SI:202608:COMEX:5000:SI"
    assert data["symbol_key"] == "FUT.SI.USD.COMEX.20260827.5000.SI"
    assert data["conId"] == 760200615
    assert data["localSymbol"] == "SIQ6"
    assert data["lastTradeDateOrContractMonth"] == "20260827"
    assert data["multiplier"] == "5000"
    assert data["tradingClass"] == "SI"
    assert data["bid"] == 38.1
    assert data["ask"] == 38.2


def test_on_exec_details_publishes_execution_topic_and_payload(proxy):
    contract = silver_contract("20260827", 760200615, "SIQ6")
    published = []

    async def fake_publish(topic, data):
        published.append((topic, data))

    async def run_execution():
        proxy.publish = fake_publish
        fill = types.SimpleNamespace(
            contract=contract,
            execution=FakeExecution(),
            commissionReport=FakeCommissionReport(),
        )
        proxy.on_exec_details(types.SimpleNamespace(), fill)
        await asyncio.sleep(0)

    asyncio.run(run_execution())

    assert len(published) == 1
    topic, data = published[0]
    assert topic == "executions.DU12345"
    assert data["account"] == "DU12345"
    assert data["conId"] == 760200615
    assert data["execution"]["execId"] == "0001.01"
    assert data["execution"]["orderId"] == 321
    assert data["commission"]["commission"] == 2.52


def test_place_order_with_conid_uses_registered_contract(proxy):
    contract = silver_contract("20260827", 760200615, "SIQ6")
    proxy._register_contract(contract, "FUT:SI:202608:COMEX:5000:SI")
    fake_ib = FakeIBForProxyCommands()
    proxy.ib = fake_ib

    response = proxy._place_order_from_request({
        "con_id": 760200615,
        "qty": 1,
        "action_type": "BUY",
        "order_type": "LMT",
        "lmt_price": 38.0,
    })

    assert response == {"status": "success", "order_id": 987}
    placed_contract, placed_order = fake_ib.placed_orders[0]
    assert placed_contract is contract
    assert placed_order.action == "BUY"
    assert placed_order.totalQuantity == 1
    assert placed_order.orderType == "LMT"
    assert placed_order.lmtPrice == 38.0


def test_order_update_data_normalizes_trade(proxy):
    trade = types.SimpleNamespace(
        contract=FakeStock("AAPL", "SMART", "USD"),
        order=FakeOrder(),
        orderStatus=FakeOrderStatus(),
    )

    data = proxy._order_update_data(trade)

    assert data["secType"] == "STK"
    assert data["symbol"] == "AAPL"
    assert data["currency"] == "USD"
    assert data["exchange"] == "SMART"
    assert data["account"] == "DU12345"
    assert data["order_id"] == 321
    assert data["orderRef"] == "strategy-ref"
    assert data["action"] == "BUY"
    assert data["status"] == "Submitted"
    assert data["filled"] == 4
    assert data["remaining"] == 6
    assert data["price"] == 150.0


def test_subscribe_market_data_requests_each_contract(proxy):
    fake_ib = FakeIBForSubscriptions()
    proxy.ib = fake_ib
    contracts = [
        FakeStock("AAPL", "SMART", "USD"),
        FakeForex("USDCNH", "IDEALPRO"),
    ]
    contracts[0].conId = 1001
    contracts[1].conId = 1002

    proxy.subscribe_market_data(contracts)

    assert fake_ib.market_data_requests == contracts
