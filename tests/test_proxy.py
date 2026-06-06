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


class FakeStock(FakeContract):
    def __init__(self, symbol, exchange, currency):
        super().__init__(symbol, exchange, currency, "STK")


class FakeFuture(FakeContract):
    def __init__(self, symbol, expiry, exchange, currency="USD"):
        super().__init__(symbol, exchange, currency, "FUT", expiry)


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


class FakeIBForSubscriptions:
    def __init__(self):
        self.market_data_requests = []

    def reqMktData(self, contract):
        self.market_data_requests.append(contract)


def install_fake_ib_async():
    fake_ib_async = types.ModuleType("ib_async")
    fake_ib_async.IB = FakeIB
    fake_ib_async.Event = FakeEvent
    fake_ib_async.Stock = FakeStock
    fake_ib_async.Future = FakeFuture
    fake_ib_async.Forex = FakeForex
    fake_ib_async.Crypto = FakeCrypto
    fake_ib_async.MarketOrder = object
    fake_ib_async.LimitOrder = object
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

    proxy.subscribe_market_data(contracts)

    assert fake_ib.market_data_requests == contracts
