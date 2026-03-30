import importlib
import json
import os
import sys
import tempfile
import threading
import time
import types
import unittest


class _FakeVnstock(types.SimpleNamespace):
    pass


class _FakeResponse:
    def __init__(self, payload=None):
        self._payload = payload or {}

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeSession:
    def mount(self, *args, **kwargs):
        return None

    def get(self, *args, **kwargs):
        return _FakeResponse({})

    def close(self):
        return None


def _fake_requests_module():
    return types.SimpleNamespace(
        Session=lambda: _FakeSession(),
        adapters=types.SimpleNamespace(HTTPAdapter=lambda *args, **kwargs: object()),
    )


def _load_handler():
    sys.modules.pop("handler", None)
    sys.modules["requests"] = _fake_requests_module()
    fake_vnstock = _FakeVnstock(
        listing_companies=lambda: [],
        price_board=lambda symbol_ls=None: [],
        ohlc_data=lambda **kwargs: [],
        stock_historical_data=lambda *args, **kwargs: [],
        indices_listing=lambda: [],
        organ_listing=lambda: [],
        price_depth=lambda *args, **kwargs: [],
        stock_intraday_data=lambda *args, **kwargs: [],
        longterm_ohlc_data=lambda *args, **kwargs: [],
        live_stock_list=lambda: [],
        offline_stock_list=lambda: [],
    )
    sys.modules["vnstock"] = fake_vnstock
    return importlib.import_module("handler")


class HandlerCacheTests(unittest.TestCase):
    def setUp(self):
        self.handler = _load_handler()

    def test_coalescing_cache_only_loads_once(self):
        cache = self.handler._CoalescingTTLCache("test", max_entries=16)
        lock = threading.Lock()
        state = {"calls": 0}
        results = []

        def loader():
            with lock:
                state["calls"] += 1
            time.sleep(0.05)
            return {"ok": True}

        def worker():
            results.append(cache.get_or_load("same-key", loader, ttl_s=30, stale_s=0))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(state["calls"], 1)
        self.assertEqual(len(results), 8)
        self.assertTrue(all(item == {"ok": True} for item in results))

    def test_listing_companies_endpoint_uses_memory_cache(self):
        calls = {"count": 0}

        def fake_listing_companies():
            calls["count"] += 1
            return [{"ticker": "AAA", "comGroupCode": "HOSE"}]

        self.handler.vnstock.listing_companies = fake_listing_companies
        self.handler._LISTING_COMPANIES_CACHE = self.handler._CoalescingTTLCache("listing_companies", max_entries=4)
        with tempfile.TemporaryDirectory() as tmpdir:
            self.handler.LISTING_COMPANIES_CACHE_FILE = os.path.join(tmpdir, "listing_companies.json")
            event = {"httpMethod": "GET", "queryStringParameters": {"cmd": "listing_companies"}}
            first = self.handler.lambda_handler(event, None)
            second = self.handler.lambda_handler(event, None)

        self.assertEqual(calls["count"], 1)
        self.assertIn('"ticker": "AAA"', first["body"])
        self.assertEqual(first["body"], second["body"])

    def test_price_board_endpoint_caches_by_symbol_set(self):
        calls = {"count": 0}

        def fake_price_board(symbol_ls=None):
            calls["count"] += 1
            tickers = symbol_ls if isinstance(symbol_ls, list) else [symbol_ls]
            return [{"ticker": ticker, "close": 1000.0} for ticker in tickers]

        self.handler.vnstock.price_board = fake_price_board
        self.handler._PRICE_BOARD_CACHE = self.handler._CoalescingTTLCache("price_board", max_entries=16)
        event = {
            "httpMethod": "GET",
            "queryStringParameters": {"cmd": "price_board", "symbols": "BBB,AAA", "realtime": "0"},
        }
        reverse_event = {
            "httpMethod": "GET",
            "queryStringParameters": {"cmd": "price_board", "symbols": "AAA,BBB", "realtime": "0"},
        }

        first = self.handler.lambda_handler(event, None)
        second = self.handler.lambda_handler(reverse_event, None)

        self.assertEqual(calls["count"], 1)
        first_data = json.loads(first["body"])["data"]
        second_data = json.loads(second["body"])["data"]
        self.assertEqual([row["ticker"] for row in first_data], ["BBB", "AAA"])
        self.assertEqual([row["ticker"] for row in second_data], ["AAA", "BBB"])


if __name__ == "__main__":
    unittest.main()
