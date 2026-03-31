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
        self.handler._CHART_CACHE = self.handler._CoalescingTTLCache("chart", max_entries=16)

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

    def test_stock_intraday_endpoint_falls_back_to_ohlc_when_primary_empty(self):
        self.handler.vnstock.stock_intraday_data = lambda **kwargs: []
        self.handler._tcbs_intraday = lambda symbol, page_size: []
        self.handler.vnstock.ohlc_data = lambda **kwargs: [
            {"time": "2026-03-30 09:15:00", "close": 74.4, "ticker": "FPT"}
        ]

        event = {
            "httpMethod": "GET",
            "queryStringParameters": {"cmd": "stock_intraday_data", "symbol": "FPT", "page_size": "50"},
        }
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])

        self.assertTrue(payload["success"])
        self.assertEqual(payload["cmd"], "stock_intraday_data")
        self.assertEqual(len(payload["data"]), 1)
        self.assertEqual(payload["data"][0]["ticker"], "FPT")

    def test_stock_intraday_endpoint_falls_back_to_history_when_ohlc_empty(self):
        self.handler.vnstock.stock_intraday_data = lambda **kwargs: []
        self.handler._tcbs_intraday = lambda symbol, page_size: []
        self.handler.vnstock.ohlc_data = lambda **kwargs: []
        self.handler.vnstock.stock_historical_data = lambda **kwargs: [
            {"time": "2026-03-30 09:16:00", "close": 74.5, "ticker": "FPT"}
        ]

        event = {
            "httpMethod": "GET",
            "queryStringParameters": {"cmd": "stock_intraday_data", "symbol": "FPT", "page_size": "50"},
        }
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])

        self.assertTrue(payload["success"])
        self.assertEqual(payload["cmd"], "stock_intraday_data")
        self.assertEqual(len(payload["data"]), 1)
        self.assertEqual(payload["data"][0]["time"], "2026-03-30 09:16:00")

    def test_price_depth_invalid_symbol_returns_400_without_calling_vnstock(self):
        calls = {"count": 0}

        def fake_price_depth(*args, **kwargs):
            calls["count"] += 1
            return [{"ticker": "AAA"}]

        self.handler.vnstock.price_depth = fake_price_depth
        self.handler._LISTING_COMPANIES_CACHE = self.handler._CoalescingTTLCache("listing_companies", max_entries=4)
        self.handler._LISTING_COMPANIES_CACHE.store(
            "listing_companies",
            [{"ticker": "FPT", "comGroupCode": "HOSE"}],
            ttl_s=60,
            stale_s=60,
        )

        event = {
            "httpMethod": "GET",
            "queryStringParameters": {"cmd": "price_depth", "symbols": "BTCUSDT"},
        }
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])

        self.assertEqual(response["statusCode"], 400)
        self.assertFalse(payload["success"])
        self.assertEqual(payload["error"], "invalid_symbol")
        self.assertEqual(calls["count"], 0)

    def test_price_depth_invalid_format_short_circuit_without_listing_lookup(self):
        self.handler._listed_ticker_set = lambda: (_ for _ in ()).throw(RuntimeError("must not be called"))
        self.handler.vnstock.price_depth = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("must not be called"))

        event = {
            "httpMethod": "GET",
            "queryStringParameters": {"cmd": "price_depth", "symbol": "BTCUSDT"},
        }
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])

        self.assertEqual(response["statusCode"], 400)
        self.assertFalse(payload["success"])
        self.assertEqual(payload["error"], "invalid_symbol")

    def test_price_depth_symbols_param_uses_only_valid_symbols(self):
        calls = {"args": []}

        def fake_price_depth(value):
            calls["args"].append(value)
            ticker = value[0] if isinstance(value, list) else value
            return [{"ticker": ticker, "bid": 1}]

        self.handler.vnstock.price_depth = fake_price_depth
        self.handler._LISTING_COMPANIES_CACHE = self.handler._CoalescingTTLCache("listing_companies", max_entries=4)
        self.handler._LISTING_COMPANIES_CACHE.store(
            "listing_companies",
            [{"ticker": "TIG", "comGroupCode": "HNX"}],
            ttl_s=60,
            stale_s=60,
        )

        event = {
            "httpMethod": "GET",
            "queryStringParameters": {"cmd": "price_depth", "symbols": "TIG,BTCUSDT"},
        }
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])

        self.assertEqual(response["statusCode"], 200)
        self.assertTrue(payload["success"])
        self.assertEqual(calls["args"], ["TIG"])
        self.assertEqual(payload["invalid_symbols"], ["BTCUSDT"])
        self.assertEqual(payload["data"][0]["ticker"], "TIG")

    def test_price_depth_requires_symbol_or_symbols(self):
        event = {
            "httpMethod": "GET",
            "queryStringParameters": {"cmd": "price_depth"},
        }
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])

        self.assertEqual(response["statusCode"], 400)
        self.assertFalse(payload["success"])
        self.assertEqual(payload["error"], "symbol_required")

    def test_price_depth_upstream_invalid_symbol_payload_becomes_failed_symbol(self):
        self.handler.vnstock.price_depth = lambda value: {
            "status": 400,
            "code": "BAD_REQUEST",
            "message": "invalid symbol",
        }
        self.handler._LISTING_COMPANIES_CACHE = self.handler._CoalescingTTLCache("listing_companies", max_entries=4)
        self.handler._LISTING_COMPANIES_CACHE.store(
            "listing_companies",
            [{"ticker": "SSI", "comGroupCode": "HOSE"}],
            ttl_s=60,
            stale_s=60,
        )

        event = {
            "httpMethod": "GET",
            "queryStringParameters": {"cmd": "price_depth", "symbol": "SSI"},
        }
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])

        self.assertEqual(response["statusCode"], 200)
        self.assertFalse(payload["success"])
        self.assertEqual(payload["data"], [])
        self.assertEqual(payload["failed_symbols"][0]["symbol"], "SSI")
        self.assertIn("invalid symbol", payload["failed_symbols"][0]["error"].lower())

    def test_price_depth_listing_failure_does_not_crash(self):
        self.handler._get_listing_companies_cached = lambda: (_ for _ in ()).throw(RuntimeError("listing down"))
        self.handler.vnstock.price_depth = lambda value: [{"ticker": "SSI", "bid": 1}]

        event = {
            "httpMethod": "GET",
            "queryStringParameters": {"cmd": "price_depth", "symbol": "SSI"},
        }
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])

        self.assertEqual(response["statusCode"], 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["data"][0]["ticker"], "SSI")

    def test_price_depth_timeout_does_not_crash_server(self):
        self.handler.PRICE_DEPTH_CALL_TIMEOUT_S = 1
        self.handler.vnstock.price_depth = lambda value: (time.sleep(1.2), [{"ticker": "KBC"}])[1]
        self.handler._LISTING_COMPANIES_CACHE = self.handler._CoalescingTTLCache("listing_companies", max_entries=4)
        self.handler._LISTING_COMPANIES_CACHE.store(
            "listing_companies",
            [{"ticker": "KBC", "comGroupCode": "HOSE"}],
            ttl_s=60,
            stale_s=60,
        )

        event = {
            "httpMethod": "GET",
            "queryStringParameters": {"cmd": "price_depth", "symbol": "KBC"},
        }
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])

        self.assertEqual(response["statusCode"], 200)
        self.assertFalse(payload["success"])
        self.assertEqual(payload["data"], [])
        self.assertEqual(payload["failed_symbols"][0]["symbol"], "KBC")
        self.assertIn("timeout", payload["failed_symbols"][0]["error"].lower())

    def test_technical_endpoint_uses_local_indicator_fallback(self):
        rows = []
        for day in range(1, 45):
            close = 100 + day
            rows.append(
                {
                    "time": f"2026-02-{day:02d} 00:00:00" if day <= 28 else f"2026-03-{(day-28):02d} 00:00:00",
                    "open": close - 1,
                    "high": close + 2,
                    "low": close - 2,
                    "close": close,
                    "volume": 1_000_000 + day * 1000,
                    "ticker": "FPT",
                }
            )
        self.handler.vnstock.stock_historical_data = lambda *args, **kwargs: rows

        event = {
            "httpMethod": "GET",
            "queryStringParameters": {"cmd": "technical", "symbol": "FPT", "indicator": "rsi"},
        }
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])

        self.assertEqual(response["statusCode"], 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["cmd"], "technical")
        self.assertTrue(len(payload["data"]) > 0)
        self.assertIn("rsi14", payload["data"][-1])

    def test_ticker_price_volatility_endpoint_returns_metrics(self):
        rows = []
        for day in range(1, 35):
            close = 50 + (day * 0.7)
            rows.append(
                {
                    "time": f"2026-03-{day:02d} 00:00:00" if day <= 30 else f"2026-04-{(day-30):02d} 00:00:00",
                    "open": close - 0.3,
                    "high": close + 1.0,
                    "low": close - 1.2,
                    "close": close,
                    "volume": 500_000 + day * 1000,
                    "ticker": "SSI",
                }
            )
        self.handler.vnstock.stock_historical_data = lambda *args, **kwargs: rows

        event = {
            "httpMethod": "GET",
            "queryStringParameters": {"cmd": "ticker_price_volatility", "symbol": "SSI", "window": "20"},
        }
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])

        self.assertEqual(response["statusCode"], 200)
        self.assertTrue(payload["success"])
        self.assertIn("annualized_volatility_pct", payload["data"])
        self.assertIn("max_drawdown_pct", payload["data"])

    def test_stock_screening_insights_filters_and_sorts(self):
        self.handler._LISTING_COMPANIES_CACHE = self.handler._CoalescingTTLCache("listing_companies", max_entries=4)
        self.handler._LISTING_COMPANIES_CACHE.store(
            "listing_companies",
            [
                {"ticker": "AAA", "comGroupCode": "HOSE"},
                {"ticker": "BBB", "comGroupCode": "HOSE"},
            ],
            ttl_s=60,
            stale_s=60,
        )

        def fake_history(*args, **kwargs):
            symbol = kwargs.get("symbol") or (args[0] if args else "AAA")
            if symbol == "AAA":
                return [
                    {"time": "2026-03-28 00:00:00", "open": 10, "high": 12, "low": 9, "close": 10.5, "volume": 1_500_000, "ticker": "AAA"},
                    {"time": "2026-03-29 00:00:00", "open": 10.6, "high": 12.2, "low": 10.2, "close": 11.8, "volume": 2_000_000, "ticker": "AAA"},
                ]
            return [
                {"time": "2026-03-28 00:00:00", "open": 8, "high": 8.3, "low": 7.7, "close": 8.0, "volume": 100_000, "ticker": "BBB"},
                {"time": "2026-03-29 00:00:00", "open": 7.9, "high": 8.1, "low": 7.6, "close": 7.7, "volume": 120_000, "ticker": "BBB"},
            ]

        self.handler.vnstock.stock_historical_data = fake_history

        event = {
            "httpMethod": "GET",
            "queryStringParameters": {
                "cmd": "stock_screening_insights",
                "exchange": "HOSE",
                "limit": "20",
                "min_volume": "500000",
                "sort_by": "traded_value",
            },
        }
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])

        self.assertEqual(response["statusCode"], 200)
        self.assertTrue(payload["success"])
        self.assertGreaterEqual(payload["count"], 1)
        self.assertEqual(payload["data"][0]["ticker"], "AAA")

    def test_vndirect_realtime_status_endpoint_returns_tagged_payload(self):
        class _DummyCollector:
            def status(self):
                return {
                    "success": True,
                    "api_group": "vndirect_realtime",
                    "api_source": "vndirect_websocket",
                    "connected": True,
                }

        self.handler._get_vndirect_collector = lambda: (_DummyCollector(), None)
        event = {"httpMethod": "GET", "queryStringParameters": {"cmd": "vndirect_realtime_status"}}
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])

        self.assertEqual(response["statusCode"], 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["api_group"], "vndirect_realtime")
        self.assertEqual(payload["api_source"], "vndirect_websocket")
        self.assertEqual(payload["cmd"], "vndirect_realtime_status")

    def test_vndirect_realtime_snapshot_endpoint_filters_input(self):
        class _DummyCollector:
            def snapshot(self, symbols=None, feeds=None, limit_per_feed=0):
                return {
                    "success": True,
                    "api_group": "vndirect_realtime",
                    "api_source": "vndirect_websocket",
                    "echo": {
                        "symbols": sorted(list(symbols or [])),
                        "feeds": sorted(list(feeds or [])),
                        "limit_per_feed": limit_per_feed,
                    },
                    "data": {"BA": []},
                }

        self.handler._get_vndirect_collector = lambda: (_DummyCollector(), None)
        event = {
            "httpMethod": "GET",
            "queryStringParameters": {
                "cmd": "vndirect_realtime_snapshot",
                "symbols": "fpt,vcb",
                "feeds": "ba,sp,invalid",
                "limit": "150",
            },
        }
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])

        self.assertEqual(response["statusCode"], 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["input"]["symbols"], ["FPT", "VCB"])
        self.assertEqual(payload["input"]["feeds"], ["BA", "SP"])
        self.assertEqual(payload["echo"]["symbols"], ["FPT", "VCB"])
        self.assertEqual(payload["echo"]["feeds"], ["BA", "SP"])
        self.assertEqual(payload["echo"]["limit_per_feed"], 150)


if __name__ == "__main__":
    unittest.main()
