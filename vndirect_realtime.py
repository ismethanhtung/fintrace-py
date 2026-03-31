import asyncio
import json
import logging
import os
import re
import ssl
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

LOGGER = logging.getLogger(__name__)

BA = "BA"
SP = "SP"
MI = "MI"
DE = "DE"

_FEED_TO_WIRE_NAME = {
    BA: "BA",
    SP: "SP",
    MI: "MI",
    DE: "DERIVATIVE_OPT",
}

_WIRE_TO_FEED = {
    "BA": BA,
    "SP": SP,
    "MI": MI,
    "DERIVATIVE_OPT": DE,
}

_SYMBOL_RE = re.compile(r"^[A-Z0-9]{1,15}$")
_MARKET_ID_RE = re.compile(r"^[0-9]{2}$")


BA_FIELDS: List[Tuple[str, str]] = [
    ("time", "str"),
    ("code", "str"),
    ("bidPrice01", "float"),
    ("bidQtty01", "float"),
    ("bidPrice02", "float"),
    ("bidQtty02", "float"),
    ("bidPrice03", "float"),
    ("bidQtty03", "float"),
    ("offerPrice01", "float"),
    ("offerQtty01", "float"),
    ("offerPrice02", "float"),
    ("offerQtty02", "float"),
    ("offerPrice03", "float"),
    ("offerQtty03", "float"),
    ("accumulatedVol", "float"),
    ("matchPrice", "float"),
    ("matchQtty", "float"),
    ("matchValue", "float"),
    ("totalOfferQtty", "float"),
    ("totalBidQtty", "float"),
]

SP_FIELDS: List[Tuple[str, str]] = [
    ("floorCode", "str"),
    ("tradingDate", "str"),
    ("time", "str"),
    ("code", "str"),
    ("companyName", "str"),
    ("stockType", "str"),
    ("totalRoom", "float"),
    ("currentRoom", "float"),
    ("basicPrice", "float"),
    ("openPrice", "float"),
    ("closePrice", "float"),
    ("currentPrice", "float"),
    ("currentQtty", "float"),
    ("highestPrice", "float"),
    ("lowestPrice", "float"),
    ("ceilingPrice", "float"),
    ("floorPrice", "float"),
    ("averagePrice", "float"),
    ("accumulatedVal", "float"),
    ("buyForeignQtty", "float"),
    ("sellForeignQtty", "float"),
    ("projectOpen", "float"),
    ("sequence", "str"),
]

MI_FIELDS: List[Tuple[str, str]] = [
    ("marketID", "str"),
    ("totalTrade", "float"),
    ("totalShareTraded", "float"),
    ("totalValueTraded", "float"),
    ("advance", "float"),
    ("decline", "float"),
    ("noChange", "float"),
    ("indexValue", "float"),
    ("changed", "float"),
    ("tradingTime", "str"),
    ("tradingDate", "str"),
    ("floorCode", "str"),
    ("marketIndex", "float"),
    ("priorMarketIndex", "float"),
    ("highestIndex", "float"),
    ("lowestIndex", "float"),
    ("shareTraded", "float"),
    ("status", "str"),
    ("sequence", "str"),
    ("predictionMarketIndex", "float"),
]

DE_FIELDS: List[Tuple[str, str]] = [
    ("accumulatedVal", "float"),
    ("accumulatedVol", "float"),
    ("basicPrice", "float"),
    ("bidPrice01", "float"),
    ("bidPrice02", "float"),
    ("bidPrice03", "float"),
    ("bidQtty01", "float"),
    ("bidQtty02", "float"),
    ("bidQtty03", "float"),
    ("buyForeignQtty", "float"),
    ("ceilingPrice", "float"),
    ("code", "str"),
    ("currentPrice", "float"),
    ("currentQtty", "float"),
    ("highestPrice", "float"),
    ("lastTradingDate", "str"),
    ("lowestPrice", "float"),
    ("matchPrice", "float"),
    ("matchQtty", "float"),
    ("offerPrice01", "float"),
    ("offerPrice02", "float"),
    ("offerPrice03", "float"),
    ("offerQtty01", "float"),
    ("offerQtty02", "float"),
    ("offerQtty03", "float"),
    ("openInterest", "float"),
    ("openPrice", "float"),
    ("sellForeignQtty", "float"),
    ("tradingSessionId", "str"),
    ("bidPrice04", "float"),
    ("bidPrice05", "float"),
    ("bidPrice06", "float"),
    ("bidPrice07", "float"),
    ("bidPrice08", "float"),
    ("bidPrice09", "float"),
    ("bidPrice10", "float"),
    ("bidQtty04", "float"),
    ("bidQtty05", "float"),
    ("bidQtty06", "float"),
    ("bidQtty07", "float"),
    ("bidQtty08", "float"),
    ("bidQtty09", "float"),
    ("bidQtty10", "float"),
    ("offerPrice04", "float"),
    ("offerPrice05", "float"),
    ("offerPrice06", "float"),
    ("offerPrice07", "float"),
    ("offerPrice08", "float"),
    ("offerPrice09", "float"),
    ("offerPrice10", "float"),
    ("offerQtty04", "float"),
    ("offerQtty05", "float"),
    ("offerQtty06", "float"),
    ("offerQtty07", "float"),
    ("offerQtty08", "float"),
    ("offerQtty09", "float"),
    ("offerQtty10", "float"),
    ("time", "str"),
    ("floorPrice", "float"),
]

_FEED_FIELDS = {
    BA: BA_FIELDS,
    SP: SP_FIELDS,
    MI: MI_FIELDS,
    DE: DE_FIELDS,
}


def _to_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(str(value).replace(",", "").strip())
    except Exception:
        return 0.0


def _coerce(value: str, typ: str) -> Any:
    if typ == "float":
        return _to_float(value)
    return value


def _to_utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _normalize_csv_upper(value: Optional[str]) -> List[str]:
    if not value:
        return []
    out = []
    seen = set()
    for raw in str(value).split(","):
        token = raw.strip().upper()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _read_symbol_file(path: Path) -> List[str]:
    if not path.exists() or not path.is_file():
        return []
    out = []
    seen = set()
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        token = line.strip().upper()
        if not token or token in seen:
            continue
        if _SYMBOL_RE.match(token):
            seen.add(token)
            out.append(token)
    return out


def _load_default_equity_symbols() -> List[str]:
    base = Path(__file__).resolve().parent / "Real-time-data-vndirect-master" / "StockIDs"
    files = [base / "HSX.txt", base / "HNX.txt", base / "UPC.txt"]
    merged = []
    seen = set()
    for file_path in files:
        for token in _read_symbol_file(file_path):
            if token in seen:
                continue
            seen.add(token)
            merged.append(token)

    env_symbols = _normalize_csv_upper(os.getenv("VNDIRECT_DEFAULT_SYMBOLS", ""))
    for token in env_symbols:
        if token in seen:
            continue
        if _SYMBOL_RE.match(token):
            seen.add(token)
            merged.append(token)

    limit_raw = os.getenv("VNDIRECT_DEFAULT_SYMBOL_LIMIT", "0").strip()
    try:
        limit = max(0, int(limit_raw or "0"))
    except Exception:
        limit = 0
    if limit > 0:
        return merged[:limit]
    return merged


def _load_default_derivative_symbols() -> List[str]:
    defaults = ["VN30F1M", "VN30F2M", "VN30F1Q", "VN30F2Q"]
    env_symbols = _normalize_csv_upper(os.getenv("VNDIRECT_DERIVATIVE_CODES", ""))
    merged = []
    seen = set()
    for token in env_symbols + defaults:
        if token in seen:
            continue
        if _SYMBOL_RE.match(token):
            seen.add(token)
            merged.append(token)
    return merged


def _load_default_market_ids() -> List[str]:
    env_ids = _normalize_csv_upper(os.getenv("VNDIRECT_MARKET_IDS", "10,11,12,13,02,03"))
    out = []
    seen = set()
    for token in env_ids:
        if token in seen:
            continue
        if _MARKET_ID_RE.match(token):
            seen.add(token)
            out.append(token)
    return out


def _parse_feed_payload(feed_type: str, payload: str) -> Dict[str, Any]:
    fields = _FEED_FIELDS.get(feed_type)
    if fields is None:
        return {"raw": payload}
    parts = str(payload or "").split("|")
    out: Dict[str, Any] = {}
    for idx, (name, typ) in enumerate(fields):
        val = parts[idx] if idx < len(parts) else ""
        out[name] = _coerce(val, typ)
    if len(parts) > len(fields):
        out["_extra"] = parts[len(fields) :]
    return out


class VndirectRealtimeCollector:
    def __init__(
        self,
        ws_url: str,
        equity_symbols: Optional[Iterable[str]] = None,
        derivative_symbols: Optional[Iterable[str]] = None,
        market_ids: Optional[Iterable[str]] = None,
        reconnect_delay_s: float = 2.0,
        recv_timeout_s: float = 10.0,
    ):
        self.ws_url = ws_url
        self.reconnect_delay_s = max(0.5, float(reconnect_delay_s))
        self.recv_timeout_s = max(2.0, float(recv_timeout_s))

        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws: Any = None
        self._connected = False
        self._started = False

        self._equity_symbols: List[str] = self._normalize_symbols(equity_symbols or [])
        self._derivative_symbols: List[str] = self._normalize_symbols(derivative_symbols or [])
        self._market_ids: List[str] = self._normalize_market_ids(market_ids or [])

        self._last_message_at: Optional[str] = None
        self._last_connected_at: Optional[str] = None
        self._last_error: Optional[str] = None
        self._reconnect_count = 0
        self._messages_total = 0
        self._messages_by_feed: Dict[str, int] = {BA: 0, SP: 0, MI: 0, DE: 0}

        self._snapshots: Dict[str, Dict[str, Dict[str, Any]]] = {
            BA: {},
            SP: {},
            DE: {},
            MI: {},
        }

    @staticmethod
    def _normalize_symbols(values: Iterable[str]) -> List[str]:
        out = []
        seen = set()
        for raw in values:
            token = str(raw or "").strip().upper()
            if not token or token in seen:
                continue
            if not _SYMBOL_RE.match(token):
                continue
            seen.add(token)
            out.append(token)
        return out

    @staticmethod
    def _normalize_market_ids(values: Iterable[str]) -> List[str]:
        out = []
        seen = set()
        for raw in values:
            token = str(raw or "").strip().upper()
            if not token or token in seen:
                continue
            if not _MARKET_ID_RE.match(token):
                continue
            seen.add(token)
            out.append(token)
        return out

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_thread, name="vndirect-realtime", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            self._stop_event.set()
            thread = self._thread
            loop = self._loop
        if loop is not None:
            try:
                loop.call_soon_threadsafe(lambda: None)
            except Exception:
                pass
        if thread is not None:
            thread.join(timeout=5)
        with self._lock:
            self._connected = False
            self._started = False
            self._thread = None
            self._loop = None
            self._ws = None

    def configure(
        self,
        equity_symbols: Optional[Iterable[str]] = None,
        derivative_symbols: Optional[Iterable[str]] = None,
        market_ids: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            if equity_symbols is not None:
                self._equity_symbols = self._normalize_symbols(equity_symbols)
            if derivative_symbols is not None:
                self._derivative_symbols = self._normalize_symbols(derivative_symbols)
            if market_ids is not None:
                self._market_ids = self._normalize_market_ids(market_ids)
            state = {
                "equity_symbols": list(self._equity_symbols),
                "derivative_symbols": list(self._derivative_symbols),
                "market_ids": list(self._market_ids),
            }
        self._request_live_resubscribe()
        return state

    def add_symbols(
        self,
        equity_symbols: Optional[Iterable[str]] = None,
        derivative_symbols: Optional[Iterable[str]] = None,
        market_ids: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            if equity_symbols is not None:
                self._equity_symbols = self._normalize_symbols(list(self._equity_symbols) + list(equity_symbols))
            if derivative_symbols is not None:
                self._derivative_symbols = self._normalize_symbols(list(self._derivative_symbols) + list(derivative_symbols))
            if market_ids is not None:
                self._market_ids = self._normalize_market_ids(list(self._market_ids) + list(market_ids))
            state = {
                "equity_symbols": list(self._equity_symbols),
                "derivative_symbols": list(self._derivative_symbols),
                "market_ids": list(self._market_ids),
            }
        self._request_live_resubscribe()
        return state

    def snapshot(self, symbols: Optional[Set[str]] = None, feeds: Optional[Set[str]] = None, limit_per_feed: int = 2000) -> Dict[str, Any]:
        safe_limit = max(1, min(10000, int(limit_per_feed)))
        with self._lock:
            feed_keys = list(self._snapshots.keys())
            if feeds:
                feed_keys = [f for f in feed_keys if f in feeds]

            feed_data: Dict[str, List[Dict[str, Any]]] = {}
            for feed in feed_keys:
                values = list(self._snapshots.get(feed, {}).values())
                if symbols and feed in (BA, SP, DE):
                    values = [row for row in values if str(row.get("symbol") or "").upper() in symbols]
                values.sort(key=lambda row: str(row.get("received_at") or ""), reverse=True)
                feed_data[feed] = values[:safe_limit]

            return {
                "success": True,
                "api_group": "vndirect_realtime",
                "api_source": "vndirect_websocket",
                "api_tags": [f"vndirect_ws.{f}" for f in feed_data.keys()],
                "counts": {feed: len(rows) for feed, rows in feed_data.items()},
                "data": feed_data,
            }

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "success": True,
                "api_group": "vndirect_realtime",
                "api_source": "vndirect_websocket",
                "connected": self._connected,
                "started": self._started,
                "ws_url": self.ws_url,
                "last_connected_at": self._last_connected_at,
                "last_message_at": self._last_message_at,
                "last_error": self._last_error,
                "reconnect_count": self._reconnect_count,
                "messages_total": self._messages_total,
                "messages_by_feed": dict(self._messages_by_feed),
                "subscription": {
                    "equity_symbols_count": len(self._equity_symbols),
                    "derivative_symbols_count": len(self._derivative_symbols),
                    "market_ids": list(self._market_ids),
                },
                "cached_snapshot_counts": {feed: len(rows) for feed, rows in self._snapshots.items()},
            }

    def _run_thread(self) -> None:
        loop = asyncio.new_event_loop()
        with self._lock:
            self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._run_forever())
        finally:
            try:
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
            except Exception:
                pass
            loop.close()

    async def _run_forever(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._connect_and_consume_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                with self._lock:
                    self._last_error = str(e)
                    self._reconnect_count += 1
                LOGGER.warning("VNDIRECT realtime reconnecting after error: %s", e)
                await asyncio.sleep(self.reconnect_delay_s)

    async def _connect_and_consume_once(self) -> None:
        import websockets

        ssl_ctx = ssl.create_default_context()
        async with websockets.connect(
            self.ws_url,
            ssl=ssl_ctx,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
            max_queue=20000,
        ) as ws:
            with self._lock:
                self._connected = True
                self._ws = ws
                self._last_connected_at = _to_utc_iso()
                self._last_error = None
            await self._subscribe_all(ws)

            while not self._stop_event.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=self.recv_timeout_s)
                except asyncio.TimeoutError:
                    continue
                self._handle_message(raw)

        with self._lock:
            self._connected = False
            self._ws = None

    async def _subscribe_all(self, ws: Any) -> None:
        with self._lock:
            eq_codes = list(self._equity_symbols)
            de_codes = list(self._derivative_symbols)
            mi_codes = list(self._market_ids)

        if eq_codes:
            await self._send_subscribe(ws, BA, eq_codes)
            await self._send_subscribe(ws, SP, eq_codes)
        if de_codes:
            await self._send_subscribe(ws, DE, de_codes)
        if mi_codes:
            await self._send_subscribe(ws, MI, mi_codes)

    async def _send_subscribe(self, ws: Any, feed_type: str, codes: List[str]) -> None:
        wire_name = _FEED_TO_WIRE_NAME.get(feed_type)
        if not wire_name or not codes:
            return
        payload = {
            "type": "registConsumer",
            "data": {
                "sequence": 0,
                "params": {
                    "name": wire_name,
                    "codes": codes,
                },
            },
        }
        await ws.send(json.dumps(payload, separators=(",", ":")))

    async def _resubscribe_on_current_socket(self) -> None:
        ws = None
        with self._lock:
            ws = self._ws
        if ws is not None:
            await self._subscribe_all(ws)

    def _request_live_resubscribe(self) -> None:
        loop = None
        with self._lock:
            loop = self._loop
        if loop is not None and loop.is_running():
            try:
                asyncio.run_coroutine_threadsafe(self._resubscribe_on_current_socket(), loop)
            except Exception:
                pass

    def _handle_message(self, raw: Any) -> None:
        received_at = _to_utc_iso()
        try:
            obj = json.loads(raw)
            wire_type = str(obj.get("type") or "")
            feed_type = _WIRE_TO_FEED.get(wire_type)
            if not feed_type:
                return
            payload = _parse_feed_payload(feed_type, obj.get("data") or "")
        except Exception as e:
            with self._lock:
                self._last_error = f"parse_error:{e}"
            return

        symbol = ""
        if feed_type == MI:
            symbol = str(payload.get("marketID") or "")
        else:
            symbol = str(payload.get("code") or "").upper()
        if not symbol:
            return

        row = {
            "api_group": "vndirect_realtime",
            "api_source": "vndirect_websocket",
            "api_tag": f"vndirect_ws.{feed_type}",
            "feed_type": feed_type,
            "symbol": symbol,
            "received_at": received_at,
            "data": payload,
        }

        with self._lock:
            self._last_message_at = received_at
            self._messages_total += 1
            self._messages_by_feed[feed_type] = self._messages_by_feed.get(feed_type, 0) + 1
            bucket = self._snapshots.setdefault(feed_type, {})
            bucket[symbol] = row


def build_default_collector() -> VndirectRealtimeCollector:
    ws_url = os.getenv("VNDIRECT_WS_URL", "wss://price-cmc-04.vndirect.com.vn/realtime/websocket")
    reconnect_delay_s = float(os.getenv("VNDIRECT_RECONNECT_DELAY_S", "2"))
    recv_timeout_s = float(os.getenv("VNDIRECT_RECV_TIMEOUT_S", "10"))

    return VndirectRealtimeCollector(
        ws_url=ws_url,
        equity_symbols=_load_default_equity_symbols(),
        derivative_symbols=_load_default_derivative_symbols(),
        market_ids=_load_default_market_ids(),
        reconnect_delay_s=reconnect_delay_s,
        recv_timeout_s=recv_timeout_s,
    )
