import json
import os
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import vnstock

LOGO_URL_PREFIX = os.getenv("LOGO_URL_PREFIX", "/stock/image")
LOGGER = logging.getLogger(__name__)


def _env_int(name: str, default: int, min_v: int, max_v: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except Exception:
        return default
    return max(min_v, min(max_v, value))


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


PRICE_BOARD_CACHE_TTL_S = _env_int("PRICE_BOARD_CACHE_TTL_S", 15, 1, 3600)
PRICE_BOARD_CACHE_STALE_S = _env_int("PRICE_BOARD_CACHE_STALE_S", 45, 0, 86400)
PRICE_BOARD_REALTIME_TTL_S = _env_int("PRICE_BOARD_REALTIME_TTL_S", 5, 1, 300)
PRICE_BOARD_REALTIME_STALE_S = _env_int("PRICE_BOARD_REALTIME_STALE_S", 10, 0, 3600)
LISTING_COMPANIES_CACHE_TTL_S = _env_int("LISTING_COMPANIES_CACHE_TTL_S", 21600, 60, 604800)
LISTING_COMPANIES_CACHE_STALE_S = _env_int("LISTING_COMPANIES_CACHE_STALE_S", 604800, 60, 2592000)
LISTING_RESPONSE_CACHE_TTL_S = _env_int("LISTING_RESPONSE_CACHE_TTL_S", 60, 1, 3600)
LISTING_RESPONSE_CACHE_STALE_S = _env_int("LISTING_RESPONSE_CACHE_STALE_S", 120, 0, 3600)
CACHE_REFRESH_WORKERS = _env_int("CACHE_REFRESH_WORKERS", 4, 1, 32)
PRICE_BOARD_INTRADAY_WORKERS = _env_int("PRICE_BOARD_INTRADAY_WORKERS", 8, 2, 32)
PRICE_BOARD_CACHE_MAX_ENTRIES = _env_int("PRICE_BOARD_CACHE_MAX_ENTRIES", 512, 32, 4096)
REALTIME_CACHE_MAX_ENTRIES = _env_int("REALTIME_CACHE_MAX_ENTRIES", 4096, 128, 20000)
CACHE_DIR = os.getenv("CACHE_DIR", "/tmp/fintrace-cache")
LISTING_COMPANIES_CACHE_FILE = os.path.join(CACHE_DIR, "listing_companies.json")
PRICE_BOARD_DIRECT_SOURCE_ENABLED = _env_bool("PRICE_BOARD_DIRECT_SOURCE_ENABLED", False)
INTRADAY_DIRECT_SOURCE_ENABLED = _env_bool("INTRADAY_DIRECT_SOURCE_ENABLED", False)
PRICE_BOARD_REALTIME_DEFAULT = _env_bool("PRICE_BOARD_REALTIME_DEFAULT", False)
PRICE_BOARD_REALTIME_MAX_SYMBOLS = _env_int("PRICE_BOARD_REALTIME_MAX_SYMBOLS", 8, 1, 200)
CHART_CACHE_MAX_ENTRIES = _env_int("CHART_CACHE_MAX_ENTRIES", 1024, 64, 10000)
CHART_INTRADAY_CACHE_TTL_S = _env_int("CHART_INTRADAY_CACHE_TTL_S", 12, 1, 600)
CHART_INTRADAY_CACHE_STALE_S = _env_int("CHART_INTRADAY_CACHE_STALE_S", 30, 0, 3600)
CHART_DAILY_CACHE_TTL_S = _env_int("CHART_DAILY_CACHE_TTL_S", 300, 5, 86400)
CHART_DAILY_CACHE_STALE_S = _env_int("CHART_DAILY_CACHE_STALE_S", 1800, 0, 604800)

os.makedirs(CACHE_DIR, exist_ok=True)

_REQUEST_SESSION = requests.Session()
_REQUEST_SESSION.mount("https://", requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=64))
_REQUEST_SESSION.mount("http://", requests.adapters.HTTPAdapter(pool_connections=16, pool_maxsize=32))
_REFRESH_EXECUTOR = ThreadPoolExecutor(max_workers=CACHE_REFRESH_WORKERS)


class _CacheEntry:
    __slots__ = ("value", "fresh_until", "stale_until", "stored_at")

    def __init__(self, value: Any, fresh_until: float, stale_until: float, stored_at: float):
        self.value = value
        self.fresh_until = fresh_until
        self.stale_until = stale_until
        self.stored_at = stored_at


class _CoalescingTTLCache:
    def __init__(self, name: str, max_entries: int):
        self.name = name
        self.max_entries = max_entries
        self._lock = threading.RLock()
        self._data: "OrderedDict[str, _CacheEntry]" = OrderedDict()
        self._inflight: Dict[str, threading.Event] = {}

    def _prune_locked(self, now: float) -> None:
        expired_keys = [k for k, v in self._data.items() if v.stale_until <= now]
        for key in expired_keys:
            self._data.pop(key, None)
        while len(self._data) > self.max_entries:
            self._data.popitem(last=False)

    def _store_locked(self, key: str, value: Any, ttl_s: int, stale_s: int, now: Optional[float] = None) -> _CacheEntry:
        now = now if now is not None else time.time()
        entry = _CacheEntry(
            value=value,
            fresh_until=now + max(ttl_s, 0),
            stale_until=now + max(ttl_s + stale_s, ttl_s),
            stored_at=now,
        )
        self._data[key] = entry
        self._data.move_to_end(key)
        self._prune_locked(now)
        return entry

    def store(self, key: str, value: Any, ttl_s: int, stale_s: int) -> None:
        with self._lock:
            self._store_locked(key, value, ttl_s, stale_s)

    def seed(self, key: str, value: Any, fresh_remaining_s: int, stale_remaining_s: int) -> None:
        now = time.time()
        fresh_until = now + max(fresh_remaining_s, 0)
        stale_until = now + max(stale_remaining_s, 0)
        if stale_until <= now:
            return
        with self._lock:
            self._data[key] = _CacheEntry(value=value, fresh_until=fresh_until, stale_until=stale_until, stored_at=now)
            self._data.move_to_end(key)
            self._prune_locked(now)

    def _do_refresh(self, key: str, loader, ttl_s: int, stale_s: int) -> None:
        try:
            value = loader()
        except Exception:
            LOGGER.exception("Background refresh failed for cache=%s key=%s", self.name, key)
        else:
            with self._lock:
                self._store_locked(key, value, ttl_s, stale_s)
        finally:
            with self._lock:
                event = self._inflight.pop(key, None)
                if event:
                    event.set()

    def get_or_load(self, key: str, loader, ttl_s: int, stale_s: int = 0) -> Any:
        stale_entry = None
        now = time.time()
        with self._lock:
            self._prune_locked(now)
            entry = self._data.get(key)
            if entry:
                self._data.move_to_end(key)
                if entry.fresh_until > now:
                    return entry.value
                if entry.stale_until > now:
                    stale_entry = entry
                    if key not in self._inflight:
                        event = threading.Event()
                        self._inflight[key] = event
                        _REFRESH_EXECUTOR.submit(self._do_refresh, key, loader, ttl_s, stale_s)
                    return entry.value

            event = self._inflight.get(key)
            if event is None:
                event = threading.Event()
                self._inflight[key] = event
                owner = True
            else:
                owner = False

        if owner:
            try:
                value = loader()
                with self._lock:
                    self._store_locked(key, value, ttl_s, stale_s)
                return value
            except Exception:
                if stale_entry:
                    LOGGER.exception("Serving stale cache after refresh failure: cache=%s key=%s", self.name, key)
                    return stale_entry.value
                raise
            finally:
                with self._lock:
                    waiter = self._inflight.pop(key, None)
                    if waiter:
                        waiter.set()

        event.wait(timeout=30)
        with self._lock:
            entry = self._data.get(key)
            if entry:
                self._data.move_to_end(key)
                return entry.value
        if stale_entry:
            return stale_entry.value
        raise TimeoutError(f"cache load timeout: {self.name}:{key}")


_PRICE_BOARD_CACHE = _CoalescingTTLCache("price_board", max_entries=PRICE_BOARD_CACHE_MAX_ENTRIES)
_REALTIME_QUOTE_CACHE = _CoalescingTTLCache("realtime_quote", max_entries=REALTIME_CACHE_MAX_ENTRIES)
_LISTING_COMPANIES_CACHE = _CoalescingTTLCache("listing_companies", max_entries=4)
_LISTING_RESPONSE_CACHE = _CoalescingTTLCache("listing_response", max_entries=4)
_CHART_CACHE = _CoalescingTTLCache("chart", max_entries=CHART_CACHE_MAX_ENTRIES)

def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    # phổ biến từ vnstock: "YYYY-mm-dd HH:MM:SS" hoặc "YYYY-mm-dd"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None
def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _response(status_code: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return _response_raw_json(status_code, _json_dumps(payload))


def _response_raw_json(status_code: int, body_json: str, extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
    }
    if extra_headers:
        headers.update(extra_headers)
    return {
        "statusCode": status_code,
        "headers": headers,
        "body": body_json,
    }


def _is_intraday_resolution(resolution: str) -> bool:
    return str(resolution or "").upper() not in ("1D", "1W", "1M", "D", "W", "M")


def _chart_cache_policy(is_intraday: bool) -> tuple:
    if is_intraday:
        return CHART_INTRADAY_CACHE_TTL_S, CHART_INTRADAY_CACHE_STALE_S
    return CHART_DAILY_CACHE_TTL_S, CHART_DAILY_CACHE_STALE_S


def _chart_cache_key(cmd: str, symbol: str, resolution: str, start_date: str, end_date: str, extra: str = "") -> str:
    return "|".join([
        str(cmd or "").lower(),
        str(symbol or "").upper(),
        str(resolution or ""),
        str(start_date or ""),
        str(end_date or ""),
        str(extra or ""),
    ])
def _get_query_params(event: Dict[str, Any]) -> Dict[str, str]:
    # API Gateway REST (v1): queryStringParameters
    # API Gateway HTTP API (v2): rawQueryString + queryStringParameters or event["queryStringParameters"]
    qsp = event.get("queryStringParameters") or {}
    if isinstance(qsp, dict):
        return {str(k): "" if v is None else str(v) for k, v in qsp.items()}
    return {}
def _get_json_body(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    body = event.get("body")
    if not body:
        return None
    if event.get("isBase64Encoded"):
        # Tránh phụ thuộc base64 nếu không cần; trường hợp này ít gặp với JSON nhỏ
        return None
    try:
        parsed = json.loads(body)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None
def _param(params: Dict[str, Any], key: str, default: Optional[str] = None) -> Optional[str]:
    v = params.get(key)
    if v is None:
        return default
    if isinstance(v, str):
        v = v.strip()
    return str(v)
def _as_int(v: Optional[str], default: int, min_v: int, max_v: int) -> int:
    if v is None or v == "":
        return default
    try:
        n = int(v)
    except Exception:
        return default
    return max(min_v, min(max_v, n))
def _as_symbol(v: Optional[str], default: str = "FPT") -> str:
    s = (v or default).strip().upper()
    # Cho phép "FPT,VCB" (nhiều mã)
    return s
def _as_list_csv(v: Optional[str]) -> Optional[list]:
    if not v:
        return None
    items = [x.strip().upper() for x in str(v).split(",") if x.strip()]
    return items or None
def _df_to_records(df: Any) -> Any:
    # vnstock thường trả pandas.DataFrame; nhưng để nhẹ phụ thuộc, chỉ cần to_dict nếu có.
    if hasattr(df, "to_dict"):
        return df.to_dict(orient="records")
    return df
def _attach_logo_url(data: Any) -> Any:
    if not isinstance(data, list):
        return data
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = item.get("ticker") or item.get("symbol") or item.get("code")
        if not symbol:
            continue
        item["logo_url"] = f"{LOGO_URL_PREFIX}/{str(symbol).upper()}"
    return data


def _normalize_ticker_list(items: Any) -> list:
    if not items:
        return []
    seen = set()
    out = []
    for raw in items:
        ticker = str(raw or "").strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        out.append(ticker)
    return out


def _price_board_cache_key(tickers: list) -> str:
    return ",".join(sorted(_normalize_ticker_list(tickers)))


def _persist_json_cache(file_path: str, value: Any, ttl_s: int, stale_s: int) -> None:
    payload = {
        "stored_at": int(time.time()),
        "ttl_s": int(ttl_s),
        "stale_s": int(stale_s),
        "value": value,
    }
    tmp_path = f"{file_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp_path, file_path)


def _seed_cache_from_disk(cache: _CoalescingTTLCache, key: str, file_path: str) -> bool:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except FileNotFoundError:
        return False
    except Exception:
        LOGGER.exception("Failed to read cache file: %s", file_path)
        return False

    if not isinstance(payload, dict) or "value" not in payload:
        return False

    stored_at = int(payload.get("stored_at") or 0)
    ttl_s = int(payload.get("ttl_s") or 0)
    stale_s = int(payload.get("stale_s") or 0)
    age = max(0, int(time.time()) - stored_at)
    fresh_remaining_s = max(0, ttl_s - age)
    stale_remaining_s = max(0, ttl_s + stale_s - age)
    if stale_remaining_s <= 0:
        return False
    cache.seed(key=key, value=payload["value"], fresh_remaining_s=fresh_remaining_s, stale_remaining_s=stale_remaining_s)
    return True


def _load_listing_companies_payload() -> Any:
    df = vnstock.listing_companies()
    data = _attach_logo_url(_df_to_records(df))
    try:
        _persist_json_cache(
            file_path=LISTING_COMPANIES_CACHE_FILE,
            value=data,
            ttl_s=LISTING_COMPANIES_CACHE_TTL_S,
            stale_s=LISTING_COMPANIES_CACHE_STALE_S,
        )
    except Exception:
        LOGGER.exception("Failed to persist listing_companies cache")
    return data


def _get_listing_companies_cached() -> Any:
    cache_key = "listing_companies"
    return _LISTING_COMPANIES_CACHE.get_or_load(
        key=cache_key,
        loader=_load_listing_companies_payload,
        ttl_s=LISTING_COMPANIES_CACHE_TTL_S,
        stale_s=LISTING_COMPANIES_CACHE_STALE_S,
    )


_seed_cache_from_disk(_LISTING_COMPANIES_CACHE, "listing_companies", LISTING_COMPANIES_CACHE_FILE)


def _get_listing_companies_response_json(cmd: str) -> str:
    return _LISTING_RESPONSE_CACHE.get_or_load(
        key=f"listing_response:{cmd}",
        loader=lambda: _json_dumps({"success": True, "cmd": cmd, "data": _get_listing_companies_cached()}),
        ttl_s=LISTING_RESPONSE_CACHE_TTL_S,
        stale_s=LISTING_RESPONSE_CACHE_STALE_S,
    )


def _http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout_s: int = 15) -> Dict[str, Any]:
    headers = {
        "accept": "application/json, text/plain, */*",
        "user-agent": "Mozilla/5.0 (compatible; fintrace/1.0; +https://example.invalid)",
        "origin": "https://tcinvest.tcbs.com.vn",
        "referer": "https://tcinvest.tcbs.com.vn/",
    }
    r = _REQUEST_SESSION.get(url, params=params, headers=headers, timeout=timeout_s)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, dict) else {"_raw": data}
def _tcbs_price_board(tickers: list) -> Any:
    # Endpoint quan sát từ vnstock legacy error log
    url = "https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/second-tc-price"
    # TCBS nhận tickers là chuỗi dạng JSON list.
    payload = _http_get_json(url, params={"tickers": json.dumps(tickers)})
    # Một số thời điểm TCBS trả {data:[...]} hoặc {data:{...}}; nếu thiếu thì trả nguyên payload để debug.
    return payload.get("data", payload)
def _tcbs_intraday(symbol: str, page_size: int) -> Any:
    url = f"https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{symbol}/investor/his/paging"
    payload = _http_get_json(url, params={"page": 0, "size": page_size, "headIndex": -1})
    return payload.get("data", payload)
def _tcbs_longterm_ohlc(symbol: str) -> Any:
    url = "https://apipubaws.tcbs.com.vn/stock-insight/v2/stock/bars-long-term"
    to_ts = int(datetime.now().timestamp())
    payload = _http_get_json(
        url,
        params={
            "ticker": symbol,
            "type": "stock",
            "resolution": "D",
            "to": to_ts,
            "countBack": 365,
        },
        timeout_s=20,
    )
    return payload.get("data", payload)
def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, str):
            v = v.replace(",", "").strip()
        return float(v)
    except Exception:
        return default

def _normalize_quote_price(v: Any) -> float:
    """
    Chuẩn hóa đơn vị giá:
    - Dữ liệu intraday đôi khi ở đơn vị "nghìn đồng" (vd 26.3),
      trong khi bảng giá/1D dùng đơn vị VND (vd 26300).
    """
    price = _safe_float(v, 0.0)
    if 0 < price < 1000:
        return round(price * 1000.0, 2)
    return price

def _pick_latest_row(rows: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(rows, list) or not rows:
        return None
    latest = None
    latest_dt = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        dt = _parse_datetime(
            row.get("time")
            or row.get("tradingTime")
            or row.get("matchTime")
            or row.get("date")
            or row.get("tradingDate")
        )
        if latest is None:
            latest = row
            latest_dt = dt
            continue
        if latest_dt is None or (dt is not None and dt >= latest_dt):
            latest = row
            latest_dt = dt
    return latest


def _extract_intraday_quote(symbol: str, rows: Any) -> Optional[Dict[str, Any]]:
    row = _pick_latest_row(rows)
    if not row:
        return None

    price = _normalize_quote_price(
        row.get("last_price")
        or row.get("lastPrice")
        or row.get("matchPrice")
        or row.get("price")
        or row.get("close")
        or row.get("match_price")
    )
    if price <= 0:
        return None

    return {
        "ticker": symbol,
        "last_price": price,
        "last_time": row.get("time") or row.get("tradingTime") or row.get("matchTime") or row.get("date"),
    }


def _latest_intraday_quote_loader(symbol: str) -> Optional[Dict[str, Any]]:
    if INTRADAY_DIRECT_SOURCE_ENABLED:
        try:
            row = _extract_intraday_quote(symbol, _tcbs_intraday(symbol, page_size=5))
            if row:
                return row
        except Exception:
            pass

    today = datetime.now().strftime("%Y-%m-%d")
    try:
        df = vnstock.ohlc_data(
            symbol=symbol,
            start_date=today,
            end_date=today,
            resolution="1",
            type="stock",
        )
    except Exception:
        return None

    records = _df_to_records(df)
    if not isinstance(records, list) or not records:
        return None

    last = records[-1]
    price = _normalize_quote_price(last.get("close"))
    if price <= 0:
        return None
    return {
        "ticker": symbol,
        "last_price": price,
        "last_time": last.get("time"),
    }


def _latest_intraday_quote_cached(symbol: str) -> Optional[Dict[str, Any]]:
    cache_key = str(symbol or "").strip().upper()
    if not cache_key:
        return None
    return _REALTIME_QUOTE_CACHE.get_or_load(
        key=cache_key,
        loader=lambda: _latest_intraday_quote_loader(cache_key),
        ttl_s=PRICE_BOARD_REALTIME_TTL_S,
        stale_s=PRICE_BOARD_REALTIME_STALE_S,
    )


def _merge_price_board_rows(rows: Any, realtime_map: Dict[str, Dict[str, Any]]) -> Any:
    if not isinstance(rows, list):
        return rows
    merged = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        item = dict(item)
        ticker = str(item.get("ticker") or item.get("symbol") or "").upper()
        if not ticker:
            merged.append(item)
            continue

        rt = realtime_map.get(ticker)
        daily_close = _safe_float(item.get("close"))
        item["daily_close"] = daily_close
        item["price_source"] = "daily_1D"
        item["last_price"] = daily_close

        if not rt:
            merged.append(item)
            continue

        live_price = _safe_float(rt.get("last_price"))
        if live_price <= 0:
            merged.append(item)
            continue

        item["close"] = live_price
        item["last_price"] = live_price
        item["time"] = rt.get("last_time") or item.get("time")
        item["price_source"] = "intraday_1m"

        # Với giá realtime trong phiên, mốc so sánh đúng là close phiên gần nhất (daily_close),
        # không phải prev_close của bản ghi 1D.
        ref_close = daily_close if daily_close > 0 else _safe_float(item.get("prev_close"))
        if ref_close > 0:
            item["prev_close"] = ref_close
            change = live_price - ref_close
            item["change"] = change
            item["change_percent"] = (change / ref_close) * 100.0
        merged.append(item)
    return merged


def _enrich_price_board_with_intraday(rows: Any, tickers: list, workers: int = PRICE_BOARD_INTRADAY_WORKERS) -> Any:
    tickers = _normalize_ticker_list(tickers)
    if not isinstance(rows, list) or not tickers:
        return rows

    realtime_map: Dict[str, Dict[str, Any]] = {}
    max_workers = min(max(workers, 2), max(len(tickers), 2))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_map = {ex.submit(_latest_intraday_quote_cached, t): t for t in tickers}
        for fut in as_completed(fut_map):
            try:
                row = fut.result()
            except Exception:
                continue
            if row and row.get("ticker"):
                realtime_map[str(row["ticker"]).upper()] = row

    return _merge_price_board_rows(rows, realtime_map)


def _order_price_board_rows(rows: Any, tickers: list) -> Any:
    if not isinstance(rows, list):
        return rows
    tickers = _normalize_ticker_list(tickers)
    if not tickers:
        return rows
    by_ticker = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or row.get("symbol") or "").upper()
        if ticker and ticker not in by_ticker:
            by_ticker[ticker] = row
    ordered = [by_ticker[t] for t in tickers if t in by_ticker]
    used_ids = {id(x) for x in ordered}
    ordered.extend([row for row in rows if id(row) not in used_ids])
    return ordered


def _load_price_board_payload(tickers: list) -> Any:
    tickers = _normalize_ticker_list(tickers)
    if not tickers:
        return []

    if PRICE_BOARD_DIRECT_SOURCE_ENABLED:
        direct_rows = None
        try:
            direct_rows = _tcbs_price_board(tickers)
        except Exception:
            direct_rows = None
        if isinstance(direct_rows, list) and direct_rows:
            return _order_price_board_rows(direct_rows, tickers)

    try:
        df = vnstock.price_board(symbol_ls=tickers if len(tickers) > 1 else tickers[0])
        rows = _df_to_records(df)
        if isinstance(rows, list):
            return _order_price_board_rows(rows, tickers)
    except Exception:
        pass

    workers = min(max(len(tickers), 4), 24)
    rows = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        fut_map = {ex.submit(_latest_two_daily, t): t for t in tickers}
        for fut in as_completed(fut_map):
            try:
                row = fut.result()
                if row:
                    rows.append(row)
            except Exception:
                continue
    return _order_price_board_rows(rows, tickers)


def _get_price_board_cached(tickers: list) -> Any:
    normalized = _normalize_ticker_list(tickers)
    cache_key = _price_board_cache_key(normalized)
    return _PRICE_BOARD_CACHE.get_or_load(
        key=cache_key,
        loader=lambda: _load_price_board_payload(normalized),
        ttl_s=PRICE_BOARD_CACHE_TTL_S,
        stale_s=PRICE_BOARD_CACHE_STALE_S,
    )


def warm_server_caches() -> None:
    try:
        _get_listing_companies_cached()
        _get_listing_companies_response_json("listing_companies")
    except Exception:
        LOGGER.exception("Warmup failed for listing_companies")


def shutdown_server_runtime() -> None:
    try:
        _REQUEST_SESSION.close()
    except Exception:
        pass
    _REFRESH_EXECUTOR.shutdown(wait=False)
def _aggregate_bars(rows: Any, factor: int) -> Any:
    records = _df_to_records(rows)
    if not isinstance(records, list) or factor <= 1:
        return records
    parsed = []
    for r in records:
        if not isinstance(r, dict):
            continue
        dt = _parse_datetime(r.get("time") or r.get("date") or r.get("tradingDate"))
        if not dt:
            continue
        parsed.append((dt, r))
    if not parsed:
        return records
    parsed.sort(key=lambda x: x[0])

    # Group theo từng ngày để không gộp nến xuyên phiên giao dịch (gây rời rạc).
    by_day = {}
    for dt, r in parsed:
        by_day.setdefault(dt.date(), []).append((dt, r))

    out = []
    for day in sorted(by_day.keys()):
        day_rows = by_day[day]
        day_rows.sort(key=lambda x: x[0])
        day_records = [x[1] for x in day_rows]
        day_times = [x[0] for x in day_rows]
        for i in range(0, len(day_records), factor):
            chunk = day_records[i:i+factor]
            if not chunk:
                continue
            first = chunk[0]
            last = chunk[-1]
            high = max(_safe_float(x.get("high")) for x in chunk)
            low = min(_safe_float(x.get("low"), 10**18) for x in chunk)
            volume = sum(_safe_float(x.get("volume")) for x in chunk)
            first_dt = day_times[i]
            out.append({
                "time": first_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "open": _safe_float(first.get("open")),
                "high": high,
                "low": low if low != 10**18 else _safe_float(first.get("low")),
                "close": _safe_float(last.get("close")),
                "volume": volume,
                "ticker": first.get("ticker") or last.get("ticker"),
            })
    return out

def _aggregate_daily_to_calendar(rows: Any, period: str) -> Any:
    records = _df_to_records(rows)
    if not isinstance(records, list):
        return []

    keyed = {}
    for r in records:
        if not isinstance(r, dict):
            continue
        dt = _parse_datetime(r.get("time") or r.get("date") or r.get("tradingDate"))
        if not dt:
            continue
        if period == "1W":
            y, w, _ = dt.isocalendar()
            key = (y, w)
        elif period == "1M":
            key = (dt.year, dt.month)
        else:
            return records
        keyed.setdefault(key, []).append((dt, r))

    out = []
    for _, bucket in sorted(keyed.items(), key=lambda kv: kv[0]):
        bucket.sort(key=lambda x: x[0])
        first_dt, first = bucket[0]
        _, last = bucket[-1]
        high = max(_safe_float(x[1].get("high")) for x in bucket)
        low = min(_safe_float(x[1].get("low"), 10**18) for x in bucket)
        volume = sum(_safe_float(x[1].get("volume")) for x in bucket)
        out.append({
            "time": first_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "open": _safe_float(first.get("open")),
            "high": high,
            "low": low if low != 10**18 else _safe_float(first.get("low")),
            "close": _safe_float(last.get("close")),
            "volume": volume,
            "ticker": first.get("ticker") or last.get("ticker"),
        })
    return out
def _latest_two_daily(symbol: str) -> Optional[Dict[str, Any]]:
    end_d = datetime.now().strftime("%Y-%m-%d")
    start_d = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
    df = vnstock.stock_historical_data(
        symbol=symbol,
        start_date=start_d,
        end_date=end_d,
        resolution="1D",
        type="stock"
    )
    records = _df_to_records(df)
    if not isinstance(records, list) or len(records) < 1:
        return None
    curr = records[-1]
    prev = records[-2] if len(records) >= 2 else records[-1]
    close = _safe_float(curr.get("close"))
    prev_close = _safe_float(prev.get("close"), close)
    change = close - prev_close
    change_percent = (change / prev_close * 100.0) if prev_close else 0.0
    return {
        "ticker": symbol,
        "time": curr.get("time"),
        "open": _safe_float(curr.get("open")),
        "high": _safe_float(curr.get("high")),
        "low": _safe_float(curr.get("low")),
        "close": close,
        "volume": _safe_float(curr.get("volume")),
        "prev_close": prev_close,
        "change": change,
        "change_percent": change_percent,
    }


def _build_stock_historical_payload(cmd: str, symbol: str, clean_res: str, start_date: str, end_date: str) -> Dict[str, Any]:
    is_intraday = _is_intraday_resolution(clean_res)
    try:
        if is_intraday:
            try:
                df = vnstock.ohlc_data(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    resolution=clean_res,
                    type="stock",
                )
            except Exception:
                if clean_res in ("60", "120", "240"):
                    base_df = vnstock.ohlc_data(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                        resolution="30",
                        type="stock",
                    )
                    factor_map = {"60": 2, "120": 4, "240": 8}
                    agg = _aggregate_bars(base_df, factor_map.get(clean_res, 1))
                    return {
                        "success": True,
                        "cmd": cmd,
                        "input": {"symbol": symbol, "res": clean_res, "start": start_date, "fallback": "aggregate_from_30m"},
                        "data": agg,
                    }
                raise
        else:
            if clean_res in ("1W", "1M"):
                base_df = vnstock.stock_historical_data(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    resolution="1D",
                    type="stock",
                )
                agg = _aggregate_daily_to_calendar(base_df, clean_res)
                return {
                    "success": True,
                    "cmd": cmd,
                    "input": {"symbol": symbol, "res": clean_res, "start": start_date, "fallback": "aggregate_from_1D"},
                    "data": agg,
                }
            df = vnstock.stock_historical_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                resolution=clean_res,
                type="stock",
            )

        if df is None or (hasattr(df, "empty") and df.empty):
            return {
                "success": False,
                "error": f"Server nguồn (TCBS) từ chối nến {clean_res}. Thử lại với nến 1D hoặc giảm số ngày.",
                "data": [],
            }

        return {
            "success": True,
            "cmd": cmd,
            "input": {"symbol": symbol, "res": clean_res, "start": start_date},
            "data": _df_to_records(df),
        }
    except Exception as e:
        return {"success": False, "error": f"Lỗi: {str(e)}"}
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    if (event.get("httpMethod") == "OPTIONS") or (
        isinstance(event.get("requestContext"), dict)
        and isinstance(event["requestContext"].get("http"), dict)
        and event["requestContext"]["http"].get("method") == "OPTIONS"
    ):
        return _response(200, {"ok": True})
    q = _get_query_params(event)
    body = _get_json_body(event) or {}
    params: Dict[str, Any] = {**q, **body}
    cmd = (_param(params, "cmd", "health") or "health").lower()
    symbol = _as_symbol(_param(params, "symbol", None))
    symbols_csv = _param(params, "symbols", None)
    symbols_ls = _as_list_csv(symbols_csv)  # ưu tiên list khi gọi bảng giá
    realtime_default_raw = "1" if PRICE_BOARD_REALTIME_DEFAULT else "0"
    realtime = (_param(params, "realtime", realtime_default_raw) or realtime_default_raw).lower() in ("1", "true", "yes", "y")
    days = _as_int(_param(params, "days", None), default=30, min_v=1, max_v=3650)
    resolution = _param(params, "resolution", _param(params, "res", "1D")) or "1D"
    start_date = _param(params, "start_date", None)
    end_date = _param(params, "end_date", None)
    # Config nhẹ để sau này tune
    default_page_size = int(os.getenv("INTRADAY_PAGE_SIZE", "100"))
    intraday_max_days = _as_int(
        os.getenv("INTRADAY_MAX_DAYS", "365"),
        default=365,
        min_v=7,
        max_v=3650,
    )
    intraday_page_size = _as_int(_param(params, "page_size", None), default=default_page_size, min_v=10, max_v=1000)
    try:
        if cmd in ("health", "ping"):
            return _response(
                200,
                {
                    "success": True,
                    "service": "stock_lambda_1",
                    "ts": _now_iso(),
                    "supported_cmd": [
                        # nhóm 9
                        "listing_companies",
                        "indices_listing",
                        "organ_listing",
                        # nhóm 1
                        "price_board",
                        "price_depth",
                        "stock_intraday_data",
                        "stock_historical_data",
                        "ohlc_data",
                        "longterm_ohlc_data",
                        "live_stock_list",
                        "offline_stock_list",
                        "bulk_snapshot",
                    ],
                },
            )
        # =========================
        # NHÓM 9 — LISTING
        # =========================
        if cmd in ("listing_companies", "list"):
            return _response_raw_json(200, _get_listing_companies_response_json(cmd))
        if cmd == "indices_listing":
            df = vnstock.indices_listing()
            return _response(200, {"success": True, "cmd": cmd, "data": _df_to_records(df)})
        if cmd == "organ_listing":
            df = vnstock.organ_listing()
            return _response(200, {"success": True, "cmd": cmd, "data": _df_to_records(df)})
        # =========================
        # NHÓM 1 — CORE DATA
        # =========================
# 1. PRICE BOARD: Bản sửa lỗi triệt để cho Library cũ
        if cmd in ("price_board", "price"):
            tickers = _normalize_ticker_list(symbols_ls or [symbol])
            data = _get_price_board_cached(tickers)
            if isinstance(data, list):
                data = _order_price_board_rows(data, tickers)

            realtime_applied = realtime and len(tickers) <= PRICE_BOARD_REALTIME_MAX_SYMBOLS
            # Đồng bộ giá hiển thị với chart: ưu tiên giá intraday mới nhất trong phiên.
            if realtime_applied and isinstance(data, list):
                try:
                    data = _enrich_price_board_with_intraday(data, tickers, workers=PRICE_BOARD_INTRADAY_WORKERS)
                except Exception:
                    pass

            return _response(200, {
                "success": True, 
                "cmd": cmd, 
                "input": {
                    "tickers": tickers,
                    "realtime_requested": realtime,
                    "realtime_applied": realtime_applied,
                    "realtime_max_symbols": PRICE_BOARD_REALTIME_MAX_SYMBOLS,
                },
                "data": data
            })
        if cmd == "market_snapshot":
            exchange = _param(params, "exchange", "HOSE").upper()
            try:
                # 1. Lấy danh sách công ty trên sàn đó
                listing_rows = _get_listing_companies_cached()
                selected_rows = [
                    row for row in listing_rows
                    if isinstance(row, dict) and str(row.get("comGroupCode", "")).upper() == exchange
                ]
                
                # 2. Để tránh 'cháy máy', lấy 20 mã tiêu biểu nhất kèm giá (hoặc bạn có thể tăng lên)
                # Vì lấy lịch sử cho 1600 mã lúc nửa đêm sẽ rất chậm trên Lambda
                tickers = [
                    str(row.get("ticker", "")).upper()
                    for row in selected_rows
                    if row.get("ticker")
                ][:50]
                
                results = []
                for t in tickers:
                    try:
                        # Lấy nến ngày gần nhất
                        hist = vnstock.stock_historical_data(t, "2026-03-20", "2026-03-26", "1D", "stock")
                        if not hist.empty:
                            last_row = hist.iloc[-1].to_dict()
                            results.append(last_row)
                    except:
                        continue
                return _response(200, {
                    "success": True, 
                    "exchange": exchange,
                    "count": len(results),
                    "data": results,
                    "note": "Data fetched from Historical source to avoid Midnight Error"
                })
            except Exception as e:
                return _response(500, {"success": False, "error": str(e)})
        if cmd == "bulk_snapshot":
            exchange = (_param(params, "exchange", "ALL") or "ALL").upper()
            limit = _as_int(_param(params, "limit", None), default=450, min_v=1, max_v=2000)
            workers = _as_int(_param(params, "workers", None), default=24, min_v=2, max_v=64)
            try:
                if symbols_ls:
                    tickers = symbols_ls[:limit]
                else:
                    listing_rows = _get_listing_companies_cached()
                    if exchange in ("HOSE", "HNX", "UPCOM"):
                        listing_rows = [
                            row for row in listing_rows
                            if isinstance(row, dict) and str(row.get("comGroupCode", "")).upper() == exchange
                        ]
                    tickers = [
                        str(row.get("ticker", "")).upper()
                        for row in listing_rows
                        if isinstance(row, dict) and row.get("ticker")
                    ][:limit]
                results = []
                with ThreadPoolExecutor(max_workers=workers) as ex:
                    fut_map = {ex.submit(_latest_two_daily, t): t for t in tickers}
                    for fut in as_completed(fut_map):
                        try:
                            row = fut.result()
                            if row:
                                results.append(row)
                        except Exception:
                            continue
                results.sort(key=lambda r: _safe_float(r.get("volume")) * _safe_float(r.get("close")), reverse=True)
                return _response(200, {
                    "success": True,
                    "cmd": cmd,
                    "input": {"exchange": exchange, "limit": limit, "workers": workers, "symbols": symbols_ls},
                    "count": len(results),
                    "data": results,
                })
            except Exception as e:
                return _response(500, {"success": False, "cmd": cmd, "error": str(e)})
        if cmd == "price_depth":
            # Một số phiên bản nhận symbol string; một số nhận list. Thử string trước, fallback list.
            try:
                df = vnstock.price_depth(symbol)
            except Exception:
                df = vnstock.price_depth([symbol])
            return _response(200, {"success": True, "cmd": cmd, "input": {"symbol": symbol}, "data": _df_to_records(df)})
        # 2. INTRADAY: Giới hạn page_size để tránh lỗi 502/Timeout
        if cmd in ("stock_intraday_data", "intraday"):
            # Ép safe_size để tránh dữ liệu quá nặng làm treo Lambda
            safe_size = min(intraday_page_size, 100)
            chart_key = _chart_cache_key(cmd, symbol, f"intraday_{safe_size}", "", "", "")
            ttl_s, stale_s = _chart_cache_policy(is_intraday=True)

            def _load_intraday_rows():
                try:
                    df = vnstock.stock_intraday_data(symbol=symbol, page_size=safe_size)
                    return _df_to_records(df)
                except Exception:
                    # Nếu vnstock lỗi, thử dùng hàm fallback cũ của bạn
                    try:
                        return _tcbs_intraday(symbol, safe_size)
                    except Exception:
                        return []  # Trả về list rỗng thay vì lỗi 500

            data = _CHART_CACHE.get_or_load(
                key=chart_key,
                loader=_load_intraday_rows,
                ttl_s=ttl_s,
                stale_s=stale_s,
            )
                
            return _response(200, {
                "success": True, 
                "cmd": cmd, 
                "input": {"symbol": symbol, "page_size": safe_size},
                "data": data
            })
        if cmd in ("stock_historical_data", "history"):
            now = datetime.now()
            if not end_date:
                end_date = now.strftime("%Y-%m-%d")
            
            # Map Resolution chuẩn cho nến phút/giờ
            res_map = {
                "1m": "1", "1": "1",
                "5m": "5", "5": "5",
                "15m": "15", "15": "15",
                "30m": "30", "30": "30",
                "1h": "60", "1H": "60", "60": "60",
                "2h": "120", "2H": "120",
                "4h": "240", "4H": "240",
                "1d": "1D", "1D": "1D",
                "1w": "1W", "1W": "1W",
                "1M": "1M"
            }
            clean_res = str(res_map.get(resolution, resolution))
            
            # Kiểm tra xem có phải nến Intraday (phút/giờ) không
            is_intraday = clean_res not in ["1D", "1W", "1M"]

            if not start_date:
                # Nến intraday giới hạn theo env để tránh timeout nhưng vẫn cho phép backfill xa.
                safe_days = days if days else (7 if is_intraday else 30)
                if is_intraday:
                    safe_days = min(safe_days, intraday_max_days)
                try:
                    end_anchor = datetime.strptime(end_date, "%Y-%m-%d")
                except Exception:
                    end_anchor = now
                start_date = (end_anchor - timedelta(days=safe_days)).strftime("%Y-%m-%d")

            chart_key = _chart_cache_key(cmd, symbol, clean_res, start_date, end_date, "history")
            ttl_s, stale_s = _chart_cache_policy(is_intraday=is_intraday)
            payload = _CHART_CACHE.get_or_load(
                key=chart_key,
                loader=lambda: _build_stock_historical_payload(cmd, symbol, clean_res, start_date, end_date),
                ttl_s=ttl_s,
                stale_s=stale_s,
            )
            return _response(200, payload)
        if cmd == "ohlc_data":
            # vnstock.ohlc_data signature có thể khác nhau theo version;
            # support các param phổ biến: symbol, start_date, end_date, resolution/type (nếu có)
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")
            if not start_date:
                start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            is_intraday = _is_intraday_resolution(resolution)
            chart_key = _chart_cache_key(cmd, symbol, resolution, start_date, end_date, "ohlc")
            ttl_s, stale_s = _chart_cache_policy(is_intraday=is_intraday)

            def _load_ohlc_payload():
                try:
                    df = vnstock.ohlc_data(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                        resolution=resolution,
                        type="stock",
                    )
                except TypeError:
                    df = vnstock.ohlc_data(symbol=symbol, start_date=start_date, end_date=end_date)
                return {
                    "success": True,
                    "cmd": cmd,
                    "input": {"symbol": symbol, "start_date": start_date, "end_date": end_date, "resolution": resolution},
                    "data": _df_to_records(df),
                }

            payload = _CHART_CACHE.get_or_load(
                key=chart_key,
                loader=_load_ohlc_payload,
                ttl_s=ttl_s,
                stale_s=stale_s,
            )
            return _response(200, payload)
        if cmd == "longterm_ohlc_data":
            chart_key = _chart_cache_key(cmd, symbol, "1D", "", "", "longterm")

            def _load_longterm_payload():
                data = None
                try:
                    # Lấy 5 năm dữ liệu bằng hàm historical (rất ổn định)
                    curr_end = datetime.now().strftime("%Y-%m-%d")
                    curr_start = (datetime.now() - timedelta(days=365*5)).strftime("%Y-%m-%d")

                    df = vnstock.stock_historical_data(
                        symbol=symbol,
                        start_date=curr_start,
                        end_date=curr_end,
                        resolution="1D",
                        type="stock",
                    )
                    data = _df_to_records(df)
                except Exception as e:
                    # Nếu fail, thử nốt hàm nguyên bản hoặc trả về lỗi sạch sẽ
                    try:
                        df = vnstock.longterm_ohlc_data(symbol=symbol)
                        data = _df_to_records(df)
                    except Exception:
                        data = {"error": f"Longterm data unavailable: {str(e)}"}
                return {
                    "success": True,
                    "cmd": cmd,
                    "input": {"symbol": symbol},
                    "data": data,
                }

            payload = _CHART_CACHE.get_or_load(
                key=chart_key,
                loader=_load_longterm_payload,
                ttl_s=CHART_DAILY_CACHE_TTL_S,
                stale_s=CHART_DAILY_CACHE_STALE_S,
            )
            return _response(200, payload)
        if cmd == "live_stock_list":
            df = vnstock.live_stock_list()
            data = _attach_logo_url(_df_to_records(df))
            return _response(200, {"success": True, "cmd": cmd, "data": data})
        if cmd == "offline_stock_list":
            df = vnstock.offline_stock_list()
            data = _attach_logo_url(_df_to_records(df))
            return _response(200, {"success": True, "cmd": cmd, "data": data})
        return _response(
            400,
            {
                "success": False,
                "error": f"cmd_not_supported: {cmd}",
                "hint": "Dùng cmd=health để xem danh sách cmd hỗ trợ.",
            },
        )
    except Exception as e:
        return _response(
            500,
            {
                "success": False,
                "cmd": cmd,
                "error": str(e),
            },
        )
