import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import vnstock

LOGO_URL_PREFIX = os.getenv("LOGO_URL_PREFIX", "/stock/image")

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
def _response(status_code: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json; charset=utf-8",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
        },
        "body": json.dumps(payload, ensure_ascii=False, default=str),
    }
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
def _http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout_s: int = 15) -> Dict[str, Any]:
    headers = {
        "accept": "application/json, text/plain, */*",
        "user-agent": "Mozilla/5.0 (compatible; fintrace/1.0; +https://example.invalid)",
        "origin": "https://tcinvest.tcbs.com.vn",
        "referer": "https://tcinvest.tcbs.com.vn/",
    }
    r = requests.get(url, params=params, headers=headers, timeout=timeout_s)
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

def _latest_intraday_quote(symbol: str) -> Optional[Dict[str, Any]]:
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

def _enrich_price_board_with_intraday(rows: Any, tickers: list, workers: int = 12) -> Any:
    if not isinstance(rows, list) or not tickers:
        return rows

    realtime_map: Dict[str, Dict[str, Any]] = {}
    max_workers = min(max(workers, 2), max(len(tickers), 2))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_map = {ex.submit(_latest_intraday_quote, t): t for t in tickers}
        for fut in as_completed(fut_map):
            row = fut.result()
            if row and row.get("ticker"):
                realtime_map[str(row["ticker"]).upper()] = row

    for item in rows:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or item.get("symbol") or "").upper()
        if not ticker:
            continue

        rt = realtime_map.get(ticker)
        daily_close = _safe_float(item.get("close"))
        item["daily_close"] = daily_close
        item["price_source"] = "daily_1D"
        item["last_price"] = daily_close

        if not rt:
            continue

        live_price = _safe_float(rt.get("last_price"))
        if live_price <= 0:
            continue

        item["close"] = live_price
        item["last_price"] = live_price
        item["time"] = rt.get("last_time") or item.get("time")
        item["price_source"] = "intraday_1m"

        prev_close = _safe_float(item.get("prev_close"))
        if prev_close > 0:
            change = live_price - prev_close
            item["change"] = change
            item["change_percent"] = (change / prev_close) * 100.0

    return rows
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
    realtime = (_param(params, "realtime", "1") or "1").lower() in ("1", "true", "yes", "y")
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
            df = vnstock.listing_companies()
            data = _attach_logo_url(_df_to_records(df))
            return _response(200, {"success": True, "cmd": cmd, "data": data})
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
            tickers = symbols_ls or [symbol]
            data = None
            try:
                # Thử dùng price_board chuẩn
                df = vnstock.price_board(symbol_ls=tickers if len(tickers) > 1 else tickers[0])
                data = _df_to_records(df)
            except Exception:
                # FALLBACK: lấy close gần nhất cho từng mã (song song), không chỉ mã đầu tiên.
                try:
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
                    data = rows
                except Exception as e2:
                    data = {"error": "All sources failed", "details": str(e2)}

            # Đồng bộ giá hiển thị với chart: ưu tiên giá intraday mới nhất trong phiên.
            if realtime and isinstance(data, list):
                try:
                    data = _enrich_price_board_with_intraday(data, tickers, workers=12)
                except Exception:
                    pass

            return _response(200, {
                "success": True, 
                "cmd": cmd, 
                "input": {"tickers": tickers, "realtime": realtime},
                "data": data
            })
        if cmd == "market_snapshot":
            exchange = _param(params, "exchange", "HOSE").upper()
            try:
                # 1. Lấy danh sách công ty trên sàn đó
                df_list = vnstock.listing_companies()
                df_selected = df_list[df_list['comGroupCode'] == exchange]
                
                # 2. Để tránh 'cháy máy', lấy 20 mã tiêu biểu nhất kèm giá (hoặc bạn có thể tăng lên)
                # Vì lấy lịch sử cho 1600 mã lúc nửa đêm sẽ rất chậm trên Lambda
                tickers = df_selected['ticker'].tolist()[:50] 
                
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
                    df_list = vnstock.listing_companies()
                    if exchange in ("HOSE", "HNX", "UPCOM"):
                        df_list = df_list[df_list["comGroupCode"] == exchange]
                    tickers = df_list["ticker"].tolist()[:limit]
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
            try:
                df = vnstock.stock_intraday_data(symbol=symbol, page_size=safe_size)
                data = _df_to_records(df)
            except Exception:
                # Nếu vnstock lỗi, thử dùng hàm fallback cũ của bạn
                try:
                    data = _tcbs_intraday(symbol, safe_size)
                except:
                    data = [] # Trả về list rỗng thay vì lỗi 500
                
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

            try:
                # TUYỆT CHIÊU: Nếu là nến giờ/phút, dùng ohlc_data thay vì historical_data
                if is_intraday:
                    try:
                        df = vnstock.ohlc_data(
                            symbol=symbol, 
                            start_date=start_date, 
                            end_date=end_date, 
                            resolution=clean_res, 
                            type='stock'
                        )
                    except Exception:
                        # Fallback cho 60/120/240 nếu nguồn không trả trực tiếp:
                        # kéo 30 phút rồi gộp nến để giữ data "thật" (không mock).
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
                            return _response(200, {
                                "success": True,
                                "cmd": cmd,
                                "input": {"symbol": symbol, "res": clean_res, "start": start_date, "fallback": "aggregate_from_30m"},
                                "data": agg,
                            })
                        raise
                else:
                    # 1W/1M từ nguồn thường thiếu/đứt dữ liệu, nên lấy 1D rồi aggregate lịch.
                    if clean_res in ("1W", "1M"):
                        base_df = vnstock.stock_historical_data(
                            symbol=symbol,
                            start_date=start_date,
                            end_date=end_date,
                            resolution="1D",
                            type="stock",
                        )
                        agg = _aggregate_daily_to_calendar(base_df, clean_res)
                        return _response(200, {
                            "success": True,
                            "cmd": cmd,
                            "input": {"symbol": symbol, "res": clean_res, "start": start_date, "fallback": "aggregate_from_1D"},
                            "data": agg,
                        })
                    df = vnstock.stock_historical_data(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                        resolution=clean_res,
                        type="stock"
                    )
                
                if df is None or (hasattr(df, "empty") and df.empty):
                    return _response(200, {
                        "success": False, 
                        "error": f"Server nguồn (TCBS) từ chối nến {clean_res}. Thử lại với nến 1D hoặc giảm số ngày.",
                        "data": []
                    })
                
                return _response(200, {
                    "success": True,
                    "cmd": cmd,
                    "input": {"symbol": symbol, "res": clean_res, "start": start_date},
                    "data": _df_to_records(df),
                })

            except Exception as e:
                return _response(200, {"success": False, "error": f"Lỗi: {str(e)}"})
        if cmd == "ohlc_data":
            # vnstock.ohlc_data signature có thể khác nhau theo version;
            # support các param phổ biến: symbol, start_date, end_date, resolution/type (nếu có)
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")
            if not start_date:
                start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            try:
                df = vnstock.ohlc_data(symbol=symbol, start_date=start_date, end_date=end_date, resolution=resolution, type="stock")
            except TypeError:
                df = vnstock.ohlc_data(symbol=symbol, start_date=start_date, end_date=end_date)
            return _response(
                200,
                {
                    "success": True,
                    "cmd": cmd,
                    "input": {"symbol": symbol, "start_date": start_date, "end_date": end_date, "resolution": resolution},
                    "data": _df_to_records(df),
                },
            )
        if cmd == "longterm_ohlc_data":
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
                    type="stock"
                )
                data = _df_to_records(df)
            except Exception as e:
                # Nếu fail, thử nốt hàm nguyên bản hoặc trả về lỗi sạch sẽ
                try:
                    df = vnstock.longterm_ohlc_data(symbol=symbol)
                    data = _df_to_records(df)
                except:
                    data = {"error": f"Longterm data unavailable: {str(e)}"}
                
            return _response(200, {
                "success": True, 
                "cmd": cmd, 
                "input": {"symbol": symbol}, 
                "data": data
            })
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
