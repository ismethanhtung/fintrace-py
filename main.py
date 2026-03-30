import json
import os
import re
import asyncio
import time
import threading
from typing import Optional

import httpx
import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
from fastapi.concurrency import run_in_threadpool
# Hãy chắc chắn bạn đã đổi tên file lambda.py thành handler.py
from handler import lambda_handler, shutdown_server_runtime, warm_server_caches

app = FastAPI()

LOGO_DIR = os.getenv("LOGO_DIR", "static/logos")
DEFAULT_LOGO_PATH = os.getenv("DEFAULT_LOGO_PATH", "static/default_logo.svg")
LOGO_UPSTREAM_TEMPLATE = os.getenv(
    "LOGO_UPSTREAM_TEMPLATE",
    "https://files.bsc.com.vn/websitebsc/logo/{symbol}.svg",
)
LOGO_CONNECT_TIMEOUT_SECONDS = float(os.getenv("LOGO_CONNECT_TIMEOUT_SECONDS", "1.2"))
LOGO_READ_TIMEOUT_SECONDS = float(os.getenv("LOGO_READ_TIMEOUT_SECONDS", "2.0"))
LOGO_NOT_FOUND_TTL_SECONDS = int(os.getenv("LOGO_NOT_FOUND_TTL_SECONDS", "86400"))
LOGO_CACHE_CONTROL = os.getenv("LOGO_CACHE_CONTROL", "public, max-age=2592000, stale-while-revalidate=86400")
DEFAULT_LOGO_CACHE_CONTROL = os.getenv("DEFAULT_LOGO_CACHE_CONTROL", "public, max-age=3600")
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9._-]{1,20}$")
GZIP_MINIMUM_SIZE = int(os.getenv("GZIP_MINIMUM_SIZE", "1024"))
GZIP_COMPRESSLEVEL = int(os.getenv("GZIP_COMPRESSLEVEL", "5"))
HANDLER_TIMEOUT_SECONDS = float(os.getenv("HANDLER_TIMEOUT_SECONDS", "45"))

_MISSING_LOGO_CACHE = {}
_MISSING_LOGO_LOCK = threading.Lock()
_SYMBOL_LOCKS = {}
_SYMBOL_LOCKS_GUARD = threading.Lock()

os.makedirs(LOGO_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DEFAULT_LOGO_PATH) or ".", exist_ok=True)
if not os.path.exists(DEFAULT_LOGO_PATH):
    with open(DEFAULT_LOGO_PATH, "w", encoding="utf-8") as f:
        f.write(
            "<svg xmlns='http://www.w3.org/2000/svg' width='128' height='128' "
            "viewBox='0 0 128 128'><rect width='128' height='128' fill='#f2f4f7'/>"
            "<text x='64' y='72' text-anchor='middle' font-size='16' "
            "font-family='Arial, sans-serif' fill='#667085'>N/A</text></svg>"
        )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(
    GZipMiddleware,
    minimum_size=max(256, GZIP_MINIMUM_SIZE),
    compresslevel=max(1, min(9, GZIP_COMPRESSLEVEL)),
)


def _get_symbol_lock(symbol: str) -> asyncio.Lock:
    with _SYMBOL_LOCKS_GUARD:
        lock = _SYMBOL_LOCKS.get(symbol)
        if lock is None:
            lock = asyncio.Lock()
            _SYMBOL_LOCKS[symbol] = lock
        return lock


def _is_missing_logo(symbol: str) -> bool:
    now = time.time()
    with _MISSING_LOGO_LOCK:
        expires_at = _MISSING_LOGO_CACHE.get(symbol)
        if not expires_at:
            return False
        if expires_at <= now:
            _MISSING_LOGO_CACHE.pop(symbol, None)
            return False
        return True


def _mark_missing_logo(symbol: str) -> None:
    with _MISSING_LOGO_LOCK:
        _MISSING_LOGO_CACHE[symbol] = time.time() + max(60, LOGO_NOT_FOUND_TTL_SECONDS)


def _logo_headers(found: bool) -> dict:
    return {"Cache-Control": LOGO_CACHE_CONTROL if found else DEFAULT_LOGO_CACHE_CONTROL}


@app.on_event("startup")
async def preload_server_caches():
    timeout = httpx.Timeout(
        connect=max(0.1, LOGO_CONNECT_TIMEOUT_SECONDS),
        read=max(0.1, LOGO_READ_TIMEOUT_SECONDS),
        write=max(0.1, LOGO_READ_TIMEOUT_SECONDS),
        pool=max(0.1, LOGO_CONNECT_TIMEOUT_SECONDS),
    )
    limits = httpx.Limits(max_connections=128, max_keepalive_connections=32, keepalive_expiry=30)
    app.state.logo_client = httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=True)
    threading.Thread(target=warm_server_caches, daemon=True).start()


@app.on_event("shutdown")
async def close_server_runtime():
    logo_client: Optional[httpx.AsyncClient] = getattr(app.state, "logo_client", None)
    if logo_client is not None:
        await logo_client.aclose()
    shutdown_server_runtime()

@app.get("/stock/image/{symbol}")
async def get_cached_stock_logo(symbol: str):
    symbol = symbol.upper().strip()
    if not SYMBOL_PATTERN.match(symbol):
        return FileResponse(DEFAULT_LOGO_PATH, media_type="image/svg+xml", headers=_logo_headers(found=False))

    file_path = os.path.join(LOGO_DIR, f"{symbol}.svg")
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="image/svg+xml", headers=_logo_headers(found=True))

    if _is_missing_logo(symbol):
        return FileResponse(DEFAULT_LOGO_PATH, media_type="image/svg+xml", headers=_logo_headers(found=False))

    lock = _get_symbol_lock(symbol)
    async with lock:
        if os.path.exists(file_path):
            return FileResponse(file_path, media_type="image/svg+xml", headers=_logo_headers(found=True))
        if _is_missing_logo(symbol):
            return FileResponse(DEFAULT_LOGO_PATH, media_type="image/svg+xml", headers=_logo_headers(found=False))

        target_url = LOGO_UPSTREAM_TEMPLATE.format(symbol=symbol)
        logo_client: Optional[httpx.AsyncClient] = getattr(app.state, "logo_client", None)
        close_after = False
        if logo_client is None:
            logo_client = httpx.AsyncClient(timeout=max(0.1, LOGO_READ_TIMEOUT_SECONDS))
            close_after = True
        try:
            upstream = await logo_client.get(target_url)
            if upstream.status_code == 200 and upstream.content:
                with open(file_path, "wb") as f:
                    f.write(upstream.content)
                return Response(content=upstream.content, media_type="image/svg+xml", headers=_logo_headers(found=True))
            _mark_missing_logo(symbol)
        except Exception:
            _mark_missing_logo(symbol)
        finally:
            if close_after:
                await logo_client.aclose()

    return FileResponse(DEFAULT_LOGO_PATH, media_type="image/svg+xml", headers=_logo_headers(found=False))

# Thay @app.all bằng @app.api_route để nhận mọi loại HTTP Method
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
async def proxy_lambda(request: Request, path: str):
    query_params = dict(request.query_params)
    body_bytes = await request.body()
    body_str = body_bytes.decode() if body_bytes else None
    
    event = {
        "resource": "/" + path,
        "path": "/" + path,
        "httpMethod": request.method,
        "queryStringParameters": query_params,
        "body": body_str,
    }
    
    try:
        # Gọi logic Lambda
        result = await asyncio.wait_for(
            run_in_threadpool(lambda_handler, event, None),
            timeout=max(1.0, HANDLER_TIMEOUT_SECONDS),
        )
        
        status_code = result.get("statusCode", 200)
        body_content = result.get("body", "{}")
        headers = dict(result.get("headers", {}) or {})

        if isinstance(body_content, (dict, list)):
            return JSONResponse(status_code=status_code, content=body_content, headers=headers)

        if not isinstance(body_content, str):
            body_content = json.dumps(body_content, ensure_ascii=False, default=str)

        return Response(
            status_code=status_code,
            content=body_content,
            headers=headers,
            media_type=None,
        )
    except asyncio.TimeoutError:
        return JSONResponse(
            status_code=504,
            content={
                "success": False,
                "error": "gateway_timeout",
                "detail": f"handler timeout after {max(1.0, HANDLER_TIMEOUT_SECONDS):.0f}s",
            },
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
