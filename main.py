import json
import os
import re

import httpx
import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
# Hãy chắc chắn bạn đã đổi tên file lambda.py thành handler.py
from handler import lambda_handler

app = FastAPI()

LOGO_DIR = os.getenv("LOGO_DIR", "static/logos")
DEFAULT_LOGO_PATH = os.getenv("DEFAULT_LOGO_PATH", "static/default_logo.svg")
LOGO_UPSTREAM_TEMPLATE = os.getenv(
    "LOGO_UPSTREAM_TEMPLATE",
    "https://files.bsc.com.vn/websitebsc/logo/{symbol}.svg",
)
LOGO_TIMEOUT_SECONDS = float(os.getenv("LOGO_TIMEOUT_SECONDS", "8"))
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9._-]{1,20}$")

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

@app.get("/stock/image/{symbol}")
async def get_cached_stock_logo(symbol: str):
    symbol = symbol.upper().strip()
    if not SYMBOL_PATTERN.match(symbol):
        return FileResponse(DEFAULT_LOGO_PATH, media_type="image/svg+xml")

    file_path = os.path.join(LOGO_DIR, f"{symbol}.svg")
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="image/svg+xml")

    target_url = LOGO_UPSTREAM_TEMPLATE.format(symbol=symbol)
    try:
        async with httpx.AsyncClient(timeout=LOGO_TIMEOUT_SECONDS) as client:
            upstream = await client.get(target_url)
        if upstream.status_code == 200 and upstream.content:
            with open(file_path, "wb") as f:
                f.write(upstream.content)
            return Response(content=upstream.content, media_type="image/svg+xml")
    except Exception:
        pass

    return FileResponse(DEFAULT_LOGO_PATH, media_type="image/svg+xml")

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
        result = lambda_handler(event, None)
        
        status_code = result.get("statusCode", 200)
        body_content = result.get("body", "{}")
        
        if isinstance(body_content, str):
            try:
                body_content = json.loads(body_content)
            except:
                pass
                
        return JSONResponse(
            status_code=status_code,
            content=body_content,
            headers=result.get("headers", {})
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
