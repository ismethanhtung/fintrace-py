from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import json
import uvicorn
# Hãy chắc chắn bạn đã đổi tên file lambda.py thành handler.py
from handler import lambda_handler 

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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

