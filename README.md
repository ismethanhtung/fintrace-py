# fintrace-py

API server cho dữ liệu chứng khoán Việt Nam, gồm:
- Nhóm API `vnstock` (listing, price board, intraday, historical, technical, volatility, analysis...)
- Nhóm API realtime VNDIRECT qua WebSocket (`BA`, `SP`, `DE`, `MI`) với cache snapshot

## Run nhanh
```bash
docker build -t fintrace-local:realtime .
docker run -d --name fintrace-realtime -p 18000:8000 fintrace-local:realtime
```

## Health
```bash
curl -sS "http://127.0.0.1:18000/?cmd=health"
```

## Docs
- Realtime VNDIRECT: `docs/VNDIRECT_REALTIME_API.md`
