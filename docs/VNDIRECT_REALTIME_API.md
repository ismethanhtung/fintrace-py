# VNDIRECT Realtime API (Server fintrace-py)

## Mục tiêu
Module realtime mới dùng WebSocket VNDIRECT để thu thập dữ liệu liên tục và lưu snapshot in-memory cho các feed:
- `BA` (Bid/Ask Top 3)
- `SP` (Stock Partial)
- `DE` (Derivative Opt)
- `MI` (Market Information / Index)

Tất cả response realtime đều có nhãn phân biệt API rõ ràng:
- `api_group: "vndirect_realtime"`
- `api_source: "vndirect_websocket"`
- `api_tag: "vndirect_ws.<FEED>"` (trong từng bản ghi)

## Các cmd mới

### 1) `cmd=vndirect_realtime_status`
Kiểm tra trạng thái collector realtime.

Ví dụ:
```bash
curl -sS "http://127.0.0.1:8000/?cmd=vndirect_realtime_status"
```

Thông tin trả về gồm:
- `connected`, `started`
- `messages_total`, `messages_by_feed`
- `subscription` (số mã equity/phái sinh + market id)
- `cached_snapshot_counts` theo từng feed

### 2) `cmd=vndirect_realtime_snapshot`
Lấy snapshot mới nhất đã cache của các feed realtime.

Ví dụ lấy toàn bộ snapshot (theo giới hạn):
```bash
curl -sS "http://127.0.0.1:8000/?cmd=vndirect_realtime_snapshot"
```

Ví dụ lọc theo mã + feed:
```bash
curl -sS "http://127.0.0.1:8000/?cmd=vndirect_realtime_snapshot&symbols=FPT,VCB&feeds=BA,SP&limit=200"
```

Query params:
- `symbols`: CSV mã cổ phiếu (tuỳ chọn)
- `feeds`: CSV feed (`BA,SP,DE,MI`) (tuỳ chọn)
- `limit`: số record tối đa cho mỗi feed (10..10000)

### 3) `cmd=vndirect_realtime_subscribe`
Cập nhật danh sách subscribe runtime.

Ví dụ thêm mã (không ghi đè):
```bash
curl -sS "http://127.0.0.1:8000/?cmd=vndirect_realtime_subscribe&mode=add&symbols=FPT,VCB"
```

Ví dụ set mới hoàn toàn:
```bash
curl -sS "http://127.0.0.1:8000/?cmd=vndirect_realtime_subscribe&mode=set&symbols=FPT,VCB,SSI&derivative_symbols=VN30F1M,VN30F2M&market_ids=10,11,12,13,02,03"
```

Query params:
- `mode`: `add` (mặc định) hoặc `set`
- `symbols`: CSV equity symbols
- `derivative_symbols`: CSV mã phái sinh
- `market_ids`: CSV mã chỉ số thị trường (02,03,10,11,12,13...)

## Biến môi trường mới
- `VNDIRECT_REALTIME_ENABLED` (default: `true`)
- `VNDIRECT_WS_URL` (default: `wss://price-cmc-04.vndirect.com.vn/realtime/websocket`)
- `VNDIRECT_RECONNECT_DELAY_S` (default: `2`)
- `VNDIRECT_RECV_TIMEOUT_S` (default: `10`)
- `VNDIRECT_DEFAULT_SYMBOLS` (CSV, bổ sung thêm mã mặc định)
- `VNDIRECT_DEFAULT_SYMBOL_LIMIT` (default: `0` = không giới hạn)
- `VNDIRECT_DERIVATIVE_CODES` (CSV)
- `VNDIRECT_MARKET_IDS` (default: `10,11,12,13,02,03`)
- `VNDIRECT_SNAPSHOT_DEFAULT_LIMIT` (default: `1200`)

## Mặc định dữ liệu subscribe
Khi khởi động server:
- Equity symbols được nạp tự động từ:
  - `Real-time-data-vndirect-master/StockIDs/HSX.txt`
  - `Real-time-data-vndirect-master/StockIDs/HNX.txt`
  - `Real-time-data-vndirect-master/StockIDs/UPC.txt`
- Feed mặc định: `BA`, `SP`, `DE`, `MI`

## Triển khai server
Build image:
```bash
docker build -t fintrace-local:realtime .
```

Run:
```bash
docker run -d --name fintrace-realtime -p 18000:8000 fintrace-local:realtime
```

Health check:
```bash
curl -sS "http://127.0.0.1:18000/?cmd=health"
```

Realtime status:
```bash
curl -sS "http://127.0.0.1:18000/?cmd=vndirect_realtime_status"
```
