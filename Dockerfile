# Sử dụng Python 3.14 (theo yêu cầu của bạn)
FROM python:3.14-slim

WORKDIR /app

# Cài đặt các thư viện tương đương với 2 Layer của bạn
# AWSSDKPandas tương đương awswrangler, vnstock là thư viện bạn dùng


RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    awswrangler \
    pandas \
    requests \
    beautifulsoup4 \
    lxml \
    vnstock==0.2.9.1



# Copy toàn bộ code vào trong image
COPY . .

# Mở cổng 8000
EXPOSE 8000

# Lệnh chạy server
CMD ["python", "main.py"]
