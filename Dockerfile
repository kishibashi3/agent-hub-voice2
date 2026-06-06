FROM python:3.12-slim

WORKDIR /app

# システム依存 (Pipecat Silero VAD が onnxruntime を使用)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# 依存パッケージ
COPY server/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーション
COPY server/ ./server/
COPY client/ ./client/

WORKDIR /app/server

CMD ["python", "main.py"]
