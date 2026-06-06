# syntax=docker/dockerfile:1.4
# BuildKit 必須: Docker 23+ はデフォルト有効。古い場合は DOCKER_BUILDKIT=1 を設定すること。
FROM python:3.12-slim

WORKDIR /app

# システム依存
# - build-essential: C 拡張のコンパイルに必要
# - libgomp1: OpenMP (onnxruntime が使用)
# - git: agent-hub-sdk を GitHub URL でインストールするために必要
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgomp1 \
    git \
    && rm -rf /var/lib/apt/lists/*

# レイヤー 1: onnxruntime を先にインストール (変更頻度低 → キャッシュ効果大)
# ARM64 対応確認済み: onnxruntime-1.20.x-cp312-cp312-manylinux_2_27_aarch64.whl
# ※ pipecat-ai の silero エクストラは空 ([]) のため onnxruntime の明示インストールが必要
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install "onnxruntime>=1.19.0,<2.0"

# レイヤー 2: 残りの依存パッケージ (requirements.txt 変更時のみ再実行)
COPY server/requirements.txt ./
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

# アプリケーション
COPY server/ ./server/
COPY client/ ./client/

WORKDIR /app/server

CMD ["python", "main.py"]
