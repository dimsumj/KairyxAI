#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BACKEND_DIR="$ROOT_DIR/backend/services"

export DATA_BACKEND_MODE=${DATA_BACKEND_MODE:-mock}
export PYTHONUNBUFFERED=1

echo "[KairyxAI] Starting local demo in DATA_BACKEND_MODE=$DATA_BACKEND_MODE"

action_cleanup() {
  echo "[KairyxAI] Stopping local demo..."
  kill ${BACKEND_PID:-0} >/dev/null 2>&1 || true
}
trap action_cleanup EXIT INT TERM

cd "$BACKEND_DIR"
python3 -m pip install -r requirements.txt >/dev/null 2>&1 || true
uvicorn main_service:app --host 0.0.0.0 --port 8000 --reload --reload-dir ../../frontend &
BACKEND_PID=$!

echo "[KairyxAI] Backend: http://localhost:8000"
echo "[KairyxAI] Frontend (served by backend): http://localhost:8000"
echo "[KairyxAI] Press Ctrl+C to stop."

wait $BACKEND_PID
