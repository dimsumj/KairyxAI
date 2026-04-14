#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BACKEND_DIR="$ROOT_DIR/backend/services"
FRONTEND_DIR="$ROOT_DIR/frontend"
VENV_DIR="$ROOT_DIR/.venv"
APP_HOST="${KAIRYX_HOST:-0.0.0.0}"
APP_PORT="${KAIRYX_PORT:-8000}"
DISPLAY_HOST="${KAIRYX_DISPLAY_HOST:-localhost}"

export DATA_BACKEND_MODE=${DATA_BACKEND_MODE:-mock}
export PYTHONUNBUFFERED=1
export CONTROL_PLANE_DATABASE_URL=${CONTROL_PLANE_DATABASE_URL:-sqlite:///$BACKEND_DIR/.kairyx_control_plane.db}
export CONTROL_PLANE_SECRET_KEY=${CONTROL_PLANE_SECRET_KEY:-local-demo-control-plane-secret-key}
export KAIRYX_LOCAL_DB_PATH=${KAIRYX_LOCAL_DB_PATH:-$BACKEND_DIR/.kairyx_local.db}
PYTHON_BIN=${KAIRYX_PYTHON_BIN:-python3.14}

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "[KairyxAI] Required interpreter '$PYTHON_BIN' was not found."
  echo "[KairyxAI] Install Python 3.14 or set KAIRYX_PYTHON_BIN to a Python 3.14 binary."
  exit 1
fi

PY_VERSION="$("$PYTHON_BIN" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
if [[ "$PY_VERSION" != "3.14" ]]; then
  echo "[KairyxAI] Python 3.14 is required. '$PYTHON_BIN' resolved to Python $PY_VERSION."
  echo "[KairyxAI] Set KAIRYX_PYTHON_BIN to a Python 3.14 binary and retry."
  exit 1
fi

if [[ ! -d "$VENV_DIR" ]]; then
  echo "[KairyxAI] Creating local virtualenv with $PYTHON_BIN"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

VENV_PYTHON="$VENV_DIR/bin/python"
VENV_UVICORN="$VENV_DIR/bin/uvicorn"

if [[ ! -x "$VENV_PYTHON" ]]; then
  echo "[KairyxAI] Virtualenv is incomplete. Recreating with $PYTHON_BIN"
  rm -rf "$VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

VENV_VERSION="$("$VENV_PYTHON" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
if [[ "$VENV_VERSION" != "3.14" ]]; then
  echo "[KairyxAI] Existing .venv uses Python $VENV_VERSION. Recreating with $PYTHON_BIN"
  rm -rf "$VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

echo "[KairyxAI] Starting local demo in DATA_BACKEND_MODE=$DATA_BACKEND_MODE using $("$VENV_PYTHON" --version)"
echo "[KairyxAI] Control plane DB: $CONTROL_PLANE_DATABASE_URL"
echo "[KairyxAI] Local identity/checkpoint DB: $KAIRYX_LOCAL_DB_PATH"

if ! command -v npm >/dev/null 2>&1; then
  echo "[KairyxAI] npm is required to build the React frontend."
  echo "[KairyxAI] Install Node.js with npm and retry."
  exit 1
fi

action_cleanup() {
  echo "[KairyxAI] Stopping local demo..."
  if [[ -n "${FRONTEND_PID:-}" ]] && kill -0 "$FRONTEND_PID" >/dev/null 2>&1; then
    kill "$FRONTEND_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "${BACKEND_PID:-}" ]] && kill -0 "$BACKEND_PID" >/dev/null 2>&1; then
    pkill -TERM -P "$BACKEND_PID" >/dev/null 2>&1 || true
    kill "$BACKEND_PID" >/dev/null 2>&1 || true
    sleep 1
    pkill -KILL -P "$BACKEND_PID" >/dev/null 2>&1 || true
    kill -KILL "$BACKEND_PID" >/dev/null 2>&1 || true
  fi
}
trap action_cleanup EXIT INT TERM

echo "[KairyxAI] Installing frontend dependencies"
(cd "$FRONTEND_DIR" && npm install >/dev/null)
echo "[KairyxAI] Building frontend bundle"
(cd "$FRONTEND_DIR" && npm run build >/dev/null)
echo "[KairyxAI] Watching frontend bundle for changes"
(cd "$FRONTEND_DIR" && npm run build:watch >/dev/null) &
FRONTEND_PID=$!

cd "$BACKEND_DIR"
"$VENV_PYTHON" -m pip install -r requirements.txt >/dev/null
"$VENV_UVICORN" main_service:app --host "$APP_HOST" --port "$APP_PORT" --reload --reload-dir ../../frontend --reload-dir ../../frontend/dist &
BACKEND_PID=$!

echo "[KairyxAI] Backend: http://$DISPLAY_HOST:$APP_PORT"
echo "[KairyxAI] Frontend (served by backend): http://$DISPLAY_HOST:$APP_PORT"
echo "[KairyxAI] Press Ctrl+C to stop."

wait $BACKEND_PID
