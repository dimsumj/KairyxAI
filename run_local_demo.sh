#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BACKEND_DIR="$ROOT_DIR/backend/services"
VENV_DIR="$ROOT_DIR/.venv"

export DATA_BACKEND_MODE=${DATA_BACKEND_MODE:-mock}
export PYTHONUNBUFFERED=1
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

action_cleanup() {
  echo "[KairyxAI] Stopping local demo..."
  kill ${BACKEND_PID:-0} >/dev/null 2>&1 || true
}
trap action_cleanup EXIT INT TERM

cd "$BACKEND_DIR"
"$VENV_PYTHON" -m pip install -r requirements.txt >/dev/null
"$VENV_UVICORN" main_service:app --host 0.0.0.0 --port 8000 --reload --reload-dir ../../frontend &
BACKEND_PID=$!

echo "[KairyxAI] Backend: http://localhost:8000"
echo "[KairyxAI] Frontend (served by backend): http://localhost:8000"
echo "[KairyxAI] Press Ctrl+C to stop."

wait $BACKEND_PID
