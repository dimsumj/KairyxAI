#!/bin/sh
set -eu

SERVICE_ROLE="${SERVICE_ROLE:-operator-api}"
PORT="${PORT:-8080}"
WEB_CONCURRENCY="${WEB_CONCURRENCY:-4}"
GUNICORN_TIMEOUT="${GUNICORN_TIMEOUT:-300}"

cd /app/backend/services

case "$SERVICE_ROLE" in
  operator-api)
    exec gunicorn \
      -w "$WEB_CONCURRENCY" \
      -k uvicorn.workers.UvicornWorker \
      app.main:app \
      --bind "0.0.0.0:${PORT}" \
      --timeout "$GUNICORN_TIMEOUT"
    ;;
  import-worker)
    exec uvicorn workers.import_worker_app:app --host 0.0.0.0 --port "$PORT"
    ;;
  prediction-worker)
    exec uvicorn workers.prediction_worker_app:app --host 0.0.0.0 --port "$PORT"
    ;;
  export-worker)
    exec uvicorn workers.export_worker_app:app --host 0.0.0.0 --port "$PORT"
    ;;
  scheduler-worker)
    exec uvicorn workers.scheduler_worker_app:app --host 0.0.0.0 --port "$PORT"
    ;;
  *)
    echo "Unsupported SERVICE_ROLE: $SERVICE_ROLE" >&2
    exit 1
    ;;
esac
