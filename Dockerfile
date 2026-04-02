FROM node:22-bookworm-slim AS frontend-builder

WORKDIR /app/frontend

COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci

COPY frontend ./
RUN npm run build

FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080
ENV WEB_CONCURRENCY=4
ENV GUNICORN_TIMEOUT=300

WORKDIR /app/backend/services

COPY backend/services/requirements.txt /app/backend/services/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/services/requirements.txt

COPY backend/services /app/backend/services
COPY --from=frontend-builder /app/frontend/dist /app/frontend/dist
COPY docker/entrypoint.sh /usr/local/bin/kairyx-entrypoint

RUN chmod +x /usr/local/bin/kairyx-entrypoint

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/kairyx-entrypoint"]
