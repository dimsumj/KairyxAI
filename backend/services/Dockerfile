FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /workspace/backend/services

COPY backend/services/requirements.txt /workspace/backend/services/requirements.txt
RUN pip install --no-cache-dir -r /workspace/backend/services/requirements.txt

COPY backend/services /workspace/backend/services
COPY frontend /workspace/frontend

ENV PORT=8080
CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "app.main:app", "--bind", "0.0.0.0:8080"]
