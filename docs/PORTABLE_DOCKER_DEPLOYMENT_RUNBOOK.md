# KairyxAI Portable Docker Deployment Runbook

## 1) Purpose
This runbook describes the portable container path for KairyxAI:

- one repo-root Docker image
- one Docker Compose baseline for single-host deployments
- one shared image digest promoted to GCP Cloud Run or AWS ECS/Fargate

This runbook assumes the current repository runtime model:

- `operator-api`
- `import-worker`
- `prediction-worker`
- `export-worker`
- `scheduler-worker`

It also assumes the first portability phase is `portable compute, GCP data plane`. The containers can run on GCP, AWS, or self-hosted Docker, but production data services still remain BigQuery, GCS, Pub/Sub, and Google Secret Manager compatible.

## 2) What The Container Contract Looks Like

### 2.1 One Image, Multiple Roles
The same image digest is reused for every runtime role. `SERVICE_ROLE` selects the process:

| `SERVICE_ROLE` | Process |
| --- | --- |
| `operator-api` | `gunicorn app.main:app` |
| `import-worker` | `uvicorn workers.import_worker_app:app` |
| `prediction-worker` | `uvicorn workers.prediction_worker_app:app` |
| `export-worker` | `uvicorn workers.export_worker_app:app` |
| `scheduler-worker` | `uvicorn workers.scheduler_worker_app:app` |

### 2.2 Required Runtime Settings
Common settings:

- `APP_ENV`
- `SERVICE_ROLE`
- `CONTROL_PLANE_DATABASE_URL`
- `DATA_BACKEND_MODE`
- `CORS_ALLOWED_ORIGINS`
- `GCP_PROJECT_ID`
- `GCP_SECRET_PROJECT_ID`
- `IMPORT_COMMAND_TOPIC`
- `PREDICTION_COMMAND_TOPIC`
- `EXPORT_COMMAND_TOPIC`
- `PUBSUB_TOPIC_NAME`
- `PORT`

API-specific tuning:

- `WEB_CONCURRENCY`
- `GUNICORN_TIMEOUT`

Worker-specific auth:

- `WORKER_SHARED_TOKEN`

### 2.3 Worker Endpoint Auth
Worker-only endpoints require `WORKER_SHARED_TOKEN`.

Supported request shapes:

- `Authorization: Bearer <WORKER_SHARED_TOKEN>`
- `?token=<WORKER_SHARED_TOKEN>`

The query-string form exists so platform-managed `Authorization` headers can still be used for Cloud Run authenticated invocations and similar provider-controlled ingress paths.

## 3) Build And Smoke The Image

### 3.1 Local Build
Build from the repository root:

```bash
docker build -t kairyxai:local .
```

### 3.2 Local Single-Container Smoke
Run the API in local mock mode:

```bash
docker run --rm -p 8000:8080 \
  -e APP_ENV=local \
  -e SERVICE_ROLE=operator-api \
  -e DATA_BACKEND_MODE=mock \
  -e LEGACY_HEADER_AUTH_ENABLED=true \
  -e CONTROL_PLANE_DATABASE_URL=sqlite:////tmp/kairyx-control-plane.db \
  -e KAIRYX_LOCAL_DB_PATH=/tmp/kairyx-local.db \
  kairyxai:local
```

Verify:

- `GET http://127.0.0.1:8000/health/live`
- `GET http://127.0.0.1:8000/`
- `GET http://127.0.0.1:8000/static/operator-console.js`
- `GET http://127.0.0.1:8000/static/operator-console.css`

## 4) Docker Compose Baseline

### 4.1 What Compose Provides
The repository includes:

- `Dockerfile`
- `docker-compose.yml`
- `deploy/docker/compose.env`

This Compose baseline is designed for:

- local multi-container smoke tests
- single-host self-managed environments
- role-by-role validation against one Postgres instance

It is not a clustered HA deployment design.

### 4.2 Start Compose
From the repository root:

```bash
docker compose up --build
```

Default published ports:

- `operator-api` -> `http://127.0.0.1:8000`
- `import-worker` -> `http://127.0.0.1:18081`
- `prediction-worker` -> `http://127.0.0.1:18082`
- `export-worker` -> `http://127.0.0.1:18083`
- `scheduler-worker` -> `http://127.0.0.1:18084`

The worker ports are loopback-only in the baseline file so they are reachable for local smoke tests without being broadly exposed from the host.

### 4.3 Self-Hosted Notes
For self-hosted production-like use:

- keep `operator-api` behind HTTPS
- expose worker services only if an external scheduler or callback source must reach them
- if external ingress is required, place the worker endpoints behind an auth-aware reverse proxy and preserve the `token=` or bearer-token contract
- replace the tracked `deploy/docker/compose.env` defaults with environment-appropriate values before using real external systems

## 5) GCP Cloud Run

### 5.1 Image Promotion
Push one immutable image digest to Artifact Registry and reuse it for all services.

The repository Cloud Run manifests under `backend/services/cloudrun/` now all point to the same image placeholder and vary only by service-specific environment.

### 5.2 Deploy Shape

- `operator-api` stays public
- worker services stay authenticated at the platform layer
- worker services also require `WORKER_SHARED_TOKEN`

Recommended worker endpoint patterns:

- `https://IMPORT_WORKER_HOST/pubsub/push?token=WORKER_SHARED_TOKEN`
- `https://PREDICTION_WORKER_HOST/pubsub/push?token=WORKER_SHARED_TOKEN`
- `https://EXPORT_WORKER_HOST/pubsub/push?token=WORKER_SHARED_TOKEN`
- `https://SCHEDULER_WORKER_HOST/run?token=WORKER_SHARED_TOKEN`

### 5.3 Pub/Sub And Cloud Scheduler
When you create authenticated Pub/Sub push subscriptions or Cloud Scheduler HTTP jobs, use the tokenized endpoint URL so Cloud Run ingress auth and application-level worker auth can coexist on the same request.

## 6) AWS ECS/Fargate

### 6.1 Repository Assets
AWS ECS templates live under:

- `deploy/aws/ecs/task-definitions/`
- `deploy/aws/ecs/service-definitions/`

They are starter templates, not Terraform.

### 6.2 Recommended AWS Shape

- `operator-api` as an ECS service behind an ALB
- `import-worker`, `prediction-worker`, and `export-worker` as separate ECS services with HTTPS-reachable endpoints if GCP Pub/Sub push or another external caller must reach them
- `scheduler-worker` invoked on a schedule by an HTTP-capable scheduler path such as EventBridge plus API Destination, or another internal scheduler that can call `/run?token=...`

### 6.3 GCP Dependency Note
Because the application still uses `DATA_BACKEND_MODE=gcp` in production, AWS tasks must also have Google Cloud credentials and network reachability to:

- BigQuery
- Cloud Storage
- Pub/Sub
- Google Secret Manager if `gsm://` references are used

Recommended order of preference:

1. workload identity federation from AWS to GCP
2. a securely mounted GCP service account credential file as a fallback

Do not treat ECS deployment as cloud-agnostic unless the GCP service dependency has been explicitly removed in code.

## 7) Release Checklist

1. Build the image from the repository root.
2. Smoke the image as `operator-api`.
3. Smoke at least one worker endpoint with `WORKER_SHARED_TOKEN`.
4. Promote one immutable image digest.
5. Deploy the same digest to every runtime role.
6. Verify `/health/live` for every deployed service.
7. Verify one authenticated worker callback path per target platform.
