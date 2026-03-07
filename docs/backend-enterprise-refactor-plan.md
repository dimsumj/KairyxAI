# Backend Enterprise Refactor Plan

## Summary

This document is the implementation contract for the backend control-plane refactor.

The refactor establishes:

- a modular FastAPI control plane under `/api/v1`
- SQLAlchemy + Alembic control-plane persistence
- queue-backed workers for imports, predictions, and exports
- paged connector ingestion and checkpointed raw shard publishing
- BigQuery-backed prediction result storage and paginated reads

The target operating model is single-tenant, GCP-native, and batch + nearline.

## Priority Deliverables

1. Create `backend/services/app/` with routers, schemas, services, repositories, and app factory.
2. Introduce versioned `/api/v1` routes for health, connectors, mappings, imports, predictions, exports, and experiments.
3. Replace control-plane globals and local JSON caches in the new path with SQLAlchemy-backed repositories.
4. Add worker entrypoints for import, prediction, and export jobs.
5. Move prediction outputs into BigQuery-backed storage with paginated API access.
6. Add paged connector ingestion contracts and checkpoint-aware import worker execution.

## Runtime Shape

- `operator-api`: FastAPI control plane
- `import-worker`: connector paging, GCS shard writes, Pub/Sub manifest publishing
- `prediction-worker`: aggregate-table prediction execution and result persistence
- `export-worker`: provider export execution with retry-aware job state
- `dataflow`: manifest-driven normalization into BigQuery staging and curated tables

## Control-Plane Persistence

The system of record for control-plane metadata is SQLAlchemy over Postgres in production, with SQLite as the local-dev fallback.

Persisted entities:

- connector configurations
- field mappings
- import jobs
- prediction jobs
- export jobs
- experiment configuration
- action history
- ingestion checkpoints

## API Surface

New API prefix: `/api/v1`

Route groups:

- `/health`
- `/connectors`
- `/mappings`
- `/imports`
- `/predictions`
- `/exports`
- `/experiments`

Standard job response shape:

- `id`
- `type`
- `status`
- `created_at`
- `updated_at`
- `progress`
- `error`
- `links`

Large result sets must be paginated.

## Delivery Notes

- `main_service.py` becomes a temporary shim to the new app.
- Existing low-level pipeline services remain reusable behind the new application layer.
- The new API can break old endpoint contracts; all new backend work should target `/api/v1`.
