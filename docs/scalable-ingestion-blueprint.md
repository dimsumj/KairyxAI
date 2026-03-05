# Scalable Ingestion and Normalization Blueprint

This document defines the target production architecture for KairyxAI's data ingestion and normalization pipeline while preserving the existing local demo and mock workflow.

## Goals

- Handle millions of events per day with bounded memory and retry-safe processing.
- Preserve the current local developer flow based on `DATA_BACKEND_MODE=mock`.
- Keep FastAPI as the orchestration and operator API layer.
- Make production ingestion replayable, observable, and idempotent.

## Operating Modes

KairyxAI should continue to support two modes:

### 1. Local demo mode

Used for:
- local UI testing
- connector mock flows
- end-to-end experimentation without cloud infrastructure

Characteristics:
- local file-backed raw storage
- in-process or local queue simulation
- local parquet / sqlite persistence
- current FastAPI-driven synchronous pipeline remains available

This mode should stay simple and optimized for developer convenience, not scale.

### 2. Production GCP mode

Used for:
- large connector pulls
- replayable ingestion
- distributed normalization
- warehouse-backed dedupe and aggregates

Characteristics:
- GCS for raw immutable storage
- Pub/Sub for shard work distribution
- Dataflow for normalization
- BigQuery for staging, curation, identity, and serving

The key design rule is: local mode and GCP mode share schema contracts and service interfaces, but not necessarily the same runtime implementation.

## Target Production Architecture

### Control plane

FastAPI in [backend/services/main_service.py](/Users/jeremyz/Projects/KairyxAI/backend/services/main_service.py) remains responsible for:
- connector configuration
- job creation
- job status
- field mapping management
- experiment and churn configuration
- operator workbench APIs

### Data plane

Production data flow:

1. Connector fetcher pages external events.
2. Each page is written to GCS as one compressed JSONL shard.
3. A Pub/Sub message is published with shard metadata only.
4. Dataflow consumes shard messages and normalizes events into a canonical schema.
5. Valid rows land in `events_staging`.
6. Invalid rows land in `pipeline_dead_letters`.
7. BigQuery SQL produces `events_curated` and aggregate serving tables.
8. API and decision services read curated and aggregate tables rather than raw events.

## Data Contracts

### Raw shard contract

One GCS object per bounded shard:

```text
gs://<bucket>/raw/source=<source>/dt=YYYY-MM-DD/hour=HH/job=<job_id>/part-000123.jsonl.gz
```

Format:
- newline-delimited JSON
- gzip compressed
- one source event per line

### Shard manifest contract

Published to Pub/Sub for each shard:

```json
{
  "job_id": "job_20260305_0001",
  "source": "appsflyer",
  "source_config_id": "appsflyer_1",
  "gcs_uri": "gs://bucket/raw/source=appsflyer/dt=2026-03-05/hour=14/job=job_20260305_0001/part-000001.jsonl.gz",
  "event_count": 25000,
  "start_date": "20260301",
  "end_date": "20260303",
  "schema_version": "v1",
  "published_at": "2026-03-05T14:21:00Z"
}
```

Pub/Sub must never carry the raw event list.

### Canonical event schema

All normalized events should conform to a versioned schema:

```json
{
  "schema_version": "v1",
  "job_id": "job_20260305_0001",
  "source": "appsflyer",
  "source_config_id": "appsflyer_1",
  "raw_gcs_uri": "gs://...",
  "ingested_at": "2026-03-05T14:21:12Z",
  "event_time": "2026-03-03T18:12:44Z",
  "event_date": "2026-03-03",
  "player_id": "raw-or-source-player-id",
  "canonical_user_id": "canon_123",
  "source_event_id": "evt_456",
  "event_fingerprint": "sha256(...)",
  "event_type": "session_start",
  "event_properties": {},
  "user_properties": {},
  "campaign": "spring_sale",
  "adset": "retargeting_a",
  "media_source": "facebook",
  "data_quality_flags": []
}
```

## BigQuery Table Layout

Recommended dataset tables:

- `raw_ingestion_audit`
- `events_staging`
- `events_curated`
- `pipeline_dead_letters`
- `identity_links`
- `player_daily_metrics`
- `player_latest_state`
- `player_churn_features`
- `experiment_exposures`
- `experiment_outcomes`

### `events_staging`

Purpose:
- append-only landing table for normalized events
- supports replay and debugging

Recommended design:
- partition by `DATE(event_date)`
- cluster by `source`, `canonical_user_id`, `event_fingerprint`

### `events_curated`

Purpose:
- deduped and cleaned event table used by downstream product logic

Recommended design:
- partition by `DATE(event_date)`
- cluster by `canonical_user_id`, `event_type`

### `player_latest_state`

Purpose:
- low-latency reads for profile and churn APIs

Recommended fields:
- `canonical_user_id`
- `first_seen_at`
- `last_seen_at`
- `lifetime_events`
- `lifetime_revenue_usd`
- `sessions_7d`
- `sessions_30d`
- `days_since_last_seen`
- `last_campaign`
- `last_media_source`

## Pipeline Responsibilities by Component

### `IngestionService`

Current file:
[backend/services/ingestion_service.py](/Users/jeremyz/Projects/KairyxAI/backend/services/ingestion_service.py)

New responsibility split:

- `mock` mode:
  - keep current developer flow
  - local file storage is acceptable
  - local simulated queue is acceptable

- `gcp` mode:
  - fetch connector data page by page
  - write each page to GCS as one shard
  - emit one Pub/Sub message per shard
  - persist job progress checkpoints

Required interface direction:

```python
class IngestionService:
    def fetch_and_stage_events(self, start_date: str, end_date: str) -> dict:
        ...
```

Suggested return payload:
- `job_id`
- `source`
- `shards_created`
- `events_staged`
- `last_checkpoint`

### `DataProcessingService`

Current file:
[backend/services/data_processing_service.py](/Users/jeremyz/Projects/KairyxAI/backend/services/data_processing_service.py)

New responsibility split:

- `mock` mode:
  - preserve local processing for demo and tests
  - process one local shard at a time
  - keep file-backed rejected/conflict logs

- `gcp` mode:
  - do not process all events in one FastAPI request
  - move normalization execution into Dataflow
  - keep this module as contract/shared transform code or local fallback

The current anti-patterns that should not exist in production mode:
- accumulating `all_normalized` in memory
- in-process dedupe maps over full jobs
- writing all job output from a single worker pass

### `BigQueryService`

Current file:
[backend/services/bigquery_service.py](/Users/jeremyz/Projects/KairyxAI/backend/services/bigquery_service.py)

This service should evolve into a warehouse facade with explicit table-level methods.

Recommended production-facing API:

```python
class BigQueryService:
    def write_events_staging(self, rows: list[dict]) -> None:
        ...

    def write_pipeline_dead_letters(self, rows: list[dict]) -> None:
        ...

    def run_events_curation(self, job_id: str | None = None, event_date: str | None = None) -> dict:
        ...

    def refresh_player_latest_state(self, event_date: str | None = None) -> dict:
        ...

    def get_player_latest_state(self, player_id: str) -> dict | None:
        ...

    def get_player_events_curated(self, player_id: str, limit: int = 1000) -> list[dict]:
        ...
```

In `mock` mode this service can still use local parquet or sqlite-backed tables, but the public methods should mirror production concepts.

### `connectors/normalizer.py`

Current file:
[backend/services/connectors/normalizer.py](/Users/jeremyz/Projects/KairyxAI/backend/services/connectors/normalizer.py)

This module should remain lightweight and deterministic:
- field extraction
- timestamp coercion
- schema version assignment
- event fingerprint generation
- minimal required field validation

It should not own:
- large-scale dedupe state
- warehouse reconciliation
- cross-job history scans

## Dedupe Strategy

Production dedupe order:

1. use `source_event_id` when present
2. otherwise use deterministic `event_fingerprint`

Fingerprint inputs should be stable and canonicalized:
- `source`
- `canonical_user_id`
- `event_type`
- normalized `event_time`
- stable business keys such as transaction id or campaign identifiers where available

Do not dedupe across the entire history in application memory.

Use `events_staging -> events_curated` SQL transforms instead.

## Identity Strategy

Short term:
- keep deterministic source-to-canonical mappings
- persist mappings in a durable store

Recommended storage:
- BigQuery for analytics visibility and batch joins
- Cloud SQL if transactional updates become necessary

The current local identity store should remain available in demo mode, but production workers should not depend on local sqlite state.

## Serving Strategy for Churn and Decisions

Current profile building in [backend/services/player_modeling_engine.py](/Users/jeremyz/Projects/KairyxAI/backend/services/player_modeling_engine.py) reads event history directly.

Production target:
- read from `player_latest_state` and `player_churn_features`
- only query curated event history when detailed drill-down is needed

This change is necessary for low-latency actioning once event volume grows.

## Observability

Track these metrics in production:
- events ingested per connector
- shard creation latency
- Pub/Sub backlog age
- Dataflow processing latency
- normalization failure rate
- dead-letter volume
- duplicate rate
- BigQuery staging-to-curated lag
- aggregate refresh lag

Operational sinks:
- BigQuery audit tables
- Cloud Logging
- Cloud Monitoring alerts

Keep the current local JSONL files in mock mode because they are useful for debugging.

## Concrete Implementation Plan

### Phase 1: Interface refactor without behavior break

Goal:
- keep local demo working exactly as today
- introduce production-shaped interfaces

Changes:
- add explicit shard manifest model
- add `fetch_and_stage_events()` alongside existing fetch flow
- add `write_events_staging()` and `get_player_latest_state()` to `BigQueryService`
- add `event_fingerprint` to normalized events

### Phase 2: Local shard processing

Goal:
- make local mode resemble production boundaries

Changes:
- write local raw shards as JSONL files under `.cache/raw/...`
- process one shard at a time in local mode
- remove job-wide in-memory accumulation in local pipeline

This phase improves production readiness without requiring GCP infrastructure.

### Phase 3: GCP ingestion path

Goal:
- production connector fetch writes to GCS + Pub/Sub

Changes:
- add GCS raw shard writer
- add Pub/Sub publisher abstraction
- add job checkpoint persistence

### Phase 4: Dataflow normalization path

Goal:
- move normalization out of FastAPI

Changes:
- create Dataflow pipeline entrypoint
- consume shard manifests
- normalize and write to `events_staging`
- route invalid rows to dead-letter table

### Phase 5: Curated and aggregate warehouse layer

Goal:
- serve product APIs from curated data and aggregates

Changes:
- create `events_curated`
- create `player_latest_state`
- update player modeling and churn services to read aggregates first

## Repository Additions Recommended

Suggested new modules:

- `backend/services/pipeline_models.py`
  - shard manifest and canonical event models
- `backend/services/pubsub_service.py`
  - mock and GCP publisher abstraction
- `backend/services/raw_event_store.py`
  - local filesystem and GCS shard storage abstraction
- `backend/services/bigquery_queries.py`
  - curated SQL templates
- `backend/services/dataflow/`
  - production normalization pipeline entrypoint

These additions let local and production modes share contracts while using different runtime backends.

## Non-Goals

These should not be part of the first scaling refactor:
- full real-time identity graph resolution
- online feature store
- exactly-once semantics across all external connectors
- fully statistical experimentation engine

The first target is robust, replayable, high-throughput ingestion and stable warehouse-backed serving.
