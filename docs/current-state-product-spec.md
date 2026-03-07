# KairyxAI Current-State Product Spec

## Purpose

This document describes the product that currently exists in the repository as of March 2026, not the idealized end-state. It is intended to align future implementation work around the actual shipped flows, current architecture, and the most important gaps.

## Product Summary

KairyxAI is an operator console and backend pipeline for game growth teams. Its current focus is:

- configuring data and activation connectors
- importing event data into a local or GCP-shaped processing pipeline
- normalizing and deduplicating player events
- building player-level state for churn analysis
- generating churn predictions and recommended win-back actions
- exporting audiences to external campaign systems
- recording action history and basic experiment outcomes

The project is best described today as a local-first growth operations workbench with production-shaped backend interfaces.

## Source Of Truth In This Repo

The repository currently contains two frontend implementations:

- `frontend/index.html`: the active operator console served by FastAPI and exercised by Playwright
- `frontend/App.tsx` and related React files: a parallel UI implementation that is not wired into the served app

For product and roadmap purposes, the current source of truth is the FastAPI backend plus the static frontend in `frontend/index.html`.

## Problem Statement

Mobile game teams operate across fragmented tools:

- attribution and install data in AppsFlyer or Adjust
- product analytics in Amplitude
- messaging and campaign execution in SendGrid or Braze
- manual exports and rule-building across teams

KairyxAI aims to reduce that operational gap by providing one operator-facing surface that can ingest data, normalize it, score churn, suggest actions, and push target audiences to campaign providers.

## Target Users

- product managers running retention and win-back programs
- growth and CRM operators managing campaigns
- data or analytics engineers validating event quality and identity mapping
- founders or early-stage operators evaluating autonomous growth workflows

## Current Product Goals

1. Make connector setup and dataset import operable from one UI.
2. Keep the local demo path simple enough to run without cloud infrastructure.
3. Preserve a production-shaped ingestion model: raw shards, manifests, staged events, curated events, aggregate player state.
4. Turn imported datasets into actionable churn predictions and exportable campaign audiences.
5. Keep operator actions auditable.

## Non-Goals For The Current Version

- multi-tenant production deployment
- authentication or role-based access control
- strong secret management
- real-time streaming decisioning
- fully automated closed-loop optimization from real downstream outcomes
- a single finalized frontend architecture

## Current User Flows

### 1. Configure connectors

The operator can configure:

- data sources: Amplitude, Adjust, AppsFlyer
- AI/config services: Google Gemini, BigQuery, Cloud Churn
- activation providers: SendGrid, Braze

Connector configs are stored locally and listed in the UI. Health and freshness views exist for backend status, although the UX is still operational rather than polished.

### 2. Import and process a dataset

From Player Cohorts, the operator selects a configured source and date range and starts an import job.

Current behavior:

- FastAPI creates an import job and runs processing in the background
- ingestion fetches events from the connector with retry/backoff
- raw events are written as JSONL shards into local mock storage or GCS
- shard notifications are published to a mock Pub/Sub abstraction
- events are normalized, deduped, and written to staging
- curation and aggregate player state refresh run after processing

The import job is then available as a ready dataset for prediction.

### 3. Resolve mapping issues

The Data Sandbox supports manual field mapping for connector payload mismatches. Operators can:

- load a saved field mapping
- preview normalization on a sample record
- calculate mapping coverage
- resume paused imports after mapping is corrected

This makes schema mismatch handling part of the product instead of a purely engineering-side task.

### 4. Run churn prediction

From Operator Hub, the operator selects a processed dataset and starts churn prediction.

Current backend behavior:

- prediction can run in local, cloud, or parallel mode
- already churned players are handled by inactivity rules
- active players are scored by local heuristic, Gemini-backed analysis, cloud churn provider, or parallel preference logic
- player-level suggestions are generated for win-back action
- prediction runs can execute asynchronously and be stopped mid-run

### 5. Export campaign audiences

From Operator Hub, the operator can export audience rows filtered by churn risk and churn state to:

- a webhook
- Braze
- SendGrid

The exported payload is intentionally reduced to clean operational fields such as `user_id`, `email`, `predicted_churn_risk`, `suggested_action`, and metadata about the export.

### 6. Review history and experiments

The product records:

- audit-style action history
- experiment exposure logs
- experiment outcome logs

The experiment system currently supports deterministic holdout and A/B assignment for single-player analysis flows and can summarize uplift versus holdout.

## Functional Requirements

### Connector Management

The system must:

- save multiple connector configurations
- list configured connectors and available data sources
- allow connector deletion
- support health checks for supported data connectors
- track basic freshness and last-ingestion status

### Ingestion And Storage

The system must:

- fetch source events for a requested date range
- stage raw events as bounded JSONL shards
- persist checkpoint metadata per shard
- support local mock mode and GCP-shaped mode behind common interfaces
- keep raw storage replayable for later processing

### Normalization And Data Quality

The system must:

- normalize inconsistent player ID, timestamp, event name, and property structures
- attach schema version, event date, and event fingerprint
- flag critical quality failures such as missing player ID or invalid event time
- route invalid rows to rejected-event or dead-letter outputs
- support manual field mapping overrides for attribution connectors

### Identity And Curation

The system must:

- assign canonical user IDs
- deduplicate events by source event ID or fallback keys
- record cross-source attribution conflicts
- maintain staging, curated, and aggregate player state datasets

### Player Modeling And Prediction

The system must:

- build player profiles from aggregate player state when available
- fall back to raw-event scans when aggregate state is unavailable
- classify churn state using inactivity thresholds
- estimate churn risk in local, cloud, or parallel modes
- cache prediction results per dataset and mode

### Decisioning And Activation

The system must:

- decide whether to act or not act for churn reduction
- support a policy gate before LLM-backed action generation
- provide a deterministic non-LLM fallback path
- execute supported engagement actions through pluggable channel adapters

### Operator Auditability

The system must:

- log connector changes, imports, predictions, and exports
- expose action history in the UI
- retain enough metadata to debug ingestion and prediction jobs

## Current Architecture

### Backend

The backend is a single FastAPI service centered in `backend/services/main_service.py`.

It currently owns:

- API endpoints
- import job orchestration
- prediction job orchestration
- local cache loading and persistence
- connector configuration
- experiment configuration
- export logic

Supporting services provide narrower responsibilities:

- `ingestion_service.py`
- `data_processing_service.py`
- `dataflow/pipeline.py`
- `bigquery_service.py`
- `player_modeling_engine.py`
- `growth_decision_engine.py`
- `engagement_executor.py`

### Storage Model

Current storage is local-file and local-parquet heavy in mock mode:

- connector cache JSON
- local raw shard files
- parquet-backed staging, curated, latest-state, and dead-letter tables
- SQLite-backed job and identity data
- JSON prediction caches
- JSONL audit and experiment logs

This is acceptable for the current demo shape but not for a multi-user deployed product.

### Frontend

The served operator console is `frontend/index.html` with inline JavaScript. It includes pages for:

- Operator Hub
- Player Cohorts
- Action History
- Connectors
- Data Sandbox
- Safety Rails
- Service Health

The React frontend is currently a secondary implementation and should not be treated as the production UI contract without an explicit migration decision.

## Current Constraints And Risks

### 1. Frontend duplication

There are two competing UIs in the repository, and only one is wired into the shipped path. This makes feature work easy to duplicate and hard to finish.

### 2. Monolithic backend orchestration

`main_service.py` owns too much product logic. The system works for a demo environment, but adding more providers or workflows will continue to increase coupling and regression risk.

### 3. Secret handling is local and insecure

Connector credentials are accepted over API and written into local cache/env state. This is suitable only for local development.

### 4. Safety rails are only partially enforced

There is policy evaluation and configurable limits, but budgeting, frequency caps, compliance constraints, and decision caching are not yet tied to a durable operational state model.

### 5. Experiments are mostly simulated

Exposure and outcome tracking exist, but downstream engagement outcomes are not yet ingested from real providers in a robust way.

### 6. Production mode is only partially realized

The codebase has strong production-shaped abstractions for GCS, Pub/Sub, Dataflow, and BigQuery, but the default operational mode remains local mock, and some production semantics are still simulated.

### 7. Browser-side Gemini usage exists in the unused React app

The React frontend includes direct Gemini client usage from the browser. Even though that UI is not the served product today, it is a risky pattern and should not be carried forward.

## Recommended Next Phases

### Phase 1: Product Consolidation

Goal: remove ambiguity about what the product is.

Scope:

- choose one frontend architecture and retire the other
- define the operator console information architecture formally
- align Playwright coverage, backend contracts, and UI components to the same surface
- move any remaining frontend-direct AI calls behind the backend

Exit criteria:

- one frontend source of truth
- one set of user flows
- no duplicated feature development between static and React implementations

### Phase 2: Workflow Hardening

Goal: make the current operator workflows reliable enough for repeated use.

Scope:

- break `main_service.py` into domain routers or service modules
- formalize import-job, prediction-job, and export-job state machines
- persist action frequency and budget consumption in durable state
- make policy evaluation use real operational counters instead of placeholders
- tighten import pause/resume behavior around mapping and checkpoint recovery

Exit criteria:

- predictable job lifecycle behavior
- lower regression risk when adding providers or new job types

### Phase 3: Data Platform Completion

Goal: finish the production-shaped pipeline that already exists conceptually.

Scope:

- promote manifest-driven processing to the default architecture
- strengthen dead-letter and observability flows
- complete staging-to-curated contracts and refresh semantics
- add replay and backfill tooling around staged raw shards
- define warehouse schemas explicitly for serving and experimentation tables

Exit criteria:

- replayable ingestion pipeline
- stable data contracts for product features and analytics

### Phase 4: Activation And Measurement

Goal: turn predictions into measurable campaign operations.

Scope:

- make audience exports first-class jobs with status and retries
- ingest real downstream engagement/outcome events from Braze, SendGrid, or webhook callbacks
- connect experiment outcomes to actual return and conversion signals
- support campaign history, provider responses, and export diagnostics in the UI

Exit criteria:

- real closed-loop measurement for exported audiences
- experiment summaries based on actual outcomes, not only simulation

### Phase 5: Production Readiness

Goal: prepare the product for real customer or internal team usage.

Scope:

- authentication and authorization
- secret management
- multi-tenant storage boundaries
- deployment topology and environment separation
- monitoring, alerting, and operational runbooks

Exit criteria:

- safe deployment story
- clear tenant and operator boundaries
- acceptable operational controls for real usage

## Recommended Immediate Decisions

These are the decisions worth discussing before the next implementation cycle:

1. Should the project standardize on the static operator console and incrementally modernize it, or migrate fully to React and port the working flows first?
2. Is the next milestone product-facing reliability for the current operator workflows, or infrastructure-facing completion of the GCP data plane?
3. Should KairyxAI remain a churn-and-winback product first, or expand now into broader lifecycle decisioning and campaign orchestration?

## Suggested Default Sequencing

If the goal is fastest path to a coherent product, the recommended order is:

1. Phase 1 product consolidation
2. Phase 2 workflow hardening
3. Phase 4 activation and measurement
4. Phase 3 data platform completion
5. Phase 5 production readiness

Reasoning:

- the biggest current risk is not missing infrastructure, it is product ambiguity
- reliable operator workflows create a better base for real usage than deeper infra simulation
- measurement and downstream outcomes matter before scaling the data plane further

