# KairyxAI

KairyxAI is an AI-driven operator platform for game growth, retention, and lifecycle execution. It is designed to sit between messy product data and downstream engagement tools so teams can move from raw events to audiences, actions, experiments, and reports in one system.

The current repository already implements a working v1 control plane for:

- `Data Core`: connectors, mappings, imports, predictions, SQL workspace, quality, identity, and governance
- `Audience Engine`: cohort lifecycle, refresh, versions, metrics, compare, and activation controls
- `Action Orchestrator`: workflows, triggers, delivery diagnostics, policy guards, and activation callbacks
- `Experiment Hub`: config, assignment, exposure, outcome, integrity, summary, and rollout suggestion
- `Insight Copilot`: query, explain, recommend, report, anomaly, and evidence-oriented reporting

## Product Vision

Modern game teams already have analytics tools, messaging tools, attribution tools, and warehouses. What they usually do not have is a single execution layer that can:

- normalize inconsistent event data
- identify high-value or at-risk player groups quickly
- decide what action to take, or when to do nothing
- execute and measure that action in a controlled way
- explain results back to PM, growth, live ops, and data teams

KairyxAI is the decision and operations layer for that loop.

The v1 product direction is centered on one closed-loop outcome:

`unified data -> high-value audience -> workflow execution -> experiment measurement -> copilot insight -> weekly review`

The first end-to-end scenario is `Churn Rescue`, with `Monetization Lift` and `Onboarding Activation` following as reusable templates.

## Current Repository Status

This repository is no longer just an early prototype. It now contains:

- a FastAPI-based operator API under `/api/v1`
- a static operator console served by the backend
- SQLAlchemy + Alembic control-plane persistence
- mock-first local development mode
- a growing set of production-shaped contracts for imports, workflows, experiments, copilot, audit, and health

What is still in progress is not the existence of the core APIs, but the remaining hardening work around:

- production readiness
- stronger provider-backed activation and measurement
- deeper frontend productization
- stronger auth, tenant isolation, and secret handling
- more automated optimization under manual confirmation

The source of truth for that roadmap lives in:

- [KairyxAI v1 Master PRD](docs/KAIRYXAI_V1_MASTER_PRD.md)
- [Data Core v1 PRD](docs/DATA_CORE_V1_PRD.md)
- [Insight Copilot v1 PRD](docs/COPILOT_V1_PRD.md)
- [Audience Engine v1 PRD](docs/AUDIENCE_ENGINE_V1_PRD.md)
- [Action Orchestrator v1 PRD](docs/ACTION_ORCHESTRATOR_V1_PRD.md)
- [Experiment Hub v1 PRD](docs/EXPERIMENT_HUB_V1_PRD.md)

## Core Modules

### 1. Data Core

Data Core owns the upstream control plane:

- connector configuration and health
- field mapping and mapping governance
- import jobs, replay, backfill, and quality gates
- identity stitching and source-of-truth decisions
- SQL workspace and read-only warehouse access
- prediction jobs and prediction result availability

Primary backend surface:

- `/api/v1/connectors`
- `/api/v1/mappings`
- `/api/v1/imports`
- `/api/v1/predictions`
- `/api/v1/sql-workspace`

### 2. Audience Engine

Audience Engine turns unified data into reusable targeting assets:

- rule, SQL, and list-based cohorts
- versioning and refresh history
- snapshot and delta tracking
- activation preflight
- metrics and version comparison

Primary backend surface:

- `/api/v1/cohorts`

### 3. Action Orchestrator

Action Orchestrator controls how audiences become actions:

- workflow drafts and published versions
- daily, event, threshold, and manual triggers
- delivery diagnostics and callback ingestion
- policy counters, cooldowns, budgets, and guardrails
- exports and downstream execution controls

Primary backend surface:

- `/api/v1/workflows`
- `/api/v1/orchestrator`
- `/api/v1/activation`
- `/api/v1/exports`

### 4. Experiment Hub

Experiment Hub measures whether actions worked:

- experiment configs and versions
- deterministic assignment
- exposure and outcome logging
- integrity checks
- summary and decision outputs
- rollout suggestions

Primary backend surface:

- `/api/v1/experiments`

### 5. Insight Copilot

Insight Copilot is the operator-facing analysis layer:

- natural-language metric query
- anomaly explanation
- action recommendation drafts
- daily and weekly reports
- evidence-backed structured output

Primary backend surface:

- `/api/v1/copilot`

## Architecture Overview

### Current implementation

- Backend: FastAPI
- Control-plane persistence: SQLAlchemy + Alembic
- Local default database: SQLite
- Local runtime mode: `DATA_BACKEND_MODE=mock`
- Frontend: static HTML/CSS/JS operator console served by the backend
- Local smoke coverage: Playwright-driven operator console smoke script

### Runtime modes

KairyxAI currently supports two practical shapes:

1. `Local demo mode`
   - mock-backed
   - fastest way to explore the operator flows
   - best for development, UI checks, and API iteration

2. `Production-shaped mode`
   - intended path toward GCS + Pub/Sub + Dataflow + BigQuery
   - warehouse-backed and batch/stream-friendly contracts
   - still evolving according to the Data Core PRD

## Repository Layout

```text
KairyxAI/
├── backend/services/
│   ├── app/
│   │   ├── api/            # FastAPI routers and schemas
│   │   ├── application/    # Service-layer module logic
│   │   ├── core/           # settings, db, governance, errors, runtime
│   │   └── infrastructure/ # SQLAlchemy repositories and db models
│   ├── tests/              # backend test coverage
│   ├── main_service.py     # backend entrypoint shim for local demo
│   └── requirements.txt
├── frontend/
│   ├── index.html
│   └── assets/
│       ├── operator-console.css
│       ├── operator-console.js
│       └── favicon.svg
├── docs/                   # master PRD + module PRDs + development memory
├── scripts/
│   └── operator_console_smoke.sh
└── run_local_demo.sh
```

## Quick Start

### Prerequisites

- Python `3.14`
- `bash`
- optional: Playwright CLI support if you want to run the operator-console smoke flow

### Recommended local start

```bash
./run_local_demo.sh
```

This starts:

- backend API at `http://localhost:8000`
- frontend operator console at `http://localhost:8000`

Default local behavior:

- `DATA_BACKEND_MODE=mock`
- control-plane DB stored under `backend/services/.kairyx_control_plane.db`
- local runtime DB stored under `backend/services/.kairyx_local.db`

### Manual start

```bash
python3.14 -m venv .venv
source .venv/bin/activate
cd backend/services
pip install -r requirements.txt
export DATA_BACKEND_MODE=mock
uvicorn main_service:app --host 0.0.0.0 --port 8000 --reload --reload-dir ../../frontend
```

Then open:

- [http://localhost:8000](http://localhost:8000)

## Key Environment Variables

| Variable | Purpose | Default |
| --- | --- | --- |
| `DATA_BACKEND_MODE` | Data runtime mode | `mock` |
| `CONTROL_PLANE_DATABASE_URL` | Control-plane database URL | local SQLite path |
| `KAIRYX_LOCAL_DB_PATH` | Local runtime/checkpoint DB | local SQLite path |
| `API_ACCESS_KEY` | Optional API key for `/api/v1` | empty |
| `SCHEDULER_ENABLED` | Enables background control loop | `true` |
| `SQLITE_BUSY_TIMEOUT_SECONDS` | SQLite busy timeout | `15` |
| `IMPORT_NETWORK_TIMEOUT_SECONDS` | Import network timeout | `60` |
| `PREDICTION_NETWORK_TIMEOUT_SECONDS` | Prediction network timeout | `20` |

For a more production-shaped warehouse path, also expect GCP-related configuration for BigQuery, GCS, and ADC credentials.

## How to Use the Product Locally

### Operator console path

1. Start the demo
2. Open the left navigation in the operator console
3. Work through the modules in sequence:
   - `Data Core`
   - `Audience Engine`
   - `Action Orchestrator`
   - `Experiment Hub`
   - `Insight Copilot`
   - `Help`

### Typical local flow

1. Create or inspect a connector
2. Run an import
3. Run predictions
4. Create or refresh a cohort
5. Publish or test a workflow
6. Review experiment summary or integrity
7. Ask Copilot for explanation or a report

## API Surface Snapshot

The current repo exposes a resource-oriented v1 control plane under `/api/v1`.

Common resources include:

- `/api/v1/health`
- `/api/v1/connectors`
- `/api/v1/mappings`
- `/api/v1/imports`
- `/api/v1/predictions`
- `/api/v1/sql-workspace`
- `/api/v1/cohorts`
- `/api/v1/workflows`
- `/api/v1/orchestrator`
- `/api/v1/activation`
- `/api/v1/exports`
- `/api/v1/experiments`
- `/api/v1/copilot`
- `/api/v1/audit`
- `/api/v1/templates`

There is also a lightweight liveness endpoint at:

- `/api/v1/health/live`
- `/health/live`

## Validation and Local Development

### Backend tests

```bash
.venv/bin/pytest backend/services/tests/test_v1_api.py backend/services/tests/test_v1_closed_loop.py -q
```

### Frontend/operator smoke

```bash
BASE_URL=http://127.0.0.1:8000 ./scripts/operator_console_smoke.sh
```

### Lightweight frontend checks

```bash
node --check frontend/assets/operator-console.js
git diff --check
```

## Current Strengths

The repository is already strong in these areas:

- local mock closed-loop development
- resource-oriented operator APIs
- audit and health surfaces
- job lifecycle management for imports, predictions, and exports
- cohort, workflow, experiment, and copilot control-plane coverage
- frontend served directly by the backend with no build pipeline required

## Current Limitations

The remaining v1 work is mostly hardening and productionization:

- production-grade auth, tenant isolation, and secret management are not complete
- provider-backed activation and outcome measurement still need deeper stabilization
- the operator console is improving, but is not yet a fully productized multi-surface app
- automated optimization remains intentionally limited and should remain human-confirmed by default

## Roadmap Direction

Near-term v1 backlog themes:

- operator console hardening
- production readiness baseline
- real activation and measurement stabilization
- tighter evidence loops across Copilot, Audience, Action, and Experiment

Next-step themes after that:

- controlled closed-loop optimization
- more productized module consoles
- stronger deployment, monitoring, and runbook maturity

The detailed backlog and ownership live in the PRDs under `docs/`.

## Who This Repository Is For

- game product managers and growth operators
- live ops and CRM teams
- data engineers and backend engineers
- AI engineers building decision systems on top of product data

## Disclaimer

KairyxAI is still an actively evolving v1 platform. The repository is usable, but the architecture, interfaces, and operational boundaries are still being hardened as the product moves from mock-first local development toward production-grade execution.
