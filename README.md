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

- a FastAPI-based operator API with a backward-compatible global prefix at `/api/v1` and an organization-scoped prefix at `/{organization_id}/v1`
- a React/Vite operator console with a Figma-inspired SaaS shell, served by the backend bundle
- SQLAlchemy + Alembic control-plane persistence
- mock-first local development mode
- a growing set of production-shaped contracts for imports, workflows, experiments, copilot, audit, and health

What is still in progress is not the existence of the core APIs, but the remaining hardening work around:

- production readiness
- stronger provider-backed activation and measurement
- deeper frontend productization
- broader org-space/project UX polish beyond the new Google-login onboarding and workspace-switching baseline
- more automated optimization under manual confirmation

The source of truth for that roadmap lives in:

- [KairyxAI v1 Master PRD](docs/KAIRYXAI_V1_MASTER_PRD.md)
- [GitHub Wiki product user guide source](docs/GITHUB_WIKI_PRODUCT_USER_GUIDE.md)
- [Data Core v1 PRD](docs/DATA_CORE_V1_PRD.md)
- [Insight Copilot v1 PRD](docs/COPILOT_V1_PRD.md)
- [Audience Engine v1 PRD](docs/AUDIENCE_ENGINE_V1_PRD.md)
- [Action Orchestrator v1 PRD](docs/ACTION_ORCHESTRATOR_V1_PRD.md)
- [Experiment Hub v1 PRD](docs/EXPERIMENT_HUB_V1_PRD.md)
- [Self-hosted Google login plan](docs/SELF_HOST_GOOGLE_LOGIN_PLAN.md)

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
- Frontend: React/Vite operator console bundle served by the backend, with source-shell fallback for unbuilt local environments
- Operator auth: Google login via OIDC, using the returned Google ID token as the bearer JWT for organization-scoped API paths like `/{organization_id}/v1/...` + `X-Kairyx-Project`, with self-serve organization-space onboarding and legacy header auth kept only as a hidden local/demo compatibility path
- Secrets: `*_ref` resolution via environment variables or Google Secret Manager
- Local smoke coverage: Playwright-driven operator console smoke script

### Runtime modes

KairyxAI currently supports two practical shapes:

1. `Local demo mode`
   - mock-backed
   - fastest way to explore the operator flows
   - best for development, UI checks, and API iteration

2. `Production-shaped SaaS mode`
   - shared multi-tenant control plane on Postgres
   - Cloud Run services for `operator-api`, `import-worker`, `prediction-worker`, `export-worker`, and `scheduler-worker`
   - org-space + project scoped GCS prefixes, BigQuery datasets, Pub/Sub attributes, and control-plane metadata
   - Google login in the frontend via OIDC PKCE, self-serve organization-space onboarding, workspace switching, invite-link redemption support, browser URLs that become `https://<base-url>/<organization_id>` after organization resolution, and bearer-token operator traffic on `/{organization_id}/v1/...` with `/api/v1` kept for bootstrap flows such as login config, onboarding, and invite redemption

## Workspace Model

The current operator console uses a two-level workspace hierarchy:

- `Organization Space`
  - the top-level shared boundary for memberships, governance, and billing-style ownership
- `Project`
  - the isolation boundary for connectors, imports, cohorts, workflows, experiments, exports, and audit history

In the backend, `tenant` remains the internal organization-space identifier for compatibility. The user-facing console now exposes:

- a Figma-derived SaaS shell with a responsive sidebar, inline expanding and collapsible section lists, a bottom-left session profile chip with logout, a search-first top bar with a three-mode theme selector, a tighter icon rail when collapsed, collapsed-icon clicks that land on each module's first section and dismiss the temporary popout, hover-safe collapsed popouts that stay reachable while moving into the submenu, and a tabbed Settings page
- a centered full-screen Google login gate that appears before onboarding or workspace entry
- a base URL (`https://<base-url>/`) that is gateway-only in deployed Google-auth environments, with the main operator app shown only after the browser is on `https://<base-url>/<organization_id>`
- a centered full-screen first-login onboarding gate that opens immediately after Google sign-in when the user has no memberships, asks for the organization URL first and the first project name second, and preserves any organization URL the user already typed before sign-in; new organization URLs are limited to lowercase letters and numbers only, a maximum length of 16 characters, and a global uniqueness requirement across the product
- after creating the first organization and project in the gateway, the user is placed into that new organization and project by default
- a centered full-screen workspace gate that starts with an organization URL lookup, then lets users choose an existing project or add a new one inside that organization
- a browser URL that rewrites to `https://<base-url>/<organization_id>` as soon as onboarding or workspace selection resolves an active organization
- a visible startup-status line in the full-screen workspace gate so the user can still see when application startup has completed before entering the app
- module loaders that now wait for a valid organization/project workspace before loading protected data, so deployed Google-login environments do not replace page content with transient raw membership errors during session handoff or stale workspace recovery
- mock-mode imports now kick off in the background and rely on status polling instead of holding the browser request open until the full import run finishes
- invite-link redemption support for project access after login
- a tabbed Settings page with `Profile`, `Organization`, `Projects`, `Teams`, `Notifications`, and `Billing` sections, mixing placeholder management layouts with the live workspace and session controls while leaving appearance control in the top-right header
- a hidden local/demo fallback that still uses default legacy headers internally when Google login is not configured

For authenticated organization-aware traffic, the preferred API shape is:

- `https://<base-url>/<organization_id>/v1/<resource>`

Examples:

- `/northstar/v1/connectors`
- `/northstar/v1/imports`
- `/northstar/v1/predictions`

For the browser shell itself, the canonical workspace URL is:

- `https://<base-url>/<organization_id>`

In deployed Google-login environments, the bare base URL remains the gateway page only:

- `https://<base-url>/`

The older `/api/v1/...` routes remain available for bootstrap flows such as Google OIDC config, first-time onboarding before an organization exists, local/demo compatibility, and backward compatibility.

For normal logged-in operator traffic:

- identity comes from the Google account
- the active organization comes from the URL path
- the active project is sent as `X-Kairyx-Project`
- Google OAuth still returns to the base app URL, and the console rewrites the browser to `/{organization_id}` after session and workspace resolution
- manual actor-id and tenant-id entry are no longer part of the visible operator UI

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
│   ├── cloudrun/           # Cloud Run service manifests
│   ├── main_service.py     # backend entrypoint shim for local demo
│   └── requirements.txt
├── frontend/
│   ├── app/                # React/Vite app entry and source files
│   ├── dist/               # built frontend bundle served by the backend
│   ├── index.html          # source console template used by the React shell + fallback
│   ├── package.json
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
- `npm`
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
- `LEGACY_HEADER_AUTH_ENABLED=true` so the local console can operate without a live IdP

### Manual start

```bash
python3.14 -m venv .venv
source .venv/bin/activate
cd frontend
npm install
npm run build
cd ..
cd backend/services
pip install -r requirements.txt
export DATA_BACKEND_MODE=mock
uvicorn main_service:app --host 0.0.0.0 --port 8000 --reload --reload-dir ../../frontend --reload-dir ../../frontend/dist
```

Then open:

- [http://localhost:8000](http://localhost:8000)

Gemini is optional. When no trained local churn model is active, `prediction_mode=local` still runs using the built-in `heuristic_v1` fallback.

## Local Model Readiness

The `Local Model` path stays available even when no promoted supervised churn model exists yet.

- `heuristic_v1` is the always-available local baseline
- a supervised local churn model retrains in batch from holdout / untreated rows and observed outcomes
- the learned model is promoted only when it beats the heuristic baseline on validation
- until then, `prediction_mode=local` continues to run using `heuristic_v1`

Readiness is exposed through:

- `GET /api/v1/predictions/models/runs`
  - includes top-level `readiness`
  - `state`: `untrained | learning | fallback | ready`
  - `using_model_version`
  - `reason`
  - `last_trained_at`
  - `baseline_rows`
  - `min_rows_required`
  - `class_balance`
  - `validation_accuracy`
  - `heuristic_accuracy`
- `GET /api/v1/predictions/models/latest`
  - returns the latest trained local model version when one exists
- `POST /api/v1/predictions/models/train`
  - triggers a local batch retrain
  - updates `training_status` as the run progresses and completes

Prediction job metadata also records:

- `effective_local_model_version`
- `effective_local_model_state`

The operator UI uses that contract to show a `Ready`, `Learning`, or `Fallback` badge beside the prediction engine selector, warns when `Local Model` is currently using `heuristic_v1`, and includes:

- `Train Local Model`
  - manually starts a local retrain from the operator workbench
- `Refresh Model Status`
  - refreshes the latest readiness and training status without starting a new run
- inline training status
  - shows the latest training state, labeled-row count, class balance, and last update time

## Prediction Audience Selection

The churn workbench now supports two prediction audience modes:

- `Source` is the default operator path
  - choose a source such as `Amplitude 1`
  - the prediction job resolves to the latest completed import for that source when the run starts
- `Import` remains available for audit, debugging, and replay against a specific completed import

In both modes:

- the selected audience defines the roster that gets scored
- churn features come from merged tenant history across completed imports
- the prediction job records the resolved `import_job_id` that was actually used
- completed cached jobs can be viewed, but stale jobs require explicit rerun confirmation in the UI

## Import Diagnostics Load Behavior

The Imports page now keeps restart-time load lighter:

- import operations, quality, and manifest diagnostics load on demand instead of auto-fetching on first page render
- schema contracts are also loaded on demand
- import polling continues only while at least one import job is `queued`, `running`, or `stopping`
- when the control plane is temporarily busy right after restart, import detail reads retry once and then surface a retryable busy message instead of silently failing

## Key Environment Variables

| Variable | Purpose | Default |
| --- | --- | --- |
| `DATA_BACKEND_MODE` | Data runtime mode | `mock` |
| `CONTROL_PLANE_DATABASE_URL` | Control-plane database URL | local SQLite path |
| `KAIRYX_LOCAL_DB_PATH` | Local runtime/checkpoint DB | local SQLite path |
| `APP_ENV` | Runtime environment (`local`, `prod`) | `local` |
| `API_ACCESS_KEY` | Optional API key for legacy local header auth | empty |
| `LEGACY_HEADER_AUTH_ENABLED` | Enables `x-actor-*` local/demo auth headers | `true` |
| `OIDC_ISSUER` | Google issuer for operator traffic | empty |
| `OIDC_AUDIENCE` | Google OAuth client id expected in JWT audience | empty |
| `OIDC_JWKS_URL` | Google JWKS endpoint for bearer-token validation | empty |
| `OIDC_CLIENT_ID` | Google OAuth client id used by the console | empty |
| `OIDC_AUTHORIZE_URL` | Google authorize URL | empty |
| `OIDC_TOKEN_URL` | Google token URL for PKCE code exchange | empty |
| `OIDC_LOGOUT_URL` | Optional future IdP logout redirect URL; the current operator console logout returns to the organization URL gate without using this redirect | empty |
| `OIDC_JWT_SIGNING_SECRET` | HS256 signing secret for local/test bearer-token validation | empty |
| `CORS_ALLOWED_ORIGINS` | Explicit browser origins allowed in production | `*` |
| `BOOTSTRAP_TENANT_ID` | Default bootstrap tenant id | `default` |
| `SERVICE_ROLE` | Runtime role (`operator-api`, `scheduler-worker`, etc.) | `operator-api` |
| `SCHEDULER_ENABLED` | Enables background control loop | `true` |
| `SQLITE_BUSY_TIMEOUT_SECONDS` | SQLite busy timeout | `15` |
| `IMPORT_NETWORK_TIMEOUT_SECONDS` | Import network timeout | `60` |
| `PREDICTION_NETWORK_TIMEOUT_SECONDS` | Prediction network timeout | `20` |
| `MAX_SQL_PREVIEW_ROWS_PER_TENANT` | Tenant limit for SQL preview rows | `1000` |
| `MAX_IMPORT_JOBS_PER_TENANT` | Tenant limit for active imports | `10` |
| `MAX_EXPORT_JOBS_PER_TENANT` | Tenant limit for active exports | `20` |
| `MAX_COPILOT_REPORTS_PER_TENANT` | Tenant limit for stored reports | `50` |

## Production Deployment

The production entrypoint is `app.main:app`. `backend/services/main_service.py` and `backend/services/app.yaml` remain only for local/demo compatibility and are not the production deployment path.

Production deployment assets in this repository now include:

- [backend/services/Dockerfile](backend/services/Dockerfile)
- [backend/services/.env.example](backend/services/.env.example)
- [backend/services/cloudrun/operator-api.yaml](backend/services/cloudrun/operator-api.yaml)
- [backend/services/cloudrun/import-worker.yaml](backend/services/cloudrun/import-worker.yaml)
- [backend/services/cloudrun/prediction-worker.yaml](backend/services/cloudrun/prediction-worker.yaml)
- [backend/services/cloudrun/export-worker.yaml](backend/services/cloudrun/export-worker.yaml)
- [backend/services/cloudrun/scheduler-worker.yaml](backend/services/cloudrun/scheduler-worker.yaml)
- [docs/RUNBOOKS_MULTITENANT_GCP.md](docs/RUNBOOKS_MULTITENANT_GCP.md)
- [docs/SELF_HOST_GOOGLE_LOGIN_PLAN.md](docs/SELF_HOST_GOOGLE_LOGIN_PLAN.md)

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
.venv/bin/pytest backend/services/tests/test_multitenant_auth.py backend/services/tests/test_v1_api.py backend/services/tests/test_v1_closed_loop.py -q
```

### Frontend/operator smoke

```bash
PWCLI=/path/to/playwright_cli.sh BASE_URL=http://127.0.0.1:8000 ./scripts/operator_console_smoke.sh
```

### Lightweight frontend checks

```bash
npm --prefix frontend run build
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
- backend-served React shell with a Figma-based SaaS layout, inline section expansion, an icon-only collapsed rail with right-side section popouts that auto-engages below `1200px` and closes after an icon routes to the module's first section, a bottom-left session profile chip with logout, a search-first top bar with `System`, `Light`, and `Dark` theme buttons, and a tabbed Settings surface for workspace, session, and placeholder account-management layouts

## Current Limitations

The remaining v1 work is mostly hardening and productionization:

- production-grade auth, tenant isolation, and secret management are not complete
- provider-backed activation and outcome measurement still need deeper stabilization
- the React shell is in place, but much of the detailed operator behavior is still migrating from legacy DOM-driven runtime code into React-native components
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
