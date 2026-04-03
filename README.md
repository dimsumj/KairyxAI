# KairyxAI

KairyxAI is an AI-driven operator platform for game growth, retention, and lifecycle execution. It is designed to sit between messy product data and downstream engagement tools so teams can move from raw events to audiences, actions, experiments, and reports in one system.

The current repository already implements a working v1 control plane for:

- `Data Core`: connectors, mappings, imports, predictions, SQL workspace, quality, identity, and governance
- `Audience Engine`: cohort lifecycle, refresh, versions, metrics, compare, and activation controls
- `Action Orchestrator`: workflows, triggers, delivery diagnostics, policy guards, and activation callbacks
- `Experiment Hub`: config, assignment, exposure, outcome, integrity, summary, and rollout suggestion
- `Insight Copilot`: global AI assistant for grounded help, samples, summaries, and safe setup work, plus manual query, explain, recommend, report, anomaly, and evidence-oriented reporting

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

- global AI assistant bubble for grounded product help, contextual samples, dashboard summary, cohort setup, experiment setup, and connection setup from any page
- a single-column chat drawer with a disabled `Getting Agents Ready...` first-message state, immediate user-message rendering after send, an assistant thinking state, inline clarifications, artifact links, and confirmation gating for risky actions
- manual `query / explain / recommend / report` tools on the Insight Copilot page as the advanced fallback
- natural-language metric query
- anomaly explanation
- action recommendation drafts
- daily and weekly reports
- evidence-backed structured output

Primary backend surface:

- `/api/v1/copilot`
- `/api/v1/copilot/agent`

## Architecture Overview

### Current implementation

- Backend: FastAPI
- Control-plane persistence: SQLAlchemy + Alembic
- Local default database: SQLite
- Local runtime mode: `DATA_BACKEND_MODE=mock`
- Frontend: React/Vite operator console bundle served by the backend, with source-shell fallback for unbuilt local environments
- Operator auth: Google login via OIDC, with the base URL kept as a gateway-only surface, the active app shell mounted on `/{organization_id}`, organization-scoped API paths like `/{organization_id}/v1/...` + `X-Kairyx-Project`, and legacy header auth kept only as a hidden local/demo compatibility path
- Secrets: `*_ref` resolution via environment variables or Google Secret Manager
- Local smoke coverage: Playwright-driven operator console smoke script

### Runtime modes

KairyxAI currently supports three practical shapes:

1. `Local demo mode`
   - mock-backed
   - fastest way to explore the operator flows
   - best for development, UI checks, and API iteration
   - default mock persistence remains filesystem-backed via local parquet/cache files

2. `Vercel demo mode`
   - isolated write-capable demo surface
   - enabled only through the thin adapter entrypoint at `api/index.py` plus `vercel.json`
   - adapter sets `KAIRYX_PLATFORM_SURFACE=vercel_demo`
   - mock warehouse persistence switches to database-backed mode only on that adapter via `KAIRYX_MOCK_STORAGE_BACKEND=database`
   - control-plane DB fallback to runtime SQLite is fenced to `KAIRYX_PLATFORM_SURFACE=vercel_demo` + `DATA_BACKEND_MODE=mock`
   - root `/` remains a gateway page, while the main app stays on `/{organization_id}`
   - intended for public demo or preview hosting, not for the long-term production control plane

3. `Production-shaped SaaS mode`
   - shared multi-tenant control plane on Postgres
   - Cloud Run services for `operator-api`, `import-worker`, `prediction-worker`, `export-worker`, and `scheduler-worker`
   - organization + project scoped GCS prefixes, BigQuery datasets, Pub/Sub attributes, and control-plane metadata
   - Google login in the frontend via OIDC PKCE, self-serve organization onboarding, organization-level email invites, browser URLs that become `https://<base-url>/<organization_id>` after organization resolution, and bearer-token operator traffic on `/{organization_id}/v1/...` with `/api/v1` kept for bootstrap flows such as login config, onboarding, and invite redemption

## Workspace Model

The current operator console uses a two-level workspace hierarchy:

- `Organization`
  - the top-level shared boundary for memberships, governance, and billing-style ownership
- `Project`
  - the isolation boundary for connectors, imports, cohorts, workflows, experiments, exports, and audit history

The frozen v1 workspace and access contract is:

- Google login always comes before workspace entry.
- `https://<base-url>/` is the gateway only.
- `https://<base-url>/<organization_id>` is the app shell.
- After login:
  - users with `0` organizations go directly to create-organization
  - users with `1` organization and `1` active project enter that project directly
  - users with `1` organization and multiple active projects go directly to project selection for that org
  - users with `2+` organizations choose an organization first, then a project
- When a typed organization URL already exists:
  - members can continue into project selection for that organization
  - non-members see an explicit error that the organization exists but their Google account is not a member
  - duplicate creation attempts fail instead of silently reusing the existing org
- Organization roles are exactly `owner`, `admin`, and `member`.
- The organization creator becomes the only `owner`, and `owner` also has admin privileges.
- All organization members can access all active projects in that organization.
- Organization invitations are email-based and organization-level. Optional invite links are convenience wrappers around the same org invite record.
- The default project for an organization is the oldest active project by `created_at`.
- Project deletion is permanent, requires an admin or owner, and requires typing `delete` as confirmation.
- The team member list is shared across projects in the same organization, while project data remains isolated.
- Project isolation applies to connectors, imports, data layers, workflows, cohorts, predictions, AI agents, tools, experiments, exports, and project-scoped audit history.

In the backend, `tenant` remains the internal organization identifier for compatibility. The user-facing console now exposes:

- a Figma-derived SaaS shell with a responsive sidebar, inline expanding and collapsible section lists, a bottom-left session profile chip with logout, a search-first top bar with a three-mode theme selector, a tighter icon rail when collapsed, collapsed-icon clicks that land on each module's first section and dismiss the temporary popout, hover-safe collapsed popouts that stay reachable while moving into the submenu, and a tabbed Settings page
- a centered full-screen Google login gate that appears before onboarding or workspace entry
- a base URL (`https://<base-url>/`) that is gateway-only in deployed Google-auth environments, including after Google sign-in; the main operator app is shown only after the browser is on `https://<base-url>/<organization_id>`
- a centered full-screen first-login onboarding gate that opens immediately after Google sign-in when the user has no org memberships, asks for the organization URL first and the first project name second, preserves any organization URL the user already typed before sign-in, and fails creation if that organization URL already exists; new organization URLs are limited to lowercase letters and numbers only, a maximum length of 16 characters, and a global uniqueness requirement across the product
- after creating the first organization and project in the gateway, the user is placed into that new organization and project by default
- a centered full-screen workspace gate that starts with an organization URL lookup, shows the organizations already associated with the signed-in Google email, automatically enters the workspace when the selected org has only one active project, and otherwise lets users choose `Use Existing Project` or `Create New Project` inside that organization, or `Create First Project` when the organization has none yet
- an explicit gateway error when the typed organization already exists but the signed-in Google account does not belong to it
- a base-URL gateway flow where a signed-in user can open the `Switcher`, return to the root gateway, type a different organization URL, and create that new organization without being forced back into the previously active org; once the first project is created, the browser lands on the new `/{organization_id}` path by default
- a browser URL that rewrites to `https://<base-url>/<organization_id>` as soon as onboarding or workspace selection resolves an active organization
- a direct organization path (`/{organization_id}`) that is authoritative for the active workspace and does not inherit another org from stale browser storage
- a visible startup-status line in the full-screen workspace gate so the user can still see when application startup has completed before entering the app
- gateway action rows that keep `Continue` and other positive actions blue, while `Cancel` and `Close` use red styling and the `Close` button sits on the bottom action row instead of the modal header
- module loaders that now wait for a valid organization/project workspace before loading protected data, so deployed Google-login environments do not replace page content with transient raw membership errors during session handoff or stale workspace recovery
- mock-mode imports now kick off in the background and rely on status polling instead of holding the browser request open until the full import run finishes
- organization-level email invites that pre-authorize a Google account to join the organization as a `member`, plus optional shareable invite links that land the user in the invited org flow after login
- idempotent invite redemption, so a matching Google login can auto-activate a pending org invite by email and a later invite-link redeem request still succeeds for that same user
- a tabbed Settings page with `Profile`, `Organization`, `Projects`, `Teams`, `Notifications`, and `Billing` sections, where `Projects` and `Teams` are the live management surfaces for project creation/deletion and organization membership; the roster now shows `Joined YYYY-MM-DD` metadata beside each member role, row-level Save buttons for role changes, a shared invite-link generator section, member removal, and owner transfer through the same Save flow with confirmation while `Profile`, `Notifications`, and `Billing` remain lighter placeholder layouts
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

## Deployment Boundary

The repo now keeps the Vercel demo path isolated from local mock and GCP production:

- `vercel.json` and `api/index.py` are the only Vercel adapter files.
- The shared runtime does not rely on raw `VERCEL` host detection.
- Vercel-only behavior is fenced behind `KAIRYX_PLATFORM_SURFACE=vercel_demo`.
- Local mock keeps `KAIRYX_MOCK_STORAGE_BACKEND=local_files` by default.
- Cloud Run / GCP production should leave `KAIRYX_PLATFORM_SURFACE` unset and must not rely on runtime SQLite fallback.
- Health payloads now expose:
  - `control_plane_database_backend`
  - `control_plane_database_persistent`
  - `control_plane_database_fallback_active`
  - `mock_state_backend`
  - `mock_state_persistent`

Google-friendly env aliases are also supported for deployment templates:

- `GOOGLE_OIDC_CLIENT_ID`
- `GOOGLE_OIDC_HOSTED_DOMAIN`
- Google OAuth still returns to the base app URL, the gateway remains on `/` until the user finishes org/project resolution there, and the console rewrites the browser to `/{organization_id}` only after that selection or onboarding completes
- manual actor-id and tenant-id entry are no longer part of the visible operator UI

## Repository Layout

```text
KairyxAI/
|-- Dockerfile               # repo-root multi-stage image build
|-- backend/services/
|   |-- app/
|   |   |-- api/            # FastAPI routers and schemas
|   |   |-- application/    # Service-layer module logic
|   |   |-- core/           # settings, db, governance, errors, runtime
|   |   `-- infrastructure/ # SQLAlchemy repositories and db models
|   |-- tests/              # backend test coverage
|   |-- cloudrun/           # Cloud Run service manifests
|   |-- main_service.py     # backend entrypoint shim for local demo
|   `-- requirements.txt
|-- deploy/
|   |-- aws/ecs/            # ECS task and service templates
|   `-- docker/             # shared Compose environment defaults
|-- docker-compose.yml      # single-host baseline for API + workers + Postgres
|-- docker/                 # container entrypoint scripts
|-- frontend/
|   |-- app/                # React/Vite app entry and source files
|   |-- dist/               # built frontend bundle served by the backend
|   |-- index.html          # source console template used by the React shell + fallback
|   |-- package.json
|   `-- assets/
|       |-- operator-console.css
|       |-- operator-console.js
|       `-- favicon.svg
|-- docs/                   # master PRD + module PRDs + development memory
|-- scripts/
|   `-- operator_console_smoke.sh
`-- run_local_demo.sh
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
- deleting a `stopped`, `failed`, or `completed` import now also removes that job's temporary raw file objects, job-scoped staging rows, and derived sanitized/curated state tied to the deleted import

## Key Environment Variables

| Variable | Purpose | Default |
| --- | --- | --- |
| `DATA_BACKEND_MODE` | Data runtime mode | `mock` |
| `WAREHOUSE_BACKEND` | Warehouse backend (`mock`, `bigquery`, `redshift`) | derived from `DATA_BACKEND_MODE` |
| `OBJECT_STORAGE_BACKEND` | Object storage backend (`mock`, `gcs`, `s3`) | derived from `DATA_BACKEND_MODE` |
| `MESSAGE_BACKEND` | Async messaging backend (`mock`, `pubsub`, `eventbridge_sqs`) | derived from `DATA_BACKEND_MODE` |
| `SECRET_BACKEND` | Secret backend (`env`, `gcp_secret_manager`, `aws_secrets_manager`) | derived from `DATA_BACKEND_MODE` |
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
| `GOOGLE_OIDC_CLIENT_ID` | Optional shortcut for Google Sign-In; auto-fills Google issuer/JWKS/authorize/token defaults and drives the browser ID-token login flow | empty |
| `GOOGLE_OIDC_HOSTED_DOMAIN` | Optional Google Workspace hosted-domain restriction (`hd` claim) | empty |
| `CORS_ALLOWED_ORIGINS` | Explicit browser origins allowed in production | `*` |
| `BOOTSTRAP_TENANT_ID` | Default bootstrap tenant id | `default` |
| `SERVICE_ROLE` | Runtime role (`operator-api`, `scheduler-worker`, etc.) | `operator-api` |
| `WORKER_SHARED_TOKEN` | Shared bearer or query token for worker-only endpoints such as `/pubsub/push` and `/run` | empty |
| `AWS_REGION` | AWS region for ECS, S3, SQS, EventBridge, Secrets Manager, and Redshift | empty |
| `REDSHIFT_WORKGROUP_NAME` | Redshift Serverless workgroup name | empty |
| `REDSHIFT_DATABASE` | Redshift database name | empty |
| `REDSHIFT_SCHEMA` | Redshift schema used by the warehouse adapter | `public` |
| `REDSHIFT_SECRET_ARN` | Optional Secrets Manager secret ARN for Redshift Data API auth | empty |
| `S3_BUCKET_NAME` | S3 bucket for raw shards, manifests, exports, and migration landing files | empty |
| `EVENTBRIDGE_BUS_NAME` | EventBridge bus used for job dispatch | `default` |
| `SQS_IMPORT_QUEUE_URL` | Import worker queue URL | empty |
| `SQS_PREDICTION_QUEUE_URL` | Prediction worker queue URL | empty |
| `SQS_EXPORT_QUEUE_URL` | Export worker queue URL | empty |
| `SQS_SCHEDULER_QUEUE_URL` | Scheduler worker queue URL | empty |
| `SCHEDULER_ENABLED` | Enables background control loop | `true` |
| `PORT` | Container listen port for the role-aware entrypoint | `8080` |
| `WEB_CONCURRENCY` | Gunicorn worker count for `operator-api` containers | `4` |
| `GUNICORN_TIMEOUT` | Gunicorn request timeout in seconds for `operator-api` containers | `300` |
| `SQLITE_BUSY_TIMEOUT_SECONDS` | SQLite busy timeout | `15` |
| `IMPORT_NETWORK_TIMEOUT_SECONDS` | Import inactivity timeout during fetch/stage | `300` |
| `PREDICTION_NETWORK_TIMEOUT_SECONDS` | Prediction network timeout | `20` |
| `MAX_SQL_PREVIEW_ROWS_PER_TENANT` | Tenant limit for SQL preview rows | `1000` |
| `MAX_IMPORT_JOBS_PER_TENANT` | Tenant limit for active imports | `10` |
| `MAX_EXPORT_JOBS_PER_TENANT` | Tenant limit for active exports | `20` |
| `MAX_COPILOT_REPORTS_PER_TENANT` | Tenant limit for stored reports | `50` |

If you want Google auth specifically, the minimum backend setup is `GOOGLE_OIDC_CLIENT_ID=<your Google OAuth client id>` plus `LEGACY_HEADER_AUTH_ENABLED=false`. The app will automatically use Google OIDC defaults (`https://accounts.google.com`, Google JWKS, Google authorize URL, Google token URL) and use Google Identity Services in the browser so a Google client secret is not required for sign-in. Add `GOOGLE_OIDC_HOSTED_DOMAIN=<your workspace domain>` if you want to restrict sign-in to a Google Workspace domain.

## Production Deployment

The production entrypoint is `app.main:app`. `backend/services/main_service.py` and `backend/services/app.yaml` remain only for local/demo compatibility and are not the production deployment path.

Production deployment assets in this repository now include:

- [Dockerfile](Dockerfile)
- [docker-compose.yml](docker-compose.yml)
- [deploy/docker/compose.env](deploy/docker/compose.env)
- [backend/services/.env.example](backend/services/.env.example)
- [backend/services/cloudrun/operator-api.yaml](backend/services/cloudrun/operator-api.yaml)
- [backend/services/cloudrun/import-worker.yaml](backend/services/cloudrun/import-worker.yaml)
- [backend/services/cloudrun/prediction-worker.yaml](backend/services/cloudrun/prediction-worker.yaml)
- [backend/services/cloudrun/export-worker.yaml](backend/services/cloudrun/export-worker.yaml)
- [backend/services/cloudrun/scheduler-worker.yaml](backend/services/cloudrun/scheduler-worker.yaml)
- [deploy/aws/ecs/task-definitions/operator-api.json](deploy/aws/ecs/task-definitions/operator-api.json)
- [deploy/aws/ecs/service-definitions/operator-api.json](deploy/aws/ecs/service-definitions/operator-api.json)
- [deploy/aws/cloudwatch/alarms.json](deploy/aws/cloudwatch/alarms.json)
- [docs/GCP_PRODUCTION_DEPLOYMENT_RUNBOOK.md](docs/GCP_PRODUCTION_DEPLOYMENT_RUNBOOK.md)
- [docs/AWS_DATA_STACK_MIGRATION_RUNBOOK.md](docs/AWS_DATA_STACK_MIGRATION_RUNBOOK.md)
- [docs/PORTABLE_DOCKER_DEPLOYMENT_RUNBOOK.md](docs/PORTABLE_DOCKER_DEPLOYMENT_RUNBOOK.md)
- [docs/SELF_HOST_GOOGLE_LOGIN_PLAN.md](docs/SELF_HOST_GOOGLE_LOGIN_PLAN.md)

The same image digest is now intended to serve all five runtime roles through `SERVICE_ROLE`.

### Portable image roles

| `SERVICE_ROLE` | Runtime |
| --- | --- |
| `operator-api` | public API + frontend shell |
| `import-worker` | import worker with HTTP compatibility plus SQS polling in AWS mode |
| `prediction-worker` | prediction worker with HTTP compatibility plus SQS polling in AWS mode |
| `export-worker` | export worker with HTTP compatibility plus SQS polling in AWS mode |
| `scheduler-worker` | control-loop worker with HTTP compatibility plus scheduler-queue polling in AWS mode |

### Docker build

Build the portable image from the repository root:

```bash
docker build -t kairyxai:local .
```

### Docker Compose baseline

The repository includes a single-host baseline for `postgres` plus the five KairyxAI runtime roles:

```bash
docker compose up --build
```

Default local ports:

- `operator-api`: `http://127.0.0.1:8000`
- `import-worker`: `http://127.0.0.1:18081`
- `prediction-worker`: `http://127.0.0.1:18082`
- `export-worker`: `http://127.0.0.1:18083`
- `scheduler-worker`: `http://127.0.0.1:18084`

Worker endpoints require `WORKER_SHARED_TOKEN`. Use either:

- `Authorization: Bearer <WORKER_SHARED_TOKEN>`
- `?token=<WORKER_SHARED_TOKEN>`

For production-shaped deployments, use one of the native backend stacks:

- GCP: `bigquery + gcs + pubsub + gcp_secret_manager`
- AWS: `redshift + s3 + eventbridge_sqs + aws_secrets_manager`

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

### Typical local flow

1. Create or inspect a connector
2. Run an import
3. Run predictions
4. Create or refresh a cohort
5. Publish or test a workflow
6. Review experiment summary or integrity
7. Use the global AI assistant bubble for help, samples, dashboard summary, or safe setup execution from the page you are already on, then answer any inline clarification or confirmation cards directly in the same chat transcript
8. Open `Insight Copilot` only when you want the manual Query, Explain, Recommend, Report, or Evidence & Logs tools directly

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
- `/api/v1/copilot/agent`
- `/api/v1/audit`
- `/api/v1/templates`

There is also a lightweight liveness endpoint at:

- `/api/v1/health/live`
- `/health/live`

## Validation and Local Development

### Backend tests

```bash
.venv/bin/pytest backend/services/tests/test_copilot_agent.py backend/services/tests/test_multitenant_auth.py backend/services/tests/test_v1_api.py backend/services/tests/test_v1_closed_loop.py -q
```

### Frontend/operator smoke

```bash
PWCLI=/path/to/playwright_cli.sh BASE_URL=http://127.0.0.1:8000 ./scripts/operator_console_smoke.sh
```

The smoke script now checks the global AI assistant launcher and drawer, a grounded help answer, a clarification loop, one safe setup action, and a risky follow-up that remains pending across navigation.

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
- backend-served React shell with a Figma-based SaaS layout, inline section expansion, an icon-only collapsed rail with right-side section popouts that auto-engages below `1200px` and closes after an icon routes to the module's first section, a bottom-left session profile chip with logout, a search-first top bar with `System`, `Light`, and `Dark` theme buttons, and a tabbed Settings surface for workspace, project, and team management including explicit role-save buttons, `Joined YYYY-MM-DD` roster metadata, a shared invite-link generator, member removal, and owner transfer through the role Save flow alongside lighter placeholder profile, notification, and billing layouts

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
