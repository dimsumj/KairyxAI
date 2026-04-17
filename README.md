# KairyxAI

KairyxAI is an AI-driven operator platform for game growth, retention, and lifecycle execution. It sits between raw product data and downstream engagement tools so teams can move from fragmented events to audiences, actions, experiments, and AI-assisted decisions in one workspace.

The product is organized around five operator areas:

- `Data Core` for connectors, mappings, imports, predictions, data quality, and governance
- `Audience Engine` for cohort creation, refresh, comparison, and activation readiness
- `Action Orchestrator` for provider-aware lifecycle email campaigns, dedicated push workflow drafting, shared Workflow Studio scheduling and management, delivery, callbacks, and guardrails
- `Experiment Hub` for assignment, exposure, outcomes, integrity, and decision support
- `Insight Copilot` for grounded operator help, summaries, recommendations, and evidence-backed reporting

Detailed module behavior, button-level instructions, workflow examples, and operator walkthroughs live in the wiki source, not in this README.

## Product Shape

KairyxAI currently supports:

- `Local demo mode` for mock-backed development and UI iteration
- `Vercel demo mode` as an isolated write-capable preview surface
- `Production-shaped SaaS mode` on managed infrastructure, with GCP as the primary production target and AWS deployment assets also present in the repo

In Google-login deployments:

- `/` is the gateway page
- `/{organization_id}` is the active app shell
- authenticated operator traffic prefers `/{organization_id}/v1/...`

The current workspace model is:

- `Organization` as the shared membership and governance boundary
- `Project` as the isolation boundary for connectors, imports, data layers, workflows, cohorts, predictions, AI agents, tools, experiments, exports, and project-scoped audit history

## Quick Start

### Local demo

Requirements:

- Python `3.14`
- Node.js with `npm`

Start the local demo:

```bash
./run_local_demo.sh
```

This script will:

- create or repair `.venv`
- install backend requirements from `backend/services/requirements.txt`
- install frontend dependencies
- build and watch the frontend bundle
- start the backend on `http://localhost:8000`

### Deployment entrypoints

- Vercel demo adapter: `api/index.py` + `vercel.json`
- GCP deployment assets: `deploy/gcp/`
- AWS deployment assets: `deploy/aws/`
- Docker packaging: `deploy/docker/`

The shared GCP dev environment now auto-deploys from GitHub Actions after validation passes on pushes to `main`. The full dev bootstrap, GCP IAM setup, and GitHub-to-GCP CI contract are documented in `docs/GCP_DEV_ENV_BOOTSTRAP_RUNBOOK.md`.

## Documentation Map

Start here for detailed product and operational documentation:

- [Product user guide wiki source](docs/GITHUB_WIKI_PRODUCT_USER_GUIDE.md)
- [KairyxAI v1 Master PRD](docs/KAIRYXAI_V1_MASTER_PRD.md)
- [Multi-tenant production readiness PRD](docs/MULTITENANT_PRODUCTION_READINESS_V1_PRD.md)
- [GCP production deployment runbook](docs/GCP_PRODUCTION_DEPLOYMENT_RUNBOOK.md)
- [GCP dev environment bootstrap runbook](docs/GCP_DEV_ENV_BOOTSTRAP_RUNBOOK.md)
- [Self-hosted Google login plan](docs/SELF_HOST_GOOGLE_LOGIN_PLAN.md)
- [Data Core v1 PRD](docs/DATA_CORE_V1_PRD.md)
- [Audience Engine v1 PRD](docs/AUDIENCE_ENGINE_V1_PRD.md)
- [Action Orchestrator v1 PRD](docs/ACTION_ORCHESTRATOR_V1_PRD.md)
- [Experiment Hub v1 PRD](docs/EXPERIMENT_HUB_V1_PRD.md)
- [Insight Copilot v1 PRD](docs/COPILOT_V1_PRD.md)

## Current Status

This repository already contains a working v1 control plane with:

- a FastAPI backend
- a React/Vite operator console served by the backend bundle
- SQLAlchemy and Alembic control-plane persistence
- mock-first local development
- organization- and project-aware operator flows
- connector-first Data Core onboarding, including a `Connect Data Source` entry point from the main workbench plus an `AI Agents & Models` section in `Data Core -> Connectors` for Ask AI runtime setup
- BigQuery dataset connectors with tenant-scoped service account setup, dataset validation, table discovery that defaults unresolved counts to `unknown rows`, on-demand exact row counts for a selected table with query-based fallback when table metadata access is blocked, and browser-based table imports for external prediction scores and churn lists
- step-level import status tooltips for queued, staging, processing, stopping, and BigQuery table-read phases, with completed jobs no longer retaining stale timeout/failure badges after reruns, failure tooltips layering above adjacent controls, and failed imports excluded from downstream import-job selectors such as `Import Operations`
- a guided `Data Core -> Mappings` workflow for paused `Awaiting Mapping` imports that discovers raw field paths from the import manifests, lets operators bind `canonical_user_id`, `event_name`, `event_time`, and common attribution fields from dropdown selectors, shows saved source-mapping memory and cross-event identifier signals beside each suggestion, loads true raw sample events from mapping candidates into a sample picker for preview and coverage, saves the selection as a job override by default, and resumes the paused import through `Process After Mapping`
- encrypted-at-rest storage for browser-entered connector and provider secrets when `CONTROL_PLANE_SECRET_KEY` is configured, while `*_ref` secret-manager references remain supported
- provider-aware lifecycle delivery in `Action Orchestrator`, including tenant-scoped provider connections managed in `Data Core -> Connectors` through a `Connect Campaign Provider` action, row-level provider deletion guarded against active campaign references, SendGrid dynamic template browsing, Braze API campaign selection, provider-backed push workflow delivery, prediction-or-cohort audience targeting, human-readable audience labels, sampled JSON-key recipient field selectors, optional prediction risk filters, deeplink merge-field injection, push payload fields for `campaign_name`, `title`, `body`, `deep_link`, `deep_link_token`, JSON `data`, optional schedule override, simulator fallback when no push provider connection is selected, and separate upcoming versus past email campaign views
- a prompt-driven `Insight Copilot` operator agent that can use backend-managed Gemini, OpenAI, or Anthropic model profiles, with Gemini, LM Studio, Ollama, and custom OpenAI-compatible presets managed from `Data Core -> Connectors -> AI Agents & Models`; OpenAI-compatible endpoints must be reachable from the backend runtime, so localhost-style presets are for self-hosted or local deployments; the agent can draft SQL from prediction context, reuse or start prediction jobs, and create draft cohorts, email campaigns, and workflows from one chat flow while keeping publish, send, and other live actions confirmation-gated or manual

The remaining work is mostly production hardening, deeper provider-backed execution, and continued frontend productization rather than basic product existence.

## Who This Repository Is For

- game product managers and growth operators
- live ops and CRM teams
- data and backend engineers
- AI engineers building decision systems on top of product data

## Notes

- The README is intentionally brief.
- Detailed operations, module walkthroughs, sample inputs, and expected outputs are maintained in `docs/GITHUB_WIKI_PRODUCT_USER_GUIDE.md`.
- Production and deployment details are maintained in the runbooks and PRDs under `docs/` and `deploy/`.
- The `Settings -> Organization` overlays now use inline red `Cancel` actions in their main button rows. `New Project` pairs `Cancel` with `Create Project`, and `Switcher` pairs `Cancel` with `Continue` instead of showing a separate footer `Close` button.
