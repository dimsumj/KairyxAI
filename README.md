# KairyxAI

KairyxAI is an AI-driven operator platform for game growth, retention, and lifecycle execution. It sits between raw product data and downstream engagement tools so teams can move from fragmented events to audiences, actions, experiments, and AI-assisted decisions in one workspace.

The product is organized around five operator areas:

- `Data Core` for connectors, knowledge documents, mappings, imports, predictions, data quality, and governance
- `Audience Engine` for guided cohort building, multi-source prediction audience selection, SQL fallback, refresh, comparison, and activation readiness
- `Action Orchestrator` for provider-aware lifecycle email campaigns, a unified push composer for immediate, one-time scheduled, and repeated Wynn push campaigns, legacy advanced push workflow drafting, shared Workflow Studio scheduling and management, delivery, callback-authenticated Wynn push outcomes, and guardrails
- `Experiment Hub` for assignment, exposure, outcomes, integrity, and decision support
- `Insight Copilot` for the prompt-first Ask AI command center, grounded operator help, summaries, recommendations, setup handoffs, artifacts, and evidence-backed reporting

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
- tenant/project-scoped knowledge ingestion and retrieval under `Data Core -> Knowledge` and `/api/v1/knowledge`, with no-code file intake for text/markdown campaign briefs, SOPs, reports, FAQs, and playbooks; each document is stored as auditable deterministic chunks with provenance, tags, content hash, lifecycle status, config-driven embedding/vector metadata, exportable `knowledge_document.v1` and `knowledge_vector_index.v1` artifacts, managed vector-adapter receipts for configured stores such as pgvector, Pinecone, Qdrant, Weaviate, Milvus, OpenSearch, BigQuery Vector, or custom gateways, and persisted vector-index records, while retrieval evidence checks support compatibility `lexical_v1` plus hybrid semantic/reranked `hybrid_v1` evidence packs that Ask AI attaches to relevant strategy, setup, diagnostics, and copy-drafting turns
- BigQuery dataset connectors with tenant-scoped service account setup, dataset validation, table discovery that defaults unresolved counts to `unknown rows`, on-demand exact row counts for a selected table with query-based fallback when table metadata access is blocked, browser-based table imports for external prediction scores and churn lists, and tolerant processing-time schema drift handling that first aligns incoming nested values to the live BigQuery table schema, then coerces or nulls only the mismatched property value, and fails only the affected manifest checkpoint when a schema-load 400 remains unrecoverable
- step-level import status tooltips for queued, staging, processing, stopping, and BigQuery table-read phases, with completed jobs no longer retaining stale timeout/failure badges after reruns, failure tooltips layering above adjacent controls, failed imports excluded from downstream import-job selectors such as `Import Operations`, and inline disclosure arrows on `Imported Data` rows so operators can expand each import to inspect event totals, estimated user profiles, live processing progress, dedupe and reject stats, source/date range, and failure context without leaving the list
- cleaner `Connectors` and `Mappings` surfaces that remove persistent instructional copy from the main cards and move optional explanation into contextual `?` hover/focus tooltips so the UI stays minimal and action-first
- an app-wide minimal-UI pass across `Data Core`, `Audience Engine`, `Action Orchestrator`, `Experiment Hub`, `Insight Copilot`, `Settings`, and the workspace gateway that removes persistent helper paragraphs and inline sample blocks from the main page flow, keeps empty states intentionally short, and uses contextual `?` hover/focus help whenever extra explanation is still useful
- a guided `Data Core -> Mappings` workflow for paused `Awaiting Mapping` imports that discovers raw field paths from the import manifests, lets operators bind `canonical_user_id`, `event_name`, `event_time`, and common attribution fields from dropdown selectors, exposes learned mapping memory, successful-import reinforcement, sample values, cross-event identifier signals, and correction context through per-field `?` hover/focus tooltips, loads true raw sample events from mapping candidates into a sample picker for preview and coverage, separates `Save Mapping Memory` for future connector imports from `Save and Reprocess Import` for the current paused job, persists the corrected connector mapping for future imports, and starts the paused import in the background so normalization and dedupe progress continue to update on the same import row while it reruns
- encrypted-at-rest storage for browser-entered connector, provider, and Ask AI runtime secrets when `CONTROL_PLANE_SECRET_KEY` is configured, while `*_ref` secret-manager references remain supported and are exposed in the runtime setup form for production deployments without inline secret storage
- provider-aware lifecycle delivery in `Action Orchestrator`, including tenant-scoped provider connections managed in `Data Core -> Connectors` through a `Connect Campaign Provider` action, row-level provider deletion guarded against active campaign references, SendGrid dynamic template browsing, Braze API campaign selection, a unified Wynn push composer for immediate, one-time scheduled, repeated, explicit-id, all-player, and provider-filtered push campaigns, legacy cohort-based push workflow drafting, prediction-or-cohort audience targeting, human-readable audience labels, sampled recipient field selectors, optional prediction risk filters, deeplink merge-field injection, push payload fields for `campaign_name`, `title`, `body`, `deep_link`, and `deep_link_token`, internal structured payload support with JSON export artifacts, Wynn-native filter handoffs through AI-prepared provider options, simulator fallback when no push provider connection is selected for explicit user ids, and separate upcoming versus past email campaign views
- a guided `Audience Engine` builder that defaults marketers into selector-driven cohort construction, supports prediction-led audiences by source or explicit run, adds warehouse-backed reverse ETL cohorts from either managed warehouse SQL or tenant BigQuery connector tables, freezes saved-query SQL into cohort definitions for stable refreshes, enforces a tenant snapshot-size cap for warehouse audiences, and surfaces warehouse source badges across cohort, workflow, and email-campaign selectors while preserving existing cohort CRUD and SQL workspace APIs
- an AI-native prompt-first operator console where module-level starter prompts and the global `Ask AI` session are the primary path for connector setup, mapping fixes, cohort drafts, SQL drafts, campaigns, workflows, experiment configuration, health summaries, diagnostics, and supported lifecycle actions; raw JSON/code input and output text fields are removed from the operator path, structured payloads export through `Export .json`, and only compact selectors, secure credential dialogs, marketer-readable copy fields, and export-only advanced artifacts remain visible
- a prompt-driven `Insight Copilot` operator agent that can use backend-managed Gemini, OpenAI, or Anthropic model profiles, with Gemini, LM Studio, Ollama, and custom OpenAI-compatible presets managed from `Data Core -> Connectors -> AI Agents & Models`; OpenAI-compatible endpoints must be reachable from the backend runtime, so localhost-style presets are for self-hosted or local deployments; the agent retrieves cited `hybrid_v1` knowledge evidence for relevant prompts, can draft SQL from prediction context, draft guided audience-builder states, draft push/email copy for operator approval, reuse or start prediction jobs, and create draft cohorts, email campaigns, and workflows, while mapping reprocesses, push dispatches, email/workflow lifecycle changes, experiment outcome ingestion, and other live actions are prepared as module handoffs rather than executed from chat
- Experiment Hub AI/RAG evaluation telemetry under `/api/v1/experiments/ai-evaluations`, with tenant/project-scoped records for retrieval quality, citation coverage, answer relevance, campaign-copy usefulness, and prompt-to-artifact completion; deterministic auto-grading under `/api/v1/experiments/ai-evaluations/grade`; model-judge and offline evaluation run adapters under `/api/v1/experiments/ai-evaluations/judge-runs`; scheduled AI quality alert checks that persist exportable `ai_quality_alert_check.v1` artifacts and open/resolved alert resources; the `AI Quality Monitor` surface and `/api/v1/experiments/ai-quality-monitor` expose health, alerts, dimension cards, feedback diagnostics, judge-readiness lanes, latest scheduled check state, recent records, and an `ai_quality_monitor.v1` export artifact
- Experiment Hub AI feedback records under `/api/v1/experiments/ai-feedback`, with operator approvals, edits, ratings, clicks, sends, workflow results, and experiment outcomes feeding summary metrics, deterministic retrieval ranking boosts for knowledge chunks or documents, and redacted feedback-learning profiles that Ask AI can use as prompt context for future drafts
- secure credential setup stays outside chat: Ask AI can initiate connector and provider setup, but API keys, tokens, and BigQuery service account JSON are submitted through the secure input endpoint/dialog and are not recorded as chat messages

The remaining work is mostly production hardening, deeper provider-backed execution, and continued frontend productization rather than basic product existence.

Against the target growth-marketing RAG architecture, the current product already covers structured data ingestion, no-code knowledge intake and export, knowledge document chunking, managed vector-adapter receipts with export-only vector status, Ask AI hybrid retrieval-grounded evidence packs, feedback-boosted ranking, deterministic AI/RAG evaluation grading, model-judge/offline evaluation adapters, scheduled AI quality alert checks, an operator-visible AI Quality Monitor, feedback-learning prompt context, mappings, cohort/action/experiment operations, model profile management, Ask AI handoffs, artifacts, audit history, and secure credential setup. The highest-priority completion gaps are recall probes beyond scheduled monitor checks, provider-specific live vector sync beyond the managed shadow-adapter receipts, and trained optimization beyond deterministic feedback profiles.

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
