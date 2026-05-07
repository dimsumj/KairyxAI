# KairyxAI Insight Copilot v1 PRD

## 1. Background and Goals

### 1.1 Background
KairyxAI is moving from a manual configuration console toward an AI-native growth marketing workspace. The operator workflow still has too many manual steps between "understand the current state" and "prepare the next controlled action":
- Operators need to move across Data Core, Audience Engine, Experiment Hub, workflow health, and Copilot reporting to build a single tenant-level picture
- Low-risk setup work such as connector configuration, cohort draft creation, and experiment draft creation still requires form-by-form navigation
- Growth knowledge such as campaign briefs, SOPs, historical analyses, and playbooks is now retrievable as cited `hybrid_v1` evidence for recommendations and campaign drafting, with config-driven vector-index shadow records delivered and managed external vector-store adapters plus structured artifact retrieval still planned

### 1.2 Goal (v1)
Upgrade `Insight Copilot` into a constrained prompt-first growth agent that can:
- summarize the current workspace state across modules
- answer grounded product-help questions with cited context and exportable artifacts when structured payload review is needed
- collect missing inputs through structured clarifications
- preview or draft the exact control-plane setup it plans to prepare
- execute safe setup work on behalf of the operator
- hand off live/risky actions to the owning module for approval, scheduling, publishing, deletion, archive, or send

The v1 closed loop becomes:

**operator request -> intent + slot extraction -> retrieval context -> clarifications -> draft/setup preview -> safe setup execution -> artifact/module handoff**

This upgrade does not delete analytical capabilities. It moves them behind the primary Ask AI experience and keeps manual/query-style controls only as advanced fallbacks.

### 1.3 Non-Goals (Not Included in v1)
- Autonomous execution of high-risk operational actions from chat
- Destructive delete flows, even where the underlying APIs already exist
- Workflow publish, workflow run, live sends, or any action that can directly trigger downstream delivery
- A general-purpose browser-driving agent or DOM automation layer
- Full BI replacement or open-ended autonomous analysis outside the supported action registry

### 1.4 Regenerated 2026-05 RAG And Prompt-First Baseline

Current features to preserve:
- global Ask AI session across the operator console
- starter actions and module-level prompts for connectors, mappings, cohorts, SQL drafts, campaigns, workflows, experiments, diagnostics, and health summaries
- secure input flow for API keys, tokens, and service-account files outside the chat transcript
- backend-managed model profiles for Gemini, OpenAI-compatible local runtimes, OpenAI, and Anthropic
- draft email and push copy generation so operators can approve, edit, and schedule in Action Orchestrator
- artifact cards, module handoffs, and `.json` export buttons instead of raw JSON/code output text fields in the primary UI
- Data Core retrieval evidence-pack API wired into Ask AI with `hybrid_v1` semantic/reranked citations for relevant strategy, setup, diagnostics, audience, workflow, experiment, and copywriting turns
- Experiment Hub AI evaluation telemetry, deterministic auto-grading API, and AI Quality Monitor for recording and reviewing retrieval quality, citation coverage, answer relevance, copy usefulness, prompt-to-artifact completion, feedback diagnostics, and model-judge readiness
- Experiment Hub AI feedback API that records approvals, edits, ratings, sends, workflow results, and experiment outcomes, with knowledge feedback already influencing retrieval ranking and compact feedback-learning profiles feeding Ask AI copy-drafting context

Future feature plan:
1. Expand the delivered vector-index shadow record contract into managed external vector-store adapters and structured product-artifact retrieval.
2. Broaden citation-backed context packs from Ask AI turns into every module handoff card that prepares recommendations, generated email/push copy, cohort ideas, workflow setup, experiment plans, and diagnostics.
3. Wire more Ask AI and module UIs into the deterministic AI auto-grader automatically, then add model-judge/offline eval runs that feed the delivered AI Quality Monitor.
4. Expand delivered feedback-learning prompt context into model-based reranking and trained optimization.
5. Keep live sends, publishes, starts/stops, destructive actions, and archive/delete flows out of chat execution.

---

## 2. Users and Scenarios

### 2.1 Target Users
- PM / Growth PM
- Data analysts
- LiveOps / Operations teams
- Marketing and acquisition leads

### 2.2 High-Frequency Scenarios
1. Summarize the current dashboard and surface the top risks plus next steps
2. Set up an upstream connector or downstream provider connection with only the missing fields requested
3. Set up a draft cohort using SQL, rules, or a supplied member list
4. Set up a draft A/B test linked to a cohort and guardrail metrics
5. Prepare a risky follow-up such as cohort activation or experiment start, then hand off to the owning module for approval
6. Run or reuse a churn prediction job from a source or import and continue after it completes
7. Draft SQL from a prompt, preview it, save it, and turn it into a draft cohort
8. Select an existing SendGrid template or Braze API campaign, create a draft email campaign, and optionally create a linked draft workflow

---

## 3. Functional Scope (v1)

### 3.1 Global Assistant Surface
The primary Copilot surface is now a global chat-first assistant with:
- persistent bottom-right launcher
- right-side drawer on desktop and full-screen sheet on mobile
- conversation thread
- bottom composer with a standard `Send` action
- an initial disabled composer state that shows `Getting Agents Ready...` only until the first session-create call returns
- reuse of the existing session on drawer reopen so transcript refresh does not block the composer
- optimistic user-message rendering plus an inline assistant thinking state after send
- inline clarification cards that appear only when required
- inline artifact and module handoff cards inside the transcript instead of side panels

The assistant is available from every app page after workspace resolution and keeps one shared session alive across SPA navigation. Manual `query / explain / recommend / report` controls may remain as collapsed advanced fallbacks, but they are not the primary Copilot experience.

### 3.2 Deterministic Agent Loop
Every agent turn follows the same constrained lifecycle:
1. classify intent
2. extract structured slots
3. merge relevant UI context such as active module, selected cohort, or current experiment
4. request clarifications if required fields are missing
5. build an execution preview
6. validate RBAC and project scope
7. execute only allowed low-risk actions
8. summarize results and return artifact links

The agent does not call arbitrary code paths. It only uses the explicit action registry defined in the backend.

### 3.3 Dashboard Summary
The agent supports `summarize dashboard` as a read-only cross-module action.

The summary aggregates:
- Copilot overview
- recent reports
- recent anomalies
- cohort state
- workflow publication state
- experiment state
- import blockers
- platform health alerts

The output must include:
- a headline summary
- key counts
- top risks
- suggested next steps

### 3.4 Grounded Product Help
The agent supports read-only `help_support` behavior for questions such as:
- `How do I use this page?`
- `Where do I do X?`
- `Export the setup payload`
- `Why is this failing?`

Behavior:
- use current module/page context plus selected resource ids when present
- answer from a structured help catalog instead of open-ended freeform generation
- return guided steps and artifact exports instead of large inline SQL/JSON blocks when structured setup review is needed
- fall back to product-help guidance when an unsupported request is not actionable

### 3.5 Connection Setup
The agent supports `set up a connection` for both upstream connectors and downstream provider connections.

Supported connector types in v1:
- `amplitude`
- `adjust`
- `appsflyer`
- `bigquery`
- `google`

Supported provider connection types in v1:
- `braze`
- `sendgrid`
- `webhook`
- `simulator`

Behavior:
- first disambiguate `connector` vs `provider_connection`
- ask only for the required provider-specific fields
- create the connection through the existing control-plane services
- run connector health check when the target is a connector and a health check is available
- redact secret values from stored action parameters and response payloads

### 3.6 Cohort Setup
The agent supports `set up a cohort` for:
- SQL cohorts
- rule cohorts
- list cohorts

Behavior:
- request `cohort_type` if missing
- collect cohort intent through guided fields, file upload, selected artifacts, or advanced fallbacks depending on cohort type
- for SQL cohorts, run a SQL preview first
- save the supporting query when applicable
- create the cohort in `draft` by default
- optionally update an existing draft cohort when the target cohort id is provided and the request explicitly indicates update behavior

### 3.7 Experiment Setup
The agent supports `set up an A/B test` or `set up an experiment`.

Collected or defaulted fields:
- `experiment_id`
- `cohort_id`
- `primary_metric`
- `guardrail_metrics`
- `min_sample_size`
- `min_runtime_hours`
- `holdout_pct`
- `b_variant_pct`

Behavior:
- require a linked cohort
- save the experiment config through the existing Experiment Hub service
- persist it in a non-running state
- keep experiment start as a separate module-owned action

### 3.8 Prompt-Driven Prediction To Campaign Setup
The agent supports a constrained prompt-driven operator flow for requests such as:
- `Find high-risk players, create a cohort, use SendGrid template X, and set up a draft workflow`

Behavior:
- prediction target defaults to `source` mode when the request names a source and no explicit import is given
- the agent reuses a recent completed prediction when the selected audience and mode already have a non-stale completed job
- the agent starts a new background prediction when the user explicitly asks for a fresh run or no reusable job exists
- after prediction completion, the agent can draft SQL, preview it, save it, create a draft cohort, create a draft provider-backed email campaign, and create a draft workflow
- provider-side template editing remains out of scope in v1; the agent only selects existing SendGrid or Braze assets

### 3.9 Agent Model Profiles
The global assistant supports backend-managed model profiles for the agent only.

Supported providers in v1:
- `gemini`
- `openai`
- `anthropic`

Behavior:
- `Data Core -> Connectors -> AI Agents & Models` is the primary operator UI for runtime setup
- the shipped Connectors presets are `Gemini`, `LM Studio`, `Ollama`, and `Custom OpenAI-compatible`
- `LM Studio`, `Ollama`, and `Custom OpenAI-compatible` all persist as backend-managed `openai` model profiles with preset metadata and a configurable `base_url`
- existing Anthropic profiles remain supported in the agent and still appear in the runtime list when created through the API
- Gemini remains the default when a default Gemini profile or system Gemini configuration is present
- the browser never stores or calls vendor AI credentials directly
- browser-entered secrets are sent to the backend-managed profile APIs and are redacted from later reads
- OpenAI-compatible base URLs can be saved with or without a trailing `/v1`; the backend normalizes the final chat-completions URL for both cases
- OpenAI-compatible local runtimes may omit `api_key` entirely when the endpoint does not require bearer auth
- OpenAI-compatible runtime URLs are called by the backend, so localhost and other private-network hosts are only valid in self-hosted or local deployments; hosted production rejects them
- the selected model profile applies to the current agent session only
- deterministic parsing and message composition remain the fallback when the selected model is unavailable

### 3.10 Async Prediction Continuation
Prediction-backed agent flows are asynchronous.

Behavior:
- the session persists the pending operator flow while the prediction job is still queued or running
- the transcript exposes the prediction job as an artifact with status detail and a `Continue` action
- once the prediction job completes, the same session can resume and build the remaining draft artifacts
- if the prediction fails or stops, the session returns to an active state and explains why the flow could not continue

### 3.11 Retained Manual Copilot Tools
The analytical Copilot endpoints remain in scope as collapsed advanced fallbacks:
- natural-language metric query
- anomaly explanation
- action recommendation drafts
- daily and weekly reports

These tools remain valuable for debugging and open analytical work that is broader than the operator agent's constrained setup scope. They should not reintroduce primary code/JSON text fields where artifact exports or guided controls can carry the same review function.

---

## 4. Unified Output Contract

### 4.1 Agent Message Response
Every agent message response returns a structured payload with:
- `assistant_message`
- `session_state`
- `clarifications`
- `execution_preview`
- `completed_actions`
- `pending_confirmations` (legacy compatibility only; new risky flows should be represented as module handoffs)
- `artifacts`

### 4.2 Session State
The session state must expose:
- `session_id`
- `title`
- `status`
- `current_intent`
- `last_user_message`
- `ui_context`
- `latest_execution_preview`
- `latest_artifacts`
- `latest_clarifications`
- `pending_confirmation_count` (legacy compatibility only)
- `model_profile_id`
- `effective_provider`
- `effective_model_name`
- `model_selection_source`
- `async_status`
- `waiting_for_action_type`
- `waiting_for_resource_id`

### 4.3 Execution Preview
The execution preview must show:
- normalized intent
- human-readable title
- preview summary
- overall risk level
- readiness
- missing fields
- blockers
- ordered preview steps with action type, title, summary, module-approval requirement, and status

The backend still returns execution preview metadata for control-plane readiness and auditability, but the simplified chat drawer does not render a dedicated preview section.

### 4.4 Artifacts
Completed actions may return deep-linkable artifacts such as:
- prediction job
- cohort
- experiment
- connector
- provider connection
- email campaign
- workflow
- saved query

The frontend renders artifacts as inline transcript cards instead of a dedicated side rail.
- Artifacts may also expose:
  - `resume_ready`
  - `resume_message`
  - `status_detail`

---

## 5. Data and System Dependencies

### 5.1 Core Services Used By The Agent
The operator agent reuses the existing control-plane services instead of introducing a separate backend:
- `CopilotService`
- `CohortService`
- `ExperimentConfigService`
- `ConnectorService`
- `ProviderConnectionService`
- `SqlWorkspaceService`
- `HealthMonitorService`
- `PredictionService`
- `EmailCampaignService`
- `WorkflowService`
- `SendGridProviderService`
- `BrazeProviderService`
- `AgentModelProfileService`
- `KnowledgeService` (planned Data Core owner for documents, chunks, and provenance)
- `RetrievalService` (planned Copilot owner for context packs, citations, and ranking metadata)
- `EvaluationService` (planned Experiment/Copilot owner for retrieval and generation quality telemetry)

### 5.2 Resource Persistence
Agent state is persisted in the generic control-plane resource store with dedicated resource types:
- `copilot_agent_session`
- `copilot_agent_turn`
- `copilot_agent_action_run`
- `copilot_agent_confirmation_request` (legacy compatibility for older prepared-action flows)

This keeps the agent aligned with existing persistence, audit, tenant scoping, and project scoping rules.

### 5.3 Model Adapter
The model layer is intentionally narrow:
- provider-agnostic model adapter interface
- Gemini-backed implementation first, with OpenAI and Anthropic adapters under the same interface
- structured JSON output for intent parsing and response composition
- deterministic parser and deterministic response fallback when the selected provider is unavailable or malformed
- backend-managed model profiles; no browser-side vendor SDK or browser-stored AI secret handling

### 5.4 UI Context Inputs
The frontend may pass lightweight context with each turn, including:
- active module
- active page
- selected cohort id
- current experiment id

This lets the agent narrow scope without forcing the user to restate context that is already visible in the console.

---

## 6. Safety and Governance

1. The agent must use the explicit action registry and must not execute arbitrary code paths.
2. Low-risk actions may auto-execute only after all required fields are present.
3. High-risk actions must become module handoffs instead of chat confirmations.
4. Tenant and project scope must be enforced through the existing governance context and control-plane repository boundaries.
5. RBAC failures must fail closed and explain which permission blocked the step.
6. Secrets included in connection setup must be redacted from action parameters stored in the session history.
7. Every prepared or completed agent action must be auditable through the existing repository audit path.
8. Destructive delete flows remain out of scope for the v1 agent, even when the actor has the underlying permission.

### 6.1 Auto-Executable Low-Risk Actions
- `summarize_dashboard`
- `upsert_connector`
- `check_connector_health`
- `upsert_provider_connection`
- `preview_sql`
- `save_query`
- `draft_sql_from_prompt`
- `run_prediction`
- `list_provider_messaging_assets`
- `setup_email_campaign`
- `setup_workflow`
- `setup_operator_flow`
- `create_cohort_sql`
- `create_cohort_definition`
- `update_cohort_definition`
- `save_experiment_config`

### 6.2 Module-Handoff Actions
- `activate_cohort`
- `pause_cohort`
- `archive_cohort`
- `restore_cohort`
- `start_experiment`
- `stop_experiment`
- `record_experiment_decision`

### 6.3 Current Role Expectations
- `admin` can execute the full scope
- `operator` can run the operator agent plus supported setup and module-handoff actions
- `analyst` can use read-oriented agent flows such as dashboard summary, but write actions remain blocked by permission checks

---

## 7. Default Configuration (v1)
- Dashboard summary defaults to the current workspace scope unless UI context narrows it further
- New agent sessions default to an active status and store the latest preview, clarifications, and artifacts
- Agent sessions default to the configured default model profile when one exists, with Gemini preferred when multiple defaults are not explicitly set
- `Data Core -> Connectors -> AI Agents & Models` is the primary place where operators create or change the Ask AI default runtime
- Connection setup auto-generates a name when the user does not provide one
- Connector health check is attempted automatically after connector creation, but failure to run the health check does not roll back the connector
- SQL cohort setup uses a SQL preview before cohort creation and saves the query as a reusable artifact
- Prompt-driven prediction setup defaults to source-mode reuse before it starts a fresh prediction job
- Email campaign and workflow creation always default to `draft`
- New cohorts default to `draft`
- Cohort refresh mode defaults to `manual`, unless the request explicitly implies daily refresh
- New experiments default to `enabled = false`
- Experiment defaults are:
  - `primary_metric = return_rate`
  - `guardrail_metrics = [engagement_rate, policy_block_rate]`
  - `min_sample_size = 20`
  - `min_runtime_hours = 24`
  - `holdout_pct = 0.10`
  - `b_variant_pct = 0.50`

---

## 8. API Draft (v1)

### 8.1 Agent Endpoints
- `POST /api/v1/copilot/agent/sessions`
  - create a new operator agent session
  - accepts optional `model_profile_id`

- `GET /api/v1/copilot/agent/sessions/{session_id}`
  - read the current session state, latest turn, legacy pending confirmations, and module handoff artifacts

- `GET /api/v1/copilot/agent/sessions/{session_id}/turns`
  - list all recorded turns for the session

- `POST /api/v1/copilot/agent/sessions/{session_id}/messages`
  - send a new message into the agent loop

- `POST /api/v1/copilot/agent/actions/{action_id}/confirm`
  - legacy compatibility endpoint for older prepared high-risk action flows; new product UX should prefer module handoff and approval inside the owning module

- `GET /api/v1/copilot/agent/model-profiles`
  - list backend-managed model profiles for the operator agent

- `POST /api/v1/copilot/agent/model-profiles`
  - create a backend-managed model profile for Gemini, OpenAI, or Anthropic
  - the shipped Connectors UI maps `Gemini`, `LM Studio`, `Ollama`, and `Custom OpenAI-compatible` presets onto this endpoint

- `GET /api/v1/copilot/agent/model-profiles/{model_profile_id}`
  - read one backend-managed model profile

- `PATCH /api/v1/copilot/agent/model-profiles/{model_profile_id}`
  - update one backend-managed model profile
  - `Set Default` in the Connectors runtime table uses this endpoint with `is_default = true`

- `DELETE /api/v1/copilot/agent/model-profiles/{model_profile_id}`
  - delete one non-system-managed model profile

### 8.2 Retained Manual Endpoints
- `POST /api/v1/copilot/query`
- `POST /api/v1/copilot/explain`
- `POST /api/v1/copilot/recommend`
- `POST /api/v1/copilot/report`
- `GET /api/v1/copilot/overview`
- `GET /api/v1/copilot/metrics`

---

## 9. Acceptance Criteria (DoD)

1. Operators can start a Copilot agent session and the backend persists session, turn, action, artifact, and legacy confirmation resources.
2. When a request is missing required fields, the agent returns structured clarifications instead of making up values.
3. `Summarize dashboard` returns a tenant-and-project-scoped summary with counts, risks, and next steps.
4. Connection setup can create a connector or provider connection and return a linked artifact.
5. SQL cohort setup previews SQL, saves the supporting query, and creates a draft cohort without auto-activation.
6. Experiment setup saves a non-running config linked to a cohort and returns a linked artifact.
7. Risky actions such as cohort activation and experiment start return module handoff artifacts and do not execute from the chat window.
8. Permission failures do not bypass governance, and cross-project session access is denied.
9. Operators can select a backend-managed Gemini, OpenAI, or Anthropic model profile for the agent, and deterministic fallback still keeps the session usable when the selected provider is unavailable.
10. Prediction-backed operator flows can resume in the same session after the prediction job completes.
11. The agent can create a saved query, draft cohort, draft email campaign, and draft workflow from one prompt without publishing, sending, or activating anything automatically.
12. The global assistant is available from every app page after workspace resolution, and the Insight Copilot page keeps the manual analytical controls as the advanced/manual fallback.

---

## 10. P0 Implementation Priority (Delivered v1 Scope)

### P0-1 Agent Session And Turn Orchestration
**Goal**: persist a durable operator workflow instead of a single stateless text response.

**Delivered Scope**
1. Session creation and retrieval
2. Turn persistence with user message, assistant message, preview, clarifications, and artifacts
3. Action-run persistence, artifact persistence, and legacy confirmation-request persistence
4. Session state updates after each turn

**Acceptance Criteria (DoD)**
- Each turn is replayable from the resource store
- Pending module handoffs and legacy confirmations are discoverable from the session
- Session state reflects the latest preview and unresolved clarifications

---

### P0-2 Constrained Intent Parsing And Action Registry
**Goal**: keep the agent deterministic and governable.

**Delivered Scope**
1. Intent parsing for dashboard summary, grounded help support, cohort setup, experiment setup, connection setup, copy drafting, retrieval-backed recommendations, and module-handoff follow-ups
2. Structured slot extraction from natural language, named fields, advanced SQL/code blocks when present, uploaded/selected artifacts, and UI context
3. Explicit action registry with fixed permission requirements and risk levels
4. Runtime help catalog for grounded product guidance, exportable setup payloads, and troubleshooting notes
5. Gemini-backed parsing and composition with deterministic fallback
6. Backend-managed model profile selection for Gemini, OpenAI, and Anthropic

**Acceptance Criteria (DoD)**
- Unsupported intents are redirected into the supported scope
- The agent never invents identifiers, credentials, or SQL when those are required inputs
- Only registered actions can be prepared or executed

---

### P0-3 Safe Setup Execution
**Goal**: complete low-risk control-plane work directly from Copilot.

**Delivered Scope**
1. Connector and provider connection setup
2. SQL preview and saved query creation for SQL cohorts
3. Draft cohort creation and safe draft updates
4. Draft experiment config creation
5. Prediction-job reuse or async prediction start for prompt-driven operator flows
6. Draft email campaign creation from existing SendGrid or Braze assets
7. Draft workflow creation linked to cohort and optional email campaign

**Acceptance Criteria (DoD)**
- Successful safe actions return completed action records and deep-linkable artifacts
- Default behavior preserves draft or non-running status for newly created cohorts and experiments
- Secret-bearing connection fields are redacted from stored action parameters

---

### P0-4 Module Handoff And Governance
**Goal**: preserve operator control for risky actions while removing chat-window confirmation prompts from the primary workflow.

**Delivered Scope**
1. Prepared high-risk actions represented as module handoff artifacts
2. Legacy confirm endpoint retained for backwards compatibility, but not used as the primary chat UX
3. Permission checks on both preparation and module-owned execution paths
4. Tenant and project scoping through the existing governance model

**Acceptance Criteria (DoD)**
- High-risk actions do not execute from the chat window
- Cross-project access to another session returns not found
- Roles without write permissions can still use read-safe agent flows while blocked from writes

---

### P0-5 Frontend Copilot Upgrade
**Goal**: make the global AI assistant the operator entry point for guided setup, grounded help, and summary tasks.

**Delivered Scope**
1. Global assistant launcher and drawer that remain available from every in-app page
2. Shared session persistence across SPA navigation
3. Starter prompts for the supported v1 tasks and help flows
4. Model selector backed by backend-managed model profiles
5. Session status showing effective provider / model and async continuation state
6. Structured clarification rendering
7. Execution preview rendering
8. Module handoff rendering, with legacy pending confirmation rendering only for compatibility
9. Artifact deep links into the relevant module
10. `Continue` actions on async prediction artifacts
11. Collapsed advanced access to retained `query / explain / recommend / report` controls on the Insight Copilot page
12. Removal of the static Help module from visible navigation

**Acceptance Criteria (DoD)**
- The user can ask for help, samples, or safe setup work without leaving the current module
- The user can see what the agent plans to set up before draft creation or module handoff
- Manual analytical tools remain available for broader Copilot workflows

---
