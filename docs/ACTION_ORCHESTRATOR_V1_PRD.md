# KairyxAI Action Orchestrator v1 PRD

## 1. Module Goal
Turn audiences and strategy outputs from Audience and Copilot into controllable, auditable, and reversible execution flows across Push / Email / In-App / Webhook.

---

## 2. Module Scope (v1)

### 2.1 In Scope
- Operator surfaces: `Email Campaigns`, `Push Notifications`, `Workflow Studio`, `Runtime Controls`, and `Deliveries`
- Trigger definitions: time-based, event-based, and threshold-based triggers
- Action definitions: push / email / in-app / webhook (foundational support)
- Workflow orchestration: branching, frequency caps, cooldown windows
- Execution controls: draft, publish, pause, stop
- Workflow lifecycle controls: archive for non-draft workflows and delete for draft workflows
- Execution logs: send, fail, retry, skip reasons
- Audience export jobs for Braze / SendGrid / Webhook, including export status, retry, and diagnostics
- Safety mechanisms: module-level review, Kill Switch, budget thresholds, and frequency guardrails
- Shared studio summaries for `last_run_at`, `last_test_run_at`, `next_run_at`, `last_result`, and cumulative totals

### 2.2 Out of Scope
- Complex cross-channel journey orchestration with many stages
- Self-optimizing budget allocation
- Highly complex template-rendering systems

---

## 3. Detailed Submodule Design

## 3.1 Push Notifications Builder And Workflow Studio

### Functionality
- `Email Campaigns` remains the dedicated builder for one-time SendGrid and Braze lifecycle sends
- `Push Notifications` becomes the dedicated builder for provider-backed push workflows and simulator fallback
- The push builder reuses the existing workflow contract with cohort, experiment, trigger, policy, provider connection, campaign name, title, body, deep link, deep-link token, JSON data, and provider options
- `Workflow Studio` becomes the shared operating surface for email campaigns and push workflows
- Workflow Studio shows `Name`, `Channel`, `Provider`, `Status`, `Last Run`, `Next Run`, `Last Results`, `Total Results`, and actions
- Workflow Studio provides filters for `Scheduled`, `Sent`, `Archived`, and `All`
- Workflow Studio rows keep `View` and `Edit` visible and collapse less common actions into `More`
- Editing a push workflow reloads it into the `Push Notifications` builder and writes the next save as a new draft version on the same workflow resource
- Non-draft workflows can be archived; only draft workflows can be deleted
- Archived workflows remain visible for history and audit but cannot publish, resume, test-run, or execute on due-run

### DoD
1. Base push workflows can be created, edited, and saved from `Push Notifications`
2. Email campaigns and push workflows are both visible and manageable from `Workflow Studio`
3. New workflow versions can be published while keeping older versions
4. Pause, resume, archive, and draft-only delete are supported

---

## 3.2 Trigger Engine

### Functionality
- Time triggers: cron / daily / hourly
- Event triggers: react to a specific `event_type`
- Threshold triggers: fire when a metric rises above or falls below a threshold

### Execution Requirements
- Trigger deduplication so the same user and rule do not fire repeatedly within a short window
- Trigger idempotency so repeated events do not execute twice
- Idempotency-key contract (P0 mandatory):
  - `idempotency_key = workflow_id + workflow_version + user_id + action_type + window_bucket`
  - The same idempotency key can execute successfully only once within the validity window

### DoD
1. Supports all three trigger types
2. Trigger events have idempotency guarantees
3. Trigger records are auditable

---

## 3.3 Delivery Engine

### Functionality
- Channel adapters: push / email / in-app / webhook
- Pre-send validation: user reachability, frequency cap, unsubscribe state
- Retry strategy: failure retries with exponential backoff
- Fallback strategy: optional fallback path when a channel fails
- Audience export:
  - Supports Braze / SendGrid / Webhook export jobs
  - Returns provider responses, delivery/export diagnostics, and retry state
  - Export payload centers on `user_id / email / predicted_churn_risk / suggested_action / metadata`

### Runtime Ownership (Additional Clarification)
- The `/api/v1/exports` resource belongs to Action Orchestrator's execution control plane
- `export-worker` is responsible for provider export execution, retries, diagnostics write-back, and retry-aware job state
- Export-task resources follow the standard job contract: `id / type / status / created_at / updated_at / progress / error / links`

### DoD
1. At least two channels are stable, preferably push plus email
2. Failure retries are configurable and traceable
3. Every send has a `delivery_id` and delivery status
4. Audience export jobs support `status / retry / diagnostics`

---

## 3.4 Policy and Safety Guardrails

### Functionality
- Frequency cap: daily and weekly reach limits
- Cooldown window: minimum interval between similar actions
- Blacklist / sensitive-audience exclusion
- Kill Switch: one-click global stop for new sends
- Module-level review for high-risk actions prepared by Ask AI
- Three-layer frequency policy (P0 mandatory):
  1. global cap per user per day
  2. per-channel cap for push/email
  3. per-scenario cap through campaign/workflow cooldown windows
- Quiet hours are configurable and enabled by default

### Defaults (v1)
- Max daily contacts per user: 3
- Cooldown for same-category action: 24h
- Global Kill Switch stops all new executions immediately when enabled

### DoD
1. Frequency and cooldown rules are enforced by default
2. Kill Switch takes effect within 1 minute
3. High-risk Ask AI requests prepare module handoffs instead of executing from chat

---

## 3.5 Execution Observability

### Functionality
- Workflow-level metrics: trigger count, execution count, success rate, failure rate
- Channel-level metrics: delivery rate, click rate, conversion rate (foundational)
- Standardized failure attribution (P0 mandatory):
  - `policy_blocked`
  - `channel_error`
  - `template_error`
  - `data_missing`
  - `timeout`
- Top-N failure reasons with trend tracking

### DoD
1. Supports workflow-level and channel-level observability
2. Top failure reasons are visible
3. Critical execution logs can be queried by `user_id` and `workflow_id`

---

## 3.6 Copilot Draft Setup Integration

### Functionality
- `Insight Copilot` may create:
  - draft email campaigns backed by an existing SendGrid template or Braze API campaign
  - draft workflows linked to a cohort and optional email campaign
- Copilot must discover provider messaging assets through the existing provider APIs and ask for disambiguation when more than one asset matches
- Copilot-created delivery assets must remain reversible and controllable from the normal Action Orchestrator pages after creation

### Safety Boundaries
- Provider-side template editing remains out of scope in v1
- Copilot can only select existing SendGrid templates or Braze API campaigns
- Copilot-created email campaigns must always start in `draft`
- Copilot-created workflows must always start in `draft`
- Publish, send-now, workflow run, and live execution remain explicit follow-up actions outside the auto-executed Copilot flow

### Data Contract Notes
- Draft email campaigns may target either:
  - a saved cohort
  - a prediction job audience with risk filters
- Draft workflows created by Copilot must use the existing workflow create contract and produce a valid `manual_test` trigger plus supported workflow steps

### DoD
1. Copilot can list provider messaging assets for SendGrid and Braze
2. Copilot can create a draft email campaign from an existing provider asset
3. Copilot can create a draft workflow linked to the created cohort and optional email campaign
4. The created assets are fully visible and editable inside the standard Action Orchestrator UI

---

## 4. Data Objects (v1)
- `email_campaign`
- `workflow`
- `workflow_version`
- `workflow_trigger_event`
- `action_execution`
- `action_delivery`
- `action_policy_log`
- `action_audit_log`

---

## 5. API Draft (v1)
- `GET /workflows`
- `POST /workflows`
- `GET /workflows/{id}`
- `PUT /workflows/{id}`
- `POST /workflows/{id}/publish`
- `POST /workflows/{id}/pause`
- `POST /workflows/{id}/resume`
- `POST /workflows/{id}/archive`
- `DELETE /workflows/{id}`
- `POST /workflows/{id}/test-run` (sandbox only, must never reach real users)
- `GET /workflows/{id}/executions`
- `GET /email-campaigns`
- `PATCH /email-campaigns/{id}`
- `POST /email-campaigns/{id}/send-now`
- `POST /email-campaigns/{id}/cancel`
- `POST /orchestrator/kill-switch/on`
- `POST /orchestrator/kill-switch/off`

Workflow response additions:
- `archived_at`
- `runtime_summary.last_run_at`
- `runtime_summary.last_test_run_at`
- `runtime_summary.next_run_at`
- `runtime_summary.last_result`
- `runtime_summary.totals`

---

## 6. Launch Gates (Go/No-Go)
1. Foundational workflows (`Trigger -> Action`) can be published and executed stably
2. Delivery-chain success rate meets the project-defined target threshold
3. Frequency cap, cooldown, and Kill Switch are all functional
4. Full execution chain is auditable from trigger to execution to delivery
5. The minimal closed loop with Audience, Copilot, and Experiment is integrated

---

## 7. P0 Delivery Priority
1. Minimal Workflow + Trigger path
2. Two-channel execution and retry for Push / Email
3. Frequency cap / cooldown / Kill Switch
4. Execution observability and audit
5. Audience + Experiment integration

---

## 8. Current Gap Register (Based on the 2026-03 Repository State Review)

### 8.1 Already Implemented
- Workflow, trigger, policy, budget, module review, and Kill Switch already exist
- Delivery diagnostics, provider callbacks, policy counters, and event/threshold triggers already exist
- The minimal closed loop with Audience and Experiment is already connected
- `audience export job` already exists as an independent `/api/v1/exports` resource with an `export-worker` entrypoint

### 8.2 Remaining Gaps

#### Gap-O1 Delivery Engine still leans toward a demo / simulator shape
- Current state:
  - Push, email, and Braze adapters plus execution logs already exist
- Remaining work:
  - Push still relies noticeably on simulator semantics
  - In-app and webhook have not yet reached the same product maturity as push and email

#### Gap-O2 Real provider measurement is not yet fully stable
- Current state:
  - Delivery callbacks and diagnostics already exist
- Remaining work:
  - Provider receipt normalization, failure taxonomy, delayed callback handling, and retry/fallback contracts are still incomplete
  - Real engagement outcomes have not yet formed a fully consistent data contract across all channels

#### Gap-O3 Operator console and execution UX are not yet hardened
- Current state:
  - Workflow, delivery, and policy capabilities are visible in the single-page console
- Remaining work:
  - Dedicated Playwright / E2E coverage is still missing
  - Investigation views for execution failures, retries, budget consumption, and policy blocks still feel operational rather than productized

#### Gap-O4 Provider credentials and runtime boundaries are not yet productionized
- Current state:
  - Minimal governance, audit, and header-based role boundaries already exist
- Remaining work:
  - Formal secret management is still missing
  - Provider-level authentication, environment isolation, and tenant boundaries are still incomplete

#### Gap-O5 Automated optimization is not yet open
- Current state:
  - The Action layer can already execute, record, and feed back outcomes
- Remaining work:
  - Rollout, retry, and policy tuning still require manual judgment
  - The system does not yet modify workflow strategy automatically based on real outcomes

### 8.3 Next-Phase Ownership Held by This Document
- Execution / delivery / policy pages under `Phase 1 Frontend Hardening`
- Provider-grade delivery, callback, and outcome contracts under `Phase 4 Activation And Measurement`
- Provider credentials, authentication, and tenant boundaries under `Phase 5 Production Readiness`

### 8.4 V1 Backlog

#### P0 Finish-Up
1. `Provider-Grade Delivery Stabilization`
   - Strengthen callback normalization, failure classification, delayed feedback handling, and retry / fallback contracts
   - Feed real engagement outcomes back more reliably into execution and experiment layers
2. `Reduce Simulator Dependence`
   - Move the main push/email path farther away from simulator semantics
   - Ensure production channels have consistent execution, diagnostics, and receipt contracts

#### P1
1. `Channel Capability Expansion`
   - Bring in-app and webhook up to the same product maturity as push and email
   - Add fuller provider-specific retry and diagnostics behavior
2. `Execution Console Hardening`
   - Add dedicated E2E coverage for workflow, delivery, policy, and budget pages
   - Build a more productized execution-debugging and operations view
3. `Credentials and Boundary Productionization`
   - Complete provider credentials, formal auth, environment isolation, and tenant boundaries
4. `Outcome-Driven Policy Optimization`
   - Gradually support rollout, retry, and policy-tuning recommendations with module review still required
