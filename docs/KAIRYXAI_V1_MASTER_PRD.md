# KairyxAI v1 Master PRD

## 1) Product Positioning (One Line)
Upgrade growth operations into an AI-native growth marketing RAG platform where operators ask for outcomes, the system retrieves trusted data and playbooks, drafts executable setup, and keeps live action approval inside the product modules.

---

## 1.1) Target Users (Inherited from the Current Product Definition)
- PM / Growth PM
- CRM / LiveOps / Operations teams
- Data analysts / data engineers
- Marketing and acquisition leads
- Founders or early-stage operations owners

## 1.2) Current-Version Non-Goals (Inherited from Current-State Scope)
- No on-prem control-plane rewrite; shared SaaS multi-tenancy on GCP is the primary operating model for this phase
- No custom enterprise IAM product beyond OIDC federation and organization-membership governance in the control plane
- No general-purpose secret-management platform beyond product-integrated secret references and Google Secret Manager resolution
- No real-time streaming decision engine
- No fully automated high-risk closed-loop optimization
- No frontend stack rewrite as a goal for the current phase

---

## 1.3) Regenerated 2026-05 Growth-Marketing RAG Baseline

### Product Direction
KairyxAI is now specified as a prompt-first growth marketing operating system, not a code-first analytics console. `Ask AI` is the primary entry point for Data Core setup, audience drafting, campaign/workflow preparation, experiment planning, diagnostics, and growth insight synthesis. Manual controls remain available only where they support review, compact selection, advanced debugging, export, or secure credential entry.

### Current Shipped Baseline To Preserve
- `Data Core`: connector setup for Amplitude, Adjust, Appsflyer, Google/BigQuery paths; import jobs; guided mappings; field-quality and mapping diagnostics; SQL workspace as an advanced fallback; prediction jobs; local churn model readiness; backend-managed AI runtime profiles; no-code knowledge document intake; cited retrieval evidence packs; config-driven vector indexes; `.json` artifact export
- `Audience Engine`: guided cohort builder; SQL/rule/list cohort support behind marketer-first flows; prediction-backed audience creation; cohort versions, members, refresh, metrics, and AI-drafted builder-state artifacts
- `Action Orchestrator`: provider connections for SendGrid, Braze, webhook, simulator, and push provider paths; email campaign drafts; push notification drafts; Workflow Studio; scheduled/sent/archived workflow management; JSON artifacts exported through buttons instead of primary text fields
- `Experiment Hub`: experiment config, assignment, exposure, outcome, summary, integrity checks, decision logging, AI/RAG evaluation telemetry, deterministic auto-grading, AI feedback records, and feedback-learning profiles with Ask AI handoffs for setup and diagnostics
- `Insight Copilot`: global Ask AI session; starter actions; secure inputs outside chat; backend-managed model profiles; artifact cards; module handoffs for risky actions; AI copy drafting for email/push titles and bodies before operator approval and scheduling
- `Production Readiness`: Google login and organization/project routing; tenant-scoped resources; secret references; worker topology; Cloud Run/GCP deployment assets; runbooks for operations and incident response

### Priority Roadmap
1. `PRD Regeneration Baseline`: keep all current features and planned gaps aligned across the master PRD and module PRDs before additional implementation.
2. `Knowledge Ingestion`: delivered first-class growth knowledge resources and no-code `Data Core -> Knowledge` intake for campaign briefs, SOPs, product docs, historical reports, FAQs, and reusable playbooks, with chunking, provenance, tenant/project scope, lifecycle, and export support.
3. `Retrieval And Citations`: expand delivered hybrid knowledge retrieval and vector-index shadow records into managed external vector adapters plus structured product-artifact retrieval, while preserving query planning, rerank metadata, and citation-backed context packs.
4. `Evaluation And Monitoring`: expand the delivered AI/RAG evaluation telemetry and deterministic grading APIs into monitoring views, model-judge/offline evals, and alerting for retrieval quality, answer relevance, citation coverage, campaign-copy usefulness, and prompt-to-artifact completion rates.
5. `Feedback Loop`: expand delivered feedback records, retrieval-ranking boosts, and prompt-context profiles into trained learning from approvals, edits, sends, workflow results, experiment outcomes, and operator ratings.
6. `No-Code Console Hardening`: continue removing raw JSON/code text fields from primary operator surfaces, replacing them with guided controls, artifact cards, and `Export .json` buttons for inspectable setup payloads.

### Commit And Rollout Policy
Each priority feature must ship in its own commit and be pushed independently. A feature is complete only when the code, targeted tests, and relevant PRD/user documentation are updated together.

---

## 2) v1 Core Modules (Modular PRD Architecture)

> Note: the master PRD only keeps goals, boundaries, milestones, and launch gates. Each core module maintains its own sub-PRD.
>
> Cross-module production readiness for the shared SaaS operating model is tracked in `docs/MULTITENANT_PRODUCTION_READINESS_V1_PRD.md`.

### 2.1 Data Core (Real-Time Event Layer)
- Capabilities: event collection, cleaning and standardization, ID stitching, quality gates, replayability
- Sub PRD: `docs/DATA_CORE_V1_PRD.md`

### 2.2 Insight Copilot (Intelligence Layer)
- Capabilities: natural-language metric queries, anomaly explanation, action recommendation, automated reporting
- Sub PRD: `docs/COPILOT_V1_PRD.md`

### 2.3 Audience Engine (Dynamic Segmentation Layer)
- Capabilities: Rule/SQL/List cohort creation, refresh, naming and management, activation and delivery, feedback loop
- Sub PRD: `docs/AUDIENCE_ENGINE_V1_PRD.md`

### 2.4 Action Orchestrator (Execution Layer)
- Capabilities: triggers, action orchestration, workflow canvas, execution control
- Sub PRD: `docs/ACTION_ORCHESTRATOR_V1_PRD.md`

### 2.5 Experiment Hub (Experiment Layer)
- Capabilities: A/B + Holdout, metric attribution, experiment conclusions and recommendations
- Sub PRD: `docs/EXPERIMENT_HUB_V1_PRD.md`

### 2.6 Multi-Tenant Production Readiness (Cross-Module Workstream)
- Capabilities: OIDC auth, tenant governance, secret handling, runtime isolation, observability, runbooks, and production rollout gates
- Supporting PRD: `docs/MULTITENANT_PRODUCTION_READINESS_V1_PRD.md`

---

## 3) v1 Core Closed Loop (Outcome-Driven)
Insight detection -> AI explanation -> audience generation -> action trigger -> experiment validation -> effect feedback -> strategy iteration

### 3.1 Observation Windows to Improve (v1)
- T+1 day: execution-layer metrics (reach rate, click-through rate, execution success rate)
- T+7 days: intermediate business metrics (return rate, short-term conversion rate)
- T+28 days: core business metrics (retention, monetization, win-back)

### 3.2 Data Sources for Observation Metrics (v1)
- Execution-layer metrics (reach rate / CTR / execution success rate): `action_execution`, `action_delivery` (Action Orchestrator)
- Intermediate business metrics (return rate / short-term conversion rate): `experiment_summary`, `experiment_outcome`, `mart_user_daily` (Experiment Hub + Data Core)
- Core business metrics (retention / monetization / win-back): `mart_user_daily`, `fact_events_unified`, `experiment_summary` (Data Core + Experiment Hub)

### 3.3 Attribution Constraints (v1)
- Improvement conclusions should come from Experiment Hub first when a control group exists
- Results without a control group are marked only as "observational results" and are not counted as attributed revenue impact
- Output a weekly closed-loop impact report (execution -> experiment -> business impact)

---

## 4) First High-Value Scenarios (v1, Application Layer)

### 4.1 Scenario A: Churn Rescue
**Business goal**
- Reduce churn among medium- and high-risk users and improve return and win-back.

**Application-layer flow**
1. Copilot identifies rising churn risk and key drivers
2. Data Core scores a source-level or import-level prediction audience, with source mode resolving to the latest completed import at run time
3. Audience Engine generates a "high-risk users to rescue" dynamic cohort
4. Action Orchestrator executes rescue actions (push/email/in-app)
5. Experiment Hub compares holdout and treatment performance

**Core metrics**
- 7-day return rate
- 14-day win-back rate
- Negative feedback rate among contacted users (guardrail)

**v1 launch standard (scenario-level)**
- Rescue cohort refreshes automatically every day
- Operators can launch churn prediction by source, with import override retained for audit and debugging
- Operators can manually trigger local churn-model retraining and read readiness / training status directly from the churn workbench
- At least one rescue workflow runs stably
- Win-back rate improves by at least 15% relative within 4 to 8 weeks

---

### 4.2 Scenario B: Monetization Lift
**Business goal**
- Improve conversion efficiency and payment quality for high-potential users.

**Application-layer flow**
1. Data Core + Copilot identify high-potential non-payers or low-frequency payers
2. Audience Engine outputs stratified cohorts (high-potential, hesitant, silent)
3. Action Orchestrator delivers differentiated offers and benefit strategies
4. Experiment Hub validates uplift and guardrail metrics

**Core metrics**
- Payment conversion rate
- ARPPU / revenue uplift
- Refund rate or complaint rate (guardrail)

**v1 launch standard (scenario-level)**
- Support different strategies by audience layer
- Experiment conclusions can output `winner / neutral / inconclusive / invalid`
- Relative contacted-user conversion lift reaches at least 10% within 2 to 4 weeks

---

### 4.3 Scenario C: Onboarding Activation
**Business goal**
- Improve critical-path completion and first-week retention for new users.

**Application-layer flow**
1. Copilot identifies the main onboarding funnel drop-off points
2. Audience Engine generates cohorts for blocked steps, such as "viewed tutorial but did not finish"
3. Action Orchestrator triggers onboarding actions, such as tutorial prompts or reward nudges
4. Experiment Hub compares the performance of different onboarding strategies

**Core metrics**
- Completion rate of critical onboarding steps
- D1 / D7 retention
- Contact disturbance rate (guardrail)

**v1 launch standard (scenario-level)**
- Cover at least one key onboarding funnel
- Support automated triggering for funnel blockage events
- Achieve a statistically meaningful improvement in critical-step completion rate within 4 weeks, based on experiment results

---

### 4.4 Shared Application-Layer Constraints (v1)
- Every scenario must bind together: `target audience + execution action + experiment validation + guardrail metrics`
- Results without an experimental control group count only as observations, not attributed revenue impact
- Output a weekly scenario-level impact dashboard with audience size, reach, conversion, guardrails, and net uplift

---

## 5) Success Metrics (90 Days After Launch)
- Strategy launch cycle: from days to hours
- Reach-to-conversion rate lift: +10% to +20%
- Win-back rate of churned users: +15%
- Analysis-to-execution closed-loop rate: >60%

### 5.1 Minimum v1 Improvement Targets (Phase)
- Within 2 to 4 weeks: contacted-user conversion rate improves by at least 10% relative
- Within 4 to 8 weeks: churn win-back rate improves by at least 15% relative
- Within 2 weeks: strategy launch cycle drops from days to hours

---

## 6) Technical and Architecture Principles (Detailed)

### 6.1 Real-Time First (Minute-Level)
**Principle**: core business paths are designed to become visible within minutes by default, without relying on a single T+1 batch cycle.

**Execution requirements**
- Once critical events such as login, purchase, and churn signals enter the unified layer, they should be usable for segmentation and triggering within 1 to 5 minutes
- Dynamic cohorts refresh daily by default and support immediate manual refresh
- Action delivery receipts and outcomes must return quickly enough to support T+1 early-effect evaluation

### 6.2 Explainability First (Evidence-First AI)
**Principle**: every Copilot conclusion and strategy recommendation must be traceable and verifiable.

**Execution requirements**
- Every conclusion must include metric definition, time window, and data source (table/module)
- Every recommendation must include target audience definition, expected directional impact, and risk notes
- Experiment conclusions are standardized as: `winner / neutral / inconclusive / invalid`
- Results without a control group count only as observations and not revenue attribution

### 6.3 Human in the Loop
**Principle**: high-risk actions are prepared by AI but approved and executed only from their owning product module.

**Execution requirements**
- High-risk actions, such as broad outreach, sensitive cohorts, over-budget sends, publishing, deletion, archive, and start/stop decisions, must become module handoffs rather than chat-executed actions
- Ask AI may collect setup information, draft copy, prepare schedules, and produce artifacts, but it must not execute live sends, publishes, destructive changes, or activation-style operations from the chat window
- Kill Switch must stop new sends within 1 minute
- Frequency caps, cooldowns, and quiet hours are enabled by default
- Test runs must be sandboxed and must never reach real users

### 6.4 Deployment Flexibility (SaaS + Private Deployment)
**Principle**: module boundaries remain stable and support both SaaS and private deployment models.

**Execution requirements**
- Modules communicate through API contracts and avoid hard cross-module runtime dependencies
- Configurations such as mappings, rules, and experiments are versioned and reversible
- Data boundaries are configurable to satisfy compliance needs across deployment modes

### 6.5 Module Decoupling and Failure Isolation (P0 Mandatory)
**Principle**: each module is decoupled from the execution layer, and single-module failures degrade locally instead of shutting down the whole system.

**Execution requirements**
- Data Core, Copilot, Audience, Action, and Experiment communicate through stable interfaces and do not share tightly coupled in-process state
- If one module fails, the others remain available and enter degraded mode as needed, for example existing workflows continue even when Copilot is unavailable
- Every module has an independent health check, retry policy, and failure alerting
- Single-module failure must never trigger global shutdown; only local isolation and repair are allowed
- Default repair strategy is local remediation plus replay-based recovery without interrupting other modules

### 6.6 Governance and Audit Enabled by Default
**Principle**: critical behavior must be traceable, auditable, and reviewable.

**Execution requirements**
- Google login is required before workspace entry
- The bare `/` route is gateway-only and `/{organization_id}` is the operator app shell
- If a signed-in user has exactly one accessible organization and exactly one active project, the console may enter that workspace directly
- If a signed-in user has exactly one accessible organization and multiple active projects, the gateway must show project selection for that org
- Organization roles are exactly `owner`, `admin`, and `member`
- The organization creator becomes the only `owner`, and `owner` also has admin privileges
- All organization members can access all active projects in the same organization
- Organization invites are email-based and organization-level; invite links are optional convenience only
- If a typed organization already exists but the signed-in Google account is not a member, the gateway must show an explicit not-a-member error
- Duplicate organization creation must fail explicitly instead of reusing an existing org
- The default project is the oldest active project in the organization
- Project deletion is permanent, owner/admin-only, and requires typing `delete`
- Team membership is shared at the organization level while project data stays isolated
- Audit coverage: configuration changes, cohort changes, experiment decisions, execution actions
- PII masking is enabled by default
- Queries and replays must be protected by resource limits such as timeout, scan caps, and concurrency limits

### 6.7 Backend Control Plane and Runtime Contract (Inherited from Implemented Backend Refactor)
**Principle**: all new backend capabilities must land on the resource-oriented `/api/v1` control plane. Legacy `main_service.py` remains only as a compatibility shim and is no longer the place for new functionality.

**Execution requirements**
- The target operating model is fixed as:
  - `operator-api`: FastAPI control plane
  - `import-worker`: connector paging, checkpoint-aware ingestion, raw shard publishing
  - `prediction-worker`: aggregate-table prediction execution and result persistence
  - `export-worker`: provider export execution with retry-aware job state
  - `dataflow`: manifest-driven normalization into standardized / unified / curated tables
- The target operating model is fixed as `shared multi-tenant SaaS + GCP-native + batch/nearline`
- The system of record for control-plane metadata is `SQLAlchemy + Alembic`; the production default target is `Postgres`, and local development falls back to `SQLite`
- Operator traffic uses `Authorization: Bearer <OIDC JWT>` on `/{organization_id}/v1/...` plus `X-Kairyx-Project`; legacy header auth remains local/demo-only and is disabled in production
- Provider credentials, webhook signing secrets, and connector secrets are persisted as secret references and resolved through the platform secret layer
- Long-running resources must follow the standard job contract: `id / type / status / created_at / updated_at / progress / error / links`
- Large result sets are paginated by default and cannot rely on unbounded list responses
- Persisted control-plane entities must include at least:
  - `connector configuration`
  - `field mapping`
  - `import job`
  - `prediction job`
  - `export job`
  - `experiment configuration`
  - `action history`
  - `ingestion checkpoint`

---

## 7) Scope Management (Division of Responsibility Between Master and Sub PRDs)

### Owned by the Master PRD
- Product goals and boundaries
- Module dependency relationships
- Cross-module milestones
- Overall launch gates (Go/No-Go)
- Shared SaaS production-readiness direction and ownership

### Owned by the Sub PRDs
- Detailed module scope (In/Out)
- Data models and API design
- Task breakdown and Definition of Done
- Module-level launch standards

---

## 8) Cross-Module Dependencies
1. Data Core provides unified, governed data for Copilot, Audience, and Experiment
2. Copilot outputs recommendations and generates audience drafts
3. Audience provides audience inputs for Action and Experiment
4. Action execution outcomes flow back into Data Core
5. Experiment results flow back into Copilot and Audience optimization

---

## 9) Cross-Module Milestones (Suggested)
- M1: Data Core + Audience base path available
- M2: Copilot query/explain + Audience linkage
- M3: Action orchestration + Experiment closed-loop integration

---

## 10) Overall Launch Gates (Go/No-Go)
1. Data Core quality gates meet thresholds for coverage, canonical identity, and reject rate
2. Audience dynamic refresh is stable
3. Copilot output includes evidence chain and metric-definition explanation
4. Action execution supports module-owned approval and auditability
5. Experiment can output readable conclusions and feed them back downstream

---

## 11) Document List
- Master PRD (this document): `docs/KAIRYXAI_V1_MASTER_PRD.md`
- Multi-tenant production-readiness PRD: `docs/MULTITENANT_PRODUCTION_READINESS_V1_PRD.md`
- Data Core sub-PRD: `docs/DATA_CORE_V1_PRD.md`
- Copilot sub-PRD: `docs/COPILOT_V1_PRD.md`
- Audience Engine sub-PRD: `docs/AUDIENCE_ENGINE_V1_PRD.md`
- Action Orchestrator sub-PRD: `docs/ACTION_ORCHESTRATOR_V1_PRD.md`
- Experiment Hub sub-PRD: `docs/EXPERIMENT_HUB_V1_PRD.md`

---

## 12) Current Repository Gap Ownership (Based on the 2026-03 Repository State Review)

### 12.1 Current-State Assessment
- The current repository already goes beyond the "current product" snapshot described in the March 2026 repository review, especially through the resource-oriented `/api/v1` control plane and the Cohort, Workflow, Experiment, Copilot, Template, Health, and Audit capabilities
- The core backend control-plane refactor is already in the repository: `app/main.py`, SQLAlchemy/Alembic, worker entrypoints, BigQuery-backed prediction results, and the legacy `main_service.py` shim all exist
- The remaining gaps are primarily cross-module productization and production-readiness gaps, rather than missing foundational APIs

### 12.2 Cross-Module Gaps Owned by the Master PRD

#### Gap-M1 Operator Console / Frontend Hardening
- Current state:
  - The frontend is still implemented as a single `frontend/index.html` page
  - Formal Playwright / E2E contract coverage is still missing
  - Some operator views are still assembled by the frontend from generic resources instead of consuming clear backend view models
- Ownership split:
  - Data Core: import / mapping / SQL / quality view contracts
  - Audience Engine: cohort lifecycle / metrics / compare / refresh history contracts
  - Action Orchestrator: execution / delivery / policy / diagnostics contracts
  - Experiment Hub: summary / assignment / rollout / alert contracts
  - Insight Copilot: query / explain / anomaly / report contracts
- Exit criteria:
  - one coherent frontend information architecture
  - one stable set of operator flows
  - end-to-end regression coverage for key pages

#### Gap-M2 Production Readiness
- Current state:
  - The repository now includes the first production-shaped baseline for OIDC bearer auth, organization-membership governance, secret references, Cloud Run worker topology artifacts, and multi-tenant runbooks
  - The remaining work is production cutover hardening: infrastructure rollout, alerting, staged validation, provider drills, and final enforcement of production-only startup rules
  - Detailed sequencing and launch gates are owned by `MULTITENANT_PRODUCTION_READINESS_V1_PRD.md`
- Ownership split:
  - Master: authN / authZ / org gateway / org-role model / deployment topology / monitoring / runbook / rollout gates
  - Data Core: data and connector secrets / warehouse access boundary
  - Audience / Action / Experiment / Copilot: module-level RBAC, audit, and high-risk action boundaries
- Exit criteria:
  - explicit tenant and operator boundaries
  - formal secret handling
  - isolated environments and operations runbooks
  - production launch gates in `MULTITENANT_PRODUCTION_READINESS_V1_PRD.md` pass in staging and pilot rollout

#### Gap-M3 Fully Automated Closed-Loop Optimization
- Current state:
  - Recommendations, reports, experiment summaries, and basic outcome feedback already exist
  - There is still no fully automated loop of "real outcome -> automatic strategy iteration -> optimized execution"
- Ownership split:
  - Copilot: recommendation refresh based on real outcome
  - Audience Engine: audience version evolution based on performance feedback
  - Action Orchestrator: action policy auto-tuning is not yet open
  - Experiment Hub: conclusions still provide recommendations only and do not directly drive a rollout controller
- Exit criteria:
  - automated optimization remains module-approved by default
  - every recommendation is backed by real measurement evidence

#### Gap-M4 Gap-Document Ownership Rules
- Cross-module production readiness / rollout gates / auth / tenancy / secrets / runtime topology / runbooks: write into `MULTITENANT_PRODUCTION_READINESS_V1_PRD.md`
- Data platform / ingestion / identity / SQL / schema / replay: write into `DATA_CORE_V1_PRD.md`
- NL2Metric / explain / recommend / report / evidence: write into `COPILOT_V1_PRD.md`
- Cohort lifecycle / refresh / activation / feedback: write into `AUDIENCE_ENGINE_V1_PRD.md`
- Workflow / trigger / delivery / policy / safety: write into `ACTION_ORCHESTRATOR_V1_PRD.md`
- Assignment / exposure / outcome / summary / rollout: write into `EXPERIMENT_HUB_V1_PRD.md`

---

## 13) V1 Backlog (Prioritized)

### 13.1 P0 Finish-Up
1. `Knowledge Ingestion`
   - Delivered tenant/project-scoped knowledge documents and chunks for SOPs, campaign briefs, product docs, historical reports, FAQs, and reusable playbooks, including the no-code `Data Core -> Knowledge` source intake and export-only review path
   - Preserve source metadata, permissions, content hash, lifecycle state, and exportable setup payloads as the UI layer matures
2. `Retrieval And Citations`
   - Delivered lexical knowledge evidence packs, config-driven vector-index metadata, persisted vector shadow records, `hybrid_v1` semantic/reranked retrieval, and Ask AI citation attachment for relevant setup, diagnostics, audience, workflow, experiment, and copy-drafting turns
   - Next, extend the shadow-index path into managed external vector adapters, structured artifact retrieval, and richer module handoff cards
3. `Evaluation And Monitoring`
   - Delivered AI/RAG evaluation telemetry records, summaries, and deterministic auto-grading for retrieval quality, answer relevance, citation coverage, campaign-copy usefulness, and prompt-to-artifact completion
   - Next, add operator-visible diagnostics for stale sources, missing evidence, unsupported prompts, and model-judge/offline eval drift
4. `Feedback Loop`
   - Delivered AI feedback records for copy edits, approvals, sends, delivery results, experiment outcomes, and operator ratings, with deterministic ranking boosts for knowledge chunks/documents plus feedback-learning profiles for Ask AI prompt context
   - Keep automated optimization disabled; AI prepares recommendations and module-owned setup, while live execution remains product-controlled

### 13.2 P1
1. `No-Code Console Hardening`
   - Continue collapsing raw code/JSON/manual configuration fields behind advanced fallbacks
   - Prefer guided selectors, AI draft artifacts, secure credential dialogs, and `Export .json` buttons
2. `Provider-Grade Activation And Measurement`
   - Stabilize delivery, callback, outcome, return, and conversion contracts across email, push, webhook, and future channels
   - Move the Audience, Experiment, and Copilot evidence loop onto real feedback instead of mixed simulation
3. `Production-Grade Deployment Model`
   - Complete monitoring, alerting, tenant isolation validation, credential rotation drills, vector/knowledge storage governance, and runbooks on the shared SaaS topology
   - Remove any remaining production dependence on demo-only auth paths or API-process scheduling
