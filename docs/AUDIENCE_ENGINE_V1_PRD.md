# KairyxAI Audience Engine v1 PRD

## 1. Module Goal
Convert data insights into reusable, executable, and traceable audience assets that support operational outreach, experiment assignment, and model strategy.

---

## 2. Module Scope (v1)

### 2.1 In Scope
- Cohort creation through three entry points: Rule / SQL / List
- Guided cohort building for marketers with selector-based filters, multi-source prediction selection, and preview-before-create
- Cohort lifecycle management: naming, tags, versions, delete and restore
- Membership computation and refresh: snapshot + delta
- Activation and downstream distribution for Engage / Experiment / Copilot
- Performance feedback: foundational metrics and version comparison
- Auditability: create / update / refresh / activate / delete must be traceable
- AI-assisted cohort drafting that produces builder-state artifacts before draft cohort creation

### 2.2 Out of Scope
- Cross-tenant cohort sharing
- Large-scale hourly real-time refresh
- Automated causal attribution and auto-tuning closed loop
- Complex approval workflow engines

---

## 3. Detailed Submodule Design

## 3.1 A) Cohort Lifecycle

### Functionality
- Create cohorts from guided builder, Rule / SQL / List
- Manage metadata (`name`, `description`, `owner`, `tags`, `status`)
- Version definitions (`version + diff + rollback`)
- Soft delete (`deleted_at`) and restore
- Privileged permanent delete with mandatory audit trail
- Keep SQL available as an advanced escape hatch instead of the default marketer entry point

### Data Objects
- `cohort`
- `cohort_definition`
- `cohort_version_log`

### DoD
1. Creation success rate for all three entry points is >= 99%
2. Supports rename, tags, and version rollback
3. Delete / restore / permanent delete are fully auditable

---

## 3.2 B) Membership Compute

### Functionality
- Support membership computation for static, dynamic, and SQL cohorts
- Dynamic cohorts refresh daily by default
- Manual refresh entry point for admins and analysts
- Persist a snapshot after every computation
- Automatically compute deltas for newly added and dropped members
- Automatically retry refresh failures and record root cause

### Data Objects
- `cohort_snapshot`
- `cohort_membership_delta`
- `cohort_refresh_job`

### DoD
1. Dynamic cohort daily refresh success rate is >= 95%
2. Every refresh produces `member_count` and `delta`
3. Refresh failures support retry and are diagnosable

---

## 3.3 C) Activation and Downstream Delivery

### Functionality
- Cohort state machine: `draft -> active -> paused -> archived`
- Pre-activation checks:
  - non-empty cohort
  - complete `canonical_user_id`
  - healthy refresh state
- Paginated member retrieval interface
- One-click supply to Engage / Experiment / Copilot

### Suggested Interfaces
- `POST /cohorts`
- `GET /cohorts/builder/options`
- `POST /cohorts/builder/preview`
- `POST /cohorts/builder/create`
- `GET /cohorts/{id}`
- `GET /cohorts/{id}/members`
- `POST /cohorts/{id}/refresh`
- `POST /cohorts/{id}/activate`
- `POST /cohorts/{id}/pause`

### DoD
1. Active cohorts can be consumed directly by Engage, Experiment, and Copilot
2. Empty cohorts cannot be activated, though drafts may be retained
3. Member retrieval supports stable pagination

---

## 3.4 D) Measurement and Feedback

### Functionality
- Foundational performance metrics:
  - audience size reached
  - delivery rate
  - conversion rate
- Version comparison:
  - audience-size change
  - core-metric change
- Link `experiment_id` to inspect A/B/Holdout results

### Data Objects
- `cohort_metrics_daily`
- `cohort_experiment_link`

### DoD
1. Every active cohort can display foundational performance metrics
2. Supports audience-size and performance comparison across the two latest cohort versions
3. Supports reading linked experiment results

---

## 4. Global Launch Gates (Go/No-Go)
1. Creation success rate for all three cohort types is >= 99%
2. Dynamic cohort daily refresh success rate is >= 95%
3. Pre-activation checks are enforced for empty cohorts and missing keys
4. Consumption paths for Engage / Experiment / Copilot are fully integrated
5. Full-chain audit is available for create / update / refresh / activate / delete

---

## 5. Default Configuration (v1)
- Dynamic cohort refresh frequency: `daily`
- Static cohort refresh frequency: `manual`
- Refresh retry count: `up to 2`
- Replay concurrency: `1~2 jobs`
- High-risk operations: `audit required`

---

## 6. Relationship to the Master PRD
This document is the detailed design for Audience Engine. The master PRD (`DATA_CORE_V1_PRD.md`) keeps high-level goals and acceptance gates, while this document is used for implementation planning and sequencing.

---

## 7. Current Gap Register (Based on the 2026-03 Repository State Review)

### 7.1 Already Implemented
- Rule / SQL / List cohort creation all exist
- Lifecycle, versioning, rollback, archive / restore, refresh jobs, metrics, and compare all exist
- Activation preflight, paginated members, and foundational feedback metrics all exist
- Guided cohort builder endpoints, builder provenance, multi-source prediction preview/create, and AI builder-state drafting now exist in the operator stack

### 7.2 Remaining Gaps

#### Gap-A1 Feedback still depends on downstream measurement maturity
- Current state:
  - Cohort metrics can already read workflow delivery, experiment summary, and outcome results
- Remaining work:
  - Real provider-grade return, conversion, and delivery signals are not yet fully stable
  - The Audience feedback loop still depends on the maturity of Action and Experiment measurement

#### Gap-A2 Audience operator console is not yet hardened
- Current state:
  - The frontend now includes a marketer-first guided cohort builder, Advanced SQL fallback, and builder-first cohort detail rendering
- Remaining work:
  - Deeper end-to-end coverage is still needed for real workspace data and AI-assisted create flows
  - Operator UX for metrics, compare, and refresh history is still a single-page static-console pattern

#### Gap-A3 Some operator views still lack dedicated backend view models
- Current state:
  - APIs already exist for members, versions, metrics, compare, and refresh jobs
- Remaining work:
  - Some pages are still composed by the frontend from generic resources instead of stable aggregated interfaces
  - The frontend/backend contract should be tightened further to reduce UI-side composition logic

#### Gap-A4 Production-grade permissions and tenant boundaries are not complete
- Current state:
  - Minimal RBAC and audit already exist
- Remaining work:
  - Cross-tenant isolation, formal authentication, and high-risk cohort action boundaries are still incomplete

### 7.3 Next-Phase Ownership Held by This Document
- Cohort UI / metrics / compare contracts under `Phase 1 Frontend Hardening`
- Real measurement dependencies for the cohort feedback loop under `Phase 4 Activation And Measurement`
- Cohort permissions and tenant boundaries under `Phase 5 Production Readiness`

### 7.4 V1 Backlog

#### P0 Finish-Up
1. `Feedback Loop Stabilization`
   - Move cohort metrics onto more stable provider delivery, outcome, and conversion signals
   - Reduce the sensitivity of Audience feedback to downstream measurement volatility
2. `Audience View Model Tightening`
   - Add more stable aggregated interfaces for metrics, compare, and refresh-history operator views
   - Reduce frontend assembly logic based on generic resources

#### P1
1. `Audience Console Hardening`
   - Add dedicated E2E coverage for cohort lifecycle, metrics, compare, and refresh flows
   - Move from a single-page static console toward a more productized operator UX
2. `Production Access Boundary`
   - Complete formal authentication, tenant isolation, and high-risk cohort operation boundaries
   - Bring Audience assets to production-grade access control
