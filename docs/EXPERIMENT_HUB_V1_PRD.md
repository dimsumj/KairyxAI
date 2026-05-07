# KairyxAI Experiment Hub v1 PRD

## 0. Regenerated 2026-05 Baseline And RAG Extension Plan

### Current Feature Baseline
Experiment Hub provides the measurement layer for growth decisions. It already supports experiment configuration, stable assignment, exposure/outcome logging, summaries, integrity checks, and decision records. Ask AI may prepare experiment drafts and diagnostics, but starts, stops, rollout decisions, and Action sync remain module-owned approval flows.

Current capabilities to preserve:
- A/B and holdout configuration with primary and guardrail metrics
- deterministic assignments, exposure records, outcome ingestion, summaries, SRM/integrity checks, and decision logs
- integration with Audience Engine, Action Orchestrator, and Insight Copilot
- experiment recommendations that stay recommendation-only until reviewed in the owning module

### Future Growth RAG Plan
Experiment Hub should become the evaluation and feedback backbone for AI-generated growth work:
- store and expose experiment decisions as retrievable evidence for future campaign, cohort, copy, and workflow suggestions
- support evaluation runs for retrieval quality, citation coverage, answer relevance, and AI-generated copy usefulness
- connect send outcomes, cohort performance, and experiment conclusions back to Copilot feedback loops
- cite experiment evidence when Ask AI recommends rollout, stopping, iteration, or audience/copy changes

Initial acceptance criteria:
1. Experiment decisions and outcome summaries are available as structured, retrievable evidence.
2. Ask AI can explain recommendations with citations to experiment evidence and integrity warnings.
3. Evaluation telemetry can record whether retrieved evidence was useful, accepted, rejected, or edited.
4. Experiment-controlled rollout suggestions remain module-owned and do not auto-execute from chat.

---

## 1. Module Goal
Provide a reusable experimentation framework for strategy and audience decisions through A/B/Holdout flows, enabling a growth experiment loop that is verifiable, attributable, and decision-ready.

---

## 2. Module Scope (v1)

### 2.1 In Scope
- Experiment creation and configuration (A/B/Holdout)
- Traffic assignment with deterministic bucketing
- Exposure and outcome logging
- Metric views with foundational significance guidance
- Experiment conclusions (`winner / neutral / inconclusive`)
- Integration with Audience and Action

### 2.2 Out of Scope
- Multivariate testing (MVT)
- Advanced Bayesian inference engine
- Automatic full rollout

---

## 3. Detailed Submodule Design

## 3.1 Experiment Config

### Functionality
- Experiment types: A/B + Holdout
- Experiment scope: bind to `cohort_id` or rule-defined audience
- Split ratio: `holdout_pct`, `variant_pct`
- Time window: `start/end`, minimum sample size, minimum runtime
- Metric configuration (P0 mandatory):
  - 1 primary metric
  - at least 2 guardrail metrics

### DoD
1. Experiment configurations can be created and saved
2. Configurations are versioned and audited
3. Experiments can be enabled and disabled

---

## 3.2 Traffic Assignment

### Functionality
- Stable bucketing based on `experiment_id + user_id`
- Supported groups: `holdout / treatment_a / treatment_b`
- Supports exclusion lists / blacklists

### Execution Requirements
- Idempotent grouping: the same user must always resolve to the same group
- Traceable assignments: every user-level assignment result can be queried
- SRM detection (P0 mandatory):
  - continuously detect sample-ratio mismatch
  - when SRM triggers, mark experiment risk and raise an alert

### DoD
1. Bucketing is stable and reproducible
2. Group ratios remain acceptably close to configuration
3. Exposure records are queryable per user

---

## 3.3 Exposure and Outcome Logging

### Functionality
- Record exposure events
- Record outcome events
- Link to `action_id / cohort_id / workflow_id`

### DoD
1. Exposure and outcome can both be queried by `experiment_id`
2. Data can be traced back to the exact execution chain
3. Missing critical fields are rejected and alerted

---

## 3.4 Measurement and Decision

### Functionality
- Foundational metrics: `engagement_rate`, `return_rate`, `conversion_rate`
- Comparison model: treatment versus holdout
- Conclusion outputs: `winner / neutral / inconclusive / invalid`
- Foundational significance guidance for v1
- Decision gates (P0 mandatory):
  - do not output `winner` if minimum sample size or minimum runtime is not met
  - prioritize `invalid` or `inconclusive` when SRM is detected

### DoD
1. Every experiment can output group comparison and uplift
2. Supports at least 1 primary metric and 2 supporting / guardrail metrics
3. Conclusions are traceable to raw exposure and outcome logs

---

## 3.5 Rollout Suggestion

### Functionality
- Output recommendations such as continue experiment / increase rollout gradually / stop
- Show risk signals such as low sample size, unstable result, and group imbalance
- One-click handoff to Action Orchestrator for module-owned approval and execution

### DoD
1. A next-step recommendation is generated after a conclusion is produced
2. Risk conditions force an explicit warning
3. Integration with Action remains recommendation-only and never auto-executes

---

## 4. Data Objects (v1)
- `experiment`
- `experiment_config_version`
- `experiment_assignment`
- `experiment_exposure`
- `experiment_outcome`
- `experiment_summary`
- `experiment_decision_log`

---

## 5. API Draft (v1)
- `POST /experiments/config`
- `GET /experiments/config`
- `POST /experiments/{id}/start`
- `POST /experiments/{id}/stop`
- `GET /experiments/{id}/summary`
- `GET /experiments/{id}/exposures`
- `GET /experiments/{id}/outcomes`
- `POST /experiments/{id}/decision`

---

## 6. Launch Gates (Go/No-Go)
1. A/B/Holdout traffic assignment is stable and reproducible
2. Exposure and outcome logs are complete and traceable
3. Summary can output uplift and foundational conclusions
4. Primary metric and guardrail metrics are required and enforced
5. Minimum sample size and minimum runtime gates are enforced, so no `winner` is produced early
6. SRM detection is active and alertable
7. Conclusions can flow back into Copilot and Action
8. High-risk recommendations require module-owned approval by default

---

## 7. P0 Delivery Priority
1. Experiment configuration plus stable assignment
2. Exposure / outcome pipeline integration
3. Summary metrics and foundational conclusions
4. Integration with Audience / Action / Copilot
5. Recommendation and audit refinement

---

## 8. TODO (v1.1+)
- Multiple comparisons correction
  - Goal: reduce false positives when evaluating many metrics or many experiments in parallel
  - Candidate methods: Bonferroni / Holm-Bonferroni / Benjamini-Hochberg (FDR)
  - Note: v1 keeps foundational significance guidance, and v1.1 evaluates a unified correction strategy

---

## 9. Current Gap Register (Based on the 2026-03 Repository State Review)

### 9.1 Already Implemented
- Experiment config, versions, assignments, exposures, outcomes, summary, and decision already exist
- `holdout / treatment_a / treatment_b`, SRM, guardrails, and rollout suggestion already exist
- Foundational integration with Audience, Action, and Copilot already exists

### 9.2 Remaining Gaps

#### Gap-E0 Evaluation Registry For AI/RAG Is Not Yet Complete
- Current state:
  - Experiment summaries and decisions exist for growth experiments
  - Retrieval quality, citation coverage, AI copy usefulness, and recommendation acceptance are not yet tracked as first-class evaluation signals
- Remaining work:
  - Add evaluation records for retrieval/generation/campaign-copy quality
  - Make experiment decisions and integrity warnings retrievable as cited evidence for future Ask AI recommendations

#### Gap-E1 Outcome robustness still depends on Action and provider maturity
- Current state:
  - Outcome ingest, callback-to-outcome flow, summary, and decision already exist
- Remaining work:
  - Real return, conversion, and downstream engagement signals are not yet fully stable across all providers
  - Outcome completeness and delay handling still need stronger data contracts

#### Gap-E2 Measurement integrity tooling is still lightweight
- Current state:
  - SRM, guardrails, summary, and decision are already available
- Remaining work:
  - Monitoring and alerting for outcome lag, missing data, and measurement drift are still not mature enough
  - Experiment-health triage is not yet a stable operator workflow

#### Gap-E3 Experiment review console is not yet hardened
- Current state:
  - The frontend can already call experiment-related APIs
- Remaining work:
  - Dedicated Playwright / E2E contract coverage is missing
  - The operator views for summary, assignment, rollout, and alerts are still part of a single-page static console

#### Gap-E4 Rollout is still recommendation-only
- Current state:
  - The system can already output rollout suggestions
- Remaining work:
  - There is still no controlled rollout controller driven directly by Experiment
  - Expansion and stop decisions still require Action-layer execution plus module-owned approval

#### Gap-E5 Production-grade permissions and boundaries are not complete
- Current state:
  - Google login, tenant-scoped resources, RBAC, audit, and high-risk approval boundaries already exist
- Remaining work:
  - Tenant-boundary validation, production staging coverage, and secret isolation drills are not yet complete enough to treat Experiment Hub as production-grade

### 9.3 Next-Phase Ownership Held by This Document
- Real outcome and summary integrity under `Phase 4 Activation And Measurement`
- Experiment review and rollout UI under `Phase 1 Frontend Hardening`
- Experiment permissions and isolation boundaries under `Phase 5 Production Readiness`

### 9.4 V1 Backlog

#### P0 Finish-Up
1. `AI/RAG Evaluation Registry`
   - Track retrieval quality, citation coverage, answer relevance, and generated-copy usefulness
   - Make experiment decisions retrievable as evidence for future recommendations
2. `Outcome Robustness`
   - Improve completeness and delay handling for return, conversion, and downstream engagement signals
   - Make summary and decision more consistently grounded in real feedback
3. `Measurement Integrity Tooling`
   - Strengthen monitoring and alerting for outcome lag, missing data, and measurement drift
   - Build an operator triage workflow for experiment health

#### P1
1. `Experiment Review Console Hardening`
   - Add dedicated Playwright / E2E coverage for summary, assignment, rollout, and alert pages
   - Create a more stable operator review UX for experiments
2. `Controlled Rollout Controller`
   - Move rollout from recommendation-only toward a controlled executor
   - Keep integration with Action and manual review in place
3. `Production Boundary`
   - Complete formal authN, tenant boundaries, and secret isolation
   - Bring Experiment Hub closer to production-grade permissions and isolation
4. `Advanced Experimentation`
   - Introduce stronger statistical protections such as multiple-comparisons correction in v1.1+
