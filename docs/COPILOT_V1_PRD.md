# KairyxAI Insight Copilot v1 PRD

## 1. Background and Goals

### 1.1 Background
The growth team still has many manual steps in the chain of "look at the numbers -> find the cause -> decide the action":
- Metric definitions are inconsistent
- Anomaly diagnosis is slow and cross-team communication is expensive
- Analysis conclusions are hard to turn directly into executable actions

### 1.2 Goal (v1)
Build an analytical copilot that can answer metric questions, explain anomalies, and recommend actions, forming the minimum closed loop from insight to action:

**Metric query -> anomaly explanation -> action recommendation -> one-click cohort draft generation**

### 1.3 Non-Goals (Not Included in v1)
- Autonomous execution of high-risk operational actions; v1 gives recommendations only and does not auto-send
- Full BI replacement
- Complex causal-inference platform

---

## 2. Users and Scenarios

### 2.1 Target Users
- PM / Growth PM
- Data analysts
- LiveOps / Operations teams
- Marketing and acquisition leads

### 2.2 High-Frequency Scenarios
1. Fast metric query: current value and trend of a metric
2. Anomaly diagnosis: why a metric dropped today or this week
3. Operational recommendation: who should receive what action next
4. Review summary: automatic daily or weekly report generation

---

## 3. Functional Scope (v1)

## 3.1 NL2Metric (Natural-Language Metric Query)
### Capabilities
- Support natural-language queries for metrics
- Support common slices such as platform, country, channel, version, and time window
- Output metric-definition context, including definition, time range, and filters

### Examples
- "Paid conversion rate for iOS users in the US over the last 7 days"
- "How much did D1 retention change this week compared with last week?"

### Output
- Metric value
- Period-over-period comparison when available
- Read-only SQL summary
- Metric-definition notes

## 3.2 Anomaly Explain
### Capabilities
- Automatically identify anomalies in key metrics, including drops, lifts, and volatility
- Output top drivers, typically 2 to 5
- Estimate impact scope in affected users and revenue impact

### Example Drivers
- Traffic drop in a specific country
- Conversion decline in a specific app version
- Lower traffic quality from a specific campaign

### Output
- Anomaly summary
- Ranked driver list
- Supporting evidence snippets
- Confidence level

## 3.3 Action Recommendation
### Capabilities
- Generate executable recommendations based on anomalies and segmentation context
- Map recommendations to existing action types such as push, email, in-app, and experiment
- Generate a cohort draft with one click

### Output
- Recommended action
- Target audience definition
- Expected directional impact
- Risk notes such as frequency disturbance and budget risk

## 3.4 Auto Report
### Capabilities
- Generate daily and weekly reports using fixed templates
- Include core metrics, anomalies, recommended actions, and follow-up items

---

## 4. Unified Output Template
Every Copilot response must use a structured output:
1. **Conclusion** (one sentence)
2. **Key Evidence** (up to 3 items)
3. **Impact Scope** (users affected / revenue impact)
4. **Recommended Action** (executable)
5. **Confidence** (`high / medium / low`)
6. **Metric Definition and Time Window** (mandatory)

---

## 5. Data and System Dependencies

## 5.1 Read-Only Data Inputs
- `mart_user_daily`
- `fact_events_unified`
- `cohort metadata`
- `experiment summary` when available

## 5.2 Relationship to Other Modules
- Depends on Data Core for unified, governed metrics
- Calls Audience Engine to generate cohort drafts
- Can call Experiment Hub to create experiment drafts when needed

---

## 6. Safety and Governance

1. Every conclusion must be traceable to data evidence
2. When uncertain, Copilot must degrade gracefully to "low confidence + more data required"
3. High-risk actions are not auto-executed; manual confirmation is required by default
4. Outputs must not expose restricted fields and must follow RBAC and masking rules

---

## 7. Default Configuration (v1)
- Anomaly-detection windows: dual-window `7d / 14d`
- Maximum number of returned drivers: 5
- Auto-report frequency: once daily by default and configurable
- Evidence requirement: every recommendation must include at least one verifiable evidence point

---

## 8. API Draft (v1)

- `POST /copilot/query`
  - Input: natural-language question, time window, filter conditions
  - Output: structured insight result

- `POST /copilot/explain`
  - Input: metric name, time window, slice dimensions
  - Output: anomaly explanation plus drivers

- `POST /copilot/recommend`
  - Input: insight result ID or metric context
  - Output: action recommendation plus cohort-draft definition

- `POST /copilot/report`
  - Input: daily or weekly report parameters
  - Output: structured report content

---

## 9. Acceptance Criteria (DoD)

1. Support at least 20 high-frequency metric-query intents
2. Anomaly explanation can output at least 2 verifiable drivers
3. Every recommendation includes metric definition, evidence, and confidence
4. Supports one-click cohort draft generation and successful persistence
5. Daily report templates can be generated stably and used by operations and PMs

---

## 10. P0 Implementation Priority (Detailed)

### P0-1 Metric Query Capability (NL2Metric)
**Goal**: let Copilot answer high-frequency business questions reliably on top of unified definitions.

**Detailed Scope**
1. Build a `metric registry` with at least 20 metrics
   - Fields: `metric_id`, `name`, `definition`, `sql_template`, `supported_dimensions`, `default_window`
2. Build a query parser
   - Parse natural language into metric, time window, dimension filters, and comparison type
3. Build an execution layer
   - Generate SQL from the registry and query `mart_user_daily` / `fact_events_unified`
4. Build an output layer
   - Return structured result with conclusion, evidence, metric definition, and SQL summary

**Interfaces and Artifacts**
- `POST /copilot/query`
- Table: `copilot_query_logs`

**Acceptance Criteria (DoD)**
- 20 high-frequency query intents can be hit reliably
- Every result contains metric definition and time window
- Query failures return explicit reasons instead of vague errors

---

### P0-2 Anomaly Explain
**Goal**: explain key metric movement with verifiable drivers instead of only saying that a metric moved.

**Detailed Scope**
1. Anomaly-detection jobs
   - Calculate baselines and deviation for key metrics every day using 7-day and 14-day windows
2. Driver decomposition
   - Automatically decompose by dimension: `platform / country / version / channel / campaign`
3. Explanation output
   - Return top 2 to 5 drivers, evidence values for each, and estimated impact
4. Confidence rating
   - Assign `high / medium / low` confidence based on sample size and stability

**Interfaces and Artifacts**
- `POST /copilot/explain`
- Tables: `anomaly_events`, `anomaly_driver_logs`

**Acceptance Criteria (DoD)**
- Every anomaly outputs at least 2 verifiable drivers
- Every driver includes numeric evidence
- The explanation can be reproduced by metric and time window

---

### P0-3 Recommendation + Cohort Draft
**Goal**: turn analysis conclusions directly into executable actions and target audiences.

**Detailed Scope**
1. Recommendation template engine
   - Input: anomaly type plus metric context
   - Output: action type such as push, email, in-app, or experiment plus risk notes
2. Cohort draft generation
   - Bind the recommendation to a cohort definition using rule or SQL
   - Write the output into a cohort draft without activating it automatically
3. Executable linkage
   - Support one-click handoff into Audience Engine for cohort management

**Interfaces and Artifacts**
- `POST /copilot/recommend`
- Tables: `copilot_recommendations`, `cohort_drafts`

**Acceptance Criteria (DoD)**
- Recommendations can be materialized into cohort drafts
- Recommendations contain target audience, action, and risk notes
- High-risk actions are never auto-executed

---

### P0-4 Auto Report
**Goal**: let operations teams and PMs receive a consistent, actionable analytical summary every day.

**Detailed Scope**
1. Report template
   - Modules: core metrics, anomalies, recommended actions, and follow-up items
2. Scheduled execution
   - Generate reports automatically at a fixed daily time
3. Report archival
   - Persist report content, generation time, data window, and generation status

**Interfaces and Artifacts**
- `POST /copilot/report`
- Table: `copilot_reports`

**Acceptance Criteria (DoD)**
- Daily reports can be generated reliably every day
- Report structure is fixed and fields are complete
- Failed report runs can be retried and reasons are recorded

---

### P0-5 Governance and Traceability
**Goal**: guarantee that Copilot output is trustworthy, auditable, and controllable.

**Detailed Scope**
1. Evidence-chain recording
   - Every output links to `query_id`, `metric_id`, `time_window`, and `data_sources`
2. Access control and masking
   - Filter fields by RBAC; PII is masked by default
3. Audit log
   - Record query, explanation, recommendation, report generation, and manual confirmation actions
4. Risk protection
   - Force warnings for low-confidence conclusions
   - Require manual confirmation for high-risk recommendations

**Interfaces and Artifacts**
- Table: `copilot_audit_logs`
- Config: `copilot_safety_config`

**Acceptance Criteria (DoD)**
- Any Copilot conclusion can be traced back to data evidence
- Unauthorized fields are not visible
- High-risk actions have audit records and confirmation chains

---

## 11. Current Gap Register (Based on the 2026-03 Repository State Review)

### 11.1 Already Implemented
- `query / explain / recommend / report` APIs already exist
- Query logs, anomalies, reports, and weekly-report resources already exist
- Structured output, evidence envelope, and cohort-draft generation already exist

### 11.2 Remaining Gaps

#### Gap-C1 Copilot operator console is not yet hardened
- Current state:
  - Copilot pages are already available in the single-page operator console
- Remaining work:
  - Dedicated Playwright / E2E contract coverage is still missing
  - Query, explain, anomaly, and report views still depend on a single-page static-console structure

#### Gap-C2 Auto Report operations workflow is still lightweight
- Current state:
  - Daily and weekly report resources plus retry behavior already exist
- Remaining work:
  - Report subscription, review, failure triage, and operational consumption are not yet independently productized
  - The frontend still lacks a mature report-management console

#### Gap-C3 Recommendation remains recommendation-only rather than outcome-driven auto-optimization
- Current state:
  - Copilot can already recommend actions and generate cohort drafts
- Remaining work:
  - There is still no stable loop of "real outcome -> update recommendation templates automatically / auto-tune strategy"
  - Recommendations still depend on the measurement maturity of Experiment and Action

#### Gap-C4 The evidence loop still depends on downstream measurement maturity
- Current state:
  - `experiment_summary`, `cohort_snapshot`, and workflow summaries can already act as evidence inputs
- Remaining work:
  - When real provider outcome, return, or conversion signals are incomplete, the Copilot evidence chain still degrades
  - The Action and Experiment real measurement pipeline must mature further

#### Gap-C5 Production access boundary is incomplete
- Current state:
  - Minimal RBAC and masking already exist
- Remaining work:
  - Formal authN, tenant boundaries, and production-grade access control are still missing
  - The current Copilot data-access boundary should not yet be considered production-ready

### 11.3 Next-Phase Ownership Held by This Document
- Copilot pages and contract hardening under `Phase 1 Frontend Hardening`
- Evidence-feedback dependencies for Copilot under `Phase 4 Activation And Measurement`
- Copilot access boundaries and governance under `Phase 5 Production Readiness`

### 11.4 V1 Backlog

#### P0 Finish-Up
1. `Auto Report Ops Workflow`
   - Complete subscription, review, failure triage, and operational consumption for daily and weekly reports
   - Make reports not just generatable, but reliably consumable
2. `Evidence Loop Hardening`
   - Strengthen Copilot dependence on real provider outcomes, experiment summaries, and workflow summaries
   - Reduce evidence degradation when measurement is incomplete
3. `Recommendation Traceability`
   - Bind recommendation templates, cohort drafts, and experiment suggestions more explicitly to outcome evidence
   - Keep the system in recommendation mode and do not auto-execute high-risk actions

#### P1
1. `Copilot Console Hardening`
   - Add dedicated Playwright / E2E coverage for query, explain, anomaly, and report pages
   - Build a more stable operator console instead of a single-page static layout
2. `Outcome-Driven Recommendation Refresh`
   - Iterate recommendation templates and ranking based on real outcome data
   - Keep humans in the loop and do not auto-execute directly
3. `Production Access Boundary`
   - Complete formal authN, tenant boundaries, and production-grade access control
   - Make Copilot data access conform to production requirements
