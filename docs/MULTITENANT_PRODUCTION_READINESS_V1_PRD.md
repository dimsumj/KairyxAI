# KairyxAI Multi-Tenant Production Readiness v1 PRD

## 1) One-Line Goal
Take the current `/api/v1` control plane and module stack from a local/demo-capable implementation into a production-ready shared SaaS deployment on GCP with explicit tenant isolation, formal auth, secret hygiene, worker isolation, and operational runbooks.

---

## 2) Why This PRD Exists
- This document is the cross-module production-readiness source of truth for the current v1 phase.
- It supersedes the earlier single-tenant production-readiness assumption from the March 2026 repository review.
- It translates the current repository baseline plus the remaining launch work into one plan that can be used for code, infrastructure, testing, rollout, and go/no-go review.

### 2.1 Relationship to the Master PRD
- The master PRD keeps product goals, boundaries, module relationships, and overall launch gates.
- This document owns the detailed shared SaaS production-readiness workstream for auth, tenancy, secrets, runtime topology, observability, and rollout.
- Module-specific behavior still belongs in the module PRDs:
  - `DATA_CORE_V1_PRD.md`
  - `COPILOT_V1_PRD.md`
  - `AUDIENCE_ENGINE_V1_PRD.md`
  - `ACTION_ORCHESTRATOR_V1_PRD.md`
  - `EXPERIMENT_HUB_V1_PRD.md`

---

## 3) Scope

### 3.1 In Scope
- OIDC bearer-token operator auth with organization-scoped paths like `/{organization_id}/v1/...` and project selection through `X-Kairyx-Project`
- Control-plane tenant model with `tenant`, `platform_user`, organization-level membership, and org-level invite records
- Organization roles limited to `owner`, `admin`, and `member`
- Organization gateway behavior at `/`, with `/{organization_id}` as the app shell after org and project resolution
- Tenant-scoped persistence, job metadata, audit metadata, and operator context propagation
- Oldest-active-project defaulting, org-wide project access, and permanent project deletion with explicit confirmation
- Secret-reference-based connector and provider configuration with Google Secret Manager compatibility
- Dedicated Cloud Run service topology for `operator-api`, `import-worker`, `prediction-worker`, `export-worker`, and `scheduler-worker`
- Structured observability, correlation IDs, worker replay paths, and operational runbooks
- Minimum dependency guards and upstream-readiness rules required to keep tenant-scoped execution safe
- Tenant-aware repository, storage, warehouse, and provider callback contracts

### 3.2 Out Of Scope
- A streaming rewrite or real-time control-plane redesign
- A custom enterprise IAM product beyond OIDC federation and control-plane tenant memberships
- Full console redesign beyond the minimum login and tenant-switching baseline
- Fully automated outcome-driven optimization without human approval
- A private-deployment or on-prem control-plane rewrite for this phase

---

## 4) Repository Baseline As Of 2026-03-22

### 4.1 Identity And Governance Baseline
- The backend now supports bearer-token operator traffic on organization-scoped paths through `Authorization: Bearer <OIDC JWT>` plus `X-Kairyx-Project`.
- `/api/v1/auth/me` is the gateway source of truth and exposes the resolved Google user, accessible organizations, accessible projects, active organization, active project, and correlation context for the active request.
- Product access is organization-level: all organization members can access all active projects in that organization.
- Organization roles are now fixed to `owner`, `admin`, and `member`.
- `/api/v1/tenants` and `/api/v1/tenants/{tenant_id}/memberships/{user_id}` establish the platform-admin path for tenant creation and membership management.
- Product-facing team management uses organization members and organization invites rather than project memberships.
- Legacy header auth remains a local/demo compatibility path and is intended to stay disabled in production.
- Governance permissions now cover tenant admin, provider connection, callback ingestion, and module-specific operator actions.

### 4.2 Tenant-Scoped Persistence And Metadata Baseline
- The control plane now includes tenant-aware metadata tables and an Alembic migration for multi-tenant SaaS rollout.
- Resource and job responses now carry `tenant_id`, `created_by`, `updated_by`, and `correlation_id` so operator actions can be traced consistently.
- Repository helpers are tenant-scoped by default, with explicit cross-tenant behavior reserved for platform-admin use cases.
- The production-ready default system of record remains `Postgres`, while local development can still fall back to `SQLite`.

### 4.3 Secrets And Provider Boundary Baseline
- Connector, workflow, and provider credentials now use `*_ref` or `secret_ref` style references instead of storing raw production credentials in the control plane.
- Secret materialization is handled through the platform secret layer with environment-based resolution for local mode and Google Secret Manager compatibility for GCP deployments.
- Provider connections now exist as first-class tenant-scoped resources under `/api/v1/provider-connections`.
- Provider callbacks can be signature-verified and are mapped through tenant-aware callback identity rather than a global, provider-only namespace.

### 4.4 Runtime Topology Baseline
- The repository now contains Cloud Run manifests for `operator-api`, `import-worker`, `prediction-worker`, `export-worker`, and `scheduler-worker`.
- Separate worker HTTP entrypoints exist so background execution no longer has to depend on the API process.
- `main_service.py` remains a local compatibility shim, while the long-term production path is the FastAPI app entrypoint.
- The intended production runtime remains batch/nearline with Cloud Run + Pub/Sub + BigQuery/GCS integration.

### 4.5 Observability, Safety, And Operations Baseline
- Request and resource metadata now include correlation IDs, tenant metadata, and actor metadata that can be propagated across API and worker paths.
- Multi-tenant GCP runbooks now exist for tenant onboarding, secret rotation, worker replay, kill switch use, backup and restore, incident triage, and tenant offboarding.
- The documented dependency-guard direction from `DEVELOPMENT_MEMORY.md` is now part of the production-readiness baseline and no longer a purely speculative gap.

### 4.6 Frontend And Operator Baseline
- The frontend now has a minimum production-shaped auth baseline with Google PKCE login, a gateway-only `/` route, an authoritative `/{organization_id}` app shell, organization URL resolution, and project switching.
- The frozen gateway behavior is:
  - `0` accessible orgs -> create-organization flow
  - `1` accessible org + `1` active project -> direct workspace entry
  - `1` accessible org + multiple active projects -> project selection for that org
  - `2+` accessible orgs -> organization selection, then project selection
- If the typed organization exists but the signed-in Google account is not a member, the gateway must show an explicit not-a-member error.
- Duplicate organization creation must fail explicitly.
- The default project is the oldest active project in the selected organization.
- Organization invites are email-based and org-level, with optional shareable invite links layered on top.
- Project deletion is owner/admin-only, permanent, and guarded by typed `delete` confirmation.
- Local/demo operation remains supported so operators can still run the single-page console without a live IdP during development.
- Broader console productization and deeper end-to-end coverage remain separate workstreams.

---

## 5) v1 Launch Work Still Required

### 5.1 GCP Provisioning And Cutover
- Provision the production OIDC client, JWKS settings, Cloud Run identities, Pub/Sub authenticated push subscriptions, Cloud Scheduler jobs, Secret Manager bindings, BigQuery datasets, and GCS prefixes in a repeatable way.
- Replace any remaining ad hoc environment assumptions with deploy-time configuration and IAM-scoped service identity.
- Prove the full topology in staging before production cutover.

### 5.2 Production Migration Rollout And Backfill Verification
- Execute the multi-tenant Alembic migration against production Postgres with a tested rollback and recovery plan.
- Backfill the bootstrap tenant and validate row counts, unique-constraint behavior, and tenant-scoped query paths before enabling live traffic.
- Verify that every module path honors tenant scoping after migration, including audit, diagnostics, and list endpoints.

### 5.3 Monitoring, Alerting, And Capacity Policy
- Convert the documented health checks and diagnostics into Cloud Monitoring alert policies and dashboards.
- Add alert coverage for import lag, dead letters, provider callback failure rate, auth failures, cross-tenant access denials, simulator usage, and outcome lag.
- Finalize per-tenant limits for SQL preview/query, report generation, import concurrency, export concurrency, and other expensive operations.

### 5.4 Provider Readiness And Rotation Drills
- Confirm that every production-facing provider path uses tenant-scoped provider connections and secret references consistently.
- Run callback signature verification drills and replay drills for each enabled provider.
- Validate that credential rotation does not require reauthoring published workflows or exports.

### 5.5 End-To-End Operator Validation
- Expand regression coverage so JWT auth, organization-path selection, provider-backed execution, org invite acceptance, project deletion, and callback ingestion are exercised together.
- Run the console smoke flow for login, tenant switch, import, cohort, workflow, experiment, and diagnostics in a production-shaped staging environment.
- Confirm that platform-admin cross-tenant actions are limited to the explicit routes intended for them.

### 5.6 Operational Readiness Review
- Verify incident procedures, replay ownership, kill-switch decision rules, and tenant offboarding steps with a dry run.
- Confirm that production startup fails on forbidden settings such as wildcard CORS, `DATA_BACKEND_MODE=mock`, missing OIDC configuration, or local-only compatibility auth.
- Close any remaining documentation or ownership gaps between this PRD, the master PRD, and the runbooks before go-live.

---

## 6) Public Contract Summary

### 6.1 Auth Contract
- Operator traffic uses `Authorization: Bearer <OIDC JWT>`.
- Google login happens before workspace entry.
- The bare `/` route is gateway-only, while `/{organization_id}` is the active operator shell.
- Organization selection uses the URL path shape `/{organization_id}/v1/...`.
- Project selection uses `X-Kairyx-Project`.
- Effective access comes from organization membership stored in the control plane, not from request headers or project membership checks.
- Organization roles are exactly `owner`, `admin`, and `member`.
- All organization members can access all active projects inside the selected organization.
- Legacy `x-api-key`, `x-actor-role`, `x-actor-id`, and `x-tenant-id` are local/demo compatibility only and are not part of production traffic.

### 6.2 Tenant Governance Contract
- `POST /api/v1/tenants` creates a tenant through a platform-admin path.
- `GET /api/v1/tenants/{tenant_id}/memberships` lists memberships for a tenant.
- `PUT /api/v1/tenants/{tenant_id}/memberships/{user_id}` grants or updates tenant membership.
- `/api/v1/auth/me` returns the resolved Google user, accessible organizations, accessible projects for the selected organization, active role, and correlation context for the active request.
- Product-facing team management is organization-level:
  - `GET /api/v1/organization-members`
  - `POST /api/v1/organization-members`
  - `PATCH /api/v1/organization-members/{member_id}`
- Product-facing invite management is organization-level and email-based:
  - `POST /api/v1/organization-invites`
  - `POST /api/v1/organization-invites/redeem`
- Matching Google login may auto-activate a pending organization invite by email before the explicit redeem call; the explicit redeem path must remain idempotent for that same user.
- If an organization already exists but the signed-in Google account is not a member, the org-resolution contract must return `exists=true` and `accessible=false`.

### 6.3 Project Lifecycle And Isolation Contract
- The default project for an organization is the oldest active project by `created_at`.
- `POST /api/v1/projects/{project_id}/permanent-delete` permanently deletes a project and requires a confirmation payload of `delete`.
- Project deletion is allowed only to organization `owner` and `admin` users.
- The team member list is shared across projects in the organization.
- Project-scoped data must remain isolated across connectors, imports, data layers, workflows, cohorts, predictions, AI agents, tools, experiments, exports, and project-scoped audit history.

### 6.4 Resource Metadata Contract
- Control-plane resources and jobs must include `tenant_id`, `created_by`, `updated_by`, and `correlation_id`.
- Tenant-scoped IDs are immutable and must be used by downstream references instead of mutable names.
- Cross-tenant reads are not the default behavior and require explicit platform-admin handling.

### 6.5 Secret Contract
- Persist only secret references or provider-connection references in production resource config.
- Inline production secrets are rejected for published resources and are allowed only for local sandbox-style test flows where the contract explicitly allows them.
- Secret resolution must happen at execution time through the platform secret layer.

### 6.6 Provider And Callback Contract
- Provider-backed actions and exports resolve credentials from tenant-scoped provider connections.
- Callback verification uses provider-specific signing material resolved from the provider connection or secret layer.
- Callback identity must be tenant-aware so one tenant's delivery reconciliation cannot collide with another tenant's events.

---

## 7) Launch Gates
1. `Auth and membership`
   - Valid JWT plus organization membership succeeds.
   - Missing membership returns the correct denial path.
   - Wrong organization selection is rejected.
   - Platform-admin override works only on explicit routes.
   - Existing organization + non-member returns an explicit accessible-false gateway result.
   - Duplicate organization creation returns a conflict instead of silently reusing the org.
2. `Tenant isolation`
   - Connectors, imports, cohorts, workflows, experiments, exports, audit records, BigQuery datasets, and GCS prefixes are isolated per tenant.
   - Same logical names can exist across tenants without creating collisions inside a single tenant.
   - Inside one organization, team membership is shared while project data remains isolated across all project-scoped modules.
3. `Secret handling`
   - Read APIs never expose raw secret material.
   - Rotation keeps stable references where possible.
   - Published production resources cannot persist inline credentials.
4. `Project lifecycle`
   - The oldest active project is returned as the default project.
   - Owner/admin-only project deletion requires typed `delete` confirmation.
   - Deleting a project removes its project-scoped data without deleting the org or the shared team list.
5. `Runtime isolation`
   - Worker execution does not depend on in-process API threads.
   - Production startup rejects local/demo-only settings.
   - Retry, replay, and dead-letter handling preserve tenant context and correlation context.
6. `Provider and measurement integrity`
   - Signed callbacks are accepted only when signatures validate.
   - Invalid signatures are rejected.
   - Delayed callbacks still reconcile against the correct tenant-scoped delivery and experiment state.
7. `Operational readiness`
   - Runbooks have been rehearsed in staging.
   - Alert policies are active and routed.
   - Backup and restore has been validated in an isolated environment.

---

## 8) Rollout Plan

### 8.1 Phase 1: Repository Baseline Complete
- Complete code, migration, test, env-template, CI, and runbook changes in the repository.
- Keep local/demo compatibility paths for development only.

### 8.2 Phase 2: Staging Prove-Out
- Stand up the full Cloud Run worker topology in staging.
- Run migration and bootstrap tenant setup.
- Execute the launch-gate validation matrix with real IdP tokens and provider test credentials.

### 8.3 Phase 3: Limited Production Pilot
- Onboard an internal or controlled pilot tenant.
- Enable alerting, replay procedures, and incident review for every production worker path.
- Watch for tenant-isolation defects, provider callback drift, and rate-limit issues before expanding usage.

### 8.4 Phase 4: General Availability Readiness
- Promote the staging topology and config model to the default production path.
- Retire any production dependency on legacy header auth or API-process scheduling.
- Hold the final go/no-go review against the launch gates in this document and the master PRD.

---

## 9) Dependencies And Risks
- `OIDC and IAM readiness`
  - Production auth depends on stable IdP metadata, JWKS reachability, PKCE client configuration, and Cloud Run service identity setup.
- `Migration safety`
  - The multi-tenant schema rollout changes keys, metadata, and query scoping across the control plane, so migration verification is a release-critical dependency.
- `Provider variance`
  - Callback payloads, retry semantics, and signature rules vary by provider and can create reconciliation gaps if not tested per integration.
- `Operational discipline`
  - Shared SaaS increases the blast radius of configuration mistakes, so runbooks, alert routing, and replay controls are part of the product requirement, not just an operations detail.

---

## 10) Related Documents
- Master PRD: `docs/KAIRYXAI_V1_MASTER_PRD.md`
- Data Core PRD: `docs/DATA_CORE_V1_PRD.md`
- Insight Copilot PRD: `docs/COPILOT_V1_PRD.md`
- Audience Engine PRD: `docs/AUDIENCE_ENGINE_V1_PRD.md`
- Action Orchestrator PRD: `docs/ACTION_ORCHESTRATOR_V1_PRD.md`
- Experiment Hub PRD: `docs/EXPERIMENT_HUB_V1_PRD.md`
- GCP production deployment runbook: `docs/GCP_PRODUCTION_DEPLOYMENT_RUNBOOK.md`
- Multi-Tenant GCP Runbooks: `docs/RUNBOOKS_MULTITENANT_GCP.md`
- Development memory and dependency-guard notes: `docs/DEVELOPMENT_MEMORY.md`
