# Multi-Tenant GCP Runbooks

## Tenant Onboarding

1. Create the tenant through `POST /api/v1/tenants` as a platform admin.
2. Add at least one active tenant membership through `PUT /api/v1/tenants/{tenant_id}/memberships/{user_id}`.
3. Provision the tenant's OIDC users, `*_ref` secrets, BigQuery dataset prefix, and GCS prefix.
4. Verify `GET /api/v1/auth/me` with a bearer token and `X-Kairyx-Tenant`.
5. Run smoke checks for connectors, imports, SQL preview, workflow draft creation, and `/api/v1/health`.

## Secret Rotation

1. Create a new secret version in Google Secret Manager or rotate the referenced environment secret.
2. Keep the existing `*_ref` value stable when possible so published workflows and exports do not need to be rewritten.
3. Run connector health checks and a sandbox workflow/export after rotation.
4. Audit the rotation with the tenant id, actor id, correlation id, and affected provider connection ids.
5. Remove the old secret version only after the new version has passed smoke checks.

## Worker Replay

1. Confirm the tenant and upstream dependency state before replaying a failed job.
2. For imports, use the existing replay or resume endpoint only after mapping coverage is back above the gate.
3. For predictions and exports, rerun only when upstream job status is `completed`.
4. Use the dedicated worker service for the replay path, not the API process.
5. Preserve the original correlation id in operator notes and link the replay attempt to the prior diagnostic id.

## Kill Switch

1. Enable the orchestrator kill switch before incident mitigation that must stop new sends immediately.
2. Confirm no new due-workflow runs are emitted while the switch is enabled.
3. Keep imports, predictions, and analytics reads available unless the incident scope requires broader containment.
4. Document why the switch was enabled, when it was enabled, and what tenant scope was affected.
5. Disable the switch only after callback lag, provider failures, and workflow diagnostics return to an acceptable baseline.

## Backup And Restore

1. Back up Postgres control-plane data on a schedule aligned with the RPO target.
2. Back up tenant-scoped GCS prefixes and keep BigQuery retention or snapshot policies enabled.
3. Restore into an isolated environment first and verify tenant membership, resource counts, and job metadata.
4. Repoint worker services only after API auth, tenant isolation, and secret resolution have been revalidated.
5. Record the restore window, affected tenants, and residual data gaps in the incident log.

## Incident Triage

1. Capture the `correlation_id`, tenant id, actor id, job id, and resource id from logs or API responses.
2. Check `/api/v1/health`, workflow delivery diagnostics, export diagnostics, and recent audit actions for the tenant.
3. Determine whether the issue is auth, dependency locking, provider failure, warehouse lag, or callback reconciliation.
4. Use the tenant-scoped worker or resource replay path only after the root cause is understood.
5. Close the incident with the failure class, impacted tenants, mitigation, and follow-up code or runbook changes.

## Tenant Offboarding

1. Disable tenant memberships and revoke OIDC access before deleting any tenant-scoped provider access.
2. Archive or stop active workflows, exports, and background jobs for the tenant.
3. Export required audit records and confirm retention requirements.
4. Remove or revoke tenant-scoped secret references, GCS prefixes, BigQuery datasets, and downstream provider connections.
5. Delete tenant metadata only after dependencies are cleared and compliance sign-off is complete.
