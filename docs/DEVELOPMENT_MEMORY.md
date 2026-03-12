# KairyxAI Development Memory

## 2026-03-12: Job Lifecycle Edge Cases To Fix

These notes capture lifecycle bugs and contract gaps verified against the current mock backend so they can be fixed in a later development pass.

### Verified behaviors

1. Deleting an import after a prediction has been created leaves an orphaned prediction job.
   Current observed behavior:
   - `DELETE /api/v1/imports/{job_id}` returns `204`.
   - A later `POST /api/v1/predictions/{prediction_id}/run` returns `404`.
   - The error body is misleading: `"Prediction job '{id}' not found."`
   Root cause:
   - `backend/services/app/application/imports.py` deletes the import row without checking dependent prediction jobs.
   - `backend/services/app/api/routers/predictions.py` maps any `KeyError` during run to a missing prediction job, even when the missing resource is the upstream import.

2. Deleting an import while its prediction is already running succeeds and the prediction still completes.
   Current observed behavior:
   - `DELETE /api/v1/imports/{job_id}` returns `204` while the prediction request is in flight.
   - The running `POST /api/v1/predictions/{prediction_id}/run` still returns `200` and `completed`.
   Why this is still a bug:
   - The system allows upstream metadata to disappear while a downstream job is active.
   - Audit/debug flows lose the import resource even though the prediction outcome still references it.

3. Predictions can be created and run against imports that are still queued and not ready.
   Current observed behavior:
   - `POST /api/v1/predictions` against a queued import returns `201`.
   - `POST /api/v1/predictions/{id}/run` returns `200` and may complete with `0` rows instead of rejecting the request.
   Root cause:
   - `backend/services/app/application/predictions.py` checks that the import exists, but not that its status is `completed`.

4. Exports can be created and run against predictions that are not ready.
   Current observed behavior:
   - `POST /api/v1/exports` against a queued prediction returns `201`.
   - `POST /api/v1/exports/{id}/run` returns `200` and may complete with `0` rows.
   Root cause:
   - `backend/services/app/application/exports.py` checks that the prediction exists, but not that it is `completed`.

5. Deleting a connector that existing imports depend on succeeds, and later import execution fails with a misleading 404.
   Current observed behavior:
   - `DELETE /api/v1/connectors/{name}` returns `204` even when imports reference that connector.
   - A later `POST /api/v1/imports/{job_id}/run` returns `404` with `"Import job '{id}' not found."`
   Root cause:
   - `backend/services/app/application/connectors.py` deletes connectors with no dependency guard.
   - `backend/services/app/api/routers/imports.py` turns upstream `KeyError` into a missing import response.

6. Archived cohorts do not block published workflows from executing.
   Current observed behavior:
   - A workflow can be published while the cohort is active.
   - After `POST /api/v1/cohorts/{cohort_id}/archive`, `POST /api/v1/workflows/{workflow_id}/test-run` still returns `200` and executes deliveries.
   Why this is a bug:
   - Publish preflight checks `cohort active`, but execution-time checks do not enforce that contract.
   - Action Orchestrator can continue sending against cohorts that operators have intentionally archived.

7. Permanently deleting a cohort that a workflow references causes a misleading workflow 404.
   Current observed behavior:
   - `DELETE /api/v1/cohorts/{cohort_id}/permanent` returns `200`.
   - A later workflow execution returns `404` with `"Workflow '{id}' not found."`
   Root cause:
   - `backend/services/app/application/cohorts.py` permanently deletes cohorts without checking dependent workflows.
   - `backend/services/app/api/routers/workflows.py` maps any `KeyError` during test-run to a missing workflow, even when the missing resource is the referenced cohort.

8. Workflows can publish and execute against experiment IDs that do not exist.
   Current observed behavior:
   - A workflow with `experiment_id='missing_exp'` can publish and execute successfully.
   - `GET /api/v1/experiments/missing_exp/summary` then materializes a default summary for that synthetic experiment id.
   Why this is a bug:
   - The orchestrator silently creates measurement state for typoed or missing experiment ids instead of blocking with a contract error.

9. Workflows can continue executing even after the referenced experiment is explicitly stopped.
   Current observed behavior:
   - `POST /api/v1/experiments/{experiment_id}/stop` succeeds.
   - A published workflow that references that experiment still executes and writes exposure state.
   Why this is a bug:
   - `experiment stopped` is not enforced as an execution gate in Action Orchestrator.
   - This breaks the operator expectation that stopping an experiment halts new allocations/exposures.

### Code-review findings not yet smoke-tested

1. Import retention cleanup can delete warehouse data without checking dependent predictions.
   Relevant code:
   - `backend/services/app/application/imports.py`
   Impact:
   - An old completed import can be removed by retention while queued/running predictions still depend on its staged/curated data.

2. Prediction retention cleanup can delete export jobs without checking active export execution.
   Relevant code:
   - `backend/services/app/application/predictions.py`
   - `backend/services/app/infrastructure/repositories/sqlalchemy_control_plane.py`
   Impact:
   - Cleanup may remove export rows out from under retry/run logic because prediction deletion currently deletes dependent exports directly.

3. Cohort archive/pause/delete does not appear to cascade any workflow status transition.
   Relevant code:
   - `backend/services/app/application/cohorts.py`
   - `backend/services/app/application/workflows.py`
   Impact:
   - Published workflows can remain operational even though the audience lifecycle moved to `paused` or `archived`.

4. There is no explicit workflow delete/archive endpoint yet, so dependent execution state cannot be cleanly tombstoned.
   Relevant code:
   - `backend/services/app/api/routers/workflows.py`
   - `backend/services/app/application/workflows.py`
   Impact:
   - The platform currently has no first-class contract for safely retiring workflows while preserving downstream diagnostics.

### Recommended fix order

1. Add dependency guards before destructive operations.
   Needed checks:
   - import -> prediction
   - prediction -> export
   - connector -> import
   - cohort -> workflow
   - experiment -> workflow

2. Enforce upstream readiness before downstream create/run.
   Examples:
   - prediction requires import status `completed`
   - export requires prediction status `completed`
   - workflow execution requires cohort status `active`
   - workflow publish/execute requires experiment status `active`

3. Replace misleading 404s with explicit dependency errors.
   Suggested API contract:
   - `409 resource_in_use`
   - `409 upstream_not_ready`
   - `409 upstream_missing`
   - `423 resource_locked` for in-flight execution if we choose lock semantics instead of soft blocks

4. Prefer soft delete or terminal-state tombstones for referenced jobs.
   Why:
   - Downstream jobs, audits, and reports should still be able to resolve upstream metadata after deletion requests.
   - The same rule should apply to cohorts and experiments referenced by published workflows.

5. Add regression coverage for concurrent lifecycle actions.
   Minimum cases:
   - delete import while prediction is running
   - delete connector while import is queued/stopped
   - run prediction before import completes
   - run export before prediction completes
   - retention cleanup with dependent downstream jobs present
   - archive cohort while workflow is published
   - permanent-delete cohort while workflow is published
   - stop experiment while workflow is published
   - publish workflow with missing experiment id

### Reproduction summary

The observations above were verified on the local mock stack with ad hoc `TestClient` smoke checks on 2026-03-12. The important takeaway is that the current implementation does not reproduce the exact suspected `500` for mid-run import deletion in mock mode; the stronger confirmed issues are orphaned downstream jobs, silent zero-row runs, misleading `404` responses, and Action Orchestrator continuing to execute against archived/deleted/stopped upstream resources.
