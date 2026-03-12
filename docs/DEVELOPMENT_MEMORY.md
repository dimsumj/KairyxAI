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

### Recommended fix order

1. Add dependency guards before destructive operations.
   Needed checks:
   - import -> prediction
   - prediction -> export
   - connector -> import

2. Enforce upstream readiness before downstream create/run.
   Examples:
   - prediction requires import status `completed`
   - export requires prediction status `completed`

3. Replace misleading 404s with explicit dependency errors.
   Suggested API contract:
   - `409 resource_in_use`
   - `409 upstream_not_ready`
   - `409 upstream_missing`

4. Prefer soft delete or terminal-state tombstones for referenced jobs.
   Why:
   - Downstream jobs, audits, and reports should still be able to resolve upstream metadata after deletion requests.

5. Add regression coverage for concurrent lifecycle actions.
   Minimum cases:
   - delete import while prediction is running
   - delete connector while import is queued/stopped
   - run prediction before import completes
   - run export before prediction completes
   - retention cleanup with dependent downstream jobs present

### Reproduction summary

The observations above were verified on the local mock stack with ad hoc `TestClient` smoke checks on 2026-03-12. The important takeaway is that the current implementation does not reproduce the exact suspected `500` for mid-run import deletion in mock mode; the stronger confirmed issues are orphaned downstream jobs, silent zero-row runs, and misleading `404` responses.
