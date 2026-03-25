from __future__ import annotations

import base64
import json
import math
import os
import pickle
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List

import pandas as pd

from app.core.request_context import get_request_context
from bigquery_service import BigQueryService, get_shared_bigquery_service


FEATURE_COLUMNS = [
    "days_since_last_seen",
    "sessions_7d",
    "sessions_30d",
    "lifetime_events",
    "lifetime_revenue_usd",
    "prior_holdout_exposures_30d",
    "prior_treatment_exposures_30d",
]

DEFAULT_SCORE_THRESHOLDS = {
    "high": 0.70,
    "medium": 0.40,
}

DEFAULT_THRESHOLD_STEPS = [0.85, 0.75, 0.65, 0.55]
DEFAULT_MIN_TRAIN_ROWS = 12


def _utcnow() -> datetime:
    return datetime.utcnow()


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        return datetime.fromisoformat(str(value)).replace(tzinfo=None)
    except Exception:
        return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in ("", None):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _count_sessions(events: pd.DataFrame) -> int:
    if events.empty:
        return 0
    if "event_time" not in events.columns:
        return 0
    ordered = events.sort_values(by="event_time").copy()
    ordered["event_time"] = pd.to_datetime(ordered["event_time"], errors="coerce", utc=False)
    ordered = ordered[ordered["event_time"].notna()].copy()
    if ordered.empty:
        return 0
    diffs = ordered["event_time"].diff()
    return int(1 + (diffs > pd.Timedelta(minutes=15)).sum())


@dataclass
class ProfileScore:
    baseline_churn_score: float
    predicted_churn_risk: str
    model_version: str
    model_status: str
    score_timestamp: str


class TrainingInterruptedError(RuntimeError):
    pass


class LocalChurnModelService:
    MODEL_RESOURCE_TYPE = "churn_model_version"
    MODEL_RESOURCE_ID = "churn_rescue_local"
    DATASET_RESOURCE_TYPE = "training_dataset_snapshot"
    DATASET_RESOURCE_ID = "churn_rescue_local"
    STATUS_RESOURCE_TYPE = "churn_model_training_status"
    STATUS_RESOURCE_ID = "churn_rescue_local"

    def __init__(self, repository, bigquery_service: BigQueryService | None = None):
        self.repository = repository
        self.bigquery_service = bigquery_service or get_shared_bigquery_service()

    @staticmethod
    def _default_tenant_id() -> str:
        return str(os.getenv("BOOTSTRAP_TENANT_ID", "default")).strip() or "default"

    def _training_scope_key(self) -> str:
        context = get_request_context()
        return str((context.tenant_id if context is not None else None) or self._default_tenant_id()).strip() or self._default_tenant_id()

    def _commit_session(self) -> None:
        session = getattr(self.repository, "session", None)
        if session is not None:
            session.commit()

    def _rollback_session(self) -> None:
        session = getattr(self.repository, "session", None)
        if session is not None:
            session.rollback()

    def get_active_model_payload(self) -> Dict[str, Any] | None:
        record = self.repository.get_resource(self.MODEL_RESOURCE_TYPE, self.MODEL_RESOURCE_ID)
        if record is None:
            return None
        payload = dict(record.get("payload") or {})
        if str(payload.get("status") or record.get("status") or "").lower() != "active":
            return None
        return payload

    def get_latest_model_payload(self) -> Dict[str, Any] | None:
        record = self.repository.get_resource(self.MODEL_RESOURCE_TYPE, self.MODEL_RESOURCE_ID)
        return dict((record or {}).get("payload") or {}) if record is not None else None

    def list_model_versions(self) -> List[Dict[str, Any]]:
        return self.repository.list_resource_versions(self.MODEL_RESOURCE_TYPE, self.MODEL_RESOURCE_ID)

    def get_training_status(self) -> Dict[str, Any]:
        record = self.repository.get_resource(self.STATUS_RESOURCE_TYPE, self.STATUS_RESOURCE_ID)
        return dict((record or {}).get("payload") or {})

    def mark_training_started(self, *, reference_time: str | None = None, min_rows: int = DEFAULT_MIN_TRAIN_ROWS) -> Dict[str, Any]:
        resolved_time = _parse_dt(reference_time) or _utcnow()
        payload = {
            "reference_time": resolved_time.isoformat(),
            "status": "running",
            "stage": "building_dataset",
            "started_at": resolved_time.isoformat(),
            "trained_at": None,
            "min_rows_required": max(6, int(min_rows)),
            "row_count": 0,
            "class_balance": {},
        }
        self._persist_training_status(payload)
        self._commit_session()
        return payload

    def request_stop_training(self, *, reason: str = "Stopped by user.") -> Dict[str, Any]:
        training_status = self.get_training_status() or {}
        status = str(training_status.get("status") or "").lower()
        if status == "stopped":
            return training_status
        if status == "stopping":
            return training_status
        if status != "running":
            raise ValueError("Only running local model training can be stopped.")
        training_status.update(
            {
                "status": "stopping",
                "stop_requested": True,
                "stop_requested_at": _utcnow().isoformat(),
                "stop_reason": reason,
            }
        )
        self._persist_training_status(training_status)
        self._commit_session()
        return training_status

    def mark_training_stopped(self, *, reason: str = "Stopped by user.") -> Dict[str, Any]:
        training_status = self.get_training_status() or {}
        training_status.update(
            {
                "status": "stopped",
                "stage": "stopped",
                "trained_at": _utcnow().isoformat(),
                "stop_requested": False,
                "stop_reason": reason,
            }
        )
        self._persist_training_status(training_status)
        self._commit_session()
        return training_status

    def is_stop_requested(self) -> bool:
        session = getattr(self.repository, "session", None)
        if session is not None:
            session.expire_all()
        training_status = self.get_training_status() or {}
        return bool(training_status.get("stop_requested")) or str(training_status.get("status") or "").lower() in {"stopping", "stopped"}

    def get_model_readiness(self, *, min_rows: int | None = None) -> Dict[str, Any]:
        latest_model = self.get_latest_model_payload() or {}
        training_status = self.get_training_status() or {}
        min_rows_required = max(6, int(training_status.get("min_rows_required") or min_rows or DEFAULT_MIN_TRAIN_ROWS))
        return self._build_model_readiness(latest_model, training_status, min_rows_required)

    def sanitize_payload(self, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        if payload is None:
            return None
        sanitized = dict(payload)
        artifact = dict(sanitized.get("artifact") or {})
        artifact.pop("serialized_sklearn_model", None)
        sanitized["artifact"] = artifact
        dataset = dict(sanitized.get("dataset") or {})
        if dataset.get("rows"):
            dataset["rows_preview"] = dataset["rows"][:10]
            dataset.pop("rows", None)
        sanitized["dataset"] = dataset
        return sanitized

    def build_training_dataset(
        self,
        *,
        reference_time: str | None = None,
        persist: bool = True,
        should_stop: Callable[[], bool] | None = None,
        progress_callback: Callable[[Dict[str, Any]], None] | None = None,
    ) -> Dict[str, Any]:
        resolved_time = _parse_dt(reference_time) or _utcnow()
        exposures = [
            item.get("payload") or {}
            for item in self.repository.list_resource_events("experiment", event_type="exposure", limit=5000)
        ]
        outcomes = [
            item.get("payload") or {}
            for item in self.repository.list_resource_events("experiment", event_type="outcome", limit=5000)
        ]
        exposures = [
            item
            for item in exposures
            if _parse_dt(item.get("exposed_at") or item.get("recorded_at")) is not None
            and (_parse_dt(item.get("exposed_at") or item.get("recorded_at")) or resolved_time) <= resolved_time
        ]
        exposures.sort(key=lambda item: _parse_dt(item.get("exposed_at") or item.get("recorded_at")) or datetime.min)

        curated_rows = self.bigquery_service.get_rows_for_alias("fact_events_unified")
        events_by_user = self._group_events_by_user(curated_rows)
        exposure_history = self._group_exposures_by_user(exposures)
        outcomes_by_key = self._group_outcomes(outcomes)

        rows: List[Dict[str, Any]] = []
        historical_rows = 0
        measurement_rows = 0
        baseline_rows = 0
        total_users = len(events_by_user)
        total_exposures = len(exposures)
        if progress_callback is not None:
            progress_callback(
                {
                    "stage": "building_dataset",
                    "row_count": 0,
                    "historical_rows": 0,
                    "measurement_rows": 0,
                    "users_processed": 0,
                    "users_total": total_users,
                    "exposures_processed": 0,
                    "exposures_total": total_exposures,
                }
            )
        for index, (user_id, user_events) in enumerate(events_by_user.items(), start=1):
            if should_stop is not None and should_stop():
                raise TrainingInterruptedError("Stopped by user.")
            user_history = self._build_historical_training_rows(
                user_id=user_id,
                user_events=user_events,
                prior_exposures=exposure_history.get(user_id, []),
                reference_time=resolved_time,
                should_stop=should_stop,
            )
            rows.extend(user_history)
            historical_rows += len(user_history)
            baseline_rows += len(user_history)
            if progress_callback is not None and (index == total_users or index % 10 == 0):
                progress_callback(
                    {
                        "stage": "building_dataset",
                        "row_count": baseline_rows,
                        "historical_rows": historical_rows,
                        "measurement_rows": measurement_rows,
                        "users_processed": index,
                        "users_total": total_users,
                        "exposures_processed": 0,
                        "exposures_total": total_exposures,
                    }
                )
        for exposure_index, exposure in enumerate(exposures, start=1):
            if should_stop is not None and should_stop():
                raise TrainingInterruptedError("Stopped by user.")
            user_id = str(exposure.get("user_id") or exposure.get("canonical_user_id") or "").strip()
            exposure_time = _parse_dt(exposure.get("exposed_at") or exposure.get("recorded_at"))
            if not user_id or exposure_time is None:
                continue
            outcome_window_end = exposure_time + timedelta(days=7)
            if outcome_window_end > resolved_time:
                continue
            user_events = events_by_user.get(user_id, pd.DataFrame())
            prior_exposures = exposure_history.get(user_id, [])
            feature_row = self._build_feature_row(user_id, exposure_time, user_events, prior_exposures)
            feature_row.update(
                {
                    "row_source": "experiment_exposure",
                    "canonical_user_id": user_id,
                    "experiment_id": exposure.get("experiment_id"),
                    "workflow_id": exposure.get("workflow_id"),
                    "cohort_id": exposure.get("cohort_id"),
                    "group": exposure.get("group") or "holdout",
                    "template_id": exposure.get("template_id"),
                    "variant_id": exposure.get("variant_id") or exposure.get("group") or "treatment_a",
                    "channel": exposure.get("channel"),
                    "action_execution_id": exposure.get("action_execution_id"),
                    "exposure_time": exposure_time.isoformat(),
                    "outcome_window_end": outcome_window_end.isoformat(),
                }
            )

            outcome_summary = self._resolve_outcomes(
                exposure=exposure,
                exposure_time=exposure_time,
                user_events=user_events,
                outcomes_by_key=outcomes_by_key,
                max_observed_at=resolved_time,
            )
            feature_row.update(outcome_summary)

            baseline_label = None
            group = str(feature_row.get("group") or "holdout")
            execution_status = str(exposure.get("execution_status") or "")
            if group in {"holdout", "excluded"} or execution_status in {"holdout", "policy_blocked"} or not exposure.get("action_execution_id"):
                baseline_label = 0 if feature_row["campaign_outcome_label"] == 1 else 1
            feature_row["baseline_churn_label"] = baseline_label
            rows.append(feature_row)
            measurement_rows += 1
            if baseline_label in {0, 1}:
                baseline_rows += 1
            if progress_callback is not None and (exposure_index == total_exposures or exposure_index % 25 == 0):
                progress_callback(
                    {
                        "stage": "building_dataset",
                        "row_count": baseline_rows,
                        "historical_rows": historical_rows,
                        "measurement_rows": measurement_rows,
                        "users_processed": total_users,
                        "users_total": total_users,
                        "exposures_processed": exposure_index,
                        "exposures_total": total_exposures,
                    }
                )

        dataset_payload = {
            "dataset_id": f"train_{resolved_time.strftime('%Y%m%d%H%M%S')}",
            "reference_time": resolved_time.isoformat(),
            "window_days": 7,
            "feature_columns": FEATURE_COLUMNS,
            "rows": rows,
            "row_count": len(rows),
            "historical_rows": historical_rows,
            "measurement_rows": measurement_rows,
            "baseline_rows": baseline_rows,
            "treatment_rows": len([row for row in rows if str(row.get("group") or "") not in {"holdout", "excluded"}]),
        }
        if persist:
            self._upsert_versioned_resource(
                self.DATASET_RESOURCE_TYPE,
                self.DATASET_RESOURCE_ID,
                status="ready",
                payload=dataset_payload,
            )
        return dataset_payload

    def train_model(
        self,
        *,
        reference_time: str | None = None,
        min_rows: int = 12,
        should_stop: Callable[[], bool] | None = None,
        persist_initial_status: bool = True,
    ) -> Dict[str, Any]:
        resolved_time = _parse_dt(reference_time) or _utcnow()
        min_rows_required = max(6, int(min_rows))
        if persist_initial_status:
            training_status = self.mark_training_started(reference_time=resolved_time.isoformat(), min_rows=min_rows_required)
        else:
            training_status = self.get_training_status() or {}
            training_status.setdefault("reference_time", resolved_time.isoformat())
            training_status.setdefault("started_at", resolved_time.isoformat())
            training_status.setdefault("min_rows_required", min_rows_required)
            training_status.setdefault("row_count", 0)
            training_status.setdefault("class_balance", {})

        try:
            if should_stop is not None and should_stop():
                raise TrainingInterruptedError("Stopped by user.")

            def _update_progress(update: Dict[str, Any]) -> None:
                training_status.update(update)
                self._persist_training_status(training_status)
                self._commit_session()

            dataset = self.build_training_dataset(
                reference_time=resolved_time.isoformat(),
                persist=True,
                should_stop=should_stop,
                progress_callback=_update_progress,
            )
            rows = [item for item in dataset.get("rows") or [] if item.get("baseline_churn_label") in {0, 1}]
            labels = [int(item["baseline_churn_label"]) for item in rows]
            class_balance = {label: labels.count(label) for label in sorted(set(labels))}

            training_status.update(
                {
                    "dataset_id": dataset["dataset_id"],
                    "row_count": len(rows),
                    "class_balance": class_balance,
                    "stage": "evaluating_dataset",
                }
            )
            self._persist_training_status(training_status)
            self._commit_session()

            if len(rows) < min_rows_required or len(class_balance) < 2 or min(class_balance.values()) < 2:
                fallback_payload = {
                    "model_version": "heuristic_v1",
                    "status": "fallback",
                    "trained_at": resolved_time.isoformat(),
                    "reference_time": resolved_time.isoformat(),
                    "dataset": {
                        "dataset_id": dataset["dataset_id"],
                        "row_count": dataset["row_count"],
                        "baseline_rows": dataset["baseline_rows"],
                    },
                    "metrics": {
                        "validation_accuracy": None,
                        "heuristic_accuracy": None,
                    },
                    "artifact": {"feature_names": FEATURE_COLUMNS, "thresholds": DEFAULT_SCORE_THRESHOLDS},
                    "reason": "Not enough untreated rows with class balance for model training.",
                }
                self._upsert_versioned_resource(
                    self.MODEL_RESOURCE_TYPE,
                    self.MODEL_RESOURCE_ID,
                    status="fallback",
                    payload=fallback_payload,
                )
                training_status.update(
                    {
                        "status": "insufficient_data",
                        "stage": "completed",
                        "trained_at": resolved_time.isoformat(),
                    }
                )
                training_status["readiness"] = self._build_model_readiness(fallback_payload, training_status, min_rows_required)
                self._persist_training_status(training_status)
                self._commit_session()
                return fallback_payload

            train_rows, validation_rows = self._split_rows_temporally(rows)
            if not train_rows or not validation_rows:
                fallback_payload = {
                    "model_version": "heuristic_v1",
                    "status": "fallback",
                    "trained_at": resolved_time.isoformat(),
                    "reference_time": resolved_time.isoformat(),
                    "dataset": {
                        "dataset_id": dataset["dataset_id"],
                        "row_count": dataset["row_count"],
                        "baseline_rows": dataset["baseline_rows"],
                    },
                    "metrics": {
                        "validation_accuracy": None,
                        "heuristic_accuracy": None,
                    },
                    "artifact": {"feature_names": FEATURE_COLUMNS, "thresholds": DEFAULT_SCORE_THRESHOLDS},
                    "reason": "Temporal validation split did not produce train and validation rows.",
                }
                self._upsert_versioned_resource(
                    self.MODEL_RESOURCE_TYPE,
                    self.MODEL_RESOURCE_ID,
                    status="fallback",
                    payload=fallback_payload,
                )
                training_status.update(
                    {
                        "status": "insufficient_data",
                        "stage": "completed",
                        "trained_at": resolved_time.isoformat(),
                    }
                )
                training_status["readiness"] = self._build_model_readiness(fallback_payload, training_status, min_rows_required)
                self._persist_training_status(training_status)
                self._commit_session()
                return fallback_payload

            training_status.update({"stage": "fitting_model"})
            self._persist_training_status(training_status)
            self._commit_session()
            model_state = self._fit_model(train_rows)

            training_status.update({"stage": "validating_model"})
            self._persist_training_status(training_status)
            self._commit_session()
            validation_accuracy = self._score_rows(validation_rows, model_state)
            heuristic_accuracy = self._score_rows(validation_rows, None)
            model_status = "active" if validation_accuracy >= heuristic_accuracy else "fallback"
            model_version = f"crm_{resolved_time.strftime('%Y%m%d%H%M%S')}"

            payload = {
                "model_version": model_version,
                "status": model_status,
                "trained_at": resolved_time.isoformat(),
                "reference_time": resolved_time.isoformat(),
                "dataset": {
                    "dataset_id": dataset["dataset_id"],
                    "row_count": dataset["row_count"],
                    "baseline_rows": dataset["baseline_rows"],
                    "validation_rows": len(validation_rows),
                    "training_rows": len(train_rows),
                },
                "metrics": {
                    "validation_accuracy": round(validation_accuracy, 4),
                    "heuristic_accuracy": round(heuristic_accuracy, 4),
                },
                "artifact": {
                    **model_state,
                    "thresholds": DEFAULT_SCORE_THRESHOLDS,
                },
            }
            self._upsert_versioned_resource(
                self.MODEL_RESOURCE_TYPE,
                self.MODEL_RESOURCE_ID,
                status=model_status,
                payload=payload,
            )
            training_status.update(
                {
                    "status": model_status,
                    "stage": "completed",
                    "trained_at": resolved_time.isoformat(),
                    "model_version": model_version,
                    "metrics": payload["metrics"],
                }
            )
            training_status["readiness"] = self._build_model_readiness(payload, training_status, min_rows_required)
            self._persist_training_status(training_status)
            self._commit_session()
            return payload
        except TrainingInterruptedError as exc:
            self._rollback_session()
            stopped_status = self.mark_training_stopped(reason=str(exc))
            latest_model = self.get_latest_model_payload() or {}
            return {
                "model_version": str(latest_model.get("model_version") or "heuristic_v1"),
                "status": "stopped",
                "reason": str(exc),
                "training_status": stopped_status,
            }
        except Exception as exc:
            self._rollback_session()
            training_status.update(
                {
                    "status": "failed",
                    "stage": "failed",
                    "trained_at": _utcnow().isoformat(),
                    "error": str(exc),
                }
            )
            self._persist_training_status(training_status)
            self._commit_session()
            raise

    def score_profile(self, profile: Dict[str, Any]) -> ProfileScore:
        resolved_time = _utcnow().isoformat()
        model_payload = self.get_active_model_payload()
        if not model_payload:
            probability = self._heuristic_probability_from_features(self._profile_to_feature_row(profile))
            return ProfileScore(
                baseline_churn_score=round(probability, 4),
                predicted_churn_risk=self._score_to_risk(probability),
                model_version="heuristic_v1",
                model_status="fallback",
                score_timestamp=resolved_time,
            )

        artifact = dict(model_payload.get("artifact") or {})
        probability = self._predict_probability_from_artifact(self._profile_to_feature_row(profile), artifact)
        return ProfileScore(
            baseline_churn_score=round(probability, 4),
            predicted_churn_risk=self._score_to_risk(probability, thresholds=artifact.get("thresholds") or DEFAULT_SCORE_THRESHOLDS),
            model_version=str(model_payload.get("model_version") or "heuristic_v1"),
            model_status=str(model_payload.get("status") or "active"),
            score_timestamp=resolved_time,
        )

    def _fit_model(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler

        frame = pd.DataFrame(rows)
        x_train = frame[FEATURE_COLUMNS].apply(lambda column: column.map(lambda value: _safe_float(value, 0.0)))
        y_train = frame["baseline_churn_label"].astype(int)

        imputer = SimpleImputer(strategy="median")
        x_imputed = imputer.fit_transform(x_train)
        scaler = StandardScaler()
        x_scaled = scaler.fit_transform(x_imputed)
        classifier = LogisticRegression(max_iter=500, solver="liblinear")
        classifier.fit(x_scaled, y_train)

        serialized_model = base64.b64encode(pickle.dumps(classifier)).decode("utf-8")
        return {
            "feature_names": FEATURE_COLUMNS,
            "imputer_statistics": [float(value) for value in imputer.statistics_.tolist()],
            "scaler_mean": [float(value) for value in scaler.mean_.tolist()],
            "scaler_scale": [float(value if value != 0 else 1.0) for value in scaler.scale_.tolist()],
            "coefficients": [float(value) for value in classifier.coef_[0].tolist()],
            "intercept": float(classifier.intercept_[0]),
            "serialized_sklearn_model": serialized_model,
        }

    def _score_rows(self, rows: List[Dict[str, Any]], artifact: Dict[str, Any] | None) -> float:
        if not rows:
            return 0.0
        correct = 0
        for row in rows:
            expected = int(row.get("baseline_churn_label") or 0)
            if artifact is None:
                probability = self._heuristic_probability_from_features(row)
            else:
                probability = self._predict_probability_from_artifact(row, artifact)
            predicted = 1 if probability >= 0.5 else 0
            if predicted == expected:
                correct += 1
        return correct / max(1, len(rows))

    def _predict_probability_from_artifact(self, row: Dict[str, Any], artifact: Dict[str, Any]) -> float:
        feature_values = []
        statistics = list(artifact.get("imputer_statistics") or [])
        means = list(artifact.get("scaler_mean") or [])
        scales = [value if value not in (0, None) else 1.0 for value in list(artifact.get("scaler_scale") or [])]
        coefficients = list(artifact.get("coefficients") or [])
        for index, name in enumerate(FEATURE_COLUMNS):
            value = row.get(name)
            if value in ("", None):
                value = statistics[index] if index < len(statistics) else 0.0
            value = _safe_float(value, statistics[index] if index < len(statistics) else 0.0)
            mean = means[index] if index < len(means) else 0.0
            scale = scales[index] if index < len(scales) else 1.0
            feature_values.append((value - mean) / scale)
        logit = float(artifact.get("intercept") or 0.0)
        for index, value in enumerate(feature_values):
            coefficient = coefficients[index] if index < len(coefficients) else 0.0
            logit += coefficient * value
        return 1.0 / (1.0 + math.exp(-logit))

    def _group_events_by_user(self, rows: Iterable[Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            user_id = str(row.get("canonical_user_id") or row.get("player_id") or "").strip()
            event_time = _parse_dt(row.get("event_time"))
            if not user_id or event_time is None:
                continue
            payload = dict(row)
            payload["event_time"] = event_time
            grouped.setdefault(user_id, []).append(payload)
        result: Dict[str, pd.DataFrame] = {}
        for user_id, items in grouped.items():
            frame = pd.DataFrame(items)
            if frame.empty:
                continue
            frame["event_time"] = pd.to_datetime(frame["event_time"], errors="coerce", utc=False)
            frame = frame[frame["event_time"].notna()].sort_values(by="event_time").copy()
            if not frame.empty:
                result[user_id] = frame
        return result

    def _build_historical_training_rows(
        self,
        *,
        user_id: str,
        user_events: pd.DataFrame,
        prior_exposures: List[Dict[str, Any]],
        reference_time: datetime,
        should_stop: Callable[[], bool] | None = None,
    ) -> List[Dict[str, Any]]:
        if user_events.empty:
            return []
        event_times = sorted({_parse_dt(value) for value in user_events["event_time"].tolist() if _parse_dt(value) is not None})
        rows: List[Dict[str, Any]] = []
        for index, cutoff_time in enumerate(event_times, start=1):
            if should_stop is not None and index % 25 == 0 and should_stop():
                raise TrainingInterruptedError("Stopped by user.")
            if cutoff_time is None:
                continue
            window_end = cutoff_time + timedelta(days=7)
            if window_end > reference_time:
                continue
            feature_row = self._build_feature_row(user_id, cutoff_time, user_events, prior_exposures)
            future_events = user_events[
                (user_events["event_time"] > cutoff_time)
                & (user_events["event_time"] <= pd.Timestamp(window_end))
            ].copy()
            returned_within_window = not future_events.empty
            purchased_within_window = False
            if not future_events.empty and "event_type" in future_events.columns:
                purchased_within_window = future_events["event_type"].map(lambda value: str(value) == "item_purchased").any()
            feature_row.update(
                {
                    "row_source": "historical_snapshot",
                    "canonical_user_id": user_id,
                    "experiment_id": None,
                    "workflow_id": None,
                    "cohort_id": None,
                    "group": "untreated",
                    "template_id": None,
                    "variant_id": "baseline",
                    "channel": None,
                    "action_execution_id": None,
                    "exposure_time": cutoff_time.isoformat(),
                    "outcome_window_end": window_end.isoformat(),
                    "provider_engaged_within_7d": 0,
                    "product_returned_within_7d": 1 if returned_within_window else 0,
                    "product_purchase_within_7d": 1 if purchased_within_window else 0,
                    "campaign_outcome_label": 1 if returned_within_window else 0,
                    "product_outcome_type": "purchase" if purchased_within_window else ("return" if returned_within_window else None),
                    "provider_callback_ids": [],
                    "baseline_churn_label": 0 if returned_within_window else 1,
                }
            )
            rows.append(feature_row)
        return rows

    def _group_exposures_by_user(self, exposures: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for item in exposures:
            user_id = str(item.get("user_id") or item.get("canonical_user_id") or "").strip()
            if not user_id:
                continue
            grouped.setdefault(user_id, []).append(item)
        for user_id in grouped:
            grouped[user_id].sort(key=lambda item: _parse_dt(item.get("exposed_at") or item.get("recorded_at")) or datetime.min)
        return grouped

    def _group_outcomes(self, outcomes: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for item in outcomes:
            keys = {
                str(item.get("action_execution_id") or "").strip(),
                str(item.get("user_id") or "").strip(),
            }
            for key in keys:
                if key:
                    grouped.setdefault(key, []).append(item)
        return grouped

    def _build_feature_row(
        self,
        user_id: str,
        exposure_time: datetime,
        user_events: pd.DataFrame,
        prior_exposures: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if user_events.empty:
            base = {
                "canonical_user_id": user_id,
                "days_since_last_seen": 999,
                "sessions_7d": 0,
                "sessions_30d": 0,
                "lifetime_events": 0,
                "lifetime_revenue_usd": 0.0,
            }
        else:
            prior_events = user_events[user_events["event_time"] <= exposure_time].copy()
            if prior_events.empty:
                base = {
                    "canonical_user_id": user_id,
                    "days_since_last_seen": 999,
                    "sessions_7d": 0,
                    "sessions_30d": 0,
                    "lifetime_events": 0,
                    "lifetime_revenue_usd": 0.0,
                }
            else:
                last_seen = prior_events["event_time"].max().to_pydatetime().replace(tzinfo=None)
                window_7d = prior_events[prior_events["event_time"] >= pd.Timestamp(exposure_time - timedelta(days=7))]
                window_30d = prior_events[prior_events["event_time"] >= pd.Timestamp(exposure_time - timedelta(days=30))]
                purchases = prior_events[prior_events["event_type"].map(lambda value: str(value) == "item_purchased")]
                revenue = 0.0
                if not purchases.empty and "event_properties" in purchases.columns:
                    revenue = float(
                        purchases["event_properties"].apply(
                            lambda value: _safe_float((value or {}).get("revenue_usd", 0.0), 0.0)
                            if isinstance(value, dict)
                            else 0.0
                        ).sum()
                    )
                base = {
                    "canonical_user_id": user_id,
                    "days_since_last_seen": max(0, (exposure_time - last_seen).days),
                    "sessions_7d": _count_sessions(window_7d),
                    "sessions_30d": _count_sessions(window_30d),
                    "lifetime_events": int(len(prior_events)),
                    "lifetime_revenue_usd": round(revenue, 4),
                }

        prior_window_start = exposure_time - timedelta(days=30)
        holdout_exposures = 0
        treatment_exposures = 0
        for item in prior_exposures:
            item_time = _parse_dt(item.get("exposed_at") or item.get("recorded_at"))
            if item_time is None or item_time >= exposure_time or item_time < prior_window_start:
                continue
            if str(item.get("group") or "") == "holdout":
                holdout_exposures += 1
            else:
                treatment_exposures += 1
        base["prior_holdout_exposures_30d"] = holdout_exposures
        base["prior_treatment_exposures_30d"] = treatment_exposures
        return base

    def _resolve_outcomes(
        self,
        *,
        exposure: Dict[str, Any],
        exposure_time: datetime,
        user_events: pd.DataFrame,
        outcomes_by_key: Dict[str, List[Dict[str, Any]]],
        max_observed_at: datetime | None = None,
    ) -> Dict[str, Any]:
        end_time = exposure_time + timedelta(days=7)
        observed_end = min(end_time, max_observed_at) if max_observed_at is not None else end_time
        product_returned = False
        product_purchased = False
        if not user_events.empty:
            future_events = user_events[
                (user_events["event_time"] > exposure_time)
                & (user_events["event_time"] <= pd.Timestamp(observed_end))
            ].copy()
            product_returned = not future_events.empty
            if not future_events.empty and "event_type" in future_events.columns:
                product_purchased = future_events["event_type"].map(lambda value: str(value) == "item_purchased").any()

        matched_outcomes: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for key in (
            str(exposure.get("action_execution_id") or "").strip(),
            str(exposure.get("user_id") or "").strip(),
        ):
            for outcome in outcomes_by_key.get(key, []):
                dedupe_key = json.dumps(outcome, sort_keys=True, default=str)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                occurred_at = _parse_dt(outcome.get("occurred_at"))
                if occurred_at is None or occurred_at < exposure_time or occurred_at > observed_end:
                    continue
                matched_outcomes.append(outcome)

        provider_engaged = False
        provider_returned = False
        product_outcome_type = None
        provider_callback_ids = []
        for outcome in matched_outcomes:
            outcome_name = str(outcome.get("outcome_name") or "").lower()
            product_outcome_type = product_outcome_type or outcome.get("product_outcome_type")
            callback_id = str(outcome.get("provider_callback_id") or "").strip()
            if callback_id:
                provider_callback_ids.append(callback_id)
            if outcome_name in {"opened", "clicked", "engaged"}:
                provider_engaged = True
            if outcome_name in {"returned", "returned_to_game", "converted", "purchase"}:
                provider_returned = True
                if product_outcome_type is None:
                    product_outcome_type = "purchase" if outcome_name == "purchase" else "return"

        campaign_outcome = 1 if (product_returned or provider_returned) else 0
        return {
            "provider_engaged_within_7d": 1 if provider_engaged else 0,
            "product_returned_within_7d": 1 if product_returned else 0,
            "product_purchase_within_7d": 1 if product_purchased else 0,
            "campaign_outcome_label": campaign_outcome,
            "product_outcome_type": product_outcome_type or ("purchase" if product_purchased else ("return" if product_returned else None)),
            "provider_callback_ids": provider_callback_ids,
        }

    def _split_rows_temporally(self, rows: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        ordered = sorted(rows, key=lambda item: str(item.get("exposure_time") or ""))
        if len(ordered) < 4:
            return ordered, []
        split_index = max(2, int(len(ordered) * 0.8))
        split_index = min(split_index, len(ordered) - 1)
        train_rows = ordered[:split_index]
        validation_rows = ordered[split_index:]
        train_labels = {int(item.get("baseline_churn_label") or 0) for item in train_rows}
        validation_labels = {int(item.get("baseline_churn_label") or 0) for item in validation_rows}
        if len(train_labels) < 2 or len(validation_labels) < 2:
            by_label: Dict[int, List[Dict[str, Any]]] = {}
            for item in ordered:
                by_label.setdefault(int(item.get("baseline_churn_label") or 0), []).append(item)
            stratified_train: List[Dict[str, Any]] = []
            stratified_validation: List[Dict[str, Any]] = []
            for bucket in by_label.values():
                bucket_validation_count = max(1, int(len(bucket) * 0.25))
                bucket_validation_count = min(bucket_validation_count, max(1, len(bucket) - 1))
                stratified_train.extend(bucket[:-bucket_validation_count])
                stratified_validation.extend(bucket[-bucket_validation_count:])
            if stratified_train and stratified_validation:
                train_rows = sorted(stratified_train, key=lambda item: str(item.get("exposure_time") or ""))
                validation_rows = sorted(stratified_validation, key=lambda item: str(item.get("exposure_time") or ""))
        return train_rows, validation_rows

    def _profile_to_feature_row(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "days_since_last_seen": _safe_float(profile.get("days_since_last_seen"), 999.0),
            "sessions_7d": _safe_float(profile.get("sessions_7d"), _safe_float(profile.get("total_sessions"), 0.0)),
            "sessions_30d": _safe_float(profile.get("sessions_30d"), _safe_float(profile.get("total_sessions"), 0.0)),
            "lifetime_events": _safe_float(profile.get("total_events"), 0.0),
            "lifetime_revenue_usd": _safe_float(profile.get("total_revenue"), 0.0),
            "prior_holdout_exposures_30d": _safe_float(profile.get("prior_holdout_exposures_30d"), 0.0),
            "prior_treatment_exposures_30d": _safe_float(profile.get("prior_treatment_exposures_30d"), 0.0),
        }

    def _heuristic_probability_from_features(self, row: Dict[str, Any]) -> float:
        score = 0.20
        days_since_last_seen = _safe_float(row.get("days_since_last_seen"), 999.0)
        sessions_7d = _safe_float(row.get("sessions_7d"), 0.0)
        sessions_30d = _safe_float(row.get("sessions_30d"), 0.0)
        lifetime_revenue = _safe_float(row.get("lifetime_revenue_usd"), 0.0)
        prior_treatments = _safe_float(row.get("prior_treatment_exposures_30d"), 0.0)

        if days_since_last_seen >= 14:
            score += 0.18
        elif days_since_last_seen >= 7:
            score += 0.10
        elif days_since_last_seen >= 3:
            score += 0.04
        if sessions_7d == 0:
            score += 0.08
        elif sessions_7d <= 2:
            score += 0.04
        if sessions_30d <= 3:
            score += 0.06
        elif sessions_30d <= 6:
            score += 0.03
        if lifetime_revenue >= 50:
            score -= 0.03
        if prior_treatments >= 3:
            score += 0.02
        return max(0.01, min(0.99, score))

    def _score_to_risk(self, score: float, thresholds: Dict[str, Any] | None = None) -> str:
        resolved = dict(DEFAULT_SCORE_THRESHOLDS)
        resolved.update(dict(thresholds or {}))
        if score >= float(resolved.get("high", 0.70)):
            return "high"
        if score >= float(resolved.get("medium", 0.40)):
            return "medium"
        return "low"

    def _persist_training_status(self, payload: Dict[str, Any]) -> None:
        self.repository.upsert_resource(
            self.STATUS_RESOURCE_TYPE,
            self.STATUS_RESOURCE_ID,
            status=str(payload.get("status") or "ready"),
            name=self.STATUS_RESOURCE_ID,
            payload=payload,
        )

    def _build_model_readiness(
        self,
        latest_model: Dict[str, Any] | None,
        training_status: Dict[str, Any] | None,
        min_rows_required: int,
    ) -> Dict[str, Any]:
        latest_model = dict(latest_model or {})
        training_status = dict(training_status or {})
        latest_status = str(latest_model.get("status") or "").lower()
        training_state = str(training_status.get("status") or "").lower()
        latest_model_version = str(latest_model.get("model_version") or "").strip()
        metrics = dict(latest_model.get("metrics") or training_status.get("metrics") or {})
        baseline_rows = int(
            training_status.get("row_count")
            or ((latest_model.get("dataset") or {}).get("baseline_rows") or 0)
            or 0
        )
        class_balance = dict(training_status.get("class_balance") or {})
        last_trained_at = latest_model.get("trained_at") or training_status.get("trained_at")

        if latest_status == "active":
            state = "ready"
            using_model_version = latest_model_version or "heuristic_v1"
            reason = (
                f"Local supervised model {using_model_version} is active and currently used for local scoring."
            )
        elif training_state == "running":
            state = "learning"
            using_model_version = "heuristic_v1"
            reason = "Local model training is running. Local predictions continue using heuristic_v1 until training finishes."
        elif not latest_model and not training_status:
            state = "untrained"
            using_model_version = "heuristic_v1"
            reason = "No local model training has been run yet. Local predictions use heuristic_v1."
        elif training_state == "fallback" or (latest_status == "fallback" and latest_model_version not in {"", "heuristic_v1"}):
            state = "fallback"
            using_model_version = "heuristic_v1"
            reason = "Latest trained local model did not outperform heuristic_v1, so local predictions continue using heuristic_v1."
        else:
            state = "learning"
            using_model_version = "heuristic_v1"
            if baseline_rows <= 0:
                reason = (
                    f"Using heuristic_v1 until the local model has at least {min_rows_required} labeled rows with class balance."
                )
            else:
                reason = (
                    f"Using heuristic_v1 until the local model has enough labeled data. "
                    f"Current labeled rows: {baseline_rows}/{min_rows_required}."
                )

        return {
            "state": state,
            "using_model_version": using_model_version,
            "reason": reason,
            "last_trained_at": last_trained_at,
            "baseline_rows": baseline_rows,
            "min_rows_required": int(min_rows_required),
            "class_balance": class_balance,
            "validation_accuracy": metrics.get("validation_accuracy"),
            "heuristic_accuracy": metrics.get("heuristic_accuracy"),
        }

    def _upsert_versioned_resource(
        self,
        resource_type: str,
        resource_id: str,
        *,
        status: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        record = self.repository.upsert_resource(
            resource_type,
            resource_id,
            status=status,
            name=resource_id,
            payload=payload,
        )
        existing_versions = self.repository.list_resource_versions(resource_type, resource_id)
        next_version = 1 + max((int(item.get("version") or 0) for item in existing_versions), default=0)
        self.repository.create_resource_version(
            resource_type,
            resource_id,
            version=next_version,
            payload=payload,
        )
        self.repository.record_action(
            f"{resource_type}_updated",
            resource_type,
            resource_id,
            {
                "version": next_version,
                "status": status,
                "payload": {
                    key: value
                    for key, value in payload.items()
                    if key not in {"artifact"}
                },
            },
        )
        return record.get("payload") or payload
