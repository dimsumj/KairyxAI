from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from app.application.braze_provider import BrazeApiError, BrazeProviderService
from app.application.cohorts import CohortService
from app.application.provider_connections import ProviderConnectionService
from app.application.sendgrid_provider import SendGridApiError, SendGridProviderService
from app.core.settings import Settings, get_settings
from bigquery_service import BigQueryService, get_shared_bigquery_service


class EmailCampaignService:
    _RESOURCE_TYPE = "email_campaign"
    _SENDGRID_MAX_PERSONALIZATIONS = 1000
    _BRAZE_MAX_RECIPIENTS = 50
    _DEEPLINK_FIELD_DEFAULT = "deeplink_url"

    def __init__(
        self,
        repository,
        settings: Settings | None = None,
        bigquery_service: BigQueryService | None = None,
    ):
        self.repository = repository
        self.settings = settings or get_settings()
        self.bigquery_service = bigquery_service or get_shared_bigquery_service()
        self.provider_connections = ProviderConnectionService(repository)
        self.sendgrid = SendGridProviderService(repository)
        self.braze = BrazeProviderService(repository)
        self.cohorts = CohortService(repository, self.bigquery_service)

    def list_campaigns(self, *, status: str | None = None) -> List[Dict[str, Any]]:
        items = [self._to_response(item) for item in self.repository.list_resources(self._RESOURCE_TYPE)]
        status_filter = str(status or "").strip().lower()
        if not status_filter:
            return items
        return [item for item in items if self._matches_status_filter(item, status_filter)]

    def get_campaign(self, email_campaign_id: str) -> Dict[str, Any] | None:
        record = self.repository.get_resource(self._RESOURCE_TYPE, email_campaign_id)
        return self._to_response(record) if record else None

    def create_campaign(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        campaign_id = f"ec_{uuid.uuid4().hex[:20]}"
        normalized = self._normalize_campaign_payload(payload, existing=None, email_campaign_id=campaign_id)
        saved = self.repository.upsert_resource(
            self._RESOURCE_TYPE,
            campaign_id,
            status=str(normalized.get("status") or "draft"),
            name=normalized.get("name"),
            payload=normalized,
        )
        self.repository.record_resource_event(self._RESOURCE_TYPE, campaign_id, event_type="email_campaign_created", payload=normalized)
        self.repository.record_action("email_campaign_created", self._RESOURCE_TYPE, campaign_id, normalized)
        return self._to_response(saved)

    def update_campaign(self, email_campaign_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        record = self.repository.get_resource(self._RESOURCE_TYPE, email_campaign_id)
        if record is None:
            raise KeyError(email_campaign_id)
        current = dict(record.get("payload") or {})
        status = str(current.get("status") or "draft").lower()
        if status not in {"draft", "scheduled"}:
            raise ValueError("Only draft or scheduled email campaigns can be edited.")
        merged_payload = {**current, **dict(patch or {})}
        normalized = self._normalize_campaign_payload(merged_payload, existing=current, email_campaign_id=email_campaign_id)
        saved = self.repository.upsert_resource(
            self._RESOURCE_TYPE,
            email_campaign_id,
            status=str(normalized.get("status") or "draft"),
            name=normalized.get("name"),
            payload=normalized,
        )
        self.repository.record_resource_event(
            self._RESOURCE_TYPE,
            email_campaign_id,
            event_type="email_campaign_updated",
            payload={"patch": patch, "status": normalized.get("status")},
        )
        self.repository.record_action("email_campaign_updated", self._RESOURCE_TYPE, email_campaign_id, {"patch": patch})
        return self._to_response(saved)

    def send_now(self, email_campaign_id: str, *, reference_time: str | None = None) -> Dict[str, Any]:
        return self._execute_campaign(email_campaign_id, reference_time=reference_time, trigger="send_now")

    def cancel_campaign(self, email_campaign_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource(self._RESOURCE_TYPE, email_campaign_id)
        if record is None:
            raise KeyError(email_campaign_id)
        payload = dict(record.get("payload") or {})
        if str(payload.get("status") or "").lower() != "scheduled":
            raise ValueError("Only scheduled email campaigns can be cancelled.")
        payload["status"] = "cancelled"
        payload["cancelled_at"] = datetime.utcnow().isoformat()
        saved = self.repository.upsert_resource(
            self._RESOURCE_TYPE,
            email_campaign_id,
            status="cancelled",
            name=payload.get("name"),
            payload=payload,
        )
        self.repository.record_resource_event(self._RESOURCE_TYPE, email_campaign_id, event_type="email_campaign_cancelled", payload={"status": "cancelled"})
        self.repository.record_action("email_campaign_cancelled", self._RESOURCE_TYPE, email_campaign_id, {"email_campaign_id": email_campaign_id})
        return self._to_response(saved)

    def delete_campaign(self, email_campaign_id: str) -> bool:
        record = self.repository.get_resource(self._RESOURCE_TYPE, email_campaign_id)
        if record is None:
            return False
        payload = dict(record.get("payload") or {})
        if str(payload.get("status") or "").lower() != "draft":
            raise ValueError("Only draft email campaigns can be deleted.")
        deleted = self.repository.delete_resource(self._RESOURCE_TYPE, email_campaign_id)
        if deleted:
            self.repository.record_action("email_campaign_deleted", self._RESOURCE_TYPE, email_campaign_id, {"email_campaign_id": email_campaign_id})
        return deleted

    def run_due_campaigns(self, *, reference_time: str | None = None, limit: int = 100) -> Dict[str, Any]:
        resolved_time = self._parse_datetime(reference_time) or datetime.utcnow()
        scheduled_items = []
        for item in self.repository.list_resources(self._RESOURCE_TYPE):
            payload = dict(item.get("payload") or {})
            if str(payload.get("status") or "").lower() != "scheduled":
                continue
            schedule_at = self._parse_datetime(payload.get("schedule_at"))
            if schedule_at is None or schedule_at > resolved_time:
                continue
            scheduled_items.append(payload)
        results = []
        for payload in scheduled_items[: max(1, int(limit))]:
            results.append(
                self._execute_campaign(
                    str(payload.get("email_campaign_id") or ""),
                    reference_time=resolved_time.isoformat(),
                    trigger="scheduler",
                )
            )
        return {"executed_at": resolved_time.isoformat(), "items": results}

    def _execute_campaign(self, email_campaign_id: str, *, reference_time: str | None, trigger: str) -> Dict[str, Any]:
        record = self.repository.get_resource(self._RESOURCE_TYPE, email_campaign_id)
        if record is None:
            raise KeyError(email_campaign_id)
        payload = dict(record.get("payload") or {})
        status = str(payload.get("status") or "draft").lower()
        if status not in {"draft", "scheduled"}:
            raise ValueError("Only draft or scheduled email campaigns can be sent.")

        resolved_time = self._parse_datetime(reference_time) or datetime.utcnow()
        payload["status"] = "sending"
        payload["last_send_started_at"] = resolved_time.isoformat()
        payload["send_attempts"] = int(payload.get("send_attempts") or 0) + 1
        self.repository.upsert_resource(self._RESOURCE_TYPE, email_campaign_id, status="sending", name=payload.get("name"), payload=payload)

        audience_rows = self._resolve_audience_rows(dict(payload.get("audience") or {}))
        provider = str(payload.get("provider") or "").strip().lower() or self._resolve_campaign_provider(
            str(payload.get("provider_connection_id") or "")
        )[0]
        recipient_email_field = str(payload.get("recipient_email_field") or "email").strip() or "email"
        recipient_external_id_field = str(payload.get("recipient_external_id_field") or "user_id").strip() or "user_id"
        deeplink_field = str(payload.get("deeplink_template_field") or self._DEEPLINK_FIELD_DEFAULT).strip() or self._DEEPLINK_FIELD_DEFAULT
        prepared_recipients: List[Dict[str, Any]] = []
        skipped_missing_email = 0
        skipped_missing_recipient = 0
        preparation_errors: List[Dict[str, Any]] = []

        for row in audience_rows:
            try:
                merge_payload = self._build_merge_payload(dict(row or {}), payload, deeplink_field=deeplink_field)
            except ValueError as exc:
                preparation_errors.append(
                    {
                        "user_id": self._row_user_identifier(row),
                        "provider": provider,
                        "error": str(exc),
                    }
                )
                continue
            if provider == "sendgrid":
                recipient_email = self._normalized_lookup_text(self._lookup_row_value(row, recipient_email_field))
                if not recipient_email:
                    skipped_missing_email += 1
                    skipped_missing_recipient += 1
                    continue
                prepared_recipients.append(
                    {
                        "to": [{"email": recipient_email}],
                        "dynamic_template_data": merge_payload,
                        "custom_args": {
                            "email_campaign_id": str(payload.get("email_campaign_id") or email_campaign_id),
                            "user_id": self._row_user_identifier(row),
                        },
                    }
                )
                continue
            recipient_external_id = self._normalized_lookup_text(self._lookup_row_value(row, recipient_external_id_field))
            if not recipient_external_id:
                skipped_missing_recipient += 1
                continue
            prepared_recipients.append(
                {
                    "external_user_id": recipient_external_id,
                    "trigger_properties": merge_payload,
                    "send_to_existing_only": True,
                }
            )

        chunk_results: List[Dict[str, Any]] = []
        sent_count = 0
        failed_count = len(preparation_errors)
        chunk_errors: List[Dict[str, Any]] = []
        sender_email = str(payload.get("from_email") or "").strip() or None
        sender_name = str(payload.get("from_name") or "").strip() or None
        subject = str(payload.get("subject") or "").strip() or None

        if provider == "sendgrid":
            for chunk in self._chunk_list(prepared_recipients, self._SENDGRID_MAX_PERSONALIZATIONS):
                try:
                    result = self.sendgrid.send_templated_mail(
                        str(payload.get("provider_connection_id") or ""),
                        template_id=str(payload.get("template_id") or ""),
                        personalizations=chunk,
                        from_email=sender_email,
                        from_name=sender_name,
                        subject=subject,
                    )
                    chunk_results.append({**result, "recipient_count": len(chunk)})
                    sent_count += len(chunk)
                except (SendGridApiError, ValueError) as exc:
                    failed_count += len(chunk)
                    chunk_errors.append({"error": str(exc), "recipient_count": len(chunk)})
        elif provider == "braze":
            for chunk in self._chunk_list(prepared_recipients, self._BRAZE_MAX_RECIPIENTS):
                try:
                    result = self.braze.send_campaign(
                        str(payload.get("provider_connection_id") or ""),
                        campaign_id=str(payload.get("template_id") or ""),
                        recipients=chunk,
                    )
                    chunk_results.append({**result, "recipient_count": len(chunk)})
                    sent_count += len(chunk)
                except (BrazeApiError, ValueError) as exc:
                    failed_count += len(chunk)
                    chunk_errors.append({"error": str(exc), "recipient_count": len(chunk)})
        else:
            raise ValueError(f"Unsupported email campaign provider '{provider}'.")

        final_status = self._final_status(sent_count, failed_count, skipped_missing_recipient, preparation_errors, chunk_errors)
        payload["status"] = final_status
        payload["last_send_completed_at"] = datetime.utcnow().isoformat()
        payload["last_error"] = self._compose_last_error(preparation_errors, chunk_errors)
        payload["result_summary"] = {
            "trigger": trigger,
            "provider": provider,
            "audience_count": len(audience_rows),
            "prepared_recipients": len(prepared_recipients),
            "sent_count": sent_count,
            "failed_count": failed_count,
            "skipped_missing_recipient": skipped_missing_recipient,
            "skipped_missing_email": skipped_missing_email,
            "chunk_results": chunk_results,
            "errors": [*preparation_errors[:10], *chunk_errors[:10]],
        }
        saved = self.repository.upsert_resource(
            self._RESOURCE_TYPE,
            email_campaign_id,
            status=final_status,
            name=payload.get("name"),
            payload=payload,
        )
        event_type = "email_campaign_sent" if final_status in {"sent", "sent_with_errors"} else "email_campaign_failed"
        self.repository.record_resource_event(
            self._RESOURCE_TYPE,
            email_campaign_id,
            event_type=event_type,
            payload={
                "status": final_status,
                "trigger": trigger,
                "result_summary": payload.get("result_summary") or {},
            },
        )
        self.repository.record_action("email_campaign_executed", self._RESOURCE_TYPE, email_campaign_id, payload.get("result_summary") or {})
        return self._to_response(saved)

    def _normalize_campaign_payload(
        self,
        payload: Dict[str, Any],
        *,
        existing: Dict[str, Any] | None,
        email_campaign_id: str,
    ) -> Dict[str, Any]:
        provider_connection_id = str(payload.get("provider_connection_id") or "").strip()
        template_id = str(payload.get("template_id") or "").strip()
        name = str(payload.get("name") or "").strip()
        if not name:
            raise ValueError("Email campaign name is required.")
        if not provider_connection_id:
            raise ValueError("provider_connection_id is required.")
        if not template_id:
            raise ValueError("template_id is required.")

        provider, connection = self._resolve_campaign_provider(provider_connection_id)
        audience = self._normalize_audience(dict(payload.get("audience") or {}))
        merge_fields = self._normalize_merge_fields(dict(payload.get("merge_fields") or {}))
        schedule_at = self._normalize_schedule_at(payload.get("schedule_at"))
        from_email = self._optional_text(payload.get("from_email"))
        from_name = self._optional_text(payload.get("from_name"))
        subject = self._optional_text(payload.get("subject"))
        body = self._optional_text(payload.get("body"))
        existing_payload = dict(existing or {})
        recipient_email_field = self._optional_text(payload.get("recipient_email_field")) or self._optional_text(existing_payload.get("recipient_email_field"))
        recipient_external_id_field = self._optional_text(payload.get("recipient_external_id_field")) or self._optional_text(
            existing_payload.get("recipient_external_id_field")
        )
        if provider == "sendgrid":
            sender_defaults = dict(connection.get("config") or {})
            if not (from_email or sender_defaults.get("from_email")):
                raise ValueError("SendGrid campaigns require from_email either on the provider connection or the campaign override.")
            template_summary = self.sendgrid.get_template_summary(provider_connection_id, template_id)
            recipient_email_field = recipient_email_field or "email"
            recipient_external_id_field = recipient_external_id_field or None
        elif provider == "braze":
            template_summary = self.braze.get_campaign_summary(provider_connection_id, template_id)
            from_email = None
            from_name = None
            subject = None
            recipient_email_field = recipient_email_field or "email"
            recipient_external_id_field = recipient_external_id_field or "user_id"
        else:
            raise ValueError(f"Provider '{provider}' is not supported for email campaigns.")
        current_status = str((existing or {}).get("status") or "").lower()
        normalized_status = "scheduled" if schedule_at else "draft"
        if current_status in {"draft", "scheduled"}:
            normalized_status = "scheduled" if schedule_at else "draft"

        return {
            "email_campaign_id": email_campaign_id,
            "name": name,
            "status": normalized_status,
            "provider": provider,
            "provider_connection_id": provider_connection_id,
            "template_id": template_id,
            "template_summary": template_summary,
            "from_email": from_email,
            "from_name": from_name,
            "subject": subject,
            "body": body,
            "audience": audience,
            "recipient_email_field": recipient_email_field,
            "recipient_external_id_field": recipient_external_id_field,
            "merge_fields": merge_fields,
            "deeplink_template": self._optional_text(payload.get("deeplink_template")),
            "deeplink_override_field": self._optional_text(payload.get("deeplink_override_field")),
            "deeplink_template_field": self._optional_text(payload.get("deeplink_template_field")) or self._DEEPLINK_FIELD_DEFAULT,
            "schedule_at": schedule_at,
            "send_attempts": int(existing_payload.get("send_attempts") or 0),
            "last_send_started_at": existing_payload.get("last_send_started_at"),
            "last_send_completed_at": existing_payload.get("last_send_completed_at"),
            "last_error": existing_payload.get("last_error"),
            "cancelled_at": existing_payload.get("cancelled_at"),
            "result_summary": dict(existing_payload.get("result_summary") or {}),
        }

    @staticmethod
    def _normalize_audience(audience: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(audience or {})
        if not payload.get("prediction_job_id") and not payload.get("cohort_id"):
            raise ValueError("audience.prediction_job_id or audience.cohort_id is required.")
        include_risks = payload.get("include_risks")
        normalized = {
            "prediction_job_id": EmailCampaignService._optional_text(payload.get("prediction_job_id")),
            "cohort_id": EmailCampaignService._optional_text(payload.get("cohort_id")),
            "include_churned": bool(payload.get("include_churned")),
            "include_risks": [
                str(item).strip().lower()
                for item in list(include_risks or [])
                if str(item).strip()
            ],
        }
        return normalized

    @staticmethod
    def _normalize_merge_fields(merge_fields: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        normalized: Dict[str, Dict[str, Any]] = {}
        for template_var, spec in dict(merge_fields or {}).items():
            key = str(template_var or "").strip()
            if not key:
                continue
            if isinstance(spec, dict):
                source = str(spec.get("source") or ("literal" if "literal" in spec else "field")).strip().lower() or "field"
                value = spec.get("value")
                if value is None and source == "field":
                    value = spec.get("field")
                if value is None and source == "literal":
                    value = spec.get("literal")
            else:
                source = "field"
                value = spec
            if source not in {"field", "literal"}:
                raise ValueError(f"Unsupported merge_fields source '{source}' for '{key}'.")
            normalized[key] = {"source": source, "value": value}
        return normalized

    def _resolve_campaign_provider(self, provider_connection_id: str) -> tuple[str, Dict[str, Any]]:
        connection = self.provider_connections.resolve_connection(provider_connection_id)
        provider = str(connection.get("provider") or "").strip().lower()
        return provider, connection

    def _resolve_audience_rows(self, audience: Dict[str, Any]) -> List[Dict[str, Any]]:
        cohort_id = str(audience.get("cohort_id") or "").strip()
        if cohort_id:
            cohort = self.cohorts.get_cohort(cohort_id)
            if cohort is None:
                raise KeyError(cohort_id)
            return list(cohort.get("latest_members") or [])

        prediction_job_id = str(audience.get("prediction_job_id") or "").strip()
        prediction_job = self.repository.get_prediction_job(prediction_job_id)
        if prediction_job is None:
            raise KeyError(prediction_job_id)
        rows: List[Dict[str, Any]] = []
        page = 1
        while True:
            batch = self.bigquery_service.list_prediction_results(
                job_id=prediction_job_id,
                page=page,
                page_size=self.settings.export_batch_size,
            )
            items = list(batch.get("items") or [])
            if not items:
                break
            rows.extend(items)
            if len(rows) >= int(batch.get("total") or 0):
                break
            page += 1
        return self._filter_prediction_rows(
            rows,
            include_churned=bool(audience.get("include_churned")),
            include_risks=list(audience.get("include_risks") or []),
        )

    @staticmethod
    def _filter_prediction_rows(rows: List[Dict[str, Any]], *, include_churned: bool, include_risks: List[str]) -> List[Dict[str, Any]]:
        risk_set = {str(value or "").strip().lower() for value in include_risks if str(value or "").strip()}
        filtered: List[Dict[str, Any]] = []
        for row in rows:
            churn_state = str(row.get("churn_state", "")).lower()
            risk = str(row.get("predicted_churn_risk", "")).lower()
            if churn_state == "churned":
                if include_churned:
                    filtered.append(row)
                continue
            if not risk_set or risk in risk_set:
                filtered.append(row)
        return filtered

    def _build_merge_payload(self, row: Dict[str, Any], campaign: Dict[str, Any], *, deeplink_field: str) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        for template_var, spec in dict(campaign.get("merge_fields") or {}).items():
            source = str((spec or {}).get("source") or "field").lower()
            value = (spec or {}).get("value")
            if source == "literal":
                payload[template_var] = value
                continue
            payload[template_var] = self._lookup_row_value(row, value)
        body = self._optional_text(campaign.get("body"))
        if body:
            payload.setdefault("body", body)
            payload.setdefault("email_body", body)

        deeplink_url = self._resolve_deeplink_url(row, campaign)
        if deeplink_url:
            payload[deeplink_field] = deeplink_url
            payload.setdefault(self._DEEPLINK_FIELD_DEFAULT, deeplink_url)
        return payload

    def _resolve_deeplink_url(self, row: Dict[str, Any], campaign: Dict[str, Any]) -> str | None:
        override_field = str(campaign.get("deeplink_override_field") or "").strip()
        if override_field:
            override_value = self._lookup_row_value(row, override_field)
            if override_value not in (None, ""):
                return str(override_value)
        template = str(campaign.get("deeplink_template") or "").strip()
        if not template:
            return None

        campaign_context = {
            "campaign_id": campaign.get("email_campaign_id"),
            "campaign_name": campaign.get("name"),
            "template_id": campaign.get("template_id"),
            "provider_connection_id": campaign.get("provider_connection_id"),
        }

        def replace(match: re.Match[str]) -> str:
            key = str(match.group(1) or "").strip()
            if not key:
                return ""
            if key in campaign_context and campaign_context[key] not in (None, ""):
                return str(campaign_context[key])
            resolved = self._lookup_row_value(row, key)
            if resolved in (None, ""):
                raise ValueError(f"deeplink_template is missing required field '{key}'.")
            return str(resolved)

        return re.sub(r"\{([^{}]+)\}", replace, template)

    @staticmethod
    def _lookup_row_value(row: Dict[str, Any], field_path: Any) -> Any:
        field = str(field_path or "").strip()
        if not field:
            return None
        current: Any = row
        for segment in field.split("."):
            if isinstance(current, dict) and segment in current:
                current = current.get(segment)
                continue
            return None
        return current

    @staticmethod
    def _normalized_lookup_text(value: Any) -> str | None:
        if value in (None, ""):
            return None
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none", "null"}:
            return None
        return text

    @staticmethod
    def _row_user_identifier(row: Dict[str, Any]) -> str:
        return str(row.get("user_id") or row.get("canonical_user_id") or row.get("email") or "").strip()

    @staticmethod
    def _chunk_list(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
        if size <= 0:
            yield list(items)
            return
        for index in range(0, len(items), size):
            yield items[index:index + size]

    @staticmethod
    def _final_status(
        sent_count: int,
        failed_count: int,
        skipped_missing_recipient: int,
        preparation_errors: List[Dict[str, Any]],
        chunk_errors: List[Dict[str, Any]],
    ) -> str:
        if sent_count > 0 and (failed_count > 0 or skipped_missing_recipient > 0 or preparation_errors or chunk_errors):
            return "sent_with_errors"
        if sent_count > 0:
            return "sent"
        return "failed"

    @staticmethod
    def _compose_last_error(preparation_errors: List[Dict[str, Any]], chunk_errors: List[Dict[str, Any]]) -> str | None:
        if chunk_errors:
            return str(chunk_errors[0].get("error") or "")
        if preparation_errors:
            return str(preparation_errors[0].get("error") or "")
        return None

    @staticmethod
    def _normalize_schedule_at(value: Any) -> str | None:
        if value in (None, ""):
            return None
        parsed = EmailCampaignService._parse_datetime(value)
        if parsed is None:
            raise ValueError("schedule_at must be a valid ISO timestamp.")
        return parsed.isoformat()

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed

    @staticmethod
    def _optional_text(value: Any) -> str | None:
        text = str(value or "").strip()
        return text or None

    @staticmethod
    def _matches_status_filter(item: Dict[str, Any], status_filter: str) -> bool:
        status = str(item.get("status") or "").lower()
        if status_filter == "upcoming":
            return status == "scheduled"
        if status_filter == "draft":
            return status == "draft"
        if status_filter == "past":
            return status in {"sent", "sent_with_errors", "failed", "cancelled"}
        return status == status_filter

    @staticmethod
    def _to_response(record: Dict[str, Any] | None) -> Dict[str, Any] | None:
        if record is None:
            return None
        payload = dict(record.get("payload") or {})
        return {
            "email_campaign_id": payload.get("email_campaign_id") or record.get("resource_id"),
            "name": payload.get("name") or record.get("name"),
            "status": payload.get("status") or record.get("status") or "draft",
            "provider": payload.get("provider") or "sendgrid",
            "provider_connection_id": payload.get("provider_connection_id"),
            "template_id": payload.get("template_id"),
            "template_summary": dict(payload.get("template_summary") or {}),
            "from_email": payload.get("from_email"),
            "from_name": payload.get("from_name"),
            "subject": payload.get("subject"),
            "body": payload.get("body"),
            "audience": dict(payload.get("audience") or {}),
            "recipient_email_field": payload.get("recipient_email_field") or "email",
            "recipient_external_id_field": payload.get("recipient_external_id_field"),
            "merge_fields": dict(payload.get("merge_fields") or {}),
            "deeplink_template": payload.get("deeplink_template"),
            "deeplink_override_field": payload.get("deeplink_override_field"),
            "deeplink_template_field": payload.get("deeplink_template_field") or EmailCampaignService._DEEPLINK_FIELD_DEFAULT,
            "schedule_at": payload.get("schedule_at"),
            "send_attempts": int(payload.get("send_attempts") or 0),
            "last_send_started_at": payload.get("last_send_started_at"),
            "last_send_completed_at": payload.get("last_send_completed_at"),
            "last_error": payload.get("last_error"),
            "cancelled_at": payload.get("cancelled_at"),
            "result_summary": dict(payload.get("result_summary") or {}),
            "tenant_id": record.get("tenant_id") or payload.get("tenant_id"),
            "project_id": record.get("project_id") or payload.get("project_id"),
            "created_by": record.get("created_by") or payload.get("created_by") or "system",
            "updated_by": record.get("updated_by") or payload.get("updated_by") or "system",
            "correlation_id": record.get("correlation_id") or payload.get("correlation_id") or "",
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
        }
