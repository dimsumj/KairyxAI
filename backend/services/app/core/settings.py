from __future__ import annotations

import os
from dataclasses import dataclass

from runtime_paths import default_control_plane_database_url, normalize_sqlite_database_url


@dataclass(frozen=True)
class Settings:
    app_name: str = "KairyxAI Operator API"
    api_v1_prefix: str = "/api/v1"
    app_env: str = "local"
    api_access_key: str = ""
    legacy_header_auth_enabled: bool = True
    control_plane_database_url: str = ""
    data_backend_mode: str = "mock"
    service_role: str = "operator-api"
    bootstrap_tenant_id: str = "default"
    bootstrap_tenant_name: str = "Default Tenant"
    bootstrap_project_id: str = "default"
    bootstrap_project_name: str = "Default Project"
    cors_allowed_origins: tuple[str, ...] = ("*",)
    oidc_issuer: str = ""
    oidc_audience: str = ""
    oidc_jwks_url: str = ""
    oidc_client_id: str = ""
    oidc_authorize_url: str = ""
    oidc_token_url: str = ""
    oidc_logout_url: str = ""
    oidc_jwt_signing_secret: str = ""
    oidc_provider: str = ""
    oidc_google_hosted_domain: str = ""
    max_sql_preview_rows_per_tenant: int = 1000
    max_import_jobs_per_tenant: int = 10
    max_export_jobs_per_tenant: int = 20
    max_copilot_reports_per_tenant: int = 50
    import_command_topic: str = "kairyx-import-jobs"
    prediction_command_topic: str = "kairyx-prediction-jobs"
    export_command_topic: str = "kairyx-export-jobs"
    raw_shard_topic: str = "kairyx-raw-shards"
    default_prediction_page_size: int = 100
    max_prediction_page_size: int = 1000
    worker_page_size: int = 1000
    export_batch_size: int = 500
    export_retry_attempts: int = 3
    job_retention_days: int = 7
    scheduler_enabled: bool = True
    scheduler_interval_seconds: int = 60
    scheduler_daily_optimizer_hour: int = 8
    scheduler_daily_report_hour: int = 9
    scheduler_weekly_report_hour: int = 9
    scheduler_weekly_report_weekday: int = 0
    sqlite_busy_timeout_seconds: float = 15.0
    import_network_timeout_seconds: float = 60.0
    import_stop_poll_interval_seconds: float = 0.1
    prediction_network_timeout_seconds: float = 20.0
    prediction_stop_poll_interval_seconds: float = 0.1


def get_settings() -> Settings:
    database_url = normalize_sqlite_database_url(
        os.getenv("CONTROL_PLANE_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or default_control_plane_database_url()
    )
    raw_origins = str(os.getenv("CORS_ALLOWED_ORIGINS", "*")).strip()
    cors_allowed_origins = tuple(origin.strip() for origin in raw_origins.split(",") if origin.strip()) or ("*",)
    google_client_id = str(os.getenv("GOOGLE_CLIENT_ID", "")).strip()
    google_hosted_domain = str(os.getenv("OIDC_GOOGLE_HOSTED_DOMAIN", "")).strip() or str(os.getenv("GOOGLE_HOSTED_DOMAIN", "")).strip()
    oidc_client_id = str(os.getenv("OIDC_CLIENT_ID", "")).strip() or google_client_id
    oidc_provider = str(os.getenv("OIDC_PROVIDER", "")).strip().lower()
    if not oidc_provider:
        oidc_provider = "google" if google_client_id else ("oidc" if oidc_client_id else "")
    return Settings(
        app_env=str(os.getenv("APP_ENV", "local")).strip().lower(),
        api_access_key=os.getenv("API_ACCESS_KEY", "").strip(),
        legacy_header_auth_enabled=str(os.getenv("LEGACY_HEADER_AUTH_ENABLED", "true")).strip().lower() not in {"0", "false", "no", "off"},
        control_plane_database_url=database_url,
        data_backend_mode=os.getenv("DATA_BACKEND_MODE", "mock").strip().lower(),
        service_role=str(os.getenv("SERVICE_ROLE", "operator-api")).strip().lower() or "operator-api",
        bootstrap_tenant_id=str(os.getenv("BOOTSTRAP_TENANT_ID", "default")).strip() or "default",
        bootstrap_tenant_name=str(os.getenv("BOOTSTRAP_TENANT_NAME", "Default Tenant")).strip() or "Default Tenant",
        bootstrap_project_id=str(os.getenv("BOOTSTRAP_PROJECT_ID", "default")).strip() or "default",
        bootstrap_project_name=str(os.getenv("BOOTSTRAP_PROJECT_NAME", "Default Project")).strip() or "Default Project",
        cors_allowed_origins=cors_allowed_origins,
        oidc_issuer=str(os.getenv("OIDC_ISSUER", "")).strip(),
        oidc_audience=str(os.getenv("OIDC_AUDIENCE", "")).strip(),
        oidc_jwks_url=str(os.getenv("OIDC_JWKS_URL", "")).strip(),
        oidc_client_id=oidc_client_id,
        oidc_authorize_url=str(os.getenv("OIDC_AUTHORIZE_URL", "")).strip(),
        oidc_token_url=str(os.getenv("OIDC_TOKEN_URL", "")).strip(),
        oidc_logout_url=str(os.getenv("OIDC_LOGOUT_URL", "")).strip(),
        oidc_jwt_signing_secret=str(os.getenv("OIDC_JWT_SIGNING_SECRET", "")).strip(),
        oidc_provider=oidc_provider,
        oidc_google_hosted_domain=google_hosted_domain,
        max_sql_preview_rows_per_tenant=max(1, int(os.getenv("MAX_SQL_PREVIEW_ROWS_PER_TENANT", "1000"))),
        max_import_jobs_per_tenant=max(1, int(os.getenv("MAX_IMPORT_JOBS_PER_TENANT", "10"))),
        max_export_jobs_per_tenant=max(1, int(os.getenv("MAX_EXPORT_JOBS_PER_TENANT", "20"))),
        max_copilot_reports_per_tenant=max(1, int(os.getenv("MAX_COPILOT_REPORTS_PER_TENANT", "50"))),
        import_command_topic=os.getenv("IMPORT_COMMAND_TOPIC", "kairyx-import-jobs"),
        prediction_command_topic=os.getenv("PREDICTION_COMMAND_TOPIC", "kairyx-prediction-jobs"),
        export_command_topic=os.getenv("EXPORT_COMMAND_TOPIC", "kairyx-export-jobs"),
        raw_shard_topic=os.getenv("PUBSUB_TOPIC_NAME", "kairyx-raw-shards"),
        default_prediction_page_size=max(1, int(os.getenv("DEFAULT_PREDICTION_PAGE_SIZE", "100"))),
        max_prediction_page_size=max(1, int(os.getenv("MAX_PREDICTION_PAGE_SIZE", "1000"))),
        worker_page_size=max(1, int(os.getenv("WORKER_PAGE_SIZE", "1000"))),
        export_batch_size=max(1, int(os.getenv("EXPORT_BATCH_SIZE", "500"))),
        export_retry_attempts=max(1, int(os.getenv("EXPORT_RETRY_ATTEMPTS", "3"))),
        job_retention_days=max(1, int(os.getenv("JOB_RETENTION_DAYS", "7"))),
        scheduler_enabled=str(os.getenv("SCHEDULER_ENABLED", "true")).strip().lower() not in {"0", "false", "no", "off"},
        scheduler_interval_seconds=max(5, int(os.getenv("SCHEDULER_INTERVAL_SECONDS", "60"))),
        scheduler_daily_optimizer_hour=min(23, max(0, int(os.getenv("SCHEDULER_DAILY_OPTIMIZER_HOUR", "8")))),
        scheduler_daily_report_hour=min(23, max(0, int(os.getenv("SCHEDULER_DAILY_REPORT_HOUR", "9")))),
        scheduler_weekly_report_hour=min(23, max(0, int(os.getenv("SCHEDULER_WEEKLY_REPORT_HOUR", "9")))),
        scheduler_weekly_report_weekday=min(6, max(0, int(os.getenv("SCHEDULER_WEEKLY_REPORT_WEEKDAY", "0")))),
        sqlite_busy_timeout_seconds=max(0.1, float(os.getenv("SQLITE_BUSY_TIMEOUT_SECONDS", "15"))),
        import_network_timeout_seconds=max(0.5, float(os.getenv("IMPORT_NETWORK_TIMEOUT_SECONDS", "60"))),
        import_stop_poll_interval_seconds=max(0.05, float(os.getenv("IMPORT_STOP_POLL_INTERVAL_SECONDS", "0.1"))),
        prediction_network_timeout_seconds=max(0.5, float(os.getenv("PREDICTION_NETWORK_TIMEOUT_SECONDS", "20"))),
        prediction_stop_poll_interval_seconds=max(0.05, float(os.getenv("PREDICTION_STOP_POLL_INTERVAL_SECONDS", "0.1"))),
    )


def validate_runtime_settings(settings: Settings) -> None:
    if settings.app_env != "prod":
        return
    if settings.control_plane_database_url.startswith("sqlite"):
        raise RuntimeError("APP_ENV=prod requires Postgres; SQLite is not allowed.")
    if settings.data_backend_mode == "mock":
        raise RuntimeError("APP_ENV=prod requires DATA_BACKEND_MODE=gcp.")
    if settings.legacy_header_auth_enabled:
        raise RuntimeError("APP_ENV=prod requires LEGACY_HEADER_AUTH_ENABLED=false.")
    if "*" in settings.cors_allowed_origins:
        raise RuntimeError("APP_ENV=prod requires explicit CORS_ALLOWED_ORIGINS.")
    if not settings.oidc_issuer or not settings.oidc_audience or not (settings.oidc_jwks_url or settings.oidc_jwt_signing_secret):
        raise RuntimeError("APP_ENV=prod requires OIDC issuer, audience, and JWKS settings.")
    if settings.service_role == "operator-api" and settings.scheduler_enabled:
        raise RuntimeError("APP_ENV=prod operator-api must not run the in-process scheduler.")
