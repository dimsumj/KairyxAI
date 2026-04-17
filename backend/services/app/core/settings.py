from __future__ import annotations

import os
from dataclasses import dataclass

from provider_backends import (
    resolve_data_backend_mode,
    resolve_message_backend,
    resolve_object_storage_backend,
    resolve_secret_backend,
    resolve_warehouse_backend,
)
from runtime_paths import default_control_plane_database_url, normalize_env_text, normalize_sqlite_database_url


@dataclass(frozen=True)
class Settings:
    app_name: str = "KairyxAI Operator API"
    api_v1_prefix: str = "/api/v1"
    app_env: str = "local"
    platform_surface: str = ""
    api_access_key: str = ""
    legacy_header_auth_enabled: bool = True
    control_plane_database_url: str = ""
    control_plane_connect_timeout_seconds: int = 3
    data_backend_mode: str = "mock"
    warehouse_backend: str = "mock"
    object_storage_backend: str = "mock"
    message_backend: str = "mock"
    secret_backend: str = "env"
    service_role: str = "operator-api"
    bootstrap_tenant_id: str = "default"
    bootstrap_tenant_name: str = "Default Tenant"
    bootstrap_project_id: str = "default"
    bootstrap_project_name: str = "Default Project"
    cors_allowed_origins: tuple[str, ...] = ("*",)
    worker_shared_token: str = ""
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
    oidc_jwks_timeout_seconds: float = 5.0
    max_sql_preview_rows_per_tenant: int = 1000
    max_reverse_etl_members_per_snapshot: int = 5000
    max_import_jobs_per_tenant: int = 10
    max_export_jobs_per_tenant: int = 20
    max_copilot_reports_per_tenant: int = 50
    import_command_topic: str = "kairyx-import-jobs"
    prediction_command_topic: str = "kairyx-prediction-jobs"
    export_command_topic: str = "kairyx-export-jobs"
    raw_shard_topic: str = "kairyx-raw-shards"
    aws_region: str = ""
    redshift_workgroup_name: str = ""
    redshift_database: str = ""
    redshift_schema: str = "public"
    redshift_secret_arn: str = ""
    s3_bucket_name: str = ""
    eventbridge_bus_name: str = "default"
    sqs_import_queue_url: str = ""
    sqs_prediction_queue_url: str = ""
    sqs_export_queue_url: str = ""
    sqs_scheduler_queue_url: str = ""
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
    import_network_timeout_seconds: float = 300.0
    import_stop_poll_interval_seconds: float = 0.1
    prediction_network_timeout_seconds: float = 20.0
    prediction_stop_poll_interval_seconds: float = 0.1


def get_settings() -> Settings:
    platform_surface = normalize_env_text(os.getenv("KAIRYX_PLATFORM_SURFACE")).lower()
    if not platform_surface and normalize_env_text(os.getenv("VERCEL", "")):
        platform_surface = "vercel_demo"
    scheduler_default = "false" if platform_surface == "vercel_demo" else "true"
    database_url = normalize_sqlite_database_url(
        normalize_env_text(os.getenv("CONTROL_PLANE_DATABASE_URL"))
        or normalize_env_text(os.getenv("DATABASE_URL"))
        or default_control_plane_database_url()
    )
    raw_origins = normalize_env_text(os.getenv("CORS_ALLOWED_ORIGINS", "*"))
    cors_allowed_origins = tuple(origin.strip() for origin in raw_origins.split(",") if origin.strip()) or ("*",)
    google_client_id = (
        normalize_env_text(os.getenv("GOOGLE_OIDC_CLIENT_ID"))
        or normalize_env_text(os.getenv("GOOGLE_CLIENT_ID"))
    )
    google_hosted_domain = (
        normalize_env_text(os.getenv("GOOGLE_OIDC_HOSTED_DOMAIN"))
        or normalize_env_text(os.getenv("OIDC_GOOGLE_HOSTED_DOMAIN"))
        or normalize_env_text(os.getenv("GOOGLE_HOSTED_DOMAIN"))
    )
    oidc_client_id = normalize_env_text(os.getenv("OIDC_CLIENT_ID")) or google_client_id
    oidc_provider = normalize_env_text(os.getenv("OIDC_PROVIDER")).lower()
    if not oidc_provider:
        oidc_provider = "google" if google_client_id else ("oidc" if oidc_client_id else "")
    oidc_issuer = normalize_env_text(os.getenv("OIDC_ISSUER"))
    oidc_audience = normalize_env_text(os.getenv("OIDC_AUDIENCE"))
    oidc_jwks_url = normalize_env_text(os.getenv("OIDC_JWKS_URL"))
    oidc_authorize_url = normalize_env_text(os.getenv("OIDC_AUTHORIZE_URL"))
    oidc_token_url = normalize_env_text(os.getenv("OIDC_TOKEN_URL"))
    oidc_logout_url = normalize_env_text(os.getenv("OIDC_LOGOUT_URL"))
    if google_client_id:
        oidc_issuer = oidc_issuer or "https://accounts.google.com"
        oidc_audience = oidc_audience or oidc_client_id
        oidc_jwks_url = oidc_jwks_url or "https://www.googleapis.com/oauth2/v3/certs"
        oidc_authorize_url = oidc_authorize_url or "https://accounts.google.com/o/oauth2/v2/auth"
        oidc_token_url = oidc_token_url or "https://oauth2.googleapis.com/token"
    return Settings(
        app_env=normalize_env_text(os.getenv("APP_ENV", "local")).lower(),
        platform_surface=platform_surface,
        api_access_key=normalize_env_text(os.getenv("API_ACCESS_KEY")),
        legacy_header_auth_enabled=normalize_env_text(os.getenv("LEGACY_HEADER_AUTH_ENABLED", "true")).lower() not in {"0", "false", "no", "off"},
        control_plane_database_url=database_url,
        control_plane_connect_timeout_seconds=max(1, int(normalize_env_text(os.getenv("CONTROL_PLANE_CONNECT_TIMEOUT_SECONDS", "3")))),
        data_backend_mode=resolve_data_backend_mode(),
        warehouse_backend=resolve_warehouse_backend(),
        object_storage_backend=resolve_object_storage_backend(),
        message_backend=resolve_message_backend(),
        secret_backend=resolve_secret_backend(),
        service_role=normalize_env_text(os.getenv("SERVICE_ROLE", "operator-api")).lower() or "operator-api",
        bootstrap_tenant_id=normalize_env_text(os.getenv("BOOTSTRAP_TENANT_ID", "default")) or "default",
        bootstrap_tenant_name=normalize_env_text(os.getenv("BOOTSTRAP_TENANT_NAME", "Default Tenant")) or "Default Tenant",
        bootstrap_project_id=normalize_env_text(os.getenv("BOOTSTRAP_PROJECT_ID", "default")) or "default",
        bootstrap_project_name=normalize_env_text(os.getenv("BOOTSTRAP_PROJECT_NAME", "Default Project")) or "Default Project",
        cors_allowed_origins=cors_allowed_origins,
        worker_shared_token=normalize_env_text(os.getenv("WORKER_SHARED_TOKEN")),
        oidc_issuer=oidc_issuer,
        oidc_audience=oidc_audience,
        oidc_jwks_url=oidc_jwks_url,
        oidc_client_id=oidc_client_id,
        oidc_authorize_url=oidc_authorize_url,
        oidc_token_url=oidc_token_url,
        oidc_logout_url=oidc_logout_url,
        oidc_jwt_signing_secret=normalize_env_text(os.getenv("OIDC_JWT_SIGNING_SECRET")),
        oidc_provider=oidc_provider,
        oidc_google_hosted_domain=google_hosted_domain,
        oidc_jwks_timeout_seconds=max(0.5, float(normalize_env_text(os.getenv("OIDC_JWKS_TIMEOUT_SECONDS", "5")))),
        max_sql_preview_rows_per_tenant=max(1, int(normalize_env_text(os.getenv("MAX_SQL_PREVIEW_ROWS_PER_TENANT", "1000")))),
        max_reverse_etl_members_per_snapshot=max(1, int(normalize_env_text(os.getenv("MAX_REVERSE_ETL_MEMBERS_PER_SNAPSHOT", "5000")))),
        max_import_jobs_per_tenant=max(1, int(normalize_env_text(os.getenv("MAX_IMPORT_JOBS_PER_TENANT", "10")))),
        max_export_jobs_per_tenant=max(1, int(normalize_env_text(os.getenv("MAX_EXPORT_JOBS_PER_TENANT", "20")))),
        max_copilot_reports_per_tenant=max(1, int(normalize_env_text(os.getenv("MAX_COPILOT_REPORTS_PER_TENANT", "50")))),
        import_command_topic=normalize_env_text(os.getenv("IMPORT_COMMAND_TOPIC", "kairyx-import-jobs")),
        prediction_command_topic=normalize_env_text(os.getenv("PREDICTION_COMMAND_TOPIC", "kairyx-prediction-jobs")),
        export_command_topic=normalize_env_text(os.getenv("EXPORT_COMMAND_TOPIC", "kairyx-export-jobs")),
        raw_shard_topic=normalize_env_text(os.getenv("PUBSUB_TOPIC_NAME", "kairyx-raw-shards")),
        aws_region=normalize_env_text(os.getenv("AWS_REGION")),
        redshift_workgroup_name=normalize_env_text(os.getenv("REDSHIFT_WORKGROUP_NAME")),
        redshift_database=normalize_env_text(os.getenv("REDSHIFT_DATABASE")),
        redshift_schema=normalize_env_text(os.getenv("REDSHIFT_SCHEMA", "public")) or "public",
        redshift_secret_arn=normalize_env_text(os.getenv("REDSHIFT_SECRET_ARN")),
        s3_bucket_name=normalize_env_text(os.getenv("S3_BUCKET_NAME")),
        eventbridge_bus_name=normalize_env_text(os.getenv("EVENTBRIDGE_BUS_NAME", "default")) or "default",
        sqs_import_queue_url=normalize_env_text(os.getenv("SQS_IMPORT_QUEUE_URL")),
        sqs_prediction_queue_url=normalize_env_text(os.getenv("SQS_PREDICTION_QUEUE_URL")),
        sqs_export_queue_url=normalize_env_text(os.getenv("SQS_EXPORT_QUEUE_URL")),
        sqs_scheduler_queue_url=normalize_env_text(os.getenv("SQS_SCHEDULER_QUEUE_URL")),
        default_prediction_page_size=max(1, int(normalize_env_text(os.getenv("DEFAULT_PREDICTION_PAGE_SIZE", "100")))),
        max_prediction_page_size=max(1, int(normalize_env_text(os.getenv("MAX_PREDICTION_PAGE_SIZE", "1000")))),
        worker_page_size=max(1, int(normalize_env_text(os.getenv("WORKER_PAGE_SIZE", "1000")))),
        export_batch_size=max(1, int(normalize_env_text(os.getenv("EXPORT_BATCH_SIZE", "500")))),
        export_retry_attempts=max(1, int(normalize_env_text(os.getenv("EXPORT_RETRY_ATTEMPTS", "3")))),
        job_retention_days=max(1, int(normalize_env_text(os.getenv("JOB_RETENTION_DAYS", "7")))),
        scheduler_enabled=normalize_env_text(os.getenv("SCHEDULER_ENABLED", scheduler_default)).lower() not in {"0", "false", "no", "off"},
        scheduler_interval_seconds=max(5, int(normalize_env_text(os.getenv("SCHEDULER_INTERVAL_SECONDS", "60")))),
        scheduler_daily_optimizer_hour=min(23, max(0, int(normalize_env_text(os.getenv("SCHEDULER_DAILY_OPTIMIZER_HOUR", "8"))))),
        scheduler_daily_report_hour=min(23, max(0, int(normalize_env_text(os.getenv("SCHEDULER_DAILY_REPORT_HOUR", "9"))))),
        scheduler_weekly_report_hour=min(23, max(0, int(normalize_env_text(os.getenv("SCHEDULER_WEEKLY_REPORT_HOUR", "9"))))),
        scheduler_weekly_report_weekday=min(6, max(0, int(normalize_env_text(os.getenv("SCHEDULER_WEEKLY_REPORT_WEEKDAY", "0"))))),
        sqlite_busy_timeout_seconds=max(0.1, float(normalize_env_text(os.getenv("SQLITE_BUSY_TIMEOUT_SECONDS", "15")))),
        import_network_timeout_seconds=max(0.5, float(normalize_env_text(os.getenv("IMPORT_NETWORK_TIMEOUT_SECONDS", "300")))),
        import_stop_poll_interval_seconds=max(0.05, float(normalize_env_text(os.getenv("IMPORT_STOP_POLL_INTERVAL_SECONDS", "0.1")))),
        prediction_network_timeout_seconds=max(0.5, float(normalize_env_text(os.getenv("PREDICTION_NETWORK_TIMEOUT_SECONDS", "20")))),
        prediction_stop_poll_interval_seconds=max(0.05, float(normalize_env_text(os.getenv("PREDICTION_STOP_POLL_INTERVAL_SECONDS", "0.1")))),
    )


def validate_runtime_settings(settings: Settings) -> None:
    if settings.app_env != "prod":
        return
    if settings.control_plane_database_url.startswith("sqlite"):
        raise RuntimeError("APP_ENV=prod requires Postgres; SQLite is not allowed.")
    if (
        settings.warehouse_backend == "mock"
        or settings.object_storage_backend == "mock"
        or settings.message_backend == "mock"
    ):
        raise RuntimeError("APP_ENV=prod requires non-mock warehouse, object storage, and message backends.")
    if settings.legacy_header_auth_enabled:
        raise RuntimeError("APP_ENV=prod requires LEGACY_HEADER_AUTH_ENABLED=false.")
    if "*" in settings.cors_allowed_origins:
        raise RuntimeError("APP_ENV=prod requires explicit CORS_ALLOWED_ORIGINS.")
    if settings.service_role != "operator-api" and not settings.worker_shared_token:
        raise RuntimeError("APP_ENV=prod worker services require WORKER_SHARED_TOKEN.")
    if not settings.oidc_issuer or not settings.oidc_audience or not (settings.oidc_jwks_url or settings.oidc_jwt_signing_secret):
        raise RuntimeError("APP_ENV=prod requires OIDC issuer, audience, and JWKS settings.")
    if settings.service_role == "operator-api" and settings.scheduler_enabled:
        raise RuntimeError("APP_ENV=prod operator-api must not run the in-process scheduler.")
    if settings.warehouse_backend == "redshift":
        if not settings.aws_region:
            raise RuntimeError("APP_ENV=prod with WAREHOUSE_BACKEND=redshift requires AWS_REGION.")
        if not settings.redshift_workgroup_name or not settings.redshift_database:
            raise RuntimeError("APP_ENV=prod with WAREHOUSE_BACKEND=redshift requires REDSHIFT_WORKGROUP_NAME and REDSHIFT_DATABASE.")
    if settings.object_storage_backend == "s3" and not settings.s3_bucket_name:
        raise RuntimeError("APP_ENV=prod with OBJECT_STORAGE_BACKEND=s3 requires S3_BUCKET_NAME.")
    if settings.message_backend == "eventbridge_sqs":
        required_queue_urls = {
            "import-worker": settings.sqs_import_queue_url,
            "prediction-worker": settings.sqs_prediction_queue_url,
            "export-worker": settings.sqs_export_queue_url,
            "scheduler-worker": settings.sqs_scheduler_queue_url,
        }
        if settings.service_role in required_queue_urls and not required_queue_urls[settings.service_role]:
            raise RuntimeError(f"APP_ENV=prod with MESSAGE_BACKEND=eventbridge_sqs requires an SQS queue URL for {settings.service_role}.")
