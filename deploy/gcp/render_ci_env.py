from __future__ import annotations

import argparse
import os
import shlex
from pathlib import Path


ENV_KEYS = (
    "GCP_DEPLOYMENT_TIER",
    "GCP_SERVICE_PREFIX",
    "GCP_PROJECT_ID",
    "GCP_REGION",
    "GCP_ARTIFACT_REGISTRY_REPOSITORY",
    "GCP_IMAGE_NAME",
    "GCP_CLOUD_SQL_CONNECTION_NAME",
    "GCP_RUN_NETWORK",
    "GCP_RUN_SUBNET",
    "GCP_VPC_CONNECTOR",
    "GCP_VPC_EGRESS",
    "CONTROL_PLANE_DATABASE_URL_SECRET",
    "CONTROL_PLANE_SECRET_KEY_SECRET",
    "WORKER_SHARED_TOKEN_SECRET",
    "CORS_ALLOWED_ORIGINS",
    "OIDC_ISSUER",
    "OIDC_AUDIENCE",
    "OIDC_JWKS_URL",
    "OIDC_CLIENT_ID",
    "GOOGLE_OIDC_CLIENT_ID",
    "OIDC_AUTHORIZE_URL",
    "OIDC_TOKEN_URL",
    "OIDC_LOGOUT_URL",
    "GOOGLE_OIDC_HOSTED_DOMAIN",
    "GCP_SECRET_PROJECT_ID",
    "GCP_BIGQUERY_DATASET_ID",
    "GCS_BUCKET_NAME",
    "IMPORT_COMMAND_TOPIC",
    "PREDICTION_COMMAND_TOPIC",
    "EXPORT_COMMAND_TOPIC",
    "PUBSUB_TOPIC_NAME",
    "BOOTSTRAP_TENANT_ID",
    "BOOTSTRAP_TENANT_NAME",
    "BOOTSTRAP_PROJECT_ID",
    "BOOTSTRAP_PROJECT_NAME",
    "API_ACCESS_KEY_SECRET",
    "OIDC_JWT_SIGNING_SECRET_SECRET",
    "GCP_EXTRA_ENV_FILE",
    "OPERATOR_API_SERVICE_ACCOUNT",
    "IMPORT_WORKER_SERVICE_ACCOUNT",
    "PREDICTION_WORKER_SERVICE_ACCOUNT",
    "EXPORT_WORKER_SERVICE_ACCOUNT",
    "SCHEDULER_WORKER_SERVICE_ACCOUNT",
    "PUBSUB_PUSH_INVOKER_SERVICE_ACCOUNT",
    "SCHEDULER_INVOKER_SERVICE_ACCOUNT",
)

REQUIRED_ENV_KEYS = (
    "GCP_DEPLOYMENT_TIER",
    "GCP_SERVICE_PREFIX",
    "GCP_PROJECT_ID",
    "GCP_REGION",
    "GCP_ARTIFACT_REGISTRY_REPOSITORY",
    "GCP_IMAGE_NAME",
    "GCP_CLOUD_SQL_CONNECTION_NAME",
    "CONTROL_PLANE_DATABASE_URL_SECRET",
    "CONTROL_PLANE_SECRET_KEY_SECRET",
    "WORKER_SHARED_TOKEN_SECRET",
    "CORS_ALLOWED_ORIGINS",
    "OIDC_ISSUER",
    "OIDC_AUDIENCE",
    "OIDC_JWKS_URL",
    "OIDC_CLIENT_ID",
    "OIDC_AUTHORIZE_URL",
    "OIDC_TOKEN_URL",
    "GCS_BUCKET_NAME",
    "IMPORT_COMMAND_TOPIC",
    "PREDICTION_COMMAND_TOPIC",
    "EXPORT_COMMAND_TOPIC",
)

REQUIRED_WIF_KEYS = (
    "GCP_WORKLOAD_IDENTITY_PROVIDER",
    "GCP_DEPLOY_SERVICE_ACCOUNT",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render a shell-safe env file for the GitHub Actions dev deploy workflow."
    )
    parser.add_argument("--output", help="Path to the env file to write.")
    parser.add_argument("--release-tag", help="Release tag to write as GCP_RELEASE_TAG.")
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Validate the required GitHub Actions env contract and exit without writing a file.",
    )
    return parser.parse_args()


def validate_required_env() -> None:
    missing = [key for key in REQUIRED_ENV_KEYS if not os.environ.get(key)]
    missing.extend(key for key in REQUIRED_WIF_KEYS if not os.environ.get(key))
    if missing:
        missing_values = ", ".join(sorted(missing))
        raise SystemExit(
            "Missing required GitHub Actions deploy environment values: "
            f"{missing_values}. Populate the GitHub 'dev' environment variables/secrets before rerunning deploy-dev."
        )


def render_env_file(output_path: Path, release_tag: str) -> None:
    lines = [f"GCP_RELEASE_TAG={shlex.quote(release_tag)}"]
    for key in ENV_KEYS:
        lines.append(f"{key}={shlex.quote(os.environ.get(key, ''))}")
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    validate_required_env()
    if args.check_only:
        return
    if not args.output or not args.release_tag:
        raise SystemExit("--output and --release-tag are required unless --check-only is used.")
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    render_env_file(output_path, args.release_tag)


if __name__ == "__main__":
    main()
