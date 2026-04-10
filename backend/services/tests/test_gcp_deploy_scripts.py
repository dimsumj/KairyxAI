from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
GCP_DEPLOY_DIR = REPO_ROOT / "deploy" / "gcp"


def _run(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True, check=False, cwd=REPO_ROOT, env=env)


@pytest.fixture
def _stubbed_cli_environment(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    for command_name in ("gcloud", "bq", "docker", "python3"):
        script_path = bin_dir / command_name
        script_path.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        script_path.chmod(0o755)

    env = dict(os.environ)
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    return env


def test_gcp_shell_scripts_pass_bash_syntax_check():
    for script_name in (
        "bootstrap_dev_foundation.sh",
        "configure_dev_eventing.sh",
        "deploy_cloud_run.sh",
    ):
        result = _run("bash", "-n", str(GCP_DEPLOY_DIR / script_name))
        assert result.returncode == 0, result.stderr


def test_gcp_shell_scripts_expose_help_text():
    for script_name in (
        "bootstrap_dev_foundation.sh",
        "configure_dev_eventing.sh",
        "deploy_cloud_run.sh",
    ):
        result = _run("bash", str(GCP_DEPLOY_DIR / script_name), "--help")
        assert result.returncode == 0, result.stderr
        assert "Usage:" in result.stdout


def test_dev_env_example_includes_bootstrap_and_google_workspace_fields():
    content = (GCP_DEPLOY_DIR / "dev.env.example").read_text(encoding="utf-8")

    for required_line in (
        "GCP_DEPLOYMENT_TIER=dev",
        "GCP_SQL_INSTANCE=",
        "GCP_SQL_DATABASE=",
        "GCP_SQL_USER=",
        "GCP_CLOUD_SQL_CONNECTION_NAME=",
        "GCP_BIGQUERY_DATASET_ID=",
        "CONTROL_PLANE_DATABASE_URL_SECRET=",
        "WORKER_SHARED_TOKEN_SECRET=",
        "GOOGLE_OIDC_CLIENT_ID=",
        "GOOGLE_OIDC_HOSTED_DOMAIN=",
    ):
        assert required_line in content


def test_bootstrap_script_rejects_non_dev_tier(_stubbed_cli_environment, tmp_path):
    env_file = tmp_path / "qa.env"
    env_file.write_text(
        "\n".join(
            (
                "GCP_DEPLOYMENT_TIER=qa",
                "GCP_PROJECT_ID=kairyx-dev",
                "GCP_REGION=us-central1",
                "GCP_ARTIFACT_REGISTRY_REPOSITORY=kairyx",
                "GCP_RUN_NETWORK=dev-vpc",
                "GCP_RUN_SUBNET=dev-serverless",
                "GCP_SQL_INSTANCE=kairyx-dev-db",
                "GCP_SQL_DATABASE=kairyx",
                "GCP_SQL_USER=kairyx_app",
                "GCP_CLOUD_SQL_CONNECTION_NAME=kairyx-dev:us-central1:kairyx-dev-db",
                "CONTROL_PLANE_DATABASE_URL_SECRET=dev-control-plane-db-url",
                "WORKER_SHARED_TOKEN_SECRET=dev-worker-shared-token",
                "GCS_BUCKET_NAME=kairyx-dev-data",
                "IMPORT_COMMAND_TOPIC=kairyx-dev-import-jobs",
                "PREDICTION_COMMAND_TOPIC=kairyx-dev-prediction-jobs",
                "EXPORT_COMMAND_TOPIC=kairyx-dev-export-jobs",
                "PUBSUB_TOPIC_NAME=kairyx-dev-raw-shards",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    result = _run(
        "bash",
        str(GCP_DEPLOY_DIR / "bootstrap_dev_foundation.sh"),
        str(env_file),
        env=_stubbed_cli_environment,
    )

    assert result.returncode != 0
    assert "only supports GCP_DEPLOYMENT_TIER=dev" in result.stderr


def test_bootstrap_script_rejects_cross_project_service_account_override(_stubbed_cli_environment, tmp_path):
    env_file = tmp_path / "dev.env"
    env_file.write_text(
        "\n".join(
            (
                "GCP_DEPLOYMENT_TIER=dev",
                "GCP_PROJECT_ID=kairyx-dev",
                "GCP_REGION=us-central1",
                "GCP_ARTIFACT_REGISTRY_REPOSITORY=kairyx",
                "GCP_RUN_NETWORK=dev-vpc",
                "GCP_RUN_SUBNET=dev-serverless",
                "GCP_SQL_INSTANCE=kairyx-dev-db",
                "GCP_SQL_DATABASE=kairyx",
                "GCP_SQL_USER=kairyx_app",
                "GCP_CLOUD_SQL_CONNECTION_NAME=kairyx-dev:us-central1:kairyx-dev-db",
                "CONTROL_PLANE_DATABASE_URL_SECRET=dev-control-plane-db-url",
                "WORKER_SHARED_TOKEN_SECRET=dev-worker-shared-token",
                "GCS_BUCKET_NAME=kairyx-dev-data",
                "IMPORT_COMMAND_TOPIC=kairyx-dev-import-jobs",
                "PREDICTION_COMMAND_TOPIC=kairyx-dev-prediction-jobs",
                "EXPORT_COMMAND_TOPIC=kairyx-dev-export-jobs",
                "PUBSUB_TOPIC_NAME=kairyx-dev-raw-shards",
                "OPERATOR_API_SERVICE_ACCOUNT=operator-api@shared-project.iam.gserviceaccount.com",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    result = _run(
        "bash",
        str(GCP_DEPLOY_DIR / "bootstrap_dev_foundation.sh"),
        str(env_file),
        env=_stubbed_cli_environment,
    )

    assert result.returncode != 0
    assert "must be a same-project service account" in result.stderr
