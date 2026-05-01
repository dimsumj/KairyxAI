from __future__ import annotations

import os
from pathlib import Path
import shlex
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


def test_configure_dev_eventing_validate_only_accepts_complete_env_without_gcloud(tmp_path):
    env_file = tmp_path / "dev.env"
    env_file.write_text(
        "\n".join(
            (
                "GCP_DEPLOYMENT_TIER=dev",
                "GCP_PROJECT_ID=kairyx-dev",
                "GCP_REGION=us-central1",
                "WORKER_SHARED_TOKEN_SECRET=dev-worker-shared-token",
                "IMPORT_COMMAND_TOPIC=kairyx-dev-import-jobs",
                "PREDICTION_COMMAND_TOPIC=kairyx-dev-prediction-jobs",
                "EXPORT_COMMAND_TOPIC=kairyx-dev-export-jobs",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    env = dict(os.environ)
    env["PATH"] = "/bin:/usr/bin"

    result = _run(
        "bash",
        str(GCP_DEPLOY_DIR / "configure_dev_eventing.sh"),
        "--validate-only",
        str(env_file),
        env=env,
    )

    assert result.returncode == 0, result.stderr
    assert "Validated dev eventing env contract." in result.stdout


def test_configure_dev_eventing_validate_only_fails_fast_on_missing_import_topic(tmp_path):
    env_file = tmp_path / "dev.env"
    env_file.write_text(
        "\n".join(
            (
                "GCP_DEPLOYMENT_TIER=dev",
                "GCP_PROJECT_ID=kairyx-dev",
                "GCP_REGION=us-central1",
                "WORKER_SHARED_TOKEN_SECRET=dev-worker-shared-token",
                "PREDICTION_COMMAND_TOPIC=kairyx-dev-prediction-jobs",
                "EXPORT_COMMAND_TOPIC=kairyx-dev-export-jobs",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    env = dict(os.environ)
    env["PATH"] = "/bin:/usr/bin"

    result = _run(
        "bash",
        str(GCP_DEPLOY_DIR / "configure_dev_eventing.sh"),
        "--validate-only",
        str(env_file),
        env=env,
    )

    assert result.returncode != 0
    assert "Required env var is missing: IMPORT_COMMAND_TOPIC" in result.stderr


def test_configure_dev_eventing_retries_scheduler_job_update_when_gcp_reports_sync_mutate_queue(tmp_path):
    env_file = tmp_path / "dev.env"
    env_file.write_text(
        "\n".join(
            (
                "GCP_DEPLOYMENT_TIER=dev",
                "GCP_PROJECT_ID=kairyx-dev",
                "GCP_REGION=us-central1",
                "GCP_SERVICE_PREFIX=dev",
                "WORKER_SHARED_TOKEN_SECRET=dev-worker-shared-token",
                "IMPORT_COMMAND_TOPIC=kairyx-dev-import-jobs",
                "PREDICTION_COMMAND_TOPIC=kairyx-dev-prediction-jobs",
                "EXPORT_COMMAND_TOPIC=kairyx-dev-export-jobs",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    gcloud_script = bin_dir / "gcloud"
    gcloud_script.write_text(
        """#!/usr/bin/env bash
set -euo pipefail

state_dir="${GCLOUD_STUB_STATE_DIR:?}"

case "$1 $2 ${3:-}" in
  "config set project")
    exit 0
    ;;
  "projects describe kairyx-dev")
    printf '123456789012\\n'
    exit 0
    ;;
  "secrets versions access")
    printf 'worker-shared-token\\n'
    exit 0
    ;;
  "run services describe")
    printf 'https://%s.example.run.app\\n' "$4"
    exit 0
    ;;
  "run services add-iam-policy-binding")
    exit 0
    ;;
  "iam service-accounts add-iam-policy-binding")
    exit 0
    ;;
  "pubsub subscriptions describe")
    exit 1
    ;;
  "pubsub subscriptions create")
    exit 0
    ;;
  "scheduler jobs describe")
    exit 0
    ;;
  "scheduler jobs update")
    counter_file="${state_dir}/scheduler-update-count"
    current_count=0
    if [[ -f "${counter_file}" ]]; then
      current_count="$(cat "${counter_file}")"
    fi
    current_count="$((current_count + 1))"
    printf '%s' "${current_count}" > "${counter_file}"
    if [[ "${current_count}" -eq 1 ]]; then
      printf 'ERROR: (gcloud.scheduler.jobs.update.http) ABORTED: sync mutate calls cannot be queued\\n' >&2
      exit 1
    fi
    exit 0
    ;;
  *)
    printf 'unexpected gcloud command: %s\\n' "$*" >&2
    exit 1
    ;;
esac
""",
        encoding="utf-8",
    )
    gcloud_script.chmod(0o755)

    sleep_script = bin_dir / "sleep"
    sleep_script.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    sleep_script.chmod(0o755)

    env = dict(os.environ)
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["GCLOUD_STUB_STATE_DIR"] = str(tmp_path)

    result = _run(
        "bash",
        str(GCP_DEPLOY_DIR / "configure_dev_eventing.sh"),
        str(env_file),
        env=env,
    )

    assert result.returncode == 0, result.stderr
    assert "Cloud Scheduler job dev-scheduler-worker is busy; retrying" in result.stdout
    assert (tmp_path / "scheduler-update-count").read_text(encoding="utf-8") == "2"


def test_render_ci_env_helper_preserves_space_containing_values(tmp_path):
    env_file = tmp_path / "deploy-dev.env"
    env = dict(os.environ)
    env.update(
        {
            "GCP_REGION": "us-central1",
            "GCP_ARTIFACT_REGISTRY_REPOSITORY": "kairyx",
            "GCP_IMAGE_NAME": "kairyxai",
            "GCP_DEPLOYMENT_TIER": "dev",
            "GCP_SERVICE_PREFIX": "dev",
            "GCP_CLOUD_SQL_CONNECTION_NAME": "kairyx-dev:us-central1:kairyx-db",
            "CONTROL_PLANE_DATABASE_URL_SECRET": "dev-control-plane-db-url",
            "CONTROL_PLANE_SECRET_KEY_SECRET": "dev-control-plane-secret-key",
            "WORKER_SHARED_TOKEN_SECRET": "dev-worker-shared-token",
            "CORS_ALLOWED_ORIGINS": "https://dev-console.example.internal",
            "OIDC_ISSUER": "https://accounts.google.com",
            "OIDC_AUDIENCE": "client-id.apps.googleusercontent.com",
            "OIDC_JWKS_URL": "https://www.googleapis.com/oauth2/v3/certs",
            "OIDC_CLIENT_ID": "client-id.apps.googleusercontent.com",
            "OIDC_AUTHORIZE_URL": "https://accounts.google.com/o/oauth2/v2/auth",
            "OIDC_TOKEN_URL": "https://oauth2.googleapis.com/token",
            "GCS_BUCKET_NAME": "kairyx-dev-data",
            "IMPORT_COMMAND_TOPIC": "kairyx-dev-import-jobs",
            "PREDICTION_COMMAND_TOPIC": "kairyx-dev-prediction-jobs",
            "EXPORT_COMMAND_TOPIC": "kairyx-dev-export-jobs",
            "GCP_WORKLOAD_IDENTITY_PROVIDER": "projects/123456789/locations/global/workloadIdentityPools/github-actions/providers/github-main",
            "GCP_DEPLOY_SERVICE_ACCOUNT": "github-deploy@kairyx-dev.iam.gserviceaccount.com",
            "BOOTSTRAP_TENANT_NAME": "Default Tenant",
            "BOOTSTRAP_PROJECT_NAME": "Default Project",
            "GCP_PROJECT_ID": "kairyx-dev",
        }
    )

    result = _run(
        "python3",
        str(GCP_DEPLOY_DIR / "render_ci_env.py"),
        "--output",
        str(env_file),
        "--release-tag",
        "dev-abc123",
        env=env,
    )

    assert result.returncode == 0, result.stderr

    source_result = _run(
        "bash",
        "-lc",
        f"set -a && source {shlex.quote(str(env_file))} && printf '%s|%s|%s' \"$BOOTSTRAP_TENANT_NAME\" \"$BOOTSTRAP_PROJECT_NAME\" \"$GCP_RELEASE_TAG\"",
    )
    assert source_result.returncode == 0, source_result.stderr
    assert source_result.stdout == "Default Tenant|Default Project|dev-abc123"


def test_render_ci_env_helper_rejects_missing_required_values(tmp_path):
    env_file = tmp_path / "deploy-dev.env"
    env = dict(os.environ)
    env.pop("GCP_PROJECT_ID", None)
    env.pop("GCP_SERVICE_PREFIX", None)
    env.pop("GCP_WORKLOAD_IDENTITY_PROVIDER", None)
    env.pop("GCP_DEPLOY_SERVICE_ACCOUNT", None)
    env.pop("IMPORT_COMMAND_TOPIC", None)

    result = _run(
        "python3",
        str(GCP_DEPLOY_DIR / "render_ci_env.py"),
        "--check-only",
        env=env,
    )

    assert result.returncode != 0
    assert "Missing required GitHub Actions deploy environment values:" in result.stderr
    assert "GCP_PROJECT_ID" in result.stderr
    assert "GCP_SERVICE_PREFIX" in result.stderr
    assert "GCP_WORKLOAD_IDENTITY_PROVIDER" in result.stderr
    assert "GCP_DEPLOY_SERVICE_ACCOUNT" in result.stderr
    assert "IMPORT_COMMAND_TOPIC" in result.stderr


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
        "CONTROL_PLANE_SECRET_KEY_SECRET=",
        "WORKER_SHARED_TOKEN_SECRET=",
        "GOOGLE_OIDC_CLIENT_ID=",
        "GOOGLE_OIDC_HOSTED_DOMAIN=",
    ):
        assert required_line in content


def test_deploy_script_does_not_set_reserved_cloud_run_port_env_var():
    content = (GCP_DEPLOY_DIR / "deploy_cloud_run.sh").read_text(encoding="utf-8")

    assert 'env_file_line PORT "8080"' not in content


def test_deploy_script_builds_cloud_run_image_for_linux_amd64():
    content = (GCP_DEPLOY_DIR / "deploy_cloud_run.sh").read_text(encoding="utf-8")

    assert 'docker build --platform "linux/amd64" -t "${IMAGE_TAGGED}" .' in content


def test_deploy_script_exports_bigquery_runtime_env_from_gcp_settings():
    content = (GCP_DEPLOY_DIR / "deploy_cloud_run.sh").read_text(encoding="utf-8")

    assert 'env_file_line BIGQUERY_PROJECT_ID "${BIGQUERY_PROJECT_ID:-${GCP_PROJECT_ID}}"' in content
    assert 'env_file_line BIGQUERY_DATASET_ID "${BIGQUERY_DATASET_ID:-${GCP_BIGQUERY_DATASET_ID:-kairyx_platform}}"' in content


def test_bootstrap_script_uses_cloud_run_safe_default_subnet_range():
    content = (GCP_DEPLOY_DIR / "bootstrap_dev_foundation.sh").read_text(encoding="utf-8")

    assert '--range="${GCP_RUN_SUBNET_RANGE:-10.20.0.0/24}"' in content


def test_bootstrap_script_grants_scheduler_worker_bigquery_read_job_access():
    content = (GCP_DEPLOY_DIR / "bootstrap_dev_foundation.sh").read_text(encoding="utf-8")

    assert 'scheduler-worker)\n      ensure_project_binding "$member" "roles/bigquery.jobUser"' in content
    assert 'ensure_project_binding "$member" "roles/bigquery.dataViewer"' in content


def test_bootstrap_script_seeds_bootstrap_scoped_bigquery_dataset_and_dead_letter_table():
    content = (GCP_DEPLOY_DIR / "bootstrap_dev_foundation.sh").read_text(encoding="utf-8")

    assert 'dataset_id="${dataset_base}_${tenant_scope}_${project_scope}"' in content
    assert 'ensure_bigquery_dataset "$dataset_id"' in content
    assert '"pipeline_dead_letters"' in content
    assert 'payload_json:STRING' in content


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
                "CONTROL_PLANE_SECRET_KEY_SECRET=dev-control-plane-secret-key",
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
                "CONTROL_PLANE_SECRET_KEY_SECRET=dev-control-plane-secret-key",
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
