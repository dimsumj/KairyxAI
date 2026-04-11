from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
BACKEND_CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "backend-ci.yml"
DEPLOY_DEV_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "deploy-dev.yml"


def test_deploy_dev_workflow_targets_main_pushes_and_manual_dispatch():
    content = DEPLOY_DEV_WORKFLOW.read_text(encoding="utf-8")

    assert "name: deploy-dev" in content
    assert "push:" in content
    assert "branches:" in content
    assert "- main" in content
    assert "workflow_dispatch:" in content
    assert "concurrency:" in content
    assert "group: deploy-dev" in content
    assert "cancel-in-progress: true" in content


def test_deploy_dev_workflow_uses_validation_before_deploy():
    content = DEPLOY_DEV_WORKFLOW.read_text(encoding="utf-8")

    assert "validate:" in content
    assert "deploy-dev:" in content
    assert "needs: validate" in content
    assert "pip install -r requirements.txt" in content
    assert "pytest tests/test_multitenant_auth.py tests/test_v1_api.py tests/test_v1_closed_loop.py tests/test_gcp_deploy_scripts.py tests/test_github_actions_workflows.py" in content


def test_deploy_dev_workflow_uses_gcp_wif_and_repo_scripts():
    content = DEPLOY_DEV_WORKFLOW.read_text(encoding="utf-8")

    assert "environment: dev" in content
    assert "id-token: write" in content
    assert "google-github-actions/auth@v3" in content
    assert "google-github-actions/setup-gcloud@v3" in content
    assert "workload_identity_provider: ${{ secrets.GCP_WORKLOAD_IDENTITY_PROVIDER }}" in content
    assert "service_account: ${{ secrets.GCP_DEPLOY_SERVICE_ACCOUNT }}" in content
    assert 'bash deploy/gcp/deploy_cloud_run.sh "${DEPLOY_ENV_FILE}"' in content
    assert 'bash deploy/gcp/configure_dev_eventing.sh "${DEPLOY_ENV_FILE}"' in content


def test_deploy_dev_workflow_generates_temp_env_and_smoke_checks_health():
    content = DEPLOY_DEV_WORKFLOW.read_text(encoding="utf-8")

    assert 'env_file="${RUNNER_TEMP}/deploy-dev.env"' in content
    assert "python3 deploy/gcp/render_ci_env.py" in content
    assert '--release-tag "dev-${short_sha}"' in content
    assert 'printf \'DEPLOY_ENV_FILE=%s\\n\' "${env_file}" >> "${GITHUB_ENV}"' in content
    assert 'curl --fail --silent --show-error "${service_url}/health/live" >/dev/null' in content


def test_backend_ci_tracks_deploy_workflow_changes():
    content = BACKEND_CI_WORKFLOW.read_text(encoding="utf-8")

    assert '".github/workflows/deploy-dev.yml"' in content
    assert "tests/test_github_actions_workflows.py" in content
    assert "pip install -r requirements.txt" in content
