import asyncio
import json
import time

import pytest
from fastapi.testclient import TestClient

import main_service


@pytest.fixture
def client():
    return TestClient(main_service.app)


def test_churn_config_get_set(client):
    r = client.get('/churn/config')
    assert r.status_code == 200
    assert 'churn' in r.json()

    r2 = client.post('/churn/config', json={'churn_inactive_days': 21, 'third_party_for_active': False})
    assert r2.status_code == 200
    payload = r2.json()['churn']
    assert payload['churn_inactive_days'] == 21
    assert payload['third_party_for_active'] is False


def test_external_churn_validate_and_upsert(client):
    items = [
        {'user_id': 'u_1', 'churn_risk': 'high', 'source': 'crm'},
        {'email': 'abc@example.com', 'churn_risk': 'medium', 'source': 'warehouse'},
        {'churn_risk': 'bad'},
    ]

    rv = client.post('/churn/external-updates/validate', json={'items': items})
    assert rv.status_code == 200
    v = rv.json()
    assert v['total'] == 3
    assert v['valid'] == 2
    assert v['invalid'] == 1

    ru = client.post('/churn/external-updates', json={'items': items})
    assert ru.status_code == 200
    u = ru.json()
    assert u['count'] == 3
    assert u['matched_user_id'] >= 1
    assert u['matched_email'] >= 1


def test_predict_churn_endpoint_smoke(client, monkeypatch):
    async def _fake_compute(job_name, force_recalculate, prediction_mode='local'):
        return [
            {
                'user_id': 'u_1',
                'email': 'u1@example.com',
                'churn_state': 'active',
                'predicted_churn_risk': 'medium',
                'prediction_source': 'local',
                'churn_reason': 'smoke-test',
                'session_count': 5,
                'event_count': 20,
                'ltv': 12.3,
                'suggested_action': 'none',
            }
        ]

    monkeypatch.setattr(main_service, '_compute_predictions_for_job', _fake_compute)
    r = client.post('/predict-churn-for-import', json={'job_name': 'smoke-job'})
    assert r.status_code == 200
    body = r.json()
    assert 'predictions' in body
    assert len(body['predictions']) == 1


def test_export_estimate_csv_and_third_party(client, monkeypatch):
    async def _fake_compute(job_name, force_recalculate, prediction_mode='local'):
        return [
            {'user_id': 'u_1', 'email': 'u1@example.com', 'churn_state': 'churned', 'predicted_churn_risk': 'already_churned', 'prediction_source': 'rule', 'days_since_last_seen': 30, 'ltv': 10, 'session_count': 1, 'event_count': 3, 'churn_reason': 'inactive'},
            {'user_id': 'u_2', 'email': 'u2@example.com', 'churn_state': 'active', 'predicted_churn_risk': 'high', 'prediction_source': 'cloud', 'days_since_last_seen': 3, 'ltv': 99, 'session_count': 10, 'event_count': 40, 'churn_reason': 'risk'},
        ]

    monkeypatch.setattr(main_service, '_compute_predictions_for_job', _fake_compute)

    re = client.get('/churn/export/estimate', params={'job_name': 'j1', 'include_churned': 'true', 'include_risks': 'high,medium,low'})
    assert re.status_code == 200
    assert re.json()['count'] == 2

    rcsv = client.get('/churn/export/csv', params={'job_name': 'j1'})
    assert rcsv.status_code == 200
    assert rcsv.headers.get('content-type', '').startswith('text/csv')
    assert 'user_id' in rcsv.text

    class DummyResp:
        status_code = 200
        text = 'ok'

    def _fake_post(*args, **kwargs):
        return DummyResp()

    monkeypatch.setattr(main_service.requests, 'post', _fake_post)
    rtp = client.post('/churn/export/third-party', json={'job_name': 'j1', 'webhook_url': 'https://example.com/hook'})
    assert rtp.status_code == 200
    assert rtp.json()['count'] == 2


def test_campaign_audience_export_webhook_uses_clean_rows(client, monkeypatch):
    async def _fake_compute(job_name, force_recalculate, prediction_mode='local'):
        return [
            {
                'user_id': 'u_1',
                'email': 'U1@example.com',
                'churn_state': 'active',
                'predicted_churn_risk': 'high',
                'prediction_source': 'local',
                'churn_reason': 'risk',
                'session_count': 5,
                'event_count': 20,
                'days_since_last_seen': 3,
                'ltv': 12.3,
                'suggested_action': 'Send push',
                'top_signals': [{'signal': 'inactive_3d_plus', 'value': 3}],
                'prediction_details': {'selected_source': 'local'},
            }
        ]

    captured = {}

    class DummyResp:
        status_code = 202
        text = 'accepted'

    def _fake_post(url, json=None, headers=None, timeout=None):
        captured['url'] = url
        captured['json'] = json
        captured['headers'] = headers
        captured['timeout'] = timeout
        return DummyResp()

    monkeypatch.setattr(main_service, '_compute_predictions_for_job', _fake_compute)
    monkeypatch.setattr(main_service.requests, 'post', _fake_post)

    resp = client.post(
        '/campaigns/export-audience',
        json={
            'job_name': 'j1',
            'provider': 'webhook',
            'channel': 'push_notification',
            'webhook_url': 'https://example.com/audience',
            'include_churned': False,
            'include_risks': ['high'],
            'audience_name': 'winback_push_high',
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body['provider'] == 'webhook'
    assert body['count'] == 1

    payload = captured['json']
    assert captured['url'] == 'https://example.com/audience'
    assert payload['count'] == 1
    assert payload['channel'] == 'push_notification'
    assert payload['audience_name'] == 'winback_push_high'
    assert payload['fields'] == [
        'user_id',
        'email',
        'channel',
        'job_name',
        'audience_name',
        'churn_state',
        'predicted_churn_risk',
        'prediction_source',
        'suggested_action',
        'churn_reason',
        'ltv',
        'session_count',
        'event_count',
        'days_since_last_seen',
        'exported_at',
    ]
    assert payload['rows'][0]['user_id'] == 'u_1'
    assert payload['rows'][0]['email'] == 'u1@example.com'
    assert payload['rows'][0]['ltv'] == 12.3
    assert 'top_signals' not in payload['rows'][0]
    assert 'prediction_details' not in payload['rows'][0]


def test_campaign_audience_export_sendgrid_skips_rows_without_email(client, monkeypatch):
    async def _fake_compute(job_name, force_recalculate, prediction_mode='local'):
        return [
            {
                'user_id': 'u_1',
                'email': 'u1@example.com',
                'churn_state': 'active',
                'predicted_churn_risk': 'medium',
                'prediction_source': 'local',
                'churn_reason': 'risk',
                'session_count': 5,
                'event_count': 20,
                'ltv': 12.3,
                'suggested_action': 'Send email',
            },
            {
                'user_id': 'u_2',
                'email': '',
                'churn_state': 'active',
                'predicted_churn_risk': 'medium',
                'prediction_source': 'local',
                'churn_reason': 'missing-email',
                'session_count': 2,
                'event_count': 8,
                'ltv': 5.5,
                'suggested_action': 'Send email',
            },
        ]

    captured = {}

    class DummyResp:
        status_code = 202
        text = 'accepted'

    def _fake_put(url, json=None, headers=None, timeout=None):
        captured['url'] = url
        captured['json'] = json
        captured['headers'] = headers
        captured['timeout'] = timeout
        return DummyResp()

    monkeypatch.setattr(main_service, '_compute_predictions_for_job', _fake_compute)
    monkeypatch.setattr(main_service.requests, 'put', _fake_put)
    monkeypatch.setenv('SENDGRID_API_KEY', 'sendgrid-test-key')

    resp = client.post(
        '/campaigns/export-audience',
        json={
            'job_name': 'j1',
            'provider': 'sendgrid',
            'channel': 'email',
            'include_churned': False,
            'include_risks': ['medium'],
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body['provider'] == 'sendgrid'
    assert body['count'] == 1
    assert body['skipped_missing_email'] == 1
    assert captured['url'] == 'https://api.sendgrid.com/v3/marketing/contacts'
    assert captured['json'] == {
        'contacts': [
            {
                'email': 'u1@example.com',
                'external_id': 'u_1',
            }
        ]
    }


def test_action_history_only_includes_human_triggered_actions(client, monkeypatch, tmp_path):
    audit_log = tmp_path / "audit.jsonl"
    monkeypatch.setattr(main_service, "AUDIT_LOG_FILE", str(audit_log))

    with audit_log.open("w") as f:
        f.write(json.dumps({
            "ts": "2026-03-05T10:00:00",
            "action": "import_job_started",
            "detail": {
                "job_name": "20260305-100000-Amplitude",
                "source": "Amplitude 1",
                "start_date": "20260301",
                "end_date": "20260304",
                "auto_mapping": True,
            },
        }) + "\n")
        f.write(json.dumps({
            "ts": "2026-03-05T10:02:00",
            "action": "field_mapping_updated",
            "detail": {
                "connector": "Amplitude 1",
                "keys": ["canonical_user_id", "event_name", "event_time"],
            },
        }) + "\n")
        f.write(json.dumps({
            "ts": "2026-03-05T10:05:00",
            "action": "campaign_audience_exported",
            "detail": {
                "job_name": "20260305-100000-Amplitude",
                "provider": "braze",
                "channel": "push_notification",
                "audience_name": "winback_high",
                "count": 42,
                "status_code": 201,
            },
        }) + "\n")
        f.write(json.dumps({
            "ts": "2026-03-05T10:06:00",
            "action": "import_job_completed",
            "detail": {
                "job_name": "20260305-100000-Amplitude",
                "source": "Amplitude 1",
            },
        }) + "\n")

    resp = client.get("/action-history?limit=10")
    assert resp.status_code == 200
    items = resp.json()["action_history"]
    assert len(items) == 3

    assert items[0]["category"] == "campaign"
    assert items[0]["summary"] == "Push Audience to Braze"
    assert items[0]["status"] == "completed"
    assert "channel=push_notification" in items[0]["details"]
    assert "count=42" in items[0]["details"]

    assert items[1]["category"] == "mapping"
    assert items[1]["summary"] == "Update Field Mapping for Amplitude 1"
    assert items[1]["status"] == "saved"

    assert items[2]["category"] == "import"
    assert items[2]["summary"] == "Start Import from Amplitude 1"
    assert items[2]["status"] == "started"
    assert "range=20260301 to 20260304" in items[2]["details"]


def test_prediction_job_stop_transitions_to_stopped(client, monkeypatch):
    original_jobs = list(main_service.PREDICTION_JOBS)
    with main_service.PREDICTION_JOB_RUNNERS_LOCK:
        original_runners = dict(main_service.PREDICTION_JOB_RUNNERS)
        main_service.PREDICTION_JOB_RUNNERS.clear()
    main_service.PREDICTION_JOBS.clear()

    async def _fake_compute(
        job_name,
        force_recalculate,
        prediction_mode='local',
        progress_callback=None,
        stop_requested=None,
    ):
        predictions = []
        total_players = 50
        if callable(progress_callback):
            progress_callback(predictions, 0, total_players)

        for idx in range(1, total_players + 1):
            if callable(stop_requested) and stop_requested():
                return predictions

            predictions.append(
                {
                    'user_id': f'u_{idx}',
                    'email': f'u_{idx}@example.com',
                    'churn_state': 'active',
                    'predicted_churn_risk': 'medium',
                    'prediction_source': prediction_mode,
                    'churn_reason': 'stop-test',
                    'session_count': 5,
                    'event_count': 20,
                    'ltv': 12.3,
                    'suggested_action': 'none',
                }
            )
            if callable(progress_callback):
                progress_callback(predictions, idx, total_players)
            await asyncio.sleep(0.01)

        return predictions

    monkeypatch.setattr(main_service, '_compute_predictions_for_job', _fake_compute)

    try:
        start = client.post(
            '/predict-churn-for-import-async',
            json={'job_name': 'stop-job', 'force_recalculate': True, 'prediction_mode': 'local'},
        )
        assert start.status_code == 200
        prediction_job_id = start.json()['prediction_job_id']

        time.sleep(0.05)

        stop = client.post(f'/prediction-job/{prediction_job_id}/stop')
        assert stop.status_code == 200
        assert stop.json()['prediction_job']['stop_requested'] is True

        deadline = time.time() + 3.0
        latest_job = None
        while time.time() < deadline:
            status = client.get(f'/prediction-job/{prediction_job_id}')
            assert status.status_code == 200
            latest_job = status.json()['prediction_job']
            if latest_job.get('status') == 'Stopped':
                break
            time.sleep(0.02)

        assert latest_job is not None
        assert latest_job['status'] == 'Stopped'
        assert latest_job['processed_count'] < latest_job['total_count']
    finally:
        deadline = time.time() + 1.0
        while time.time() < deadline:
            runner = main_service._get_prediction_job_runner(prediction_job_id) if 'prediction_job_id' in locals() else None
            if not runner:
                break
            time.sleep(0.01)
        main_service.PREDICTION_JOBS[:] = original_jobs
        with main_service.PREDICTION_JOB_RUNNERS_LOCK:
            main_service.PREDICTION_JOB_RUNNERS.clear()
            main_service.PREDICTION_JOB_RUNNERS.update(original_runners)
