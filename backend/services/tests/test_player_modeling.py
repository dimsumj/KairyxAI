import asyncio
import pandas as pd

from player_modeling_engine import PlayerModelingEngine


class DummyBQ:
    def get_events_for_player(self, player_id):
        return pd.DataFrame([
            {
                'event_time': '2026-03-01 10:37:56.894000',
                'event_type': 'game_init_time',
                'event_properties': {},
                'user_properties': {'userId': player_id},
            }
        ])


class DummyAI:
    def get_ai_response(self, prompt: str):
        # If prompt has unescaped braces in f-string formatting, call site raises before this line.
        assert 'top_signals' in prompt
        return '{"churn_risk":"medium","reason":"test","top_signals":[{"signal":"x","value":1}]}'


def test_estimate_churn_risk_accepts_iso_and_space_timestamps():
    engine = PlayerModelingEngine(gemini_client=DummyAI(), bigquery_service=DummyBQ(), churn_inactive_days=14)
    profile = engine.build_player_profile('u-1')
    assert profile is not None
    assert profile['churn_state'] == 'active'

    out = asyncio.run(engine.estimate_churn_risk('u-1', profile))
    assert out is not None
    assert out['churn_state'] == 'active'
    assert out['churn_risk'] == 'medium'
