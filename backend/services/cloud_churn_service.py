import os
import requests
from typing import Dict, Any, Optional


class CloudChurnRequestError(RuntimeError):
    pass


class CloudChurnTimeoutError(CloudChurnRequestError):
    pass


class CloudChurnService:
    """
    Optional cloud churn prediction client.
    Expected response JSON shape:
      {
        "churn_risk": "low|medium|high",
        "reason": "..."
      }
    """

    def __init__(self, api_url: Optional[str] = None, api_token: Optional[str] = None, timeout_sec: float = 10.0):
        self.api_url = api_url or os.getenv("CHURN_API_URL")
        self.api_token = api_token if api_token is not None else os.getenv("CHURN_API_TOKEN")
        self.timeout_sec = float(os.getenv("CHURN_API_TIMEOUT_SEC", str(timeout_sec)))
        if not self.api_url:
            raise ValueError("Cloud churn service is not configured. Set CHURN_API_URL.")

    def estimate_churn_risk(self, player_id: Any, player_profile: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "player_id": player_id,
            "player_profile": player_profile,
        }
        headers = {"Content-Type": "application/json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"

        try:
            response = requests.post(self.api_url, json=payload, headers=headers, timeout=self.timeout_sec)
            response.raise_for_status()
        except requests.Timeout as exc:
            raise CloudChurnTimeoutError(f"Cloud churn request timed out after {self.timeout_sec:.1f}s.") from exc
        except requests.RequestException as exc:
            raise CloudChurnRequestError(f"Cloud churn request failed: {exc}") from exc
        data = response.json() if response.content else {}

        return {
            "player_id": player_id,
            "churn_risk": data.get("churn_risk", "unknown"),
            "reason": data.get("reason", "Cloud predictor returned no reason."),
        }
