from __future__ import annotations

from typing import Dict

from pydantic import BaseModel


class ExperimentConfigRequest(BaseModel):
    experiment_id: str = "churn_engagement_v1"
    enabled: bool = True
    holdout_pct: float = 0.10
    b_variant_pct: float = 0.50


class ExperimentConfigResponse(BaseModel):
    experiment: Dict
