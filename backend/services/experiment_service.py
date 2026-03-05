from __future__ import annotations

import hashlib
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict


class ExperimentService:
    def __init__(self, base_dir: str = "."):
        self.base = Path(base_dir)
        self.exposure_file = self.base / ".experiments_exposure.jsonl"
        self.outcome_file = self.base / ".experiments_outcome.jsonl"

    def assign(self, experiment_id: str, player_id: Any, holdout_pct: float = 0.1, b_variant_pct: float = 0.5) -> Dict[str, Any]:
        key = f"{experiment_id}:{player_id}".encode("utf-8")
        bucket = int(hashlib.sha256(key).hexdigest()[:8], 16) % 10000 / 10000.0

        if bucket < holdout_pct:
            group = "holdout"
        else:
            group = "treatment_b" if ((bucket - holdout_pct) / max(1e-9, 1 - holdout_pct)) < b_variant_pct else "treatment_a"

        assignment = {
            "ts": datetime.utcnow().isoformat(),
            "experiment_id": experiment_id,
            "player_id": str(player_id),
            "bucket": bucket,
            "group": group,
            "holdout_pct": holdout_pct,
            "b_variant_pct": b_variant_pct,
        }
        return assignment

    def record_exposure(self, record: Dict[str, Any]) -> None:
        with self.exposure_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def record_outcome(self, record: Dict[str, Any]) -> None:
        with self.outcome_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def summary(self, experiment_id: str) -> Dict[str, Any]:
        exposures = []
        outcomes = {}

        if self.exposure_file.exists():
            for line in self.exposure_file.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                rec = json.loads(line)
                if rec.get("experiment_id") == experiment_id:
                    exposures.append(rec)

        if self.outcome_file.exists():
            for line in self.outcome_file.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                rec = json.loads(line)
                if rec.get("experiment_id") == experiment_id:
                    outcomes[str(rec.get("action_id"))] = rec

        stats = {
            "holdout": {"n": 0, "engaged": 0, "returned": 0},
            "treatment_a": {"n": 0, "engaged": 0, "returned": 0},
            "treatment_b": {"n": 0, "engaged": 0, "returned": 0},
        }

        for e in exposures:
            g = e.get("group", "holdout")
            if g not in stats:
                continue
            stats[g]["n"] += 1

            action_id = e.get("action_id")
            o = outcomes.get(str(action_id)) if action_id else None
            response = (o or {}).get("simulated_response")
            if response in {"opened", "returned_to_game"}:
                stats[g]["engaged"] += 1
            if response == "returned_to_game":
                stats[g]["returned"] += 1

        def rate(num: int, den: int) -> float:
            return float(num) / den if den > 0 else 0.0

        summary = {"experiment_id": experiment_id, "groups": {}}
        for g, s in stats.items():
            summary["groups"][g] = {
                **s,
                "engagement_rate": rate(s["engaged"], s["n"]),
                "return_rate": rate(s["returned"], s["n"]),
            }

        holdout_ret = summary["groups"]["holdout"]["return_rate"]
        for g in ["treatment_a", "treatment_b"]:
            summary["groups"][g]["uplift_vs_holdout_return_rate"] = summary["groups"][g]["return_rate"] - holdout_ret

        return summary
