from __future__ import annotations

from experiment_service import ExperimentService


class ExperimentConfigService:
    def __init__(self, repository):
        self.repository = repository
        self.experiment_service = ExperimentService(base_dir=".")

    def get_config(self):
        return self.repository.get_experiment_config()

    def save_config(self, config):
        return self.repository.save_experiment_config(config)

    def get_summary(self, experiment_id: str):
        return self.experiment_service.summary(experiment_id)
