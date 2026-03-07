from __future__ import annotations

from typing import Any, Dict


class MappingService:
    def __init__(self, repository):
        self.repository = repository

    def get_mapping(self, connector_name: str) -> Dict[str, Any]:
        return self.repository.get_field_mapping(connector_name)

    def save_mapping(self, connector_name: str, mapping: Dict[str, Any]) -> Dict[str, Any]:
        return self.repository.save_field_mapping(connector_name, mapping)
