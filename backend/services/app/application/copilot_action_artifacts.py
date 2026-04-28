from __future__ import annotations

from typing import Any, Dict
from urllib.parse import quote


def artifact_for_cohort(cohort: Dict[str, Any]) -> Dict[str, Any]:
    cohort_id = str(cohort.get("cohort_id") or "")
    return {
        "resource_type": "cohort",
        "resource_id": cohort_id,
        "label": str(cohort.get("name") or cohort_id or "Cohort"),
        "module_id": "audience-engine",
        "page_id": "audience-engine",
        "api_path": f"/api/v1/cohorts/{quote(cohort_id)}" if cohort_id else "",
        "focus": {"cohort_id": cohort_id},
        "status": str(cohort.get("status") or ""),
    }


def artifact_for_experiment(experiment: Dict[str, Any]) -> Dict[str, Any]:
    experiment_id = str(experiment.get("experiment_id") or "")
    return {
        "resource_type": "experiment",
        "resource_id": experiment_id,
        "label": experiment_id or "Experiment",
        "module_id": "experiment-hub",
        "page_id": "experiment-hub",
        "api_path": f"/api/v1/experiments/config?experiment_id={quote(experiment_id)}" if experiment_id else "",
        "focus": {"experiment_id": experiment_id},
        "status": str(experiment.get("status") or ""),
    }


def artifact_for_connector(connector: Dict[str, Any]) -> Dict[str, Any]:
    connector_name = str(connector.get("name") or "")
    return {
        "resource_type": "connector",
        "resource_id": str(connector.get("connector_id") or connector_name),
        "label": connector_name or "Connector",
        "module_id": "data-core",
        "page_id": "connectors",
        "api_path": f"/api/v1/connectors/{quote(connector_name)}/health" if connector_name else "",
        "focus": {"connector_name": connector_name},
        "status": "configured",
    }


def artifact_for_provider_connection(connection: Dict[str, Any]) -> Dict[str, Any]:
    connection_id = str(connection.get("provider_connection_id") or "")
    return {
        "resource_type": "provider_connection",
        "resource_id": connection_id,
        "label": str(connection.get("name") or connection_id or "Provider Connection"),
        "module_id": "data-core",
        "page_id": "connectors",
        "api_path": f"/api/v1/provider-connections/{quote(connection_id)}" if connection_id else "",
        "focus": {"provider_connection_id": connection_id},
        "status": str(connection.get("status") or ""),
    }


def artifact_for_saved_query(saved_query: Dict[str, Any]) -> Dict[str, Any]:
    query_id = str(saved_query.get("query_id") or "")
    return {
        "resource_type": "saved_query",
        "resource_id": query_id,
        "label": str(saved_query.get("name") or query_id or "Saved Query"),
        "module_id": "audience-engine",
        "page_id": "audience-engine",
        "api_path": "",
        "focus": {"query_id": query_id},
        "status": "saved",
    }
