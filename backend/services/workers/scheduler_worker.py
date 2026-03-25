from __future__ import annotations

import argparse
import json
import os

from app.application.control_loop import ControlLoopService
from app.core.db import init_db, session_scope
from app.core.request_context import RequestContext, request_context
from app.core.settings import get_settings
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the scheduler control loop once.")
    parser.add_argument("--reference-time", required=False)
    args = parser.parse_args(argv)

    init_db()
    tenant_id = str(os.getenv("BOOTSTRAP_TENANT_ID", "default"))
    with request_context(
        RequestContext(
            actor_id="worker:scheduler",
            actor_role="admin",
            tenant_id=tenant_id,
            correlation_id="worker-scheduler",
            platform_admin=True,
            auth_mode="worker",
        )
    ):
        with session_scope() as session:
            repository = SqlAlchemyControlPlaneRepository(session)
            repository.ensure_tenant(tenant_id, os.getenv("BOOTSTRAP_TENANT_NAME", "Default Tenant"))
            payload = ControlLoopService(repository, get_settings()).tick(reference_time=args.reference_time)
            print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
