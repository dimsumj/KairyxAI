from __future__ import annotations

import argparse
import json

from app.application.predictions import PredictionService
from app.core.db import init_db, session_scope
from app.core.settings import get_settings
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a prediction job.")
    parser.add_argument("--job-id", required=True)
    args = parser.parse_args(argv)

    init_db()
    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        service = PredictionService(repository, get_settings())
        result = service.run_job(args.job_id)
        print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
