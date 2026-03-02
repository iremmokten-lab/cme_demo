from __future__ import annotations
import json
from src.db.session import init_db
from src.erp_automation.worker import register, run_loop
from src.erp_automation.orchestrator import run_ingestion

def main():
    init_db()

    def _handler(payload: dict) -> dict:
        project_id = int(payload["project_id"])
        connection_id = int(payload["connection_id"])
        dataset_type = str(payload["dataset_type"])
        run_id, upload_id, dlq = run_ingestion(project_id, connection_id, dataset_type, since=payload.get("since"), until=payload.get("until"))
        return {"run_id": run_id, "upload_id": upload_id, "dlq": dlq}

    register("erp_ingest", _handler)
    run_loop(poll_seconds=2.0, max_loops=10_000)

if __name__ == "__main__":
    main()
