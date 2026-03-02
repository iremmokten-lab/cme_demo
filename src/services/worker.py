from __future__ import annotations

import json
from typing import Dict, Any

from src.services import job_queue
from src.services.observability import log_event
from src.services.cbam_portal_package import store_cbam_portal_package

def run_once() -> bool:
    j = job_queue.fetch_next()
    if not j:
        return False
    try:
        payload = json.loads(j.payload_json or "{}")
    except Exception:
        payload = {}
    try:
        if j.job_type == "CBAM_PORTAL_PACKAGE":
            snap = int(payload.get("snapshot_id"))
            res = store_cbam_portal_package(snap)
            job_queue.complete(j.id, res)
            log_event("job_succeeded", job_type=j.job_type, job_id=j.id)
        else:
            job_queue.fail(j.id, f"Unknown job type: {j.job_type}")
            log_event("job_failed", job_type=j.job_type, job_id=j.id, error="unknown_job_type")
    except Exception as e:
        job_queue.fail(j.id, str(e))
        log_event("job_failed", job_type=j.job_type, job_id=j.id, error=str(e))
    return True
