from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from src.db.session import db
from src.db.job_models import Job

def enqueue(job_type: str, payload: Dict[str, Any]) -> int:
    with db() as s:
        j = Job(job_type=str(job_type), payload_json=json.dumps(payload or {}, ensure_ascii=False), status="queued", created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc))
        s.add(j)
        s.commit()
        return int(j.id)

def fetch_next() -> Optional[Job]:
    with db() as s:
        j = s.query(Job).filter(Job.status=="queued").order_by(Job.id.asc()).first()
        if not j:
            return None
        j.status = "running"
        j.updated_at = datetime.now(timezone.utc)
        s.commit()
        return j

def complete(job_id: int, result: Dict[str, Any]) -> None:
    with db() as s:
        j = s.get(Job, int(job_id))
        if not j:
            return
        j.status = "succeeded"
        j.result_json = json.dumps(result or {}, ensure_ascii=False)
        j.updated_at = datetime.now(timezone.utc)
        s.commit()

def fail(job_id: int, error: str) -> None:
    with db() as s:
        j = s.get(Job, int(job_id))
        if not j:
            return
        j.status = "failed"
        j.error = str(error)
        j.updated_at = datetime.now(timezone.utc)
        s.commit()
