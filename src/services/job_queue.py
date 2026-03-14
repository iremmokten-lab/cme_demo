from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import select

from src.db.job_models import Job
from src.db.session import db


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def enqueue(kind: str, payload: dict | None = None, project_id: int | None = None) -> Job:
    """Create a queued background job.

    ``project_id`` is accepted for API compatibility but not persisted because the
    current ``jobs`` table does not define that column.
    """
    with db() as s:
        job = Job(
            job_type=str(kind),
            status="queued",
            payload_json=json.dumps(payload or {}, ensure_ascii=False, sort_keys=True),
            result_json="{}",
            error="",
            created_at=_utcnow(),
            updated_at=_utcnow(),
        )
        s.add(job)
        s.commit()
        s.refresh(job)
        return job


def claim_next() -> Job | None:
    with db() as s:
        job = s.execute(select(Job).where(Job.status == "queued").order_by(Job.id.asc())).scalars().first()
        if not job:
            return None
        job.status = "running"
        job.updated_at = _utcnow()
        s.commit()
        s.refresh(job)
        return job


def finish(job_id: int, ok: bool, result: dict | None = None, error: str = "") -> None:
    with db() as s:
        job = s.get(Job, int(job_id))
        if not job:
            return
        job.status = "succeeded" if ok else "failed"
        job.result_json = json.dumps(result or {}, ensure_ascii=False, sort_keys=True)
        job.error = str(error or "")
        job.updated_at = _utcnow()
        s.commit()


def list_jobs(limit: int = 100) -> list[Job]:
    with db() as s:
        return list(s.execute(select(Job).order_by(Job.id.desc()).limit(int(limit))).scalars().all())
