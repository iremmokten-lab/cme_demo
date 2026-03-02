from __future__ import annotations
import json
from datetime import datetime, timezone
from sqlalchemy import select
from src.db.session import db
from src.db.production_step2_models import Job

def enqueue(kind:str, payload:dict|None=None, project_id:int|None=None)->Job:
    with db() as s:
        j = Job(kind=str(kind), status="queued", payload_json=json.dumps(payload or {}, ensure_ascii=False), project_id=(int(project_id) if project_id else None))
        s.add(j); s.commit(); s.refresh(j); return j

def claim_next()->Job|None:
    with db() as s:
        j = s.execute(select(Job).where(Job.status=="queued").order_by(Job.id.asc())).scalars().first()
        if not j: return None
        j.status="running"
        j.started_at=datetime.now(timezone.utc)
        s.commit(); s.refresh(j); return j

def finish(job_id:int, ok:bool, result:dict|None=None, error:str="")->None:
    with db() as s:
        j = s.get(Job, int(job_id))
        if not j: return
        j.status="success" if ok else "failed"
        j.result_json=json.dumps(result or {}, ensure_ascii=False)
        j.error=str(error or "")
        j.finished_at=datetime.now(timezone.utc)
        s.commit()

def list_jobs(limit:int=100)->list[Job]:
    with db() as s:
        return s.execute(select(Job).order_by(Job.id.desc()).limit(int(limit))).scalars().all()
