from __future__ import annotations
from sqlalchemy import select
from src.db.session import db
from src.db.models import DatasetUpload
from src.db.production_step1_models import DatasetApproval

def ensure_approval(upload_id:int)->DatasetApproval:
    with db() as s:
        a = s.execute(select(DatasetApproval).where(DatasetApproval.upload_id==int(upload_id))).scalars().first()
        if a:
            return a
        a = DatasetApproval(upload_id=int(upload_id), status="draft")
        s.add(a); s.commit(); s.refresh(a); return a

def submit(upload_id:int, notes:str, user_id:int|None=None)->None:
    with db() as s:
        a = ensure_approval(upload_id)
        a.status="submitted"
        a.notes=str(notes or "")
        a.submitted_by_user_id = int(user_id) if user_id else None
        s.merge(a); s.commit()

def review(upload_id:int, approve_flag:bool, notes:str, reviewer_user_id:int|None=None)->None:
    from datetime import datetime, timezone
    with db() as s:
        a = ensure_approval(upload_id)
        a.status = "approved" if approve_flag else "rejected"
        a.notes = str(notes or "")
        a.reviewed_by_user_id = int(reviewer_user_id) if reviewer_user_id else None
        a.reviewed_at = datetime.now(timezone.utc)
        s.merge(a); s.commit()

def list_uploads(project_id:int):
    with db() as s:
        ups = s.execute(select(DatasetUpload).where(DatasetUpload.project_id==int(project_id)).order_by(DatasetUpload.uploaded_at.desc())).scalars().all()
        out=[]
        for u in ups:
            a = s.execute(select(DatasetApproval).where(DatasetApproval.upload_id==int(u.id))).scalars().first()
            out.append((u,a))
        return out
