from __future__ import annotations
import json
from datetime import datetime, timezone
from sqlalchemy import select
from src.db.session import db
from src.db.production_step1_models import CbamPortalSubmission
from src.services.cbam_portal_client import submit_zip, check_status

def get_or_create(project_id:int, year:int, quarter:int) -> CbamPortalSubmission:
    with db() as s:
        sub = s.execute(select(CbamPortalSubmission).where(CbamPortalSubmission.project_id==int(project_id), CbamPortalSubmission.period_year==int(year), CbamPortalSubmission.period_quarter==int(quarter))).scalars().first()
        if sub: return sub
        sub = CbamPortalSubmission(project_id=int(project_id), period_year=int(year), period_quarter=int(quarter), status="draft")
        s.add(sub); s.commit(); s.refresh(sub); return sub

def mark_ready(project_id:int, year:int, quarter:int, *, portal_zip_uri:str, cbam_xml_uri:str, schema_version:str):
    with db() as s:
        sub=get_or_create(project_id,year,quarter)
        sub.status="ready"
        sub.portal_zip_uri=str(portal_zip_uri or "")
        sub.cbam_xml_uri=str(cbam_xml_uri or "")
        sub.schema_version=str(schema_version or "")
        sub.updated_at=datetime.now(timezone.utc)
        s.merge(sub); s.commit()

def submit_to_portal(project_id:int, year:int, quarter:int, *, zip_bytes:bytes, filename:str="cbam_submission.zip"):
    with db() as s:
        sub=get_or_create(project_id,year,quarter)
        resp=submit_zip(zip_bytes, filename=filename)
        sub.request_meta_json=json.dumps({"filename": filename}, ensure_ascii=False)
        sub.response_meta_json=json.dumps(resp.raw, ensure_ascii=False)
        sub.portal_reference=resp.reference
        sub.status="submitted" if resp.ok else "rejected"
        sub.updated_at=datetime.now(timezone.utc)
        s.merge(sub); s.commit()
        return resp

def refresh_status(project_id:int, year:int, quarter:int):
    with db() as s:
        sub=get_or_create(project_id,year,quarter)
        if not sub.portal_reference:
            return {"ok": False, "status": "NO_REFERENCE"}
        rep=check_status(sub.portal_reference)
        sub.response_meta_json=json.dumps(rep, ensure_ascii=False)
        if rep.get("status") in ("accepted","rejected"):
            sub.status=str(rep.get("status"))
        sub.updated_at=datetime.now(timezone.utc)
        s.merge(sub); s.commit()
        return rep
