from __future__ import annotations
import hashlib, json
import requests
from sqlalchemy import select
from src.db.session import db
from src.db.production_step2_models import RegulationSpec

def _sha256(b:bytes)->str:
    return hashlib.sha256(b).hexdigest()

def register_spec(code:str, version:str, url:str, notes:str="")->RegulationSpec:
    content=b""
    sha=""
    if url:
        r=requests.get(url, timeout=60)
        r.raise_for_status()
        content=r.content
        sha=_sha256(content)
    with db() as s:
        existing=s.execute(select(RegulationSpec).where(RegulationSpec.code==str(code), RegulationSpec.version==str(version))).scalars().first()
        if existing:
            existing.source_url=url
            existing.sha256=sha
            existing.notes=notes
            s.commit(); s.refresh(existing); return existing
        spec=RegulationSpec(code=str(code), version=str(version), source_url=str(url), sha256=str(sha), notes=str(notes))
        s.add(spec); s.commit(); s.refresh(spec); return spec

def list_specs(code:str|None=None):
    with db() as s:
        q=select(RegulationSpec).order_by(RegulationSpec.id.desc())
        if code:
            q=q.where(RegulationSpec.code==str(code))
        return s.execute(q).scalars().all()
