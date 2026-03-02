from __future__ import annotations

import json
from typing import Any, Dict
from sqlalchemy import select
from src.db.session import db
from src.db.phase_ab_models import Producer, ProducerAttestation

def create_producer(company_id: int, name: str, country: str="TR", vat_or_tax_id: str="", contact_email: str="") -> Producer:
    with db() as s:
        p = Producer(company_id=int(company_id), name=str(name).strip(), country=str(country).strip(),
                     vat_or_tax_id=str(vat_or_tax_id).strip(), contact_email=str(contact_email).strip())
        s.add(p); s.commit(); s.refresh(p); return p

def list_producers(company_id: int) -> list[Producer]:
    with db() as s:
        return s.execute(select(Producer).where(Producer.company_id==int(company_id)).order_by(Producer.id.desc())).scalars().all()

def submit_attestation(producer_id:int, project_id:int, period_year:int, period_quarter:int, declaration:Dict[str,Any], signed_by:str, evidence_doc_id:int|None=None) -> ProducerAttestation:
    with db() as s:
        a = ProducerAttestation(
            producer_id=int(producer_id), project_id=int(project_id),
            period_year=int(period_year), period_quarter=int(period_quarter),
            status="submitted",
            declaration_json=json.dumps(declaration or {}, ensure_ascii=False),
            signed_by=str(signed_by or "").strip(),
            evidence_doc_id=(int(evidence_doc_id) if evidence_doc_id else None),
        )
        s.add(a); s.commit(); s.refresh(a); return a

def list_attestations(project_id:int, period_year:int, period_quarter:int) -> list[ProducerAttestation]:
    with db() as s:
        return s.execute(
            select(ProducerAttestation).where(
                ProducerAttestation.project_id==int(project_id),
                ProducerAttestation.period_year==int(period_year),
                ProducerAttestation.period_quarter==int(period_quarter),
            ).order_by(ProducerAttestation.id.desc())
        ).scalars().all()
