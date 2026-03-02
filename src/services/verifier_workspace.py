from __future__ import annotations
from sqlalchemy import select
from src.db.session import db
from src.db.production_step1_models import (
    VerificationCaseState, VerificationSamplingItem, VerificationFinding, CorrectiveAction
)

def ensure_case_state(verification_case_id:int)->VerificationCaseState:
    with db() as s:
        stt = s.execute(select(VerificationCaseState).where(VerificationCaseState.verification_case_id==int(verification_case_id))).scalars().first()
        if stt: return stt
        stt = VerificationCaseState(verification_case_id=int(verification_case_id), status="open", approval_chain_json="[]")
        s.add(stt); s.commit(); s.refresh(stt); return stt

def add_sampling_item(verification_case_id:int, record_ref:str, reason:str=""):
    with db() as s:
        s.add(VerificationSamplingItem(verification_case_id=int(verification_case_id), record_ref=str(record_ref), reason=str(reason)))
        s.commit()

def list_sampling(verification_case_id:int):
    with db() as s:
        return s.execute(select(VerificationSamplingItem).where(VerificationSamplingItem.verification_case_id==int(verification_case_id)).order_by(VerificationSamplingItem.id.desc())).scalars().all()

def add_finding(verification_case_id:int, code:str, severity:str, description:str):
    with db() as s:
        f = VerificationFinding(verification_case_id=int(verification_case_id), code=str(code), severity=str(severity), description=str(description), status="open")
        s.add(f); s.commit(); s.refresh(f); return f

def list_findings(verification_case_id:int):
    with db() as s:
        return s.execute(select(VerificationFinding).where(VerificationFinding.verification_case_id==int(verification_case_id)).order_by(VerificationFinding.id.desc())).scalars().all()

def add_corrective_action(finding_id:int, owner:str, action:str, due_date:str="", evidence_doc_id:int|None=None):
    with db() as s:
        s.add(CorrectiveAction(finding_id=int(finding_id), owner=str(owner), action=str(action), due_date=str(due_date), evidence_doc_id=(int(evidence_doc_id) if evidence_doc_id else None)))
        s.commit()

def list_corrective_actions(finding_id:int):
    with db() as s:
        return s.execute(select(CorrectiveAction).where(CorrectiveAction.finding_id==int(finding_id)).order_by(CorrectiveAction.id.desc())).scalars().all()
