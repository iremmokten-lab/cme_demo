from pydantic import BaseModel

class VerificationCaseCreate(BaseModel):
    facility_id: str
    period_start: str
    period_end: str
    scope: str  # ETS/CBAM/MRV
    verifier_org: str | None = None
    lead_verifier: str | None = None
    notes_tr: str | None = None

class VerificationCaseOut(BaseModel):
    id: str
    facility_id: str
    period_start: str
    period_end: str
    scope: str
    status: str
    verifier_org: str | None
    lead_verifier: str | None
    notes_tr: str | None

class FindingCreate(BaseModel):
    case_id: str
    finding_type: str  # nonconformity/observation/omission
    severity: str      # major/minor/info
    title: str
    description_tr: str
    reg_reference: str | None = None
    evidence_required_tr: str | None = None

class FindingOut(BaseModel):
    id: str
    case_id: str
    finding_type: str
    severity: str
    title: str
    description_tr: str
    reg_reference: str | None
    evidence_required_tr: str | None
    status: str

class CAPACreate(BaseModel):
    finding_id: str
    action_type: str   # corrective/preventive
    owner: str | None = None
    due_date: str | None = None
    action_plan_tr: str
    closure_evidence_doc_id: str | None = None

class CAPAOut(BaseModel):
    id: str
    finding_id: str
    action_type: str
    owner: str | None
    due_date: str | None
    status: str
    action_plan_tr: str
    closure_evidence_doc_id: str | None
    verifier_note_tr: str | None
