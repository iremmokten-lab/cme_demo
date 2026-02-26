from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import insert, select, update
from services.api.routers.auth import get_current_db_with_rls
from services.api.db.models import VerificationCase, VerificationFinding, CAPAAction
from services.api.schemas.verification import (
    VerificationCaseCreate, VerificationCaseOut,
    FindingCreate, FindingOut,
    CAPACreate, CAPAOut
)
from services.api.core.audit import write_audit_log

router = APIRouter()

@router.post("/cases", response_model=VerificationCaseOut)
async def create_case(data: VerificationCaseCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(VerificationCase).values(
            tenant_id=ctx["tid"],
            facility_id=data.facility_id,
            period_start=data.period_start,
            period_end=data.period_end,
            scope=data.scope,
            status="open",
            verifier_org=data.verifier_org,
            lead_verifier=data.lead_verifier,
            notes_tr=data.notes_tr,
            created_by=ctx["uid"],
        ).returning(VerificationCase)
    )
    c = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "verification_case", str(c.id), None, data.model_dump())
    await db.commit()
    return VerificationCaseOut(
        id=str(c.id),
        facility_id=str(c.facility_id),
        period_start=c.period_start,
        period_end=c.period_end,
        scope=c.scope,
        status=c.status,
        verifier_org=c.verifier_org,
        lead_verifier=c.lead_verifier,
        notes_tr=c.notes_tr
    )

@router.get("/cases", response_model=list[VerificationCaseOut])
async def list_cases(ctx_db=Depends(get_current_db_with_rls), facility_id: str | None = None, scope: str | None = None):
    ctx, db = ctx_db
    q = select(VerificationCase).where(VerificationCase.tenant_id == ctx["tid"])
    if facility_id:
        q = q.where(VerificationCase.facility_id == facility_id)
    if scope:
        q = q.where(VerificationCase.scope == scope)
    res = await db.execute(q.order_by(VerificationCase.created_at.desc()))
    items = res.scalars().all()
    return [
        VerificationCaseOut(
            id=str(x.id),
            facility_id=str(x.facility_id),
            period_start=x.period_start,
            period_end=x.period_end,
            scope=x.scope,
            status=x.status,
            verifier_org=x.verifier_org,
            lead_verifier=x.lead_verifier,
            notes_tr=x.notes_tr
        ) for x in items
    ]

@router.post("/cases/{case_id}/status")
async def set_case_status(case_id: str, status: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    if status not in ["open", "in_review", "closed"]:
        raise HTTPException(status_code=400, detail="Geçersiz status")
    res = await db.execute(select(VerificationCase).where(VerificationCase.tenant_id == ctx["tid"], VerificationCase.id == case_id))
    c = res.scalar_one_or_none()
    if c is None:
        raise HTTPException(status_code=404, detail="Case bulunamadı")
    before = {"status": c.status}
    await db.execute(update(VerificationCase).where(VerificationCase.id == case_id).values(status=status))
    await write_audit_log(db, ctx["tid"], ctx["uid"], "update", "verification_case", case_id, before, {"status": status})
    await db.commit()
    return {"message": "Güncellendi"}

@router.post("/findings", response_model=FindingOut)
async def create_finding(data: FindingCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(VerificationFinding).values(
            tenant_id=ctx["tid"],
            case_id=data.case_id,
            finding_type=data.finding_type,
            severity=data.severity,
            title=data.title,
            description_tr=data.description_tr,
            reg_reference=data.reg_reference,
            evidence_required_tr=data.evidence_required_tr,
            status="open",
        ).returning(VerificationFinding)
    )
    f = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "verification_finding", str(f.id), None, data.model_dump())
    await db.commit()
    return FindingOut(
        id=str(f.id),
        case_id=str(f.case_id),
        finding_type=f.finding_type,
        severity=f.severity,
        title=f.title,
        description_tr=f.description_tr,
        reg_reference=f.reg_reference,
        evidence_required_tr=f.evidence_required_tr,
        status=f.status
    )

@router.get("/findings", response_model=list[FindingOut])
async def list_findings(ctx_db=Depends(get_current_db_with_rls), case_id: str):
    ctx, db = ctx_db
    res = await db.execute(
        select(VerificationFinding).where(VerificationFinding.tenant_id == ctx["tid"], VerificationFinding.case_id == case_id)
        .order_by(VerificationFinding.created_at.desc())
    )
    items = res.scalars().all()
    return [
        FindingOut(
            id=str(x.id),
            case_id=str(x.case_id),
            finding_type=x.finding_type,
            severity=x.severity,
            title=x.title,
            description_tr=x.description_tr,
            reg_reference=x.reg_reference,
            evidence_required_tr=x.evidence_required_tr,
            status=x.status
        ) for x in items
    ]

@router.post("/capa", response_model=CAPAOut)
async def create_capa(data: CAPACreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(CAPAAction).values(
            tenant_id=ctx["tid"],
            finding_id=data.finding_id,
            action_type=data.action_type,
            owner=data.owner,
            due_date=data.due_date,
            status="open",
            action_plan_tr=data.action_plan_tr,
            closure_evidence_doc_id=data.closure_evidence_doc_id,
        ).returning(CAPAAction)
    )
    a = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "capa_action", str(a.id), None, data.model_dump())
    await db.commit()
    return CAPAOut(
        id=str(a.id),
        finding_id=str(a.finding_id),
        action_type=a.action_type,
        owner=a.owner,
        due_date=a.due_date,
        status=a.status,
        action_plan_tr=a.action_plan_tr,
        closure_evidence_doc_id=str(a.closure_evidence_doc_id) if a.closure_evidence_doc_id else None,
        verifier_note_tr=a.verifier_note_tr
    )

@router.get("/capa", response_model=list[CAPAOut])
async def list_capa(ctx_db=Depends(get_current_db_with_rls), finding_id: str):
    ctx, db = ctx_db
    res = await db.execute(
        select(CAPAAction).where(CAPAAction.tenant_id == ctx["tid"], CAPAAction.finding_id == finding_id)
    )
    items = res.scalars().all()
    return [
        CAPAOut(
            id=str(x.id),
            finding_id=str(x.finding_id),
            action_type=x.action_type,
            owner=x.owner,
            due_date=x.due_date,
            status=x.status,
            action_plan_tr=x.action_plan_tr,
            closure_evidence_doc_id=str(x.closure_evidence_doc_id) if x.closure_evidence_doc_id else None,
            verifier_note_tr=x.verifier_note_tr
        ) for x in items
    ]
