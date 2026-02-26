from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import insert, select, delete, update
from services.api.routers.auth import get_current_db_with_rls
from services.api.schemas.verification import (
    VerificationCaseCreate, VerificationFindingCreate, CAPAActionCreate
)
from services.api.db.models import (
    VerificationCase, VerificationFinding, CAPAAction
)
from services.api.core.audit import write_audit_log

router = APIRouter()

# ----------------------------
# Verification Cases
# ----------------------------
@router.post("/cases", response_model=dict)
async def create_case(data: VerificationCaseCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(VerificationCase).values(
            tenant_id=ctx["tid"],
            facility_id=data.facility_id,
            scope=data.scope,
            period_start=data.period_start,
            period_end=data.period_end,
            status=data.status,
            verifier_org=data.verifier_org,
            notes=data.notes
        ).returning(VerificationCase.id)
    )
    cid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "verification_case", str(cid), None, data.model_dump())
    await db.commit()
    return {"id": str(cid)}

@router.get("/cases", response_model=list[dict])
async def list_cases(facility_id: str | None = None, scope: str | None = None, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    q = select(VerificationCase).where(VerificationCase.tenant_id == ctx["tid"])
    if facility_id:
        q = q.where(VerificationCase.facility_id == facility_id)
    if scope:
        q = q.where(VerificationCase.scope == scope)
    res = await db.execute(q)
    items = res.scalars().all()
    return [{
        "id": str(x.id),
        "facility_id": str(x.facility_id),
        "scope": x.scope,
        "period_start": x.period_start,
        "period_end": x.period_end,
        "status": x.status,
        "verifier_org": x.verifier_org,
        "notes": x.notes
    } for x in items]

@router.patch("/cases/{case_id}", response_model=dict)
async def update_case(case_id: str, data: dict, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    await db.execute(
        update(VerificationCase).where(VerificationCase.tenant_id == ctx["tid"], VerificationCase.id == case_id).values(**data)
    )
    await write_audit_log(db, ctx["tid"], ctx["uid"], "update", "verification_case", case_id, None, data)
    await db.commit()
    return {"ok": True}

@router.delete("/cases/{case_id}", response_model=dict)
async def delete_case(case_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    await db.execute(delete(VerificationCase).where(VerificationCase.tenant_id == ctx["tid"], VerificationCase.id == case_id))
    await write_audit_log(db, ctx["tid"], ctx["uid"], "delete", "verification_case", case_id, None, {})
    await db.commit()
    return {"ok": True}

# ----------------------------
# Findings
# ----------------------------
@router.post("/findings", response_model=dict)
async def create_finding(data: VerificationFindingCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(VerificationFinding).values(
            tenant_id=ctx["tid"],
            case_id=data.case_id,
            severity=data.severity,
            finding_type=data.finding_type,
            description=data.description,
            evidence_required=data.evidence_required,
            status=data.status,
            notes=data.notes
        ).returning(VerificationFinding.id)
    )
    fid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "verification_finding", str(fid), None, data.model_dump())
    await db.commit()
    return {"id": str(fid)}

@router.get("/findings", response_model=list[dict])
async def list_findings(case_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        select(VerificationFinding).where(VerificationFinding.tenant_id == ctx["tid"], VerificationFinding.case_id == case_id)
    )
    items = res.scalars().all()
    return [{
        "id": str(x.id),
        "case_id": str(x.case_id),
        "severity": x.severity,
        "finding_type": x.finding_type,
        "description": x.description,
        "evidence_required": bool(x.evidence_required),
        "status": x.status,
        "notes": x.notes
    } for x in items]

@router.patch("/findings/{finding_id}", response_model=dict)
async def update_finding(finding_id: str, data: dict, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    await db.execute(
        update(VerificationFinding).where(VerificationFinding.tenant_id == ctx["tid"], VerificationFinding.id == finding_id).values(**data)
    )
    await write_audit_log(db, ctx["tid"], ctx["uid"], "update", "verification_finding", finding_id, None, data)
    await db.commit()
    return {"ok": True}

@router.delete("/findings/{finding_id}", response_model=dict)
async def delete_finding(finding_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    await db.execute(delete(VerificationFinding).where(VerificationFinding.tenant_id == ctx["tid"], VerificationFinding.id == finding_id))
    await write_audit_log(db, ctx["tid"], ctx["uid"], "delete", "verification_finding", finding_id, None, {})
    await db.commit()
    return {"ok": True}

# ----------------------------
# CAPA Actions
# ----------------------------
@router.post("/capa", response_model=dict)
async def create_capa(data: CAPAActionCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(CAPAAction).values(
            tenant_id=ctx["tid"],
            finding_id=data.finding_id,
            action=data.action,
            owner=data.owner,
            due_date=data.due_date,
            status=data.status,
            notes=data.notes
        ).returning(CAPAAction.id)
    )
    cid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "capa_action", str(cid), None, data.model_dump())
    await db.commit()
    return {"id": str(cid)}

@router.get("/capa", response_model=list[dict])
async def list_capa(finding_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        select(CAPAAction).where(CAPAAction.tenant_id == ctx["tid"], CAPAAction.finding_id == finding_id)
    )
    items = res.scalars().all()
    return [{
        "id": str(x.id),
        "finding_id": str(x.finding_id),
        "action": x.action,
        "owner": x.owner,
        "due_date": x.due_date,
        "status": x.status,
        "notes": x.notes
    } for x in items]

@router.patch("/capa/{capa_id}", response_model=dict)
async def update_capa(capa_id: str, data: dict, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    await db.execute(
        update(CAPAAction).where(CAPAAction.tenant_id == ctx["tid"], CAPAAction.id == capa_id).values(**data)
    )
    await write_audit_log(db, ctx["tid"], ctx["uid"], "update", "capa_action", capa_id, None, data)
    await db.commit()
    return {"ok": True}

@router.delete("/capa/{capa_id}", response_model=dict)
async def delete_capa(capa_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    await db.execute(delete(CAPAAction).where(CAPAAction.tenant_id == ctx["tid"], CAPAAction.id == capa_id))
    await write_audit_log(db, ctx["tid"], ctx["uid"], "delete", "capa_action", capa_id, None, {})
    await db.commit()
    return {"ok": True}
