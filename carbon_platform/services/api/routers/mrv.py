from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import insert, select, delete, update
from services.api.routers.auth import get_current_db_with_rls
from services.api.schemas.mrv import (
    MethodologyCreate, MonitoringPlanCreate, MonitoringMethodCreate,
    MeteringAssetCreate, QAQCControlCreate
)
from services.api.db.models import (
    Methodology, MonitoringPlan, MonitoringMethod, MeteringAsset, QAQCControl
)
from services.api.core.audit import write_audit_log

router = APIRouter()

# ----------------------------
# Methodology Registry
# ----------------------------
@router.post("/methodologies", response_model=dict)
async def create_methodology(data: MethodologyCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(Methodology).values(
            tenant_id=ctx["tid"],
            name=data.name,
            scope=data.scope,
            version=data.version,
            status=data.status,
            notes=data.notes,
        ).returning(Methodology.id)
    )
    mid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "methodology", str(mid), None, data.model_dump())
    await db.commit()
    return {"id": str(mid)}

@router.get("/methodologies", response_model=list[dict])
async def list_methodologies(scope: str | None = None, status: str | None = "active", ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    q = select(Methodology).where(Methodology.tenant_id == ctx["tid"])
    if scope:
        q = q.where(Methodology.scope == scope)
    if status:
        q = q.where(Methodology.status == status)
    res = await db.execute(q)
    items = res.scalars().all()
    return [{
        "id": str(x.id),
        "name": x.name,
        "scope": x.scope,
        "version": x.version,
        "status": x.status,
        "notes": x.notes
    } for x in items]

@router.patch("/methodologies/{methodology_id}", response_model=dict)
async def update_methodology(methodology_id: str, data: dict, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    await db.execute(
        update(Methodology).where(Methodology.tenant_id == ctx["tid"], Methodology.id == methodology_id).values(**data)
    )
    await write_audit_log(db, ctx["tid"], ctx["uid"], "update", "methodology", methodology_id, None, data)
    await db.commit()
    return {"ok": True}

# ----------------------------
# Monitoring Plans
# ----------------------------
@router.post("/monitoring-plans", response_model=dict)
async def create_monitoring_plan(data: MonitoringPlanCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(MonitoringPlan).values(
            tenant_id=ctx["tid"],
            facility_id=data.facility_id,
            name=data.name,
            period_start=data.period_start,
            period_end=data.period_end,
            methodology_id=data.methodology_id,
            status=data.status,
            notes=data.notes
        ).returning(MonitoringPlan.id)
    )
    pid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "monitoring_plan", str(pid), None, data.model_dump())
    await db.commit()
    return {"id": str(pid)}

@router.get("/monitoring-plans", response_model=list[dict])
async def list_monitoring_plans(facility_id: str | None = None, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    q = select(MonitoringPlan).where(MonitoringPlan.tenant_id == ctx["tid"])
    if facility_id:
        q = q.where(MonitoringPlan.facility_id == facility_id)
    res = await db.execute(q)
    items = res.scalars().all()
    return [{
        "id": str(x.id),
        "facility_id": str(x.facility_id),
        "name": x.name,
        "period_start": x.period_start,
        "period_end": x.period_end,
        "methodology_id": str(x.methodology_id) if x.methodology_id else None,
        "status": x.status,
        "notes": x.notes
    } for x in items]

@router.delete("/monitoring-plans/{plan_id}", response_model=dict)
async def delete_monitoring_plan(plan_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    await db.execute(delete(MonitoringPlan).where(MonitoringPlan.tenant_id == ctx["tid"], MonitoringPlan.id == plan_id))
    await write_audit_log(db, ctx["tid"], ctx["uid"], "delete", "monitoring_plan", plan_id, None, {})
    await db.commit()
    return {"ok": True}

# ----------------------------
# Monitoring Methods
# ----------------------------
@router.post("/monitoring-methods", response_model=dict)
async def create_monitoring_method(data: MonitoringMethodCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(MonitoringMethod).values(
            tenant_id=ctx["tid"],
            monitoring_plan_id=data.monitoring_plan_id,
            category=data.category,
            method=data.method,
            data_source=data.data_source,
            uncertainty=data.uncertainty,
            notes=data.notes
        ).returning(MonitoringMethod.id)
    )
    mid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "monitoring_method", str(mid), None, data.model_dump())
    await db.commit()
    return {"id": str(mid)}

@router.get("/monitoring-methods", response_model=list[dict])
async def list_monitoring_methods(monitoring_plan_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        select(MonitoringMethod).where(MonitoringMethod.tenant_id == ctx["tid"], MonitoringMethod.monitoring_plan_id == monitoring_plan_id)
    )
    items = res.scalars().all()
    return [{
        "id": str(x.id),
        "monitoring_plan_id": str(x.monitoring_plan_id),
        "category": x.category,
        "method": x.method,
        "data_source": x.data_source,
        "uncertainty": float(x.uncertainty) if x.uncertainty is not None else None,
        "notes": x.notes
    } for x in items]

# ----------------------------
# Metering Assets
# ----------------------------
@router.post("/metering-assets", response_model=dict)
async def create_metering_asset(data: MeteringAssetCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(MeteringAsset).values(
            tenant_id=ctx["tid"],
            facility_id=data.facility_id,
            asset_name=data.asset_name,
            asset_type=data.asset_type,
            calibration_due=data.calibration_due,
            notes=data.notes
        ).returning(MeteringAsset.id)
    )
    aid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "metering_asset", str(aid), None, data.model_dump())
    await db.commit()
    return {"id": str(aid)}

@router.get("/metering-assets", response_model=list[dict])
async def list_metering_assets(facility_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        select(MeteringAsset).where(MeteringAsset.tenant_id == ctx["tid"], MeteringAsset.facility_id == facility_id)
    )
    items = res.scalars().all()
    return [{
        "id": str(x.id),
        "facility_id": str(x.facility_id),
        "asset_name": x.asset_name,
        "asset_type": x.asset_type,
        "calibration_due": x.calibration_due,
        "notes": x.notes
    } for x in items]

# ----------------------------
# QA/QC Controls
# ----------------------------
@router.post("/qaqc-controls", response_model=dict)
async def create_qaqc_control(data: QAQCControlCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(QAQCControl).values(
            tenant_id=ctx["tid"],
            monitoring_plan_id=data.monitoring_plan_id,
            control_name=data.control_name,
            frequency=data.frequency,
            owner=data.owner,
            evidence_required=data.evidence_required,
            notes=data.notes
        ).returning(QAQCControl.id)
    )
    qid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "qaqc_control", str(qid), None, data.model_dump())
    await db.commit()
    return {"id": str(qid)}

@router.get("/qaqc-controls", response_model=list[dict])
async def list_qaqc_controls(monitoring_plan_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        select(QAQCControl).where(QAQCControl.tenant_id == ctx["tid"], QAQCControl.monitoring_plan_id == monitoring_plan_id)
    )
    items = res.scalars().all()
    return [{
        "id": str(x.id),
        "monitoring_plan_id": str(x.monitoring_plan_id),
        "control_name": x.control_name,
        "frequency": x.frequency,
        "owner": x.owner,
        "evidence_required": bool(x.evidence_required),
        "notes": x.notes
    } for x in items]
