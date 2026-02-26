from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import insert, select, update
from services.api.routers.auth import get_current_db_with_rls
from services.api.db.models import (
    Methodology, MonitoringPlan, MonitoringMethod, MeteringAsset, QAQCControl
)
from services.api.schemas.mrv import (
    MethodologyCreate, MethodologyOut,
    MonitoringPlanCreate, MonitoringPlanOut,
    MonitoringMethodCreate, MonitoringMethodOut,
    MeteringAssetCreate, MeteringAssetOut,
    QAQCControlCreate, QAQCControlOut
)
from services.api.core.audit import write_audit_log

router = APIRouter()

# -----------------
# Methodology Registry
# -----------------
@router.post("/methodologies", response_model=MethodologyOut)
async def create_methodology(data: MethodologyCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(Methodology).values(
            tenant_id=ctx["tid"],
            code=data.code,
            name=data.name,
            scope=data.scope,
            tier_level=data.tier_level,
            reg_reference=data.reg_reference,
            description_tr=data.description_tr,
            status="active",
        ).returning(Methodology)
    )
    m = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "methodology", str(m.id), None, data.model_dump())
    await db.commit()
    return MethodologyOut(
        id=str(m.id),
        code=m.code,
        name=m.name,
        scope=m.scope,
        tier_level=m.tier_level,
        reg_reference=m.reg_reference,
        description_tr=m.description_tr,
        status=m.status,
    )

@router.get("/methodologies", response_model=list[MethodologyOut])
async def list_methodologies(ctx_db=Depends(get_current_db_with_rls), scope: str | None = None, status: str | None = "active"):
    ctx, db = ctx_db
    q = select(Methodology).where(Methodology.tenant_id == ctx["tid"])
    if scope:
        q = q.where(Methodology.scope == scope)
    if status:
        q = q.where(Methodology.status == status)
    res = await db.execute(q.order_by(Methodology.code.asc()))
    items = res.scalars().all()
    return [
        MethodologyOut(
            id=str(x.id),
            code=x.code,
            name=x.name,
            scope=x.scope,
            tier_level=x.tier_level,
            reg_reference=x.reg_reference,
            description_tr=x.description_tr,
            status=x.status,
        ) for x in items
    ]

# -----------------
# Monitoring Plan
# -----------------
@router.post("/monitoring-plans", response_model=MonitoringPlanOut)
async def create_monitoring_plan(data: MonitoringPlanCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(MonitoringPlan).values(
            tenant_id=ctx["tid"],
            facility_id=data.facility_id,
            version=data.version,
            status="draft",
            effective_from=data.effective_from,
            effective_to=data.effective_to,
            overall_notes_tr=data.overall_notes_tr,
        ).returning(MonitoringPlan)
    )
    mp = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "monitoring_plan", str(mp.id), None, data.model_dump())
    await db.commit()
    return MonitoringPlanOut(
        id=str(mp.id),
        facility_id=str(mp.facility_id),
        version=mp.version,
        status=mp.status,
        effective_from=mp.effective_from,
        effective_to=mp.effective_to,
        overall_notes_tr=mp.overall_notes_tr,
    )

@router.get("/monitoring-plans", response_model=list[MonitoringPlanOut])
async def list_monitoring_plans(ctx_db=Depends(get_current_db_with_rls), facility_id: str | None = None):
    ctx, db = ctx_db
    q = select(MonitoringPlan).where(MonitoringPlan.tenant_id == ctx["tid"])
    if facility_id:
        q = q.where(MonitoringPlan.facility_id == facility_id)
    res = await db.execute(q.order_by(MonitoringPlan.facility_id.asc(), MonitoringPlan.version.desc()))
    items = res.scalars().all()
    return [
        MonitoringPlanOut(
            id=str(x.id),
            facility_id=str(x.facility_id),
            version=x.version,
            status=x.status,
            effective_from=x.effective_from,
            effective_to=x.effective_to,
            overall_notes_tr=x.overall_notes_tr,
        ) for x in items
    ]

@router.post("/monitoring-plans/{plan_id}/approve")
async def approve_monitoring_plan(plan_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    if "admin" not in ctx.get("roles", []):
        raise HTTPException(status_code=403, detail="İzleme planı onayı için admin rolü gerekli")
    res = await db.execute(select(MonitoringPlan).where(MonitoringPlan.tenant_id == ctx["tid"], MonitoringPlan.id == plan_id))
    mp = res.scalar_one_or_none()
    if mp is None:
        raise HTTPException(status_code=404, detail="Monitoring plan bulunamadı")
    before = {"status": mp.status}
    await db.execute(update(MonitoringPlan).where(MonitoringPlan.id == plan_id).values(status="approved"))
    await write_audit_log(db, ctx["tid"], ctx["uid"], "approve", "monitoring_plan", plan_id, before, {"status": "approved"})
    await db.commit()
    return {"message": "Monitoring plan onaylandı"}

# -----------------
# Monitoring Methods
# -----------------
@router.post("/monitoring-methods", response_model=MonitoringMethodOut)
async def create_monitoring_method(data: MonitoringMethodCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(MonitoringMethod).values(
            tenant_id=ctx["tid"],
            monitoring_plan_id=data.monitoring_plan_id,
            emission_source=data.emission_source,
            method_type=data.method_type,
            tier_level=data.tier_level,
            uncertainty_class=data.uncertainty_class,
            methodology_id=data.methodology_id,
            reference_standard=data.reference_standard,
        ).returning(MonitoringMethod)
    )
    mm = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "monitoring_method", str(mm.id), None, data.model_dump())
    await db.commit()
    return MonitoringMethodOut(
        id=str(mm.id),
        monitoring_plan_id=str(mm.monitoring_plan_id),
        emission_source=mm.emission_source,
        method_type=mm.method_type,
        tier_level=mm.tier_level,
        uncertainty_class=mm.uncertainty_class,
        methodology_id=str(mm.methodology_id) if mm.methodology_id else None,
        reference_standard=mm.reference_standard,
    )

@router.get("/monitoring-methods", response_model=list[MonitoringMethodOut])
async def list_monitoring_methods(ctx_db=Depends(get_current_db_with_rls), monitoring_plan_id: str):
    ctx, db = ctx_db
    q = select(MonitoringMethod).where(MonitoringMethod.tenant_id == ctx["tid"], MonitoringMethod.monitoring_plan_id == monitoring_plan_id)
    res = await db.execute(q.order_by(MonitoringMethod.emission_source.asc()))
    items = res.scalars().all()
    return [
        MonitoringMethodOut(
            id=str(x.id),
            monitoring_plan_id=str(x.monitoring_plan_id),
            emission_source=x.emission_source,
            method_type=x.method_type,
            tier_level=x.tier_level,
            uncertainty_class=x.uncertainty_class,
            methodology_id=str(x.methodology_id) if x.methodology_id else None,
            reference_standard=x.reference_standard,
        ) for x in items
    ]

# -----------------
# Metering Assets
# -----------------
@router.post("/metering-assets", response_model=MeteringAssetOut)
async def create_metering_asset(data: MeteringAssetCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(MeteringAsset).values(
            tenant_id=ctx["tid"],
            facility_id=data.facility_id,
            asset_type=data.asset_type,
            serial_no=data.serial_no,
            calibration_schedule=data.calibration_schedule,
            last_calibration_doc_id=data.last_calibration_doc_id,
        ).returning(MeteringAsset)
    )
    ma = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "metering_asset", str(ma.id), None, data.model_dump())
    await db.commit()
    return MeteringAssetOut(
        id=str(ma.id),
        facility_id=str(ma.facility_id),
        asset_type=ma.asset_type,
        serial_no=ma.serial_no,
        calibration_schedule=ma.calibration_schedule,
        last_calibration_doc_id=str(ma.last_calibration_doc_id) if ma.last_calibration_doc_id else None,
    )

@router.get("/metering-assets", response_model=list[MeteringAssetOut])
async def list_metering_assets(ctx_db=Depends(get_current_db_with_rls), facility_id: str):
    ctx, db = ctx_db
    q = select(MeteringAsset).where(MeteringAsset.tenant_id == ctx["tid"], MeteringAsset.facility_id == facility_id)
    res = await db.execute(q.order_by(MeteringAsset.asset_type.asc()))
    items = res.scalars().all()
    return [
        MeteringAssetOut(
            id=str(x.id),
            facility_id=str(x.facility_id),
            asset_type=x.asset_type,
            serial_no=x.serial_no,
            calibration_schedule=x.calibration_schedule,
            last_calibration_doc_id=str(x.last_calibration_doc_id) if x.last_calibration_doc_id else None,
        ) for x in items
    ]

# -----------------
# QA/QC Controls
# -----------------
@router.post("/qaqc-controls", response_model=QAQCControlOut)
async def create_qaqc_control(data: QAQCControlCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(QAQCControl).values(
            tenant_id=ctx["tid"],
            monitoring_plan_id=data.monitoring_plan_id,
            control_type=data.control_type,
            frequency=data.frequency,
            acceptance_criteria_tr=data.acceptance_criteria_tr,
        ).returning(QAQCControl)
    )
    qc = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "qa_qc_control", str(qc.id), None, data.model_dump())
    await db.commit()
    return QAQCControlOut(
        id=str(qc.id),
        monitoring_plan_id=str(qc.monitoring_plan_id),
        control_type=qc.control_type,
        frequency=qc.frequency,
        acceptance_criteria_tr=qc.acceptance_criteria_tr,
    )

@router.get("/qaqc-controls", response_model=list[QAQCControlOut])
async def list_qaqc_controls(ctx_db=Depends(get_current_db_with_rls), monitoring_plan_id: str):
    ctx, db = ctx_db
    q = select(QAQCControl).where(QAQCControl.tenant_id == ctx["tid"], QAQCControl.monitoring_plan_id == monitoring_plan_id)
    res = await db.execute(q.order_by(QAQCControl.control_type.asc()))
    items = res.scalars().all()
    return [
        QAQCControlOut(
            id=str(x.id),
            monitoring_plan_id=str(x.monitoring_plan_id),
            control_type=x.control_type,
            frequency=x.frequency,
            acceptance_criteria_tr=x.acceptance_criteria_tr,
        ) for x in items
    ]
