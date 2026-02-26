import json
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import insert, select, delete, update
from services.api.routers.auth import get_current_db_with_rls
from services.api.schemas.scenario import (
    ScenarioCreate, ScenarioAssumptionCreate, ScenarioRunCreate
)
from services.api.db.models import (
    Scenario, ScenarioAssumption, ScenarioRun, Facility, EmissionFactor
)
from services.api.core.audit import write_audit_log, canonical_json, sha256_text
from packages.calc_core.engines import Scenario_Engine

router = APIRouter()

# ----------------------------
# Scenarios
# ----------------------------
@router.post("/scenarios", response_model=dict)
async def create_scenario(data: ScenarioCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(Scenario).values(
            tenant_id=ctx["tid"],
            facility_id=data.facility_id,
            name=data.name,
            description=data.description,
            status=data.status
        ).returning(Scenario.id)
    )
    sid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "scenario", str(sid), None, data.model_dump())
    await db.commit()
    return {"id": str(sid)}

@router.get("/scenarios", response_model=list[dict])
async def list_scenarios(facility_id: str | None = None, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    q = select(Scenario).where(Scenario.tenant_id == ctx["tid"])
    if facility_id:
        q = q.where(Scenario.facility_id == facility_id)
    res = await db.execute(q)
    items = res.scalars().all()
    return [{
        "id": str(x.id),
        "facility_id": str(x.facility_id),
        "name": x.name,
        "description": x.description,
        "status": x.status
    } for x in items]

@router.patch("/scenarios/{scenario_id}", response_model=dict)
async def update_scenario(scenario_id: str, data: dict, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    await db.execute(
        update(Scenario).where(Scenario.tenant_id == ctx["tid"], Scenario.id == scenario_id).values(**data)
    )
    await write_audit_log(db, ctx["tid"], ctx["uid"], "update", "scenario", scenario_id, None, data)
    await db.commit()
    return {"ok": True}

@router.delete("/scenarios/{scenario_id}", response_model=dict)
async def delete_scenario(scenario_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    await db.execute(delete(Scenario).where(Scenario.tenant_id == ctx["tid"], Scenario.id == scenario_id))
    await write_audit_log(db, ctx["tid"], ctx["uid"], "delete", "scenario", scenario_id, None, {})
    await db.commit()
    return {"ok": True}

# ----------------------------
# Assumptions
# ----------------------------
@router.post("/assumptions", response_model=dict)
async def add_assumption(data: ScenarioAssumptionCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(ScenarioAssumption).values(
            tenant_id=ctx["tid"],
            scenario_id=data.scenario_id,
            key=data.key,
            value=data.value,
            unit=data.unit,
            notes=data.notes
        ).returning(ScenarioAssumption.id)
    )
    aid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "scenario_assumption", str(aid), None, data.model_dump())
    await db.commit()
    return {"id": str(aid)}

@router.get("/assumptions", response_model=list[dict])
async def list_assumptions(scenario_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        select(ScenarioAssumption).where(ScenarioAssumption.tenant_id == ctx["tid"], ScenarioAssumption.scenario_id == scenario_id)
    )
    items = res.scalars().all()
    return [{
        "id": str(x.id),
        "scenario_id": str(x.scenario_id),
        "key": x.key,
        "value": x.value,
        "unit": x.unit,
        "notes": x.notes
    } for x in items]

@router.delete("/assumptions/{assumption_id}", response_model=dict)
async def delete_assumption(assumption_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    await db.execute(delete(ScenarioAssumption).where(ScenarioAssumption.tenant_id == ctx["tid"], ScenarioAssumption.id == assumption_id))
    await write_audit_log(db, ctx["tid"], ctx["uid"], "delete", "scenario_assumption", assumption_id, None, {})
    await db.commit()
    return {"ok": True}

# ----------------------------
# Runs
# ----------------------------
@router.post("/runs", response_model=dict)
async def run_scenario(data: ScenarioRunCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db

    scen_res = await db.execute(
        select(Scenario).where(Scenario.tenant_id == ctx["tid"], Scenario.id == data.scenario_id)
    )
    scenario = scen_res.scalar_one_or_none()
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario bulunamadı")

    fac_res = await db.execute(
        select(Facility).where(Facility.tenant_id == ctx["tid"], Facility.id == scenario.facility_id)
    )
    facility = fac_res.scalar_one_or_none()
    if not facility:
        raise HTTPException(status_code=404, detail="Facility bulunamadı")

    ass_res = await db.execute(
        select(ScenarioAssumption).where(ScenarioAssumption.tenant_id == ctx["tid"], ScenarioAssumption.scenario_id == data.scenario_id)
    )
    assumptions = ass_res.scalars().all()
    assumption_map = {a.key: a.value for a in assumptions}

    ef_res = await db.execute(
        select(EmissionFactor).where(EmissionFactor.tenant_id == ctx["tid"])
    )
    factors = ef_res.scalars().all()

    engine = Scenario_Engine()
    result = engine.run(
        facility_id=str(facility.id),
        assumptions=assumption_map,
        factors=factors,
        carbon_price=Decimal(str(data.carbon_price))
    )

    payload = {
        "scenario_id": str(data.scenario_id),
        "facility_id": str(facility.id),
        "assumptions": assumption_map,
        "carbon_price": float(data.carbon_price),
        "result": result
    }
    canonical = canonical_json(payload)
    payload_hash = sha256_text(canonical)

    run_res = await db.execute(
        insert(ScenarioRun).values(
            tenant_id=ctx["tid"],
            scenario_id=data.scenario_id,
            facility_id=facility.id,
            payload_json=canonical,
            payload_hash=payload_hash,
            status="completed"
        ).returning(ScenarioRun.id)
    )
    rid = run_res.scalar_one()

    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "scenario_run", str(rid), None, payload)
    await db.commit()
    return {"id": str(rid), "payload_hash": payload_hash}

@router.get("/runs", response_model=list[dict])
async def list_runs(scenario_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        select(ScenarioRun).where(ScenarioRun.tenant_id == ctx["tid"], ScenarioRun.scenario_id == scenario_id)
    )
    items = res.scalars().all()
    return [{
        "id": str(x.id),
        "scenario_id": str(x.scenario_id),
        "facility_id": str(x.facility_id),
        "payload_hash": x.payload_hash,
        "status": x.status
    } for x in items]
