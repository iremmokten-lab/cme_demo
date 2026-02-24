import json
from datetime import datetime
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import insert, select, update
from services.api.routers.auth import get_current_db_with_rls
from services.api.db.models import Scenario, ScenarioAssumption, ScenarioRun, Job, CalculationRun
from services.api.schemas.scenario import (
    ScenarioCreate, ScenarioOut,
    AssumptionUpsert, AssumptionOut,
    ScenarioRunRequest, ScenarioRunOut
)
from services.api.core.audit import write_audit_log, canonical_json, sha256_text

router = APIRouter()

SUPPORTED_KEYS = {"ets_price", "allowances", "reduction_pct", "grid_factor_multiplier"}

def _parse_decimal(value: str, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value).strip())
    except Exception:
        return default

@router.post("/scenarios", response_model=ScenarioOut)
async def create_scenario(data: ScenarioCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(Scenario).values(
            tenant_id=ctx["tid"],
            facility_id=data.facility_id,
            name=data.name,
            status="draft",
            base_activity_record_id=data.base_activity_record_id,
            period_start=data.period_start,
            period_end=data.period_end,
            notes_tr=data.notes_tr,
            created_by=ctx["uid"],
        ).returning(Scenario)
    )
    s = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "scenario", str(s.id), None, data.model_dump())
    await db.commit()
    return ScenarioOut(
        id=str(s.id),
        facility_id=str(s.facility_id),
        name=s.name,
        status=s.status,
        base_activity_record_id=str(s.base_activity_record_id) if s.base_activity_record_id else None,
        period_start=s.period_start,
        period_end=s.period_end,
        notes_tr=s.notes_tr
    )

@router.get("/scenarios", response_model=list[ScenarioOut])
async def list_scenarios(ctx_db=Depends(get_current_db_with_rls), facility_id: str | None = None):
    ctx, db = ctx_db
    q = select(Scenario).where(Scenario.tenant_id == ctx["tid"])
    if facility_id:
        q = q.where(Scenario.facility_id == facility_id)
    res = await db.execute(q.order_by(Scenario.created_at.desc()))
    items = res.scalars().all()
    return [
        ScenarioOut(
            id=str(x.id),
            facility_id=str(x.facility_id),
            name=x.name,
            status=x.status,
            base_activity_record_id=str(x.base_activity_record_id) if x.base_activity_record_id else None,
            period_start=x.period_start,
            period_end=x.period_end,
            notes_tr=x.notes_tr
        ) for x in items
    ]

@router.post("/assumptions", response_model=AssumptionOut)
async def upsert_assumption(data: AssumptionUpsert, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    if data.key not in SUPPORTED_KEYS:
        raise HTTPException(status_code=400, detail=f"Desteklenmeyen key. Desteklenenler: {sorted(SUPPORTED_KEYS)}")

    res = await db.execute(
        select(ScenarioAssumption).where(
            ScenarioAssumption.tenant_id == ctx["tid"],
            ScenarioAssumption.scenario_id == data.scenario_id,
            ScenarioAssumption.key == data.key
        )
    )
    existing = res.scalar_one_or_none()
    if existing:
        await db.execute(
            update(ScenarioAssumption)
            .where(ScenarioAssumption.id == existing.id)
            .values(value=data.value, unit=data.unit, notes_tr=data.notes_tr)
        )
        await write_audit_log(db, ctx["tid"], ctx["uid"], "update", "scenario_assumption", str(existing.id), None, data.model_dump())
        await db.commit()
        return AssumptionOut(
            id=str(existing.id),
            scenario_id=str(existing.scenario_id),
            key=data.key,
            value=data.value,
            unit=data.unit,
            notes_tr=data.notes_tr
        )

    ins = await db.execute(
        insert(ScenarioAssumption).values(
            tenant_id=ctx["tid"],
            scenario_id=data.scenario_id,
            key=data.key,
            value=data.value,
            unit=data.unit,
            notes_tr=data.notes_tr
        ).returning(ScenarioAssumption)
    )
    a = ins.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "scenario_assumption", str(a.id), None, data.model_dump())
    await db.commit()
    return AssumptionOut(
        id=str(a.id),
        scenario_id=str(a.scenario_id),
        key=a.key,
        value=a.value,
        unit=a.unit,
        notes_tr=a.notes_tr
    )

@router.get("/assumptions", response_model=list[AssumptionOut])
async def list_assumptions(ctx_db=Depends(get_current_db_with_rls), scenario_id: str):
    ctx, db = ctx_db
    res = await db.execute(
        select(ScenarioAssumption).where(ScenarioAssumption.tenant_id == ctx["tid"], ScenarioAssumption.scenario_id == scenario_id)
        .order_by(ScenarioAssumption.key.asc())
    )
    items = res.scalars().all()
    return [
        AssumptionOut(
            id=str(x.id),
            scenario_id=str(x.scenario_id),
            key=x.key,
            value=x.value,
            unit=x.unit,
            notes_tr=x.notes_tr
        ) for x in items
    ]

def _compute_scenario_result(base_calc: dict, assumptions: dict) -> dict:
    """
    Basit ama production-safe what-if:
      - ets_price override
      - allowances override
      - reduction_pct: facility_tco2e * (1 - pct)
    """
    totals = base_calc.get("totals", {})
    costs = base_calc.get("costs", {})

    facility = _parse_decimal(totals.get("facility_tco2e", "0"))
    default_ets_price = _parse_decimal(costs.get("ets_price_eur_per_tco2", "0"))
    default_allow = _parse_decimal(costs.get("ets_allowances_tco2", "0"))

    ets_price = _parse_decimal(assumptions.get("ets_price", str(default_ets_price)), default_ets_price)
    allowances = _parse_decimal(assumptions.get("allowances", str(default_allow)), default_allow)

    reduction_pct = _parse_decimal(assumptions.get("reduction_pct", "0"), Decimal("0"))
    if reduction_pct < 0:
        reduction_pct = Decimal("0")
    if reduction_pct > 100:
        reduction_pct = Decimal("100")
    multiplier = (Decimal("100") - reduction_pct) / Decimal("100")
    facility_adj = facility * multiplier

    ets_payable = facility_adj - allowances
    if ets_payable < 0:
        ets_payable = Decimal("0")
    ets_cost = ets_payable * ets_price

    cbam_cert = facility_adj
    cbam_cost = cbam_cert * ets_price

    out = {
        "assumptions": {
            "ets_price": str(ets_price),
            "allowances": str(allowances),
            "reduction_pct": str(reduction_pct),
        },
        "base": {
            "facility_tco2e": str(facility),
            "ets_price": str(default_ets_price),
            "allowances": str(default_allow),
        },
        "scenario": {
            "facility_tco2e": str(facility_adj),
            "ets_payable_tco2": str(ets_payable),
            "ets_cost_eur": str(ets_cost),
            "cbam_certificates_tco2e": str(cbam_cert),
            "cbam_cost_eur": str(cbam_cost),
            "objective_total_eur": str(ets_cost + cbam_cost),
        }
    }
    out_hash = sha256_text(canonical_json(out))
    out["meta"] = {"result_hash": out_hash}
    return out

@router.post("/run", response_model=ScenarioRunOut)
async def run_scenario(data: ScenarioRunRequest, ctx_db=Depends(get_current_db_with_rls)):
    """
    Scenario run:
      - base_activity_record_id -> latest calculation_run alınır
      - assumptions -> what-if uygulanır
      - sonuç scenario_run'a yazılır
      - ayrıca job tablosuna da 'scenario_run' kayıt bırakır (opsiyonel worker için)
    """
    ctx, db = ctx_db
    sres = await db.execute(select(Scenario).where(Scenario.tenant_id == ctx["tid"], Scenario.id == data.scenario_id))
    scen = sres.scalar_one_or_none()
    if scen is None:
        raise HTTPException(status_code=404, detail="Scenario bulunamadı")
    if not scen.base_activity_record_id:
        raise HTTPException(status_code=400, detail="Scenario base_activity_record_id boş. Scenario oluştururken set edin.")

    # base calc
    cres = await db.execute(
        select(CalculationRun)
        .where(CalculationRun.tenant_id == ctx["tid"], CalculationRun.activity_record_id == scen.base_activity_record_id)
        .order_by(CalculationRun.created_at.desc())
        .limit(1)
    )
    base = cres.scalar_one_or_none()
    if base is None:
        raise HTTPException(status_code=400, detail="Scenario base activity record için calculation_run yok. Önce /calc/run çalıştırın.")
    base_calc = json.loads(base.result_json)

    # assumptions
    ares = await db.execute(select(ScenarioAssumption).where(ScenarioAssumption.tenant_id == ctx["tid"], ScenarioAssumption.scenario_id == scen.id))
    assumptions = {x.key: x.value for x in ares.scalars().all()}

    # create job (optional worker) but we compute sync for Streamlit simplicity
    payload = {"scenario_id": str(scen.id), "tenant_id": ctx["tid"], "created_at": datetime.utcnow().isoformat(), "assumptions": assumptions}
    jres = await db.execute(
        insert(Job).values(
            tenant_id=ctx["tid"],
            job_type="scenario_run",
            status="succeeded",
            payload=canonical_json(payload),
            result=None,
        ).returning(Job.id)
    )
    job_id = jres.scalar_one()

    # compute
    result = _compute_scenario_result(base_calc, assumptions)
    rh = result["meta"]["result_hash"]

    rres = await db.execute(
        insert(ScenarioRun).values(
            tenant_id=ctx["tid"],
            scenario_id=scen.id,
            job_id=job_id,
            status="succeeded",
            result_json=canonical_json(result),
            result_hash=rh,
        ).returning(ScenarioRun.id)
    )
    run_id = rres.scalar_one()

    await write_audit_log(db, ctx["tid"], ctx["uid"], "scenario_run", "scenario_run", str(run_id), None, {"job_id": str(job_id), "result_hash": rh})
    await db.commit()

    return ScenarioRunOut(run_id=str(run_id), status="succeeded", job_id=str(job_id), result=result)

@router.get("/runs", response_model=list[ScenarioRunOut])
async def list_runs(ctx_db=Depends(get_current_db_with_rls), scenario_id: str):
    ctx, db = ctx_db
    res = await db.execute(
        select(ScenarioRun).where(ScenarioRun.tenant_id == ctx["tid"], ScenarioRun.scenario_id == scenario_id)
        .order_by(ScenarioRun.created_at.desc())
        .limit(20)
    )
    items = res.scalars().all()
    out = []
    for x in items:
        out.append(
            ScenarioRunOut(
                run_id=str(x.id),
                status=x.status,
                job_id=str(x.job_id) if x.job_id else None,
                result=json.loads(x.result_json) if x.result_json else None
            )
        )
    return out
