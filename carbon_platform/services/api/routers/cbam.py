import json
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import insert, select
from services.api.routers.auth import get_current_db_with_rls
from services.api.schemas.cbam import (
    ProductionRecordCreate, MaterialInputCreate, ExportRecordCreate,
    CBAMRunCreate, CBAMRunOut
)
from services.api.db.models import (
    ProductionRecord, MaterialInput, ExportRecord,
    CalculationRun, Product, Material, EmissionFactor, CBAMReport
)
from services.api.core.audit import write_audit_log, canonical_json, sha256_text
from packages.calc_core.models import ProductionInput, PrecursorInput
from packages.calc_core.engines import CBAM_Product_Engine

router = APIRouter()

@router.post("/production", response_model=dict)
async def add_production(data: ProductionRecordCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(ProductionRecord).values(
            tenant_id=ctx["tid"],
            activity_record_id=data.activity_record_id,
            product_id=data.product_id,
            quantity=data.quantity,
            unit=data.unit,
            doc_id=data.doc_id,
        ).returning(ProductionRecord.id)
    )
    pid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "production_record", str(pid), None, data.model_dump())
    await db.commit()
    return {"id": str(pid)}

@router.get("/production", response_model=list[dict])
async def list_production(ctx_db=Depends(get_current_db_with_rls), activity_record_id: str):
    ctx, db = ctx_db
    res = await db.execute(
        select(ProductionRecord).where(ProductionRecord.tenant_id == ctx["tid"], ProductionRecord.activity_record_id == activity_record_id)
    )
    items = res.scalars().all()
    return [{
        "id": str(x.id),
        "activity_record_id": str(x.activity_record_id),
        "product_id": str(x.product_id),
        "quantity": float(x.quantity),
        "unit": x.unit,
        "doc_id": str(x.doc_id) if x.doc_id else None
    } for x in items]

@router.post("/material-inputs", response_model=dict)
async def add_material_input(data: MaterialInputCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(MaterialInput).values(
            tenant_id=ctx["tid"],
            activity_record_id=data.activity_record_id,
            product_id=data.product_id,
            material_id=data.material_id,
            quantity=data.quantity,
            unit=data.unit,
            embedded_factor_id=data.embedded_factor_id,
            doc_id=data.doc_id,
        ).returning(MaterialInput.id)
    )
    mid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "material_input", str(mid), None, data.model_dump())
    await db.commit()
    return {"id": str(mid)}

@router.get("/material-inputs", response_model=list[dict])
async def list_material_inputs(ctx_db=Depends(get_current_db_with_rls), activity_record_id: str):
    ctx, db = ctx_db
    res = await db.execute(
        select(MaterialInput).where(MaterialInput.tenant_id == ctx["tid"], MaterialInput.activity_record_id == activity_record_id)
    )
    items = res.scalars().all()
    return [{
        "id": str(x.id),
        "activity_record_id": str(x.activity_record_id),
        "product_id": str(x.product_id),
        "material_id": str(x.material_id),
        "quantity": float(x.quantity),
        "unit": x.unit,
        "embedded_factor_id": str(x.embedded_factor_id) if x.embedded_factor_id else None,
        "doc_id": str(x.doc_id) if x.doc_id else None
    } for x in items]

@router.post("/exports", response_model=dict)
async def add_export(data: ExportRecordCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(ExportRecord).values(
            tenant_id=ctx["tid"],
            facility_id=data.facility_id,
            product_id=data.product_id,
            period_start=data.period_start,
            period_end=data.period_end,
            export_qty=data.export_qty,
            unit=data.unit,
            destination=data.destination,
            customs_doc_id=data.customs_doc_id,
        ).returning(ExportRecord.id)
    )
    eid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "export_record", str(eid), None, data.model_dump())
    await db.commit()
    return {"id": str(eid)}

@router.get("/exports", response_model=list[dict])
async def list_exports(ctx_db=Depends(get_current_db_with_rls), facility_id: str, period_start: str, period_end: str):
    ctx, db = ctx_db
    res = await db.execute(
        select(ExportRecord).where(
            ExportRecord.tenant_id == ctx["tid"],
            ExportRecord.facility_id == facility_id,
            ExportRecord.period_start == period_start,
            ExportRecord.period_end == period_end
        )
    )
    items = res.scalars().all()
    return [{
        "id": str(x.id),
        "facility_id": str(x.facility_id),
        "product_id": str(x.product_id),
        "period_start": x.period_start,
        "period_end": x.period_end,
        "export_qty": float(x.export_qty),
        "unit": x.unit,
        "destination": x.destination,
        "customs_doc_id": str(x.customs_doc_id) if x.customs_doc_id else None
    } for x in items]

async def _approved_factor_value(db, tenant_id: str, factor_id: str) -> Decimal:
    res = await db.execute(
        select(EmissionFactor).where(EmissionFactor.tenant_id == tenant_id, EmissionFactor.id == factor_id, EmissionFactor.status == "approved")
    )
    f = res.scalar_one_or_none()
    if f is None:
        raise HTTPException(status_code=400, detail=f"Onaylı faktör bulunamadı: {factor_id}")
    return Decimal(str(float(f.value)))

@router.post("/run", response_model=CBAMRunOut)
async def run_cbam(data: CBAMRunCreate, ctx_db=Depends(get_current_db_with_rls)):
    """
    CBAM raporu üretimi:
    - Facility calculation_run (activity_record) olmalı (facility emissions kaynağı)
    - ProductionRecord (ürün üretimi) olmalı
    - MaterialInput (precursor) opsiyonel ama önerilir
    - ExportRecord (ihracat) olmalı
    """
    ctx, db = ctx_db

    # facility calc result
    res = await db.execute(
        select(CalculationRun).where(
            CalculationRun.tenant_id == ctx["tid"],
            CalculationRun.activity_record_id == data.activity_record_id
        ).order_by(CalculationRun.created_at.desc()).limit(1)
    )
    calc = res.scalar_one_or_none()
    if calc is None:
        raise HTTPException(status_code=400, detail="Bu activity_record için calculation_run bulunamadı. Önce /calc/run çalıştırın.")
    calc_result = json.loads(calc.result_json)
    facility_totals = calc_result.get("totals", {})
    if "facility_tco2e" not in facility_totals:
        raise HTTPException(status_code=400, detail="CalculationRun totals içinde facility_tco2e yok.")

    # production records
    pres = await db.execute(
        select(ProductionRecord).where(ProductionRecord.tenant_id == ctx["tid"], ProductionRecord.activity_record_id == data.activity_record_id)
    )
    prod_rows = pres.scalars().all()
    if not prod_rows:
        raise HTTPException(status_code=400, detail="Ürün bazlı üretim yok (production_record). Allocation yapılamaz.")

    productions = [
        ProductionInput(product_id=str(x.product_id), quantity=Decimal(str(float(x.quantity))))
        for x in prod_rows
    ]

    # material inputs (precursors)
    mres = await db.execute(
        select(MaterialInput).where(MaterialInput.tenant_id == ctx["tid"], MaterialInput.activity_record_id == data.activity_record_id)
    )
    mi_rows = mres.scalars().all()

    # material catalog for default embedded factor
    material_ids = list({str(x.material_id) for x in mi_rows})
    mat_map = {}
    if material_ids:
        mats = await db.execute(select(Material).where(Material.tenant_id == ctx["tid"], Material.id.in_(material_ids)))
        for m in mats.scalars().all():
            mat_map[str(m.id)] = m

    precursors: list[PrecursorInput] = []
    for x in mi_rows:
        factor_id = str(x.embedded_factor_id) if x.embedded_factor_id else None
        if factor_id is None:
            m = mat_map.get(str(x.material_id))
            if m and m.embedded_factor_id:
                factor_id = str(m.embedded_factor_id)
        if factor_id is None:
            raise HTTPException(
                status_code=400,
                detail=f"MaterialInput için embedded factor eksik. material_id={x.material_id}. material.embedded_factor_id veya input embedded_factor_id girin."
            )
        factor_val = await _approved_factor_value(db, ctx["tid"], factor_id)
        precursors.append(
            PrecursorInput(
                product_id=str(x.product_id),
                material_id=str(x.material_id),
                quantity=Decimal(str(float(x.quantity))),
                embedded_factor=factor_val
            )
        )

    # exports
    exres = await db.execute(
        select(ExportRecord).where(
            ExportRecord.tenant_id == ctx["tid"],
            ExportRecord.facility_id == data.facility_id,
            ExportRecord.period_start == data.period_start,
            ExportRecord.period_end == data.period_end
        )
    )
    export_rows = exres.scalars().all()
    if not export_rows:
        raise HTTPException(status_code=400, detail="Bu tesis/dönem için export_record yok.")

    exports = [{
        "export_id": str(x.id),
        "product_id": str(x.product_id),
        "export_qty": str(float(x.export_qty)),
        "destination": x.destination
    } for x in export_rows]

    engine = CBAM_Product_Engine()
    report = engine.run(
        facility_totals=facility_totals,
        productions=productions,
        precursors=precursors,
        exports=exports,
        ets_price_eur_per_tco2=Decimal(str(data.ets_price_eur_per_tco2))
    )

    # Enrich report with product metadata (code/name/cn_code)
    product_ids = list({p["product_id"] for p in report.get("products", [])} | {e["product_id"] for e in report.get("exports", [])})
    prod_meta = {}
    if product_ids:
        prs = await db.execute(select(Product).where(Product.tenant_id == ctx["tid"], Product.id.in_(product_ids)))
        for p in prs.scalars().all():
            prod_meta[str(p.id)] = {"product_code": p.product_code, "name": p.name, "unit": p.unit, "cn_code": p.cn_code}

    for row in report.get("products", []):
        row["product"] = prod_meta.get(row["product_id"])
    for row in report.get("exports", []):
        row["product"] = prod_meta.get(row["product_id"])

    # Persist CBAMReport
    report_json = canonical_json(report)
    report_hash = sha256_text(report_json)
    ins = await db.execute(
        insert(CBAMReport).values(
            tenant_id=ctx["tid"],
            period_start=data.period_start,
            period_end=data.period_end,
            status="generated",
            report_json=report_json,
            report_hash=report_hash,
            created_by=ctx["uid"],
        ).returning(CBAMReport)
    )
    r = ins.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "cbam_run", "cbam_report", str(r.id), None, {"report_hash": report_hash})
    await db.commit()

    return CBAMRunOut(report_id=str(r.id), report_hash=report_hash, report=report)
