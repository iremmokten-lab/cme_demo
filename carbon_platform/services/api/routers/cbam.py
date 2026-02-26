import json
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import insert, select, delete
from services.api.routers.auth import get_current_db_with_rls
from services.api.schemas.cbam import (
    ProductionRecordCreate, MaterialInputCreate, ExportRecordCreate,
    CBAMRunCreate, CBAMRunOut
)
from services.api.db.models import (
    ProductionRecord, MaterialInput, ExportRecord,
    CalculationRun, Product, Material, EmissionFactor,
    CBAMReport, CBAMReportLine, ComplianceCheck
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
async def list_production(activity_record_id: str, ctx_db=Depends(get_current_db_with_rls)):
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
            unit=data.unit
        ).returning(MaterialInput.id)
    )
    mid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "material_input", str(mid), None, data.model_dump())
    await db.commit()
    return {"id": str(mid)}

@router.get("/material-inputs", response_model=list[dict])
async def list_material_inputs(activity_record_id: str, ctx_db=Depends(get_current_db_with_rls)):
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
        "unit": x.unit
    } for x in items]

@router.post("/exports", response_model=dict)
async def add_export(data: ExportRecordCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(ExportRecord).values(
            tenant_id=ctx["tid"],
            facility_id=data.facility_id,
            product_id=data.product_id,
            quantity=data.quantity,
            unit=data.unit,
            destination=data.destination,
            period_start=data.period_start,
            period_end=data.period_end
        ).returning(ExportRecord.id)
    )
    eid = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "export_record", str(eid), None, data.model_dump())
    await db.commit()
    return {"id": str(eid)}

@router.get("/exports", response_model=list[dict])
async def list_exports(facility_id: str, period_start: str, period_end: str, ctx_db=Depends(get_current_db_with_rls)):
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
        "quantity": float(x.quantity),
        "unit": x.unit,
        "destination": x.destination,
        "period_start": x.period_start,
        "period_end": x.period_end
    } for x in items]

@router.post("/run", response_model=CBAMRunOut)
async def run_cbam(data: CBAMRunCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db

    prod_res = await db.execute(
        select(ProductionRecord).where(
            ProductionRecord.tenant_id == ctx["tid"],
            ProductionRecord.activity_record_id == data.activity_record_id
        )
    )
    productions = prod_res.scalars().all()

    mat_res = await db.execute(
        select(MaterialInput).where(
            MaterialInput.tenant_id == ctx["tid"],
            MaterialInput.activity_record_id == data.activity_record_id
        )
    )
    materials = mat_res.scalars().all()

    prod_ids = {p.product_id for p in productions}
    if not prod_ids:
        raise HTTPException(status_code=400, detail="Üretim kaydı yok. Önce production girin.")

    prod_catalog_res = await db.execute(
        select(Product).where(Product.tenant_id == ctx["tid"], Product.id.in_(list(prod_ids)))
    )
    prod_catalog = {str(p.id): p for p in prod_catalog_res.scalars().all()}

    mat_ids = {m.material_id for m in materials}
    mat_catalog = {}
    if mat_ids:
        mat_catalog_res = await db.execute(
            select(Material).where(Material.tenant_id == ctx["tid"], Material.id.in_(list(mat_ids)))
        )
        mat_catalog = {str(m.id): m for m in mat_catalog_res.scalars().all()}

    ef_res = await db.execute(
        select(EmissionFactor).where(EmissionFactor.tenant_id == ctx["tid"])
    )
    factors = ef_res.scalars().all()

    factor_index = {}
    for f in factors:
        key = f"{f.factor_type}:{f.scope}:{f.key}"
        factor_index[key] = f

    engine = CBAM_Product_Engine()

    out_lines = []
    total_embedded = Decimal("0")

    for p in productions:
        prod_obj = prod_catalog.get(str(p.product_id))
        if not prod_obj:
            continue

        direct = Decimal(str(getattr(prod_obj, "direct_emissions_per_unit", 0) or 0)) * Decimal(str(p.quantity))
        elec = Decimal(str(getattr(prod_obj, "electricity_emissions_per_unit", 0) or 0)) * Decimal(str(p.quantity))
        process = Decimal(str(getattr(prod_obj, "process_emissions_per_unit", 0) or 0)) * Decimal(str(p.quantity))

        prec_inputs = []
        for m in materials:
            if str(m.product_id) != str(p.product_id):
                continue
            mat_obj = mat_catalog.get(str(m.material_id))
            if not mat_obj:
                continue
            embedded_per_unit = Decimal(str(getattr(mat_obj, "embedded_emissions_per_unit", 0) or 0))
            prec_inputs.append(
                PrecursorInput(
                    precursor_id=str(mat_obj.id),
                    quantity=Decimal(str(m.quantity)),
                    embedded_emissions=embedded_per_unit * Decimal(str(m.quantity))
                )
            )

        prod_input = ProductionInput(
            product_id=str(p.product_id),
            quantity=Decimal(str(p.quantity)),
            direct_emissions=direct,
            electricity_emissions=elec,
            process_emissions=process,
            precursors=prec_inputs
        )

        result = engine.compute_product(prod_input)

        embedded = result.embedded_emissions
        total_embedded += embedded

        out_lines.append({
            "product_id": str(p.product_id),
            "quantity": float(p.quantity),
            "embedded_emissions": float(embedded),
            "direct_emissions": float(result.direct_emissions),
            "electricity_emissions": float(result.electricity_emissions),
            "process_emissions": float(result.process_emissions),
            "precursor_emissions": float(result.precursor_emissions),
            "intensity": float(result.intensity)
        })

    payload = {
        "activity_record_id": data.activity_record_id,
        "lines": out_lines,
        "total_embedded_emissions": float(total_embedded),
        "ets_price": float(data.ets_price),
        "cbam_cost": float(total_embedded * Decimal(str(data.ets_price))),
        "method": data.method,
        "notes": data.notes
    }
    canonical = canonical_json(payload)
    payload_hash = sha256_text(canonical)

    run_res = await db.execute(
        insert(CalculationRun).values(
            tenant_id=ctx["tid"],
            facility_id=data.facility_id,
            run_type="cbam",
            payload_json=canonical,
            payload_hash=payload_hash,
            status="completed"
        ).returning(CalculationRun.id)
    )
    run_id = run_res.scalar_one()

    rep_res = await db.execute(
        insert(CBAMReport).values(
            tenant_id=ctx["tid"],
            facility_id=data.facility_id,
            activity_record_id=data.activity_record_id,
            period_start=data.period_start,
            period_end=data.period_end,
            run_id=run_id,
            total_embedded_emissions=total_embedded,
            ets_price=Decimal(str(data.ets_price)),
            cbam_cost=total_embedded * Decimal(str(data.ets_price)),
            method=data.method,
            notes=data.notes
        ).returning(CBAMReport.id)
    )
    report_id = rep_res.scalar_one()

    for line in out_lines:
        await db.execute(
            insert(CBAMReportLine).values(
                tenant_id=ctx["tid"],
                report_id=report_id,
                product_id=line["product_id"],
                quantity=Decimal(str(line["quantity"])),
                embedded_emissions=Decimal(str(line["embedded_emissions"])),
                direct_emissions=Decimal(str(line["direct_emissions"])),
                electricity_emissions=Decimal(str(line["electricity_emissions"])),
                process_emissions=Decimal(str(line["process_emissions"])),
                precursor_emissions=Decimal(str(line["precursor_emissions"])),
                intensity=Decimal(str(line["intensity"]))
            )
        )

    await db.execute(
        insert(ComplianceCheck).values(
            tenant_id=ctx["tid"],
            facility_id=data.facility_id,
            check_type="cbam_report_generated",
            status="pass",
            details_json=json.dumps({"report_id": str(report_id), "run_id": str(run_id)})
        )
    )

    await write_audit_log(
        db, ctx["tid"], ctx["uid"],
        "create", "cbam_report", str(report_id),
        None, payload
    )

    await db.commit()

    return CBAMRunOut(
        run_id=str(run_id),
        report_id=str(report_id),
        total_embedded_emissions=float(total_embedded),
        ets_price=float(data.ets_price),
        cbam_cost=float(total_embedded * Decimal(str(data.ets_price))),
        payload_hash=payload_hash
    )

@router.delete("/report/{report_id}", response_model=dict)
async def delete_report(report_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    await db.execute(delete(CBAMReportLine).where(CBAMReportLine.tenant_id == ctx["tid"], CBAMReportLine.report_id == report_id))
    await db.execute(delete(CBAMReport).where(CBAMReport.tenant_id == ctx["tid"], CBAMReport.id == report_id))
    await write_audit_log(db, ctx["tid"], ctx["uid"], "delete", "cbam_report", report_id, None, {})
    await db.commit()
    return {"ok": True}
