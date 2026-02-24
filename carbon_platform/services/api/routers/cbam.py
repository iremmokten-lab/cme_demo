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

def _cbam_checks(report_row: dict) -> list[dict]:
    """
    Transitional checklist (minimal ama audit-grade):
      - declarant_name, installation_name, installation_country required
      - each export line must have cn_code, export_qty > 0, embedded_intensity not null
    """
    checks = []
    hdr = report_row.get("header", {})
    def add(rule, sev, status, msg, hint=None):
        checks.append({
            "rule_code": rule,
            "severity": sev,
            "status": status,
            "message_tr": msg,
            "evidence_hint_tr": hint
        })

    # header required
    for k, rule in [
        ("declarant_name", "CBAM_IR_HDR_001"),
        ("installation_name", "CBAM_IR_HDR_002"),
        ("installation_country", "CBAM_IR_HDR_003"),
    ]:
        if not hdr.get(k):
            add(rule, "error", "fail", f"CBAM rapor üst bilgisi eksik: {k}", "Deklaran / tesis kimlik bilgisi kanıtı (kurumsal kayıtlar).")
        else:
            add(rule, "info", "pass", f"CBAM rapor üst bilgisi mevcut: {k}")

    # lines
    exports = report_row.get("exports", [])
    if not exports:
        add("CBAM_IR_LINE_000", "error", "fail", "CBAM raporunda ihracat satırı yok.", "ExportRecord + gümrük dokümanı.")
        return checks

    for i, ex in enumerate(exports, start=1):
        meta = ex.get("product") or {}
        cn = meta.get("cn_code")
        if not cn:
            add(f"CBAM_IR_LINE_{i:03d}_CN", "error", "fail", f"Satır {i}: CN/KN code eksik.", "Ürün CN/KN sınıflandırması (tarife belgesi).")
        else:
            add(f"CBAM_IR_LINE_{i:03d}_CN", "info", "pass", f"Satır {i}: CN/KN code mevcut.")

        qty = Decimal(str(ex.get("export_qty", "0")))
        if qty <= 0:
            add(f"CBAM_IR_LINE_{i:03d}_QTY", "error", "fail", f"Satır {i}: export_qty <= 0.", "Gümrük beyanı / invoice.")
        else:
            add(f"CBAM_IR_LINE_{i:03d}_QTY", "info", "pass", f"Satır {i}: export_qty geçerli.")

        inten = ex.get("embedded_intensity_tco2e_per_unit")
        if inten in [None, "None", ""]:
            add(f"CBAM_IR_LINE_{i:03d}_INT", "error", "fail", f"Satır {i}: embedded intensity boş.", "Ürün bazlı üretim + allocation + precursor faktörleri.")
        else:
            add(f"CBAM_IR_LINE_{i:03d}_INT", "info", "pass", f"Satır {i}: embedded intensity mevcut.")

    return checks

@router.post("/run", response_model=CBAMRunOut)
async def run_cbam(data: CBAMRunCreate, ctx_db=Depends(get_current_db_with_rls)):
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

    # material inputs
    mres = await db.execute(
        select(MaterialInput).where(MaterialInput.tenant_id == ctx["tid"], MaterialInput.activity_record_id == data.activity_record_id)
    )
    mi_rows = mres.scalars().all()

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
    export_rows_db = exres.scalars().all()
    if not export_rows_db:
        raise HTTPException(status_code=400, detail="Bu tesis/dönem için export_record yok.")
    exports = [{
        "export_id": str(x.id),
        "product_id": str(x.product_id),
        "export_qty": str(float(x.export_qty)),
        "destination": x.destination
    } for x in export_rows_db]

    # run engine
    engine = CBAM_Product_Engine()
    report_core = engine.run(
        facility_totals=facility_totals,
        productions=productions,
        precursors=precursors,
        exports=exports,
        ets_price_eur_per_tco2=Decimal(str(data.ets_price_eur_per_tco2))
    )

    # enrich product metadata
    product_ids = list({p["product_id"] for p in report_core.get("products", [])} | {e["product_id"] for e in report_core.get("exports", [])})
    prod_meta = {}
    if product_ids:
        prs = await db.execute(select(Product).where(Product.tenant_id == ctx["tid"], Product.id.in_(product_ids)))
        for p in prs.scalars().all():
            prod_meta[str(p.id)] = {"product_code": p.product_code, "name": p.name, "unit": p.unit, "cn_code": p.cn_code}

    for row in report_core.get("products", []):
        row["product"] = prod_meta.get(row["product_id"])
    for row in report_core.get("exports", []):
        row["product"] = prod_meta.get(row["product_id"])

    # add header for CBAM IR
    report = {
        "header": {
            "declarant_name": data.declarant_name,
            "installation_name": data.installation_name,
            "installation_country": data.installation_country,
            "methodology_note_tr": data.methodology_note_tr,
            "period_start": data.period_start,
            "period_end": data.period_end,
        },
        **report_core
    }

    report_json = canonical_json(report)
    report_hash = sha256_text(report_json)

    # persist cbam_report
    ins = await db.execute(
        insert(CBAMReport).values(
            tenant_id=ctx["tid"],
            period_start=data.period_start,
            period_end=data.period_end,
            status="generated",
            declarant_name=data.declarant_name,
            installation_name=data.installation_name,
            installation_country=data.installation_country,
            methodology_note_tr=data.methodology_note_tr,
            report_json=report_json,
            report_hash=report_hash,
            created_by=ctx["uid"],
        ).returning(CBAMReport)
    )
    r = ins.scalar_one()

    # persist normalized lines
    # clean existing lines for this report (safety)
    await db.execute(delete(CBAMReportLine).where(CBAMReportLine.tenant_id == ctx["tid"], CBAMReportLine.cbam_report_id == r.id))

    # We store facility_id on lines for RLS scoping
    export_index = {str(x.id): x for x in export_rows_db}

    for ex in report.get("exports", []):
        export_id = ex.get("export_id")
        xdb = export_index.get(export_id) if export_id else None
        meta = ex.get("product") or {}
        cn = meta.get("cn_code")
        pname = meta.get("name")
        unit = meta.get("unit") or "ton"

        intensity = Decimal(str(ex["embedded_intensity_tco2e_per_unit"]))
        export_qty = Decimal(str(ex["export_qty"]))
        export_emb = Decimal(str(ex["export_embedded_tco2e"]))

        await db.execute(
            insert(CBAMReportLine).values(
                tenant_id=ctx["tid"],
                cbam_report_id=r.id,
                export_record_id=export_id,
                facility_id=data.facility_id,
                product_id=ex.get("product_id"),
                cn_code=cn,
                product_name=pname,
                export_qty=float(export_qty),
                unit=unit,
                embedded_intensity=float(intensity),
                export_embedded=float(export_emb),
                direct_allocated=0,
                indirect_allocated=0,
                precursor_embedded=0,
                destination=ex.get("destination"),
            )
        )

    # compliance checks
    checks = _cbam_checks(report)

    # replace checks for entity
    await db.execute(
        delete(ComplianceCheck).where(
            ComplianceCheck.tenant_id == ctx["tid"],
            ComplianceCheck.check_type == "CBAM",
            ComplianceCheck.entity_type == "cbam_report",
            ComplianceCheck.entity_id == str(r.id),
        )
    )
    for c in checks:
        await db.execute(
            insert(ComplianceCheck).values(
                tenant_id=ctx["tid"],
                check_type="CBAM",
                entity_type="cbam_report",
                entity_id=str(r.id),
                rule_code=c["rule_code"],
                severity=c["severity"],
                status=c["status"],
                message_tr=c["message_tr"],
                evidence_hint_tr=c.get("evidence_hint_tr"),
            )
        )

    await write_audit_log(db, ctx["tid"], ctx["uid"], "cbam_run", "cbam_report", str(r.id), None, {"report_hash": report_hash})
    await db.commit()

    return CBAMRunOut(report_id=str(r.id), report_hash=report_hash, report=report, checks=checks)
