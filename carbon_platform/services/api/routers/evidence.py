import json
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, insert
from services.api.routers.auth import get_current_db_with_rls
from services.api.schemas.evidence import EvidenceBuildIn, EvidencePackOut
from services.api.db.models import (
    EvidencePack, EvidenceItem,
    CalculationRun, ActivityRecord, EmissionFactor, Facility,
    Product, Material, ProductionRecord, MaterialInput, ExportRecord, CBAMReport
)
from services.api.core.storage import storage
from services.api.core.audit import canonical_json, sha256_text

router = APIRouter()

@router.post("/build", response_model=EvidencePackOut)
async def build_evidence_pack(data: EvidenceBuildIn, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db

    q_ar = select(ActivityRecord).where(
        ActivityRecord.tenant_id == ctx["tid"],
        ActivityRecord.period_start == data.period_start,
        ActivityRecord.period_end == data.period_end
    )
    if data.facility_id:
        q_ar = q_ar.where(ActivityRecord.facility_id == data.facility_id)
    ar_res = await db.execute(q_ar)
    activity_records = ar_res.scalars().all()

    if not activity_records:
        raise HTTPException(status_code=400, detail="Bu dönem için activity record bulunamadı")

    ar_ids = [str(a.id) for a in activity_records]
    fac_ids = list({str(a.facility_id) for a in activity_records})

    # calc runs
    q_calc = select(CalculationRun).where(CalculationRun.tenant_id == ctx["tid"], CalculationRun.activity_record_id.in_(ar_ids))
    calc_res = await db.execute(q_calc)
    calc_runs = calc_res.scalars().all()

    # approved factors
    f_res = await db.execute(select(EmissionFactor).where(EmissionFactor.tenant_id == ctx["tid"], EmissionFactor.status == "approved"))
    factors = f_res.scalars().all()

    # facilities
    fac_res = await db.execute(select(Facility).where(Facility.tenant_id == ctx["tid"], Facility.id.in_(fac_ids)))
    facilities = fac_res.scalars().all()

    # catalog
    prod_res = await db.execute(select(Product).where(Product.tenant_id == ctx["tid"]))
    products = prod_res.scalars().all()
    mat_res = await db.execute(select(Material).where(Material.tenant_id == ctx["tid"]))
    materials = mat_res.scalars().all()

    # cbam product-level inputs for those activity records
    pr_res = await db.execute(select(ProductionRecord).where(ProductionRecord.tenant_id == ctx["tid"], ProductionRecord.activity_record_id.in_(ar_ids)))
    production_records = pr_res.scalars().all()

    mi_res = await db.execute(select(MaterialInput).where(MaterialInput.tenant_id == ctx["tid"], MaterialInput.activity_record_id.in_(ar_ids)))
    material_inputs = mi_res.scalars().all()

    ex_res = await db.execute(select(ExportRecord).where(
        ExportRecord.tenant_id == ctx["tid"],
        ExportRecord.period_start == data.period_start,
        ExportRecord.period_end == data.period_end
    ))
    exports = ex_res.scalars().all()

    # latest CBAM reports for period
    cb_res = await db.execute(select(CBAMReport).where(
        CBAMReport.tenant_id == ctx["tid"],
        CBAMReport.period_start == data.period_start,
        CBAMReport.period_end == data.period_end
    ).order_by(CBAMReport.created_at.desc()))
    cbam_reports = cb_res.scalars().all()

    manifest = {
        "manifest_version": "1.1",
        "tenant_id": ctx["tid"],
        "period": {"start": data.period_start, "end": data.period_end},
        "scope": data.scope,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "items": []
    }

    def add_item(item_type: str, key: str, sha: str, meta: dict):
        manifest["items"].append({
            "item_type": item_type,
            "s3_key": key,
            "sha256": sha,
            "metadata": meta
        })

    # activity snapshot
    activity_payload = [{"id": str(a.id), "facility_id": str(a.facility_id), "period_start": a.period_start, "period_end": a.period_end, "status": a.status} for a in activity_records]
    act_key = f"evidence/{ctx['tid']}/{data.period_start}_{data.period_end}/activity_snapshot.json"
    act_obj = await storage.put_bytes(act_key, canonical_json(activity_payload).encode("utf-8"))
    add_item("input_snapshot", act_obj.key, act_obj.sha256, {"type": "activity_record", "count": len(activity_records)})

    # factors snapshot
    factor_payload = [{
        "id": str(f.id),
        "factor_type": f.factor_type,
        "value": float(f.value),
        "unit": f.unit,
        "gas": f.gas,
        "version": f.version,
        "status": f.status,
        "valid_from": f.valid_from,
        "valid_to": f.valid_to
    } for f in factors]
    fac_key = f"evidence/{ctx['tid']}/{data.period_start}_{data.period_end}/factors_approved.json"
    fac_obj = await storage.put_bytes(fac_key, canonical_json(factor_payload).encode("utf-8"))
    add_item("factor_library", fac_obj.key, fac_obj.sha256, {"type": "approved_factors", "count": len(factors)})

    # facilities snapshot
    facility_payload = [{"id": str(f.id), "name": f.name, "country": f.country, "ets_in_scope": f.ets_in_scope, "cbam_in_scope": f.cbam_in_scope} for f in facilities]
    fkey = f"evidence/{ctx['tid']}/{data.period_start}_{data.period_end}/facilities.json"
    fobj = await storage.put_bytes(fkey, canonical_json(facility_payload).encode("utf-8"))
    add_item("reference", fobj.key, fobj.sha256, {"type": "facility", "count": len(facilities)})

    # catalog snapshots
    product_payload = [{"id": str(p.id), "facility_id": str(p.facility_id), "product_code": p.product_code, "name": p.name, "unit": p.unit, "cn_code": p.cn_code} for p in products]
    pkey = f"evidence/{ctx['tid']}/{data.period_start}_{data.period_end}/products.json"
    pobj = await storage.put_bytes(pkey, canonical_json(product_payload).encode("utf-8"))
    add_item("reference", pobj.key, pobj.sha256, {"type": "product_catalog", "count": len(products)})

    material_payload = [{"id": str(m.id), "material_code": m.material_code, "name": m.name, "unit": m.unit, "embedded_factor_id": str(m.embedded_factor_id) if m.embedded_factor_id else None} for m in materials]
    mkey = f"evidence/{ctx['tid']}/{data.period_start}_{data.period_end}/materials.json"
    mobj = await storage.put_bytes(mkey, canonical_json(material_payload).encode("utf-8"))
    add_item("reference", mobj.key, mobj.sha256, {"type": "material_catalog", "count": len(materials)})

    # cbam inputs snapshots
    prodrec_payload = [{"id": str(x.id), "activity_record_id": str(x.activity_record_id), "product_id": str(x.product_id), "quantity": float(x.quantity), "unit": x.unit, "doc_id": str(x.doc_id) if x.doc_id else None} for x in production_records]
    prkey = f"evidence/{ctx['tid']}/{data.period_start}_{data.period_end}/production_records.json"
    probj = await storage.put_bytes(prkey, canonical_json(prodrec_payload).encode("utf-8"))
    add_item("input_snapshot", probj.key, probj.sha256, {"type": "production_record", "count": len(production_records)})

    mi_payload = [{"id": str(x.id), "activity_record_id": str(x.activity_record_id), "product_id": str(x.product_id), "material_id": str(x.material_id), "quantity": float(x.quantity), "unit": x.unit, "embedded_factor_id": str(x.embedded_factor_id) if x.embedded_factor_id else None, "doc_id": str(x.doc_id) if x.doc_id else None} for x in material_inputs]
    mikey = f"evidence/{ctx['tid']}/{data.period_start}_{data.period_end}/material_inputs.json"
    miobj = await storage.put_bytes(mikey, canonical_json(mi_payload).encode("utf-8"))
    add_item("input_snapshot", miobj.key, miobj.sha256, {"type": "material_input", "count": len(material_inputs)})

    ex_payload = [{"id": str(x.id), "facility_id": str(x.facility_id), "product_id": str(x.product_id), "period_start": x.period_start, "period_end": x.period_end, "export_qty": float(x.export_qty), "unit": x.unit, "destination": x.destination, "customs_doc_id": str(x.customs_doc_id) if x.customs_doc_id else None} for x in exports]
    exkey = f"evidence/{ctx['tid']}/{data.period_start}_{data.period_end}/exports.json"
    exobj = await storage.put_bytes(exkey, canonical_json(ex_payload).encode("utf-8"))
    add_item("input_snapshot", exobj.key, exobj.sha256, {"type": "export_record", "count": len(exports)})

    # calc results
    calc_payload = [{"id": str(c.id), "facility_id": str(c.facility_id), "activity_record_id": str(c.activity_record_id), "result_hash": c.result_hash, "result": json.loads(c.result_json)} for c in calc_runs]
    ckey = f"evidence/{ctx['tid']}/{data.period_start}_{data.period_end}/calculation_results.json"
    cobj = await storage.put_bytes(ckey, canonical_json(calc_payload).encode("utf-8"))
    add_item("reports", cobj.key, cobj.sha256, {"type": "calculation_results", "count": len(calc_runs)})

    # cbam report snapshots
    if cbam_reports:
        cb_payload = [{"id": str(r.id), "report_hash": r.report_hash, "created_at": r.created_at.isoformat()} for r in cbam_reports]
        cbkey = f"evidence/{ctx['tid']}/{data.period_start}_{data.period_end}/cbam_reports_index.json"
        cbobj = await storage.put_bytes(cbkey, canonical_json(cb_payload).encode("utf-8"))
        add_item("reports", cbobj.key, cbobj.sha256, {"type": "cbam_reports_index", "count": len(cbam_reports)})

        # include latest report content itself
        latest = cbam_reports[0]
        rkey = f"evidence/{ctx['tid']}/{data.period_start}_{data.period_end}/cbam_report_latest.json"
        robj = await storage.put_bytes(rkey, latest.report_json.encode("utf-8"))
        add_item("reports", robj.key, robj.sha256, {"type": "cbam_report_latest", "report_hash": latest.report_hash})

    # manifest
    manifest_json = canonical_json(manifest)
    manifest_hash = sha256_text(manifest_json)
    manifest["manifest_sha256"] = manifest_hash
    mkey = f"evidence/{ctx['tid']}/{data.period_start}_{data.period_end}/manifest.json"
    mobj = await storage.put_bytes(mkey, canonical_json(manifest).encode("utf-8"))

    # persist evidence pack + items
    res = await db.execute(
        insert(EvidencePack).values(
            tenant_id=ctx["tid"],
            period_start=data.period_start,
            period_end=data.period_end,
            scope=data.scope,
            status="built",
            manifest_s3_key=mobj.key,
            created_by=ctx["uid"],
        ).returning(EvidencePack)
    )
    pack = res.scalar_one()

    for it in manifest["items"]:
        await db.execute(
            insert(EvidenceItem).values(
                tenant_id=ctx["tid"],
                evidence_pack_id=pack.id,
                item_type=it["item_type"],
                s3_key=it["s3_key"],
                sha256=it["sha256"],
                metadata_json=json.dumps(it["metadata"], ensure_ascii=False),
            )
        )
    await db.commit()
    return EvidencePackOut(id=str(pack.id), scope=pack.scope, status=pack.status, manifest_s3_key=pack.manifest_s3_key)
