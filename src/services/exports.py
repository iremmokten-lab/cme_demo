from __future__ import annotations

import base64
import hmac
import io
import json
import os
import zipfile
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

import pandas as pd
from sqlalchemy import select

from src.db.session import db
from src.db.models import (
    CalculationSnapshot,
    DatasetUpload,
    EmissionFactor,
    EvidenceDocument,
    Methodology,
    Project,
    Report,
    VerificationCase,
    VerificationFinding,
)
from src.mrv.lineage import sha256_bytes
from src.services.reporting import build_pdf
from src.services.ets_reporting import build_ets_reporting_dataset
from src.config import get_tr_ets_mode
from src.services.storage import EVIDENCE_DOCS_CATEGORIES
from src.services.signing import build_signature_block
from src.services.validators import validate_cbam_report, validate_ets_reporting


def build_xlsx_from_results(results_json: str) -> bytes:
    """
    Paket D4: XLSX export geliştirme
      - KPIs
      - CBAM_Table
      - CBAM_Goods_Summary (varsa)
      - ETS_Activity (varsa)
      - AI_Benchmark / AI_Advisor / AI_Optimizer (varsa)
    """
    results = json.loads(results_json) if results_json else {}
    kpis = results.get("kpis", {}) or {}
    table = results.get("cbam_table", []) or []

    cbam_goods = []
    try:
        cbam_goods = (results.get("cbam") or {}).get("totals", {}).get("goods_summary", []) or []
    except Exception:
        cbam_goods = []

    ets_activity = []
    try:
        ets_activity = ((results.get("ets") or {}).get("verification") or {}).get("activity_data", []) or []
    except Exception:
        ets_activity = []

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        pd.DataFrame([kpis]).to_excel(writer, sheet_name="KPIs", index=False)
        pd.DataFrame(table).to_excel(writer, sheet_name="CBAM_Table", index=False)

        if cbam_goods:
            pd.DataFrame(cbam_goods).to_excel(writer, sheet_name="CBAM_Goods_Summary", index=False)

        if ets_activity:
            pd.DataFrame(ets_activity).to_excel(writer, sheet_name="ETS_Activity", index=False)

        # Faz 3 AI sheets
        try:
            ai = (results.get("ai") or {}) if isinstance(results, dict) else {}
        except Exception:
            ai = {}

        if isinstance(ai, dict) and ai:
            try:
                bench = ai.get("benchmark") or {}
                if isinstance(bench, dict) and (bench.get("products") or bench.get("outliers") or bench.get("facility")):
                    pd.DataFrame([bench.get("facility") or {}]).to_excel(writer, sheet_name="AI_Benchmark_Facility", index=False)
                    prows = bench.get("products") or []
                    if isinstance(prows, list) and prows:
                        pd.DataFrame(prows).to_excel(writer, sheet_name="AI_Benchmark_Products", index=False)
                    orows = bench.get("outliers") or []
                    if isinstance(orows, list) and orows:
                        pd.DataFrame(orows).to_excel(writer, sheet_name="AI_Outliers", index=False)
            except Exception:
                pass

            try:
                adv = ai.get("advisor") or {}
                if isinstance(adv, dict) and (adv.get("measures") or adv.get("hotspots")):
                    pd.DataFrame([adv.get("hotspots") or {}]).to_excel(writer, sheet_name="AI_Hotspots", index=False)
                    mrows = adv.get("measures") or []
                    if isinstance(mrows, list) and mrows:
                        pd.DataFrame(mrows).to_excel(writer, sheet_name="AI_Measures", index=False)
                    miss = adv.get("evidence_missing_categories") or []
                    if isinstance(miss, list) and miss:
                        pd.DataFrame([{"missing_category": x} for x in miss]).to_excel(writer, sheet_name="AI_Evidence_Gaps", index=False)
            except Exception:
                pass

            try:
                opt = ai.get("optimizer") or {}
                if isinstance(opt, dict) and (opt.get("abatement_curve") or opt.get("portfolio")):
                    curve = opt.get("abatement_curve") or []
                    if isinstance(curve, list) and curve:
                        pd.DataFrame(curve).to_excel(writer, sheet_name="AI_MACC", index=False)
                    port = opt.get("portfolio") or {}
                    if isinstance(port, dict):
                        pd.DataFrame([port.get("summary") or {}]).to_excel(writer, sheet_name="AI_Portfolio_Summary", index=False)
                        sel = port.get("selected") or []
                        if isinstance(sel, list) and sel:
                            pd.DataFrame(sel).to_excel(writer, sheet_name="AI_Portfolio_Selected", index=False)
            except Exception:
                pass

    return out.getvalue()


def build_zip(files: dict[str, bytes]) -> bytes:
    """
    Basit ZIP builder. files: path->bytes
    """
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for path_in_zip, bts in files.items():
            z.writestr(path_in_zip, bts or b"")
    return out.getvalue()


def _safe_read_bytes(uri: str) -> bytes:
    """
    Streamlit Cloud uyumlu: storage_uri genelde yerel path olur (./data/.. veya /tmp/..).
    """
    if not uri:
        return b""
    try:
        p = Path(uri)
        if p.exists() and p.is_file():
            return p.read_bytes()
    except Exception:
        pass

    # bazı ortamlarda uri file:// olabilir
    try:
        if uri.startswith("file://"):
            p = Path(uri.replace("file://", ""))
            if p.exists() and p.is_file():
                return p.read_bytes()
    except Exception:
        pass

    # son çare: bytes yok
    return b""


def _snapshot_input_uris(snapshot: CalculationSnapshot) -> dict:
    """
    input_hashes_json -> {energy:{uri,sha256,...}, production:{...}, materials:{...}}
    """
    try:
        ih = json.loads(snapshot.input_hashes_json or "{}")
    except Exception:
        ih = {}

    # Paket D2: project upload kaydı yoksa ih boş olabilir.
    # En iyi çaba ile DB'den latest upload çek.
    if not ih:
        with db() as s:
            ups = (
                s.execute(
                    select(DatasetUpload)
                    .where(DatasetUpload.project_id == snapshot.project_id)
                    .order_by(DatasetUpload.uploaded_at.desc())
                )
                .scalars()
                .all()
            )
        ih = {}
        for u in ups:
            if u.dataset_type not in ih:
                ih[u.dataset_type] = {
                    "upload_id": u.id,
                    "sha256": u.sha256,
                    "uri": u.storage_uri,
                    "original_filename": u.original_filename,
                    "schema_version": u.schema_version,
                }

        # normalize keys
        ih = {
            "energy": ih.get("energy", {}),
            "production": ih.get("production", {}),
            "materials": ih.get("materials", {}),
        }

    return ih


def _get_secret(key: str, default=None):
    try:
        import streamlit as st

        return st.secrets.get(key, default)
    except Exception:
        return default


def _hmac_signature(payload_bytes: bytes) -> str | None:
    """
    Paketin bütünlüğü için HMAC-SHA256 imza (opsiyonel).
    Streamlit secrets: EVIDENCE_PACK_HMAC_KEY
    """
    key = _get_secret("EVIDENCE_PACK_HMAC_KEY", None)
    if not key:
        return None
    try:
        key_b = str(key).encode("utf-8")
        sig = hmac.new(key_b, payload_bytes, digestmod=sha256).hexdigest()
        return sig
    except Exception:
        return None


def _json_bytes(obj: dict) -> bytes:
    try:
        return json.dumps(obj or {}, ensure_ascii=False, sort_keys=True, default=str, indent=2).encode("utf-8")
    except Exception:
        return b"{}"


def _report_pdf_for_snapshot(snapshot: CalculationSnapshot, results: dict, cfg: dict) -> bytes:
    """
    Paket D3: PDF rapor
    """
    payload = {
        "kpis": (results.get("kpis") or {}) if isinstance(results, dict) else {},
        "config": cfg,
        "cbam": (results.get("cbam") or {}) if isinstance(results, dict) else {},
        "cbam_table": (results.get("cbam_table") or []) if isinstance(results, dict) else [],
        "ets": (results.get("ets") or {}) if isinstance(results, dict) else {},
        "qa_flags": (results.get("qa_flags") or []) if isinstance(results, dict) else [],
        "compliance_checks": (results.get("compliance_checks") or []) if isinstance(results, dict) else [],
    }
    try:
        return build_pdf(payload)
    except Exception:
        # PDF builder fail-safe
        return b""


def _report_hash_for_report_bytes(pdf_bytes: bytes) -> str:
    try:
        return sha256(pdf_bytes or b"").hexdigest()
    except Exception:
        return ""


def _ensure_categories(cat: str) -> str:
    cat_n = str(cat or "").strip()
    if not cat_n:
        return "documents"
    if cat_n not in EVIDENCE_DOCS_CATEGORIES:
        return "documents"
    return cat_n


def _evidence_files_for_project(project_id: int) -> tuple[list[dict], list[tuple[str, bytes]]]:
    """
    evidence_manifest, evidence_files_to_zip
    """
    evidence_manifest = []
    evidence_files = []
    with db() as s:
        docs = (
            s.execute(
                select(EvidenceDocument)
                .where(EvidenceDocument.project_id == int(project_id))
                .order_by(EvidenceDocument.uploaded_at.desc())
            )
            .scalars()
            .all()
        )
    for d in docs:
        cat = _ensure_categories(getattr(d, "category", "documents"))
        fname = str(getattr(d, "original_filename", "") or "evidence.bin")
        sha = str(getattr(d, "sha256", "") or "")
        uri = str(getattr(d, "storage_uri", "") or "")
        rel = f"evidence/files/{cat}/{d.id}_{fname}"
        evidence_manifest.append(
            {
                "id": d.id,
                "category": cat,
                "original_filename": fname,
                "sha256": sha,
                "storage_uri": uri,
                "path_in_pack": rel,
                "uploaded_at": (d.uploaded_at.isoformat() if getattr(d, "uploaded_at", None) else None),
                "notes": str(getattr(d, "notes", "") or ""),
            }
        )
        evidence_files.append((rel, _safe_read_bytes(uri)))
    return evidence_manifest, evidence_files


def _data_quality_from_uploads(project_id: int) -> dict:
    with db() as s:
        ups = (
            s.execute(
                select(DatasetUpload)
                .where(DatasetUpload.project_id == int(project_id))
                .order_by(DatasetUpload.uploaded_at.desc())
            )
            .scalars()
            .all()
        )

    rows = []
    for u in ups:
        try:
            rep = json.loads(getattr(u, "data_quality_report_json", "{}") or "{}")
        except Exception:
            rep = {}
        rows.append(
            {
                "dataset_type": u.dataset_type,
                "upload_id": u.id,
                "original_filename": u.original_filename,
                "sha256": u.sha256,
                "schema_version": getattr(u, "schema_version", None),
                "storage_uri": u.storage_uri,
                "data_quality_score": getattr(u, "data_quality_score", None),
                "data_quality_report": rep,
                "uploaded_at": (u.uploaded_at.isoformat() if getattr(u, "uploaded_at", None) else None),
            }
        )

    return {"project_id": int(project_id), "uploads": rows}


def _verification_payload(snapshot: CalculationSnapshot, period_year: int | None) -> dict:
    if period_year is None:
        return {"case": None, "findings": []}

    with db() as s:
        case = (
            s.execute(
                select(VerificationCase)
                .where(
                    VerificationCase.project_id == int(snapshot.project_id),
                    VerificationCase.period_year == int(period_year),
                )
                .order_by(VerificationCase.created_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )

        if not case:
            return {"case": None, "findings": []}

        findings = (
            s.execute(
                select(VerificationFinding)
                .where(VerificationFinding.case_id == int(case.id))
                .order_by(VerificationFinding.created_at.asc())
            )
            .scalars()
            .all()
        )

    case_obj = {
        "id": case.id,
        "project_id": case.project_id,
        "period_year": case.period_year,
        "verifier_org": case.verifier_org,
        "status": case.status,
        "created_at": (case.created_at.isoformat() if getattr(case, "created_at", None) else None),
        "updated_at": (case.updated_at.isoformat() if getattr(case, "updated_at", None) else None),
        "notes": case.notes,
    }

    f_objs = []
    for f in findings:
        f_objs.append(
            {
                "id": f.id,
                "case_id": f.case_id,
                "severity": f.severity,
                "title": f.title,
                "description": f.description,
                "evidence_ref": f.evidence_ref,
                "status": f.status,
                "created_at": (f.created_at.isoformat() if getattr(f, "created_at", None) else None),
                "resolved_at": (f.resolved_at.isoformat() if getattr(f, "resolved_at", None) else None),
            }
        )

    return {"case": case_obj, "findings": f_objs}


def build_evidence_pack(snapshot_id: int) -> bytes:
    """
    Evidence pack export (ZIP) — Paket B + Paket D4 uyumlu.
    İçerik:
      - input csv: energy/production/materials
      - factor library
      - methodology
      - snapshot json
      - report pdf (CBAM/ETS/DQ bölümleri)
      - evidence documents (categories)
      - data_quality.json
      - compliance_checks.json (Paket B3)
      - verification_case.json (Paket B3, varsa)
      - manifest.json (signature dahil)
    """
    with db() as s:
        snapshot = s.get(CalculationSnapshot, int(snapshot_id))
        if not snapshot:
            raise ValueError("Snapshot bulunamadı.")

    inputs = _snapshot_input_uris(snapshot)
    energy_bytes = _safe_read_bytes(inputs.get("energy", {}).get("uri", ""))
    prod_bytes = _safe_read_bytes(inputs.get("production", {}).get("uri", ""))
    mat_bytes = _safe_read_bytes(inputs.get("materials", {}).get("uri", ""))

    # Factor library
    with db() as s:
        factors = s.execute(select(EmissionFactor).order_by(EmissionFactor.factor_type, EmissionFactor.year.desc())).scalars().all()

    factor_rows = []
    factor_versions = {}
    for f in factors:
        factor_rows.append(
            {
                "factor_type": f.factor_type,
                "value": f.value,
                "unit": f.unit,
                "source": f.source,
                "year": f.year,
                "version": f.version,
                "region": f.region,
            }
        )
        key = f"{f.factor_type}:{f.region}"
        if key not in factor_versions:
            factor_versions[key] = {"version": f.version, "year": f.year}

    factors_json = {"emission_factors": factor_rows}

    # Methodology
    methodology_version = None
    meth_obj: dict = {}
    if getattr(snapshot, "methodology_id", None):
        with db() as s:
            m = s.get(Methodology, int(snapshot.methodology_id))
            if m:
                methodology_version = m.version
                meth_obj = {
                    "id": m.id,
                    "name": m.name,
                    "description": m.description,
                    "scope": m.scope,
                    "version": m.version,
                    "created_at": (m.created_at.isoformat() if getattr(m, "created_at", None) else None),
                }

    # Snapshot json
    try:
        cfg = json.loads(snapshot.config_json or "{}")
    except Exception:
        cfg = {}
    try:
        ih = json.loads(snapshot.input_hashes_json or "{}")
    except Exception:
        ih = {}
    try:
        res = json.loads(snapshot.results_json or "{}")
    except Exception:
        res = {}

    snapshot_payload = {
        "snapshot_id": snapshot.id,
        "created_at": (snapshot.created_at.isoformat() if getattr(snapshot, "created_at", None) else None),
        "engine_version": snapshot.engine_version,
        "methodology_id": getattr(snapshot, "methodology_id", None),
        "previous_snapshot_hash": getattr(snapshot, "previous_snapshot_hash", None),
        "result_hash": snapshot.result_hash,
        "config": cfg,
        "input_hashes": ih,
        "results": res,
    }

    # Faz 3 AI payloads (opsiyonel)
    ai_obj = {}
    try:
        ai_obj = (res or {}).get("ai", {}) or {}
    except Exception:
        ai_obj = {}
    if not isinstance(ai_obj, dict):
        ai_obj = {}

    ai_benchmark_bytes = _json_bytes(ai_obj.get("benchmark") or {}) if ai_obj else _json_bytes({})
    ai_advisor_bytes = _json_bytes(ai_obj.get("advisor") or {}) if ai_obj else _json_bytes({})
    ai_optimizer_bytes = _json_bytes(ai_obj.get("optimizer") or {}) if ai_obj else _json_bytes({})
    ai_full_bytes = _json_bytes(ai_obj) if ai_obj else _json_bytes({})


    # Regülasyon datasetleri (CBAM / ETS)
    cbam_xml_str = ""
    cbam_json_obj = {}
    try:
        cbam = (res or {}).get("cbam", {}) or {}
        # orchestrator: cbam.cbam_reporting XML string
        cbam_xml_str = str(cbam.get("cbam_reporting") or "")
        cbam_json_obj = {
        "schema": "cbam_report.v1",
        "facility": (res or {}).get("input_bundle", {}).get("facility", {}),
        "products": cbam.get("table") or [],
        "meta": cbam.get("meta") or {},
        "methodology": (res or {}).get("input_bundle", {}).get("methodology_ref", {}),
        "validation_status": {
            "used_default_factors": bool(((res or {}).get("energy", {}) or {}).get("used_default_factors")),
            "notes": "Default factor kullanımı varsa actual/default flag ile raporlanır.",
        },
        }
    except Exception:
        cbam_xml_str = ""
        cbam_json_obj = {}

    cbam_xml_bytes = (cbam_xml_str.encode("utf-8") if cbam_xml_str else b"")
    cbam_json_bytes = _json_bytes(cbam_json_obj)

    # ETS reporting JSON (MRR 2018/2066 / TR ETS modu)
    ets_json_obj = {}
    try:
        period = ((res or {}).get("input_bundle", {}) or {}).get("period", {}) or {}
        installation = ((res or {}).get("input_bundle", {}) or {}).get("facility", {}) or {}
        methodology_ref = ((res or {}).get("input_bundle", {}) or {}).get("methodology_ref", {}) or {}
        energy_breakdown = (res or {}).get("energy", {}) or {}
        allocation_obj = (res or {}).get("allocation", {}) or {}
        qa = {"controls": [], "passed": [], "failed": []}
        ets_json_obj = build_ets_reporting_dataset(
        installation=installation,
        period=period,
        energy_breakdown=energy_breakdown,
        methodology={
            "id": methodology_ref.get("id"),
            "name": methodology_ref.get("name"),
            "regime": methodology_ref.get("regime"),
            "config": {},
        },
        config=(cfg or {}),
        allocation=allocation_obj,
        qa_qc=qa,
        tr_ets_mode=bool(get_tr_ets_mode()),
        )
    except Exception:
        ets_json_obj = {}
        ets_json_bytes = _json_bytes(ets_json_obj)

    # Compliance dataset zaten compliance_json_bytes olarak yazılıyor; ayrıca gereksinim için kopya path'ler eklenecek.
    # Compliance checks (Paket B3): snapshot sonuçlarından çıkar
    compliance_checks = []
    try:
        compliance_checks = (res or {}).get("compliance_checks", []) or []
    except Exception:
        compliance_checks = []
    if not isinstance(compliance_checks, list):
        compliance_checks = []

    compliance_payload = {
        "snapshot_id": snapshot.id,
        "project_id": snapshot.project_id,
        "result_hash": snapshot.result_hash,
        "engine_version": snapshot.engine_version,
        "compliance_checks": compliance_checks,
    }

    # Ek doğrulamalar (regulation-grade checklist)
    validation_issues = []
    try:
        cbam_issues = validate_cbam_report(cbam_json_obj if isinstance(cbam_json_obj, dict) else {})
        validation_issues.extend([x.to_dict() for x in (cbam_issues or [])])
    except Exception as _e:
        validation_issues.append({
            "rule_id": "CBAM.VAL.ERR",
            "reg_reference": "internal",
            "severity": "warn",
            "message_tr": "CBAM validator çalıştırılamadı.",
            "remediation_tr": "CBAM rapor datasını ve şemasını kontrol edin.",
            "details": {"error": str(_e)},
        })

    try:
        ets_issues = validate_ets_reporting(ets_json_obj if isinstance(ets_json_obj, dict) else {})
        validation_issues.extend([x.to_dict() for x in (ets_issues or [])])
    except Exception as _e:
        validation_issues.append({
            "rule_id": "ETS.VAL.ERR",
            "reg_reference": "internal",
            "severity": "warn",
            "message_tr": "ETS validator çalıştırılamadı.",
            "remediation_tr": "ETS rapor datasını ve şemasını kontrol edin.",
            "details": {"error": str(_e)},
        })

    compliance_payload["validation_issues"] = validation_issues


    # Verification case (Paket B3): project + period_year bazlı dahil et (varsa)
    period_year = None
    try:
        period_year = ((res or {}).get("input_bundle") or {}).get("period", {}).get("year", None)
    except Exception:
        period_year = None
    if period_year is None:
        try:
            with db() as s:
                p = s.get(Project, int(snapshot.project_id))
            period_year = int(getattr(p, "period_year", 0) or 0) if p else None
        except Exception:
            period_year = None

    verification_payload = _verification_payload(snapshot, int(period_year) if period_year else None)

    # PDF report
    pdf_bytes = _report_pdf_for_snapshot(snapshot, res, cfg)
    report_hash = _report_hash_for_report_bytes(pdf_bytes)

    # data quality
    dq = _data_quality_from_uploads(snapshot.project_id)

    # evidence docs
    evidence_manifest, evidence_files_to_zip = _evidence_files_for_project(snapshot.project_id)

    # bytes
    snapshot_json_bytes = _json_bytes(snapshot_payload)
    factors_json_bytes = _json_bytes(factors_json)
    meth_json_bytes = _json_bytes(meth_obj)
    dq_json_bytes = _json_bytes(dq)
    evidence_index_bytes = _json_bytes({"evidence_documents": evidence_manifest})
    compliance_json_bytes = _json_bytes(compliance_payload)
    verification_json_bytes = _json_bytes(verification_payload)

    # Manifest (signature payload: signature alanı hariç)
    manifest_base = {
        "snapshot_id": snapshot.id,
        "engine_version": snapshot.engine_version,
        "created_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "input_hashes": inputs,
        "factor_versions": factor_versions,
        "methodology_version": methodology_version,
        "previous_snapshot_hash": getattr(snapshot, "previous_snapshot_hash", None),
        "report_hash": report_hash,
        "snapshot_hash": sha256_bytes(snapshot_json_bytes),
        "factors_hash": sha256_bytes(factors_json_bytes),
        "methodology_hash": sha256_bytes(meth_json_bytes),
        "data_quality_hash": sha256_bytes(dq_json_bytes),
        "evidence_index_hash": sha256_bytes(evidence_index_bytes),
        "compliance_checks_hash": sha256_bytes(compliance_json_bytes),
        "verification_case_hash": sha256_bytes(verification_json_bytes),
        "ai_benchmark_hash": sha256_bytes(ai_benchmark_bytes),
        "ai_advisor_hash": sha256_bytes(ai_advisor_bytes),
        "ai_optimizer_hash": sha256_bytes(ai_optimizer_bytes),
        "ai_full_hash": sha256_bytes(ai_full_bytes),
        "cbam_report_xml_hash": sha256_bytes(cbam_xml_bytes),
        "cbam_report_json_hash": sha256_bytes(cbam_json_bytes),
        "ets_reporting_json_hash": sha256_bytes(ets_json_bytes),
    }

    signature_block = build_signature_block(manifest_base)
    manifest = dict(manifest_base)
    manifest["signature"] = signature_block  # {'signatures': [...], 'signed_payload_hash_sha256': ...}

    # ZIP build
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("manifest.json", _json_bytes(manifest))
        # signature.json (ayrı dosya)
        signature_obj = manifest.get("signature") or {}
        z.writestr("signature.json", _json_bytes(signature_obj))

        # inputs
        z.writestr("input/energy.csv", energy_bytes or b"")
        z.writestr("input/production.csv", prod_bytes or b"")
        z.writestr("input/materials.csv", mat_bytes or b"")

        # reference data
        z.writestr("factor_library/emission_factors.json", factors_json_bytes)
        z.writestr("methodology/methodology.json", meth_json_bytes)

        # snapshot + report
        z.writestr("snapshot/snapshot.json", snapshot_json_bytes)
        z.writestr("report/report.pdf", pdf_bytes or b"")
        # Regülasyon çıktıları (zorunlu isimler)
        z.writestr("cbam_report.xml", cbam_xml_bytes or b"")
        z.writestr("cbam_report.json", cbam_json_bytes or b"")
        z.writestr("ets_reporting.json", ets_json_bytes or b"")
        # PDF alias (ETS)
        z.writestr("ets_report.pdf", pdf_bytes or b"")

        # data quality
        z.writestr("data_quality/data_quality.json", dq_json_bytes)

        # evidence index + files
        z.writestr("evidence/evidence_index.json", evidence_index_bytes)

        # compliance + verification (Paket B3)
        z.writestr("compliance/compliance_checks.json", compliance_json_bytes)
        z.writestr("verification/verification_case.json", verification_json_bytes)

        # Faz 3 AI (opsiyonel)
        z.writestr("ai/benchmark.json", ai_benchmark_bytes)
        z.writestr("ai/advisor.json", ai_advisor_bytes)
        z.writestr("ai/optimizer.json", ai_optimizer_bytes)
        z.writestr("ai/ai_full.json", ai_full_bytes)

        for path_in_zip, bts in evidence_files_to_zip:
            z.writestr(path_in_zip, bts or b"")

    return out.getvalue()
