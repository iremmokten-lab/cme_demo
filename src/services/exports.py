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
from src.db.models import CalculationSnapshot, DatasetUpload, EmissionFactor, EvidenceDocument, Methodology, Report
from src.mrv.lineage import sha256_bytes
from src.services.reporting import build_pdf
from src.services.storage import EVIDENCE_DOCS_CATEGORIES


def build_xlsx_from_results(results_json: str) -> bytes:
    """
    Paket D4: XLSX export geliştirme
      - KPIs
      - CBAM_Table
      - CBAM_Goods_Summary (varsa)
      - ETS_Activity (varsa)
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
        pd.DataFrame([kpis]).to_excel(writer, index=False, sheet_name="KPIs")
        pd.DataFrame(table).to_excel(writer, index=False, sheet_name="CBAM_Table")

        if isinstance(cbam_goods, list) and cbam_goods:
            pd.DataFrame(cbam_goods).to_excel(writer, index=False, sheet_name="CBAM_Goods")

        if isinstance(ets_activity, list) and ets_activity:
            pd.DataFrame(ets_activity).to_excel(writer, index=False, sheet_name="ETS_Activity")

    return out.getvalue()


def build_zip(snapshot_id: int, results_json: str) -> bytes:
    xlsx_bytes = build_xlsx_from_results(results_json)
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"snapshot_{snapshot_id}_results.json", results_json or "{}")
        z.writestr(f"snapshot_{snapshot_id}_export.xlsx", xlsx_bytes)
    return out.getvalue()


def _safe_read_bytes(uri: str) -> bytes:
    try:
        p = Path(str(uri))
        if p.exists():
            return p.read_bytes()
    except Exception:
        pass
    return b""


def _snapshot_input_uris(snapshot: CalculationSnapshot) -> dict:
    """
    input_hashes_json yeni format:
      {
        "energy": {"uri":..., "sha256":..., ...},
        "production": {...},
        "materials": {...}
      }
    eski formatlarla uyumluluk da korunur.
    """
    try:
        ih = json.loads(snapshot.input_hashes_json or "{}")
    except Exception:
        ih = {}

    if isinstance(ih, dict) and ("energy" in ih or "production" in ih):
        out = {}
        for k in ("energy", "production", "materials"):
            v = ih.get(k) or {}
            if isinstance(v, dict):
                out[k] = {"uri": v.get("uri") or "", "sha256": v.get("sha256") or ""}
        return out

    if isinstance(ih, dict):
        return {
            "energy": {"uri": ih.get("energy_uri") or "", "sha256": ""},
            "production": {"uri": ih.get("production_uri") or "", "sha256": ""},
            "materials": {"uri": "", "sha256": ""},
        }

    return {"energy": {"uri": "", "sha256": ""}, "production": {"uri": "", "sha256": ""}, "materials": {"uri": "", "sha256": ""}}


def _ensure_pdf_for_snapshot(snapshot: CalculationSnapshot) -> tuple[bytes, str]:
    """
    Paket D4: PDF payload artık CBAM/ETS bölümleri + DQ içerir.
    - Eğer DB’de aynı pdf sha ile kayıt varsa onu okur.
    - Yoksa build_pdf ile üretir ve Report tablosuna yazar (best-effort).
    """
    results = {}
    try:
        results = json.loads(snapshot.results_json or "{}")
    except Exception:
        results = {}

    kpis = (results.get("kpis") or {}) if isinstance(results, dict) else {}
    cbam_table = (results.get("cbam_table") or []) if isinstance(results, dict) else []
    scenario = (results.get("scenario") or {}) if isinstance(results, dict) else {}
    cbam_section = (results.get("cbam") or {}) if isinstance(results, dict) else {}
    ets_section = (results.get("ets") or {}) if isinstance(results, dict) else {}

    try:
        cfg = json.loads(snapshot.config_json or "{}")
    except Exception:
        cfg = {}

    # Methodology (zengin)
    meth_payload = None
    if getattr(snapshot, "methodology_id", None):
        with db() as s:
            m = s.get(Methodology, int(snapshot.methodology_id))
            if m:
                meth_payload = {
                    "id": m.id,
                    "name": m.name,
                    "description": m.description,
                    "scope": m.scope,
                    "version": m.version,
                    "created_at": (m.created_at.isoformat() if getattr(m, "created_at", None) else None),
                }

    title = "Rapor — CBAM + ETS (Readiness)"
    if isinstance(scenario, dict) and scenario.get("name"):
        title = f"Senaryo Raporu — {scenario.get('name')} (Readiness)"

    # DB’de hazır pdf var mı?
    with db() as s:
        r = (
            s.execute(
                select(Report)
                .where(Report.snapshot_id == snapshot.id, Report.report_type == "pdf")
                .order_by(Report.created_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        if r and getattr(r, "storage_uri", None):
            pdf_bytes = _safe_read_bytes(str(r.storage_uri))
            if pdf_bytes:
                return pdf_bytes, getattr(r, "sha256", sha256_bytes(pdf_bytes))

    # Data quality (upload’lardan derive)
    dq = {}
    inputs = _snapshot_input_uris(snapshot)
    try:
        with db() as s:
            for key in ("energy", "production", "materials"):
                sha_val = (inputs.get(key) or {}).get("sha256") or ""
                if not sha_val:
                    continue
                up = (
                    s.execute(
                        select(DatasetUpload)
                        .where(
                            DatasetUpload.project_id == snapshot.project_id,
                            DatasetUpload.dataset_type == key,
                            DatasetUpload.sha256 == sha_val,
                        )
                        .order_by(DatasetUpload.uploaded_at.desc())
                        .limit(1)
                    )
                    .scalars()
                    .first()
                )
                if up:
                    try:
                        dq_report = json.loads(up.data_quality_report_json or "{}")
                    except Exception:
                        dq_report = {}
                    dq[key] = {"score": up.data_quality_score, "report": dq_report}
    except Exception:
        dq = {}

    payload = {
        "kpis": kpis,
        "config": cfg,
        "cbam": cbam_section,
        "ets": ets_section,
        "cbam_table": cbam_table,
        "scenario": scenario,
        "methodology": meth_payload,
        "data_quality": dq,
        "data_sources": [
            "energy.csv (yüklenen dosya)",
            "production.csv (yüklenen dosya)",
            "materials.csv (opsiyonel, precursor)",
            "EmissionFactor Library (DB)",
            "Monitoring Plan (DB, facility bazlı)",
        ],
        "formulas": [
            "Direct emissions: fuel_quantity × NCV × emission_factor × oxidation_factor",
            "Indirect emissions: electricity_kwh × grid_factor (location/market)",
            "Precursor emissions: materials.material_quantity × materials.emission_factor",
        ],
    }

    pdf_uri, pdf_sha = build_pdf(snapshot.id, title, payload)
    pdf_bytes = _safe_read_bytes(pdf_uri)

    if pdf_bytes:
        try:
            with db() as s:
                ex = (
                    s.execute(
                        select(Report)
                        .where(Report.snapshot_id == snapshot.id, Report.report_type == "pdf", Report.sha256 == pdf_sha)
                        .limit(1)
                    )
                    .scalars()
                    .first()
                )
                if not ex:
                    s.add(Report(snapshot_id=snapshot.id, report_type="pdf", storage_uri=str(pdf_uri), sha256=pdf_sha))
                    s.commit()
        except Exception:
            pass

    return pdf_bytes, pdf_sha


def _hmac_signature(payload_bytes: bytes) -> str | None:
    """
    Signed manifest (Paket B).
    İmza anahtarı: env var EVIDENCE_SIGNING_KEY (Streamlit Cloud secrets -> env olarak set edilebilir).
    """
    key = os.getenv("EVIDENCE_SIGNING_KEY", "")
    if not key:
        return None
    mac = hmac.new(key.encode("utf-8"), payload_bytes, sha256).digest()
    return base64.b64encode(mac).decode("utf-8")


def _json_bytes(obj: dict) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")


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

    # PDF
    pdf_bytes, report_hash = _ensure_pdf_for_snapshot(snapshot)

    # Evidence docs: project bazlı
    evidence_manifest = []
    with db() as s:
        docs = s.execute(select(EvidenceDocument).where(EvidenceDocument.project_id == snapshot.project_id)).scalars().all()

    evidence_files_to_zip: list[tuple[str, bytes]] = []
    for d in docs:
        cat = (d.category or "documents").strip()
        if cat not in EVIDENCE_DOCS_CATEGORIES:
            cat = "documents"
        b = _safe_read_bytes(str(d.storage_uri))
        evidence_manifest.append(
            {
                "id": d.id,
                "category": cat,
                "filename": d.original_filename,
                "sha256": d.sha256,
                "storage_uri": str(d.storage_uri),
            }
        )
        evidence_files_to_zip.append((f"evidence/{cat}/{d.original_filename}", b))

    # Data quality: snapshot input upload kayıtlarından çek
    dq = {}
    try:
        with db() as s:
            for key in ("energy", "production", "materials"):
                sha_val = (inputs.get(key) or {}).get("sha256") or ""
                if not sha_val:
                    continue
                up = (
                    s.execute(
                        select(DatasetUpload)
                        .where(
                            DatasetUpload.project_id == snapshot.project_id,
                            DatasetUpload.dataset_type == key,
                            DatasetUpload.sha256 == sha_val,
                        )
                        .order_by(DatasetUpload.uploaded_at.desc())
                        .limit(1)
                    )
                    .scalars()
                    .first()
                )
                if up:
                    try:
                        dq_report = json.loads(up.data_quality_report_json or "{}")
                    except Exception:
                        dq_report = {}
                    dq[key] = {"score": up.data_quality_score, "report": dq_report}
    except Exception:
        dq = {}

    # Hashes
    snapshot_json_bytes = _json_bytes(snapshot_payload)
    factors_json_bytes = _json_bytes(factors_json)
    meth_json_bytes = _json_bytes(meth_obj)
    dq_json_bytes = _json_bytes(dq)
    evidence_index_bytes = _json_bytes({"evidence_documents": evidence_manifest})

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
    }

    sig = _hmac_signature(_json_bytes(manifest_base))
    manifest = dict(manifest_base)
    manifest["signature"] = sig  # None olabilir (anahtar yoksa)

    # ZIP build
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("manifest.json", _json_bytes(manifest))

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

        # data quality
        z.writestr("data_quality/data_quality.json", dq_json_bytes)

        # evidence index + files
        z.writestr("evidence/evidence_index.json", evidence_index_bytes)
        for path_in_zip, bts in evidence_files_to_zip:
            z.writestr(path_in_zip, bts or b"")

    return out.getvalue()
