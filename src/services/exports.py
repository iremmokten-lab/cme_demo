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
from src.services.cbam_xml import cbam_reporting_to_xml
from src.services.reporting import build_pdf
from src.services.storage import EVIDENCE_DOCS_CATEGORIES


def build_xlsx_from_results(results_json: str) -> bytes:
    """XLSX export (mevcut davranış korunur).

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
        cbam_goods = (results.get("cbam") or {}).get("goods_summary", []) or []
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

    return out.getvalue()


def build_zip(files: dict[str, bytes]) -> bytes:
    """Basit ZIP builder. files: path->bytes"""
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for path_in_zip, bts in files.items():
            z.writestr(path_in_zip, bts or b"")
    return out.getvalue()


def _safe_read_bytes(uri: str) -> bytes:
    """Streamlit Cloud uyumlu: storage_uri genelde yerel path olur (./data/.. veya /tmp/..)."""
    if not uri:
        return b""
    try:
        p = Path(uri)
        if p.exists() and p.is_file():
            return p.read_bytes()
    except Exception:
        pass

    try:
        if uri.startswith("file://"):
            p = Path(uri.replace("file://", ""))
            if p.exists() and p.is_file():
                return p.read_bytes()
    except Exception:
        pass

    return b""


def _snapshot_input_uris(snapshot: CalculationSnapshot) -> dict:
    """input_hashes_json -> {energy:{uri,sha256,...}, production:{...}, materials:{...}}"""
    try:
        ih = json.loads(snapshot.input_hashes_json or "{}")
    except Exception:
        ih = {}

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
                    "uri": str(u.storage_uri),
                    "sha256": u.sha256,
                    "original_filename": u.original_filename,
                    "schema_version": u.schema_version,
                }

    return ih


def _ensure_pdf_for_snapshot(snapshot: CalculationSnapshot) -> tuple[bytes, str]:
    """Snapshot için PDF raporu üretir veya DB'den son raporu reuse eder."""
    with db() as s:
        rep = (
            s.execute(
                select(Report)
                .where(Report.snapshot_id == snapshot.id)
                .order_by(Report.created_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )

    if rep:
        b = _safe_read_bytes(str(rep.storage_uri))
        if b:
            return b, rep.sha256

    try:
        results = json.loads(snapshot.results_json or "{}")
    except Exception:
        results = {}

    # reporting.build_pdf signature: (snapshot_id, report_title, report_data) -> (storage_uri, sha256)
    try:
        storage_uri, rep_sha = build_pdf(int(snapshot.id), "Carbon MRV Snapshot Raporu", results)
    except Exception:
        storage_uri, rep_sha = "", ""

    pdf_bytes = _safe_read_bytes(str(storage_uri)) if storage_uri else b""
    if pdf_bytes and rep_sha:
        return pdf_bytes, rep_sha

    if pdf_bytes:
        return pdf_bytes, sha256(pdf_bytes).hexdigest()

    return b"", ""


def _hmac_signature(payload_bytes: bytes) -> str | None:
    """Manifest imzası: HMAC-SHA256(base64). Anahtar env: EVIDENCE_PACK_HMAC_KEY."""
    key = os.getenv("EVIDENCE_PACK_HMAC_KEY", "").strip()
    if not key:
        return None
    try:
        try:
            key_bytes = base64.b64decode(key.encode("utf-8"))
            if not key_bytes:
                key_bytes = key.encode("utf-8")
        except Exception:
            key_bytes = key.encode("utf-8")

        sig = hmac.new(key_bytes, payload_bytes, sha256).digest()
        return base64.b64encode(sig).decode("utf-8")
    except Exception:
        return None


def _json_bytes(obj: dict) -> bytes:
    return json.dumps(obj or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def build_evidence_pack(snapshot_id: int) -> bytes:
    """Evidence pack export (ZIP).

    FAZ 1.1:
      - report/cbam_report.json
      - report/cbam_report.xml

    NOT:
      - 1.2 ETS reporting çıktısı eklenmedi.
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
        factors = (
            s.execute(select(EmissionFactor).order_by(EmissionFactor.factor_type, EmissionFactor.year.desc()))
            .scalars()
            .all()
        )

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

    # Compliance checks (snapshot sonuçlarından)
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

    # Verification case: project + period_year bazlı dahil et (varsa)
    period_year = None
    try:
        period_year = ((res or {}).get("input_bundle") or {}).get("period", {}).get("year", None)
    except Exception:
        period_year = None
    if period_year is None:
        try:
            with db() as s:
                p = s.get(Project, int(snapshot.project_id))
                period_year = int(getattr(p, "year", 0) or 0) if p else None
        except Exception:
            period_year = None

    verification_payload = {
        "snapshot_id": snapshot.id,
        "project_id": snapshot.project_id,
        "period_year": period_year,
        "cases": [],
    }

    try:
        if period_year is not None:
            with db() as s:
                cases = (
                    s.execute(
                        select(VerificationCase)
                        .where(
                            VerificationCase.project_id == int(snapshot.project_id),
                            VerificationCase.period_year == int(period_year),
                        )
                        .order_by(VerificationCase.created_at.desc())
                    )
                    .scalars()
                    .all()
                )
                for c in cases:
                    findings = (
                        s.execute(
                            select(VerificationFinding)
                            .where(VerificationFinding.case_id == int(c.id))
                            .order_by(VerificationFinding.created_at.asc())
                        )
                        .scalars()
                        .all()
                    )
                    verification_payload["cases"].append(
                        {
                            "id": c.id,
                            "project_id": c.project_id,
                            "facility_id": c.facility_id,
                            "period_year": c.period_year,
                            "verifier_org": c.verifier_org,
                            "status": c.status,
                            "created_at": (c.created_at.isoformat() if getattr(c, "created_at", None) else None),
                            "created_by_user_id": getattr(c, "created_by_user_id", None),
                            "closed_at": (c.closed_at.isoformat() if getattr(c, "closed_at", None) else None),
                            "findings": [
                                {
                                    "id": f.id,
                                    "severity": f.severity,
                                    "description": f.description,
                                    "corrective_action": f.corrective_action,
                                    "due_date": f.due_date,
                                    "status": f.status,
                                    "created_at": (f.created_at.isoformat() if getattr(f, "created_at", None) else None),
                                    "closed_at": (f.closed_at.isoformat() if getattr(f, "closed_at", None) else None),
                                }
                                for f in findings
                            ],
                        }
                    )
    except Exception:
        verification_payload = {
            "snapshot_id": snapshot.id,
            "project_id": snapshot.project_id,
            "period_year": period_year,
            "cases": [],
            "note": "Verification workflow verisi bulunamadı veya DB şeması eksik.",
        }

    # PDF
    pdf_bytes, report_hash = _ensure_pdf_for_snapshot(snapshot)

    # Evidence docs
    evidence_manifest = []
    with db() as s:
        docs = (
            s.execute(select(EvidenceDocument).where(EvidenceDocument.project_id == snapshot.project_id))
            .scalars()
            .all()
        )

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

    # Data quality (upload kayıtlarından)
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

    # CBAM report artifacts (FAZ 1.1)
    cbam_reporting_obj = {}
    try:
        cbam_reporting_obj = (res or {}).get("cbam_reporting", {}) or {}
    except Exception:
        cbam_reporting_obj = {}
    if not isinstance(cbam_reporting_obj, dict):
        cbam_reporting_obj = {}

    cbam_report_json_bytes = _json_bytes(cbam_reporting_obj)
    try:
        cbam_xml_text = cbam_reporting_to_xml(cbam_reporting_obj)
    except Exception:
        cbam_xml_text = ""
    cbam_report_xml_bytes = (cbam_xml_text or "").encode("utf-8")

    # Byte payloads
    snapshot_json_bytes = _json_bytes(snapshot_payload)
    factors_json_bytes = _json_bytes(factors_json)
    meth_json_bytes = _json_bytes(meth_obj)
    dq_json_bytes = _json_bytes(dq)
    evidence_index_bytes = _json_bytes({"evidence_documents": evidence_manifest})
    compliance_json_bytes = _json_bytes(compliance_payload)
    verification_json_bytes = _json_bytes(verification_payload)

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
        "cbam_report_json_hash": sha256_bytes(cbam_report_json_bytes),
        "cbam_report_xml_hash": sha256_bytes(cbam_report_xml_bytes),
    }

    sig = _hmac_signature(_json_bytes(manifest_base))
    manifest = dict(manifest_base)
    manifest["signature"] = sig

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("manifest.json", _json_bytes(manifest))

        z.writestr("input/energy.csv", energy_bytes or b"")
        z.writestr("input/production.csv", prod_bytes or b"")
        z.writestr("input/materials.csv", mat_bytes or b"")

        z.writestr("factor_library/emission_factors.json", factors_json_bytes)
        z.writestr("methodology/methodology.json", meth_json_bytes)

        z.writestr("snapshot/snapshot.json", snapshot_json_bytes)
        z.writestr("report/report.pdf", pdf_bytes or b"")

        z.writestr("report/cbam_report.json", cbam_report_json_bytes)
        z.writestr("report/cbam_report.xml", cbam_report_xml_bytes)

        z.writestr("data_quality/data_quality.json", dq_json_bytes)

        z.writestr("evidence/evidence_index.json", evidence_index_bytes)
        z.writestr("compliance/compliance_checks.json", compliance_json_bytes)
        z.writestr("verification/verification_case.json", verification_json_bytes)

        for path_in_zip, bts in evidence_files_to_zip:
            z.writestr(path_in_zip, bts or b"")

    return out.getvalue()
