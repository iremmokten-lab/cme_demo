from __future__ import annotations

import hmac
import io
import json
import zipfile
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Tuple, Dict, Any

import pandas as pd
from sqlalchemy import select

from src import config as app_config
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
from src.db.session import db
from src.mrv.lineage import sha256_bytes
from src.services.reporting import build_pdf
from src.services.ets_reporting import build_ets_reporting_dataset
from src.services.tr_ets_reporting import build_tr_ets_reporting
from src.services.storage import EVIDENCE_DOCS_CATEGORIES


def build_xlsx_from_results(results_json: str) -> bytes:
    results = json.loads(results_json) if results_json else {}
    kpis = results.get("kpis", {}) or {}
    table = results.get("cbam_table", []) or []

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        pd.DataFrame([kpis]).to_excel(writer, sheet_name="KPIs", index=False)
        pd.DataFrame(table).to_excel(writer, sheet_name="CBAM_Table", index=False)
    return out.getvalue()


def _safe_read_bytes(uri: str) -> bytes:
    if not uri:
        return b""
    try:
        p = Path(uri)
        if p.exists():
            return p.read_bytes()
    except Exception:
        pass
    return b""


def _snapshot_input_uris(snapshot: CalculationSnapshot) -> dict:
    try:
        ih = json.loads(snapshot.input_hashes_json or "{}")
    except Exception:
        ih = {}
    return {
        "energy": ih.get("energy") or {},
        "production": ih.get("production") or {},
        "materials": ih.get("materials") or {},
    }


def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj or {}, ensure_ascii=False, sort_keys=True, default=str, indent=2).encode("utf-8")


def _hmac_signature(manifest_obj: dict) -> str | None:
    key = app_config.get_evidence_pack_hmac_key()
    if not key:
        return None
    msg = _json_bytes({k: v for k, v in manifest_obj.items() if k != "signature"})
    return hmac.new(key.encode("utf-8"), msg, digestmod="sha256").hexdigest()


def _build_pdf_bytes(snapshot_id: int, title: str, data: dict) -> bytes:
    try:
        path, _sha = build_pdf(int(snapshot_id), str(title), data or {})
        return Path(path).read_bytes()
    except Exception:
        return b""


def _ensure_category(cat: str) -> str:
    c = str(cat or "").strip()
    if not c or c not in EVIDENCE_DOCS_CATEGORIES:
        return "documents"
    return c


def _verification_payload(project_id: int, period_year: int | None) -> dict:
    if period_year is None:
        return {"case": None, "findings": []}

    with db() as s:
        case = (
            s.execute(
                select(VerificationCase)
                .where(VerificationCase.project_id == int(project_id))
                .where(VerificationCase.period_year == int(period_year))
                .order_by(VerificationCase.created_at.desc())
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

    def _safe_load(sv: str | None, default):
        try:
            return json.loads(sv or "")
        except Exception:
            return default

    return {
        "case": {
            "id": int(case.id),
            "project_id": int(case.project_id),
            "facility_id": int(case.facility_id) if case.facility_id is not None else None,
            "period_year": int(case.period_year) if case.period_year is not None else None,
            "snapshot_id": int(case.snapshot_id) if case.snapshot_id is not None else None,
            "status": str(case.status or ""),
            "title": str(case.title or ""),
            "description": str(case.description or ""),
            "sampling_plan": _safe_load(getattr(case, "sampling_json", None), {}),
            "created_at": str(case.created_at),
            "closed_at": str(getattr(case, "closed_at", None)) if getattr(case, "closed_at", None) else None,
        },
        "findings": [
            {
                "id": int(f.id),
                "severity": str(f.severity or ""),
                "title": str(f.title or ""),
                "description": str(f.description or ""),
                "evidence_ref": str(f.evidence_ref or ""),
                "corrective_action": str(f.corrective_action or ""),
                "action_due_date": str(f.action_due_date or ""),
                "status": str(f.status or ""),
                "created_at": str(f.created_at),
                "resolved_at": str(getattr(f, "resolved_at", None)) if getattr(f, "resolved_at", None) else None,
            }
            for f in findings
        ],
    }


def build_evidence_pack(snapshot_id: int) -> bytes:
    with db() as s:
        snapshot = s.get(CalculationSnapshot, int(snapshot_id))
        if not snapshot:
            raise ValueError("Snapshot bulunamadı.")
        project = s.get(Project, int(snapshot.project_id))

    inputs = _snapshot_input_uris(snapshot)
    energy_bytes = _safe_read_bytes(inputs.get("energy", {}).get("uri", ""))
    prod_bytes = _safe_read_bytes(inputs.get("production", {}).get("uri", ""))
    mat_bytes = _safe_read_bytes(inputs.get("materials", {}).get("uri", ""))

    # factors
    with db() as s:
        factors = s.execute(select(EmissionFactor).order_by(EmissionFactor.factor_type, EmissionFactor.year.desc())).scalars().all()
    factor_rows = [
        {
            "factor_type": f.factor_type,
            "value": f.value,
            "unit": f.unit,
            "source": f.source,
            "year": f.year,
            "version": f.version,
            "region": f.region,
            "reference": getattr(f, "reference", "") or "",
        }
        for f in factors
    ]

    # methodology
    meth_obj = {}
    if getattr(snapshot, "methodology_id", None):
        with db() as s:
            m = s.get(Methodology, int(snapshot.methodology_id))
        if m:
            meth_obj = {
                "id": int(m.id),
                "name": str(m.name or ""),
                "description": str(m.description or ""),
                "scope": str(m.scope or ""),
                "version": str(m.version or ""),
                "created_at": str(getattr(m, "created_at", None)) if getattr(m, "created_at", None) else None,
            }

    # snapshot payload
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
        "snapshot_id": int(snapshot.id),
        "project_id": int(snapshot.project_id),
        "created_at": str(getattr(snapshot, "created_at", None)),
        "engine_version": str(snapshot.engine_version or ""),
        "input_hash": str(getattr(snapshot, "input_hash", "") or ""),
        "result_hash": str(getattr(snapshot, "result_hash", "") or ""),
        "config": cfg,
        "input_hashes": ih,
        "results": res,
    }

    # regulatory datasets
    ets_json_obj = build_ets_reporting_dataset(project_id=int(snapshot.project_id), snapshot_id=int(snapshot.id), results=res or {}, config=cfg or {})
    tr_ets_json_obj = {}
    if app_config.get_tr_ets_mode():
        tr_ets_json_obj = build_tr_ets_reporting(project_id=int(snapshot.project_id), snapshot_id=int(snapshot.id), results=res or {}, config=cfg or {})

    cbam_xml_str = ""
    cbam_json_obj = {}
    try:
        cbam = (res or {}).get("cbam", {}) or {}
        cbam_xml_str = str(cbam.get("cbam_reporting") or "")
        cbam_json_obj = {
            "schema": "cbam_report.v1",
            "facility": (res or {}).get("input_bundle", {}).get("facility", {}),
            "products": cbam.get("table") or [],
            "meta": cbam.get("meta") or {},
            "methodology": (res or {}).get("input_bundle", {}).get("methodology_ref", {}),
            "validation_status": {"used_default_factors": bool(((res or {}).get("energy", {}) or {}).get("used_default_factors"))},
        }
    except Exception:
        cbam_xml_str = ""
        cbam_json_obj = {}

    # compliance
    compliance_payload = {
        "snapshot_id": int(snapshot.id),
        "project_id": int(snapshot.project_id),
        "engine_version": str(snapshot.engine_version or ""),
        "compliance_checks": (res or {}).get("compliance_checks", []) if isinstance(res, dict) else [],
    }

    # verification
    period_year = None
    try:
        period_year = ((res or {}).get("input_bundle") or {}).get("period", {}).get("year", None)
    except Exception:
        period_year = None
    if period_year is None and project is not None:
        try:
            period_year = int(getattr(project, "reporting_year", None))
        except Exception:
            period_year = None
    verification_payload = _verification_payload(int(snapshot.project_id), int(period_year) if period_year else None)

    # PDFs
    base_for_pdf = {
        "kpis": (res or {}).get("kpis", {}) if isinstance(res, dict) else {},
        "config": cfg or {},
        "cbam": (res or {}).get("cbam", {}) if isinstance(res, dict) else {},
        "cbam_table": (res or {}).get("cbam_table", []) if isinstance(res, dict) else [],
        "ets": (res or {}).get("ets", {}) if isinstance(res, dict) else {},
        "qa_flags": (res or {}).get("qa_flags", []) if isinstance(res, dict) else [],
        "compliance_checks": compliance_payload.get("compliance_checks") or [],
    }

    pdf_general = _build_pdf_bytes(snapshot.id, "Genel MRV Raporu", base_for_pdf)
    pdf_ets = _build_pdf_bytes(snapshot.id, "ETS Raporu (EU ETS / MRR)", {"ets_reporting": ets_json_obj, **base_for_pdf})
    pdf_cbam = _build_pdf_bytes(snapshot.id, "CBAM Raporu", {"cbam_report": cbam_json_obj, **base_for_pdf})
    pdf_comp = _build_pdf_bytes(snapshot.id, "Uyum (Compliance) Raporu", {"compliance": compliance_payload, **base_for_pdf})

    # Evidence docs
    with db() as s:
        docs = (
            s.execute(select(EvidenceDocument).where(EvidenceDocument.project_id == int(snapshot.project_id)).order_by(EvidenceDocument.uploaded_at.desc()))
            .scalars()
            .all()
        )
    evidence_index = []
    evidence_files = []
    for d in docs:
        cat = _ensure_category(getattr(d, "category", "") or "")
        uri = str(getattr(d, "storage_uri", "") or "")
        bts = _safe_read_bytes(uri)
        sha = sha256_bytes(bts) if bts else (str(getattr(d, "sha256", "") or ""))
        fn = f"evidence/{cat}/{int(d.id)}_{Path(uri).name if uri else 'document.bin'}"
        evidence_index.append({"id": int(d.id), "category": cat, "title": str(getattr(d, "title", "") or ""), "sha256": sha, "uri": uri, "path_in_pack": fn})
        if bts:
            evidence_files.append((fn, bts))

    files: list[tuple[str, bytes]] = []
    files += [
        ("input/energy.csv", energy_bytes or b""),
        ("input/production.csv", prod_bytes or b""),
        ("input/materials.csv", mat_bytes or b""),
        ("snapshot/snapshot.json", _json_bytes(snapshot_payload)),
        ("factor_library/emission_factors.json", _json_bytes({"emission_factors": factor_rows})),
        ("methodology/methodology.json", _json_bytes(meth_obj)),
        ("cbam_report.xml", (cbam_xml_str.encode("utf-8") if cbam_xml_str else b"")),
        ("cbam_report.json", _json_bytes(cbam_json_obj)),
        ("ets_reporting.json", _json_bytes(ets_json_obj)),
        ("tr_ets_reporting.json", _json_bytes(tr_ets_json_obj)),
        ("compliance/compliance_checks.json", _json_bytes(compliance_payload)),
        ("verification/verification_case.json", _json_bytes(verification_payload)),
        ("report/report.pdf", pdf_general or b""),
        ("ets_report.pdf", pdf_ets or b""),
        ("cbam_report.pdf", pdf_cbam or b""),
        ("compliance_report.pdf", pdf_comp or b""),
        ("evidence/evidence_index.json", _json_bytes({"evidence": evidence_index})),
    ]
    files += evidence_files

    manifest = {
        "snapshot_id": int(snapshot.id),
        "project_id": int(snapshot.project_id),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine_version": str(snapshot.engine_version or ""),
        "input_hash": str(getattr(snapshot, "input_hash", "") or ""),
        "result_hash": str(getattr(snapshot, "result_hash", "") or ""),
        "files": [],
    }
    for path, bts in sorted(files, key=lambda x: x[0]):
        manifest["files"].append({"path": path, "sha256": sha256(bts or b"").hexdigest(), "bytes": len(bts or b"")})

    manifest["signature"] = _hmac_signature(manifest)

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("manifest.json", _json_bytes(manifest))
        z.writestr("signature.json", _json_bytes({"algorithm": "HMAC-SHA256", "key_id": "EVIDENCE_PACK_HMAC_KEY", "signature": manifest.get("signature")}))
        for path, bts in sorted(files, key=lambda x: x[0]):
            z.writestr(path, bts or b"")

    return out.getvalue()


def export_evidence_pack(project_id: int, snapshot_id: int) -> Tuple[bytes, Dict[str, Any]]:
    """UI/API için uyumlu wrapper: (zip_bytes, manifest_dict)."""
    zip_bytes = build_evidence_pack(int(snapshot_id))
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as z:
        manifest = json.loads(z.read("manifest.json").decode("utf-8"))
    return zip_bytes, manifest
