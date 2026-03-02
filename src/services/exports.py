from __future__ import annotations

import hmac
import io
import json
import zipfile
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd
from sqlalchemy import select

from src import config as app_config
from src.db.models import (
    CalculationSnapshot,
    EmissionFactor,
    EvidenceDocument,
    Methodology,
    Project,
    VerificationCase,
    VerificationFinding,
)
from src.db.session import db
from src.mrv.lineage import sha256_bytes
from src.services.ets_reporting import build_ets_reporting_dataset
from src.services.reporting import build_pdf
from src.services.storage import EVIDENCE_DOCS_CATEGORIES
from src.services.tr_ets_reporting import build_tr_ets_reporting


def build_xlsx_from_results(results_json: str) -> bytes:
    """Create a simple Excel export from snapshot results."""
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


def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj or {}, ensure_ascii=False, sort_keys=True, default=str, indent=2).encode("utf-8")


def _hmac_signature(manifest_obj: dict) -> str | None:
    key = app_config.get_evidence_pack_hmac_key()
    if not key:
        return None
    msg = _json_bytes({k: v for k, v in (manifest_obj or {}).items() if k != "signature"})
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
    """Build a verifier-friendly evidence pack as a ZIP."""

    with db() as s:
        snapshot = s.get(CalculationSnapshot, int(snapshot_id))
        if not snapshot:
            raise ValueError("Snapshot bulunamadı.")
        project = s.get(Project, int(snapshot.project_id))

    # Snapshot payloads
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

    # Hard-fail guard used by compliance pages
    strict = (res or {}).get("compliance_strict") or {}
    strict_overall = str((strict or {}).get("overall_status") or "")
    hard_fail = strict_overall.upper() == "FAIL"

    # Input datasets (best-effort)
    energy_bytes = _safe_read_bytes(((ih.get("energy") or {}) or {}).get("uri", ""))
    prod_bytes = _safe_read_bytes(((ih.get("production") or {}) or {}).get("uri", ""))
    mat_bytes = _safe_read_bytes(((ih.get("materials") or {}) or {}).get("uri", ""))

    # Factors
    with db() as s:
        factors = (
            s.execute(select(EmissionFactor).order_by(EmissionFactor.factor_type, EmissionFactor.year.desc()))
            .scalars()
            .all()
        )
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

    # Methodology
    meth_obj: dict = {}
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

    # Regulatory datasets
    ets_json_obj = build_ets_reporting_dataset(
        project_id=int(snapshot.project_id),
        snapshot_id=int(snapshot.id),
        results=res or {},
        config=cfg or {},
    )
    tr_ets_json_obj = {}
    if app_config.get_tr_ets_mode():
        tr_ets_json_obj = build_tr_ets_reporting(
            project_id=int(snapshot.project_id),
            snapshot_id=int(snapshot.id),
            results=res or {},
            config=cfg or {},
        )

    cbam_xml_str = ""
    cbam_json_obj: dict = {}
    if hard_fail:
        cbam_json_obj = {
            "error": "HARD_FAIL",
            "message_tr": "Zorunlu alanlar eksik: CBAM resmi raporu üretilmedi.",
            "compliance_status": strict_overall,
        }
    else:
        try:
            cbam = (res or {}).get("cbam", {}) or {}
            cbam_xml_str = str(cbam.get("cbam_reporting") or "")
            cbam_json_obj = {
                "schema": "cbam_report.v1",
                "facility": (res or {}).get("input_bundle", {}).get("facility", {}),
                "products": cbam.get("table") or [],
                "meta": cbam.get("meta") or {},
                "methodology": (res or {}).get("input_bundle", {}).get("methodology_ref", {}),
                "validation_status": {
                    "used_default_factors": bool(((res or {}).get("energy", {}) or {}).get("used_default_factors"))
                },
            }
        except Exception:
            cbam_xml_str = ""
            cbam_json_obj = {}

    compliance_payload = {
        "snapshot_id": int(snapshot.id),
        "project_id": int(snapshot.project_id),
        "engine_version": str(snapshot.engine_version or ""),
        "compliance_checks": (res or {}).get("compliance_checks", []) if isinstance(res, dict) else [],
        "compliance_strict": strict,
    }

    # Verification payload (year best-effort)
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

    # Evidence documents
    with db() as s:
        docs = (
            s.execute(
                select(EvidenceDocument)
                .where(EvidenceDocument.project_id == int(snapshot.project_id))
                .order_by(EvidenceDocument.uploaded_at.desc())
            )
            .scalars()
            .all()
        )
    evidence_index = []
    evidence_files: list[tuple[str, bytes]] = []
    for d in docs:
        cat = _ensure_category(getattr(d, "category", "") or "")
        uri = str(getattr(d, "storage_uri", "") or "")
        bts = _safe_read_bytes(uri)
        sha_val = sha256_bytes(bts) if bts else (str(getattr(d, "sha256", "") or ""))
        fn = f"evidence/{cat}/{int(d.id)}_{Path(uri).name if uri else 'document.bin'}"
        evidence_index.append(
            {
                "id": int(d.id),
                "category": cat,
                "title": str(getattr(d, "title", "") or ""),
                "sha256": sha_val,
                "uri": uri,
                "path_in_pack": fn,
            }
        )
        if bts:
            evidence_files.append((fn, bts))

    files: list[tuple[str, bytes]] = [
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

    # Optional: compliance closure extras (best-effort; do not fail pack creation)
    try:
        from src.db.cbam_compliance_models import RegulationSpecVersion, CBAMMethodologyEvidence, CBAMCarbonPricePaid
        from src.db.ets_compliance_models import (
            ETSMonitoringPlan,
            ETSUncertaintyAssessment,
            ETSTierJustification,
            ETSQAQCEvidence,
            ETSFallbackEvent,
        )
        from src.services.cbam_portal_package import build_cbam_portal_package

        # regulation versions
        reg_versions = []
        with db() as s:
            reg_versions = s.execute(select(RegulationSpecVersion).order_by(RegulationSpecVersion.fetched_at.desc())).scalars().all()
        if reg_versions:
            files.append(
                (
                    "regulation/versions.json",
                    _json_bytes(
                        {
                            "versions": [
                                {
                                    "spec_name": r.spec_name,
                                    "spec_version": r.spec_version,
                                    "spec_hash": r.spec_hash,
                                    "source": r.source,
                                    "fetched_at": str(r.fetched_at),
                                }
                                for r in reg_versions
                            ]
                        }
                    ),
                )
            )

        # CBAM portal package
        if (not hard_fail) and callable(build_cbam_portal_package):
            try:
                portal_zip, portal_manifest = build_cbam_portal_package(int(snapshot.id))
                files.append(("cbam/portal_package.zip", portal_zip))
                files.append(("cbam/portal_manifest.json", _json_bytes(portal_manifest)))
            except Exception:
                pass

        # CBAM evidence extras
        with db() as s:
            company_id = int(getattr(project, "company_id", 0) or 0)
            if company_id:
                m = (
                    s.execute(
                        select(CBAMMethodologyEvidence)
                        .where(CBAMMethodologyEvidence.company_id == company_id)
                        .where(CBAMMethodologyEvidence.snapshot_id == int(snapshot.id))
                        .order_by(CBAMMethodologyEvidence.created_at.desc())
                    )
                    .scalars()
                    .first()
                )
                if m:
                    files.append(
                        (
                            "cbam/methodology_evidence.json",
                            _json_bytes(
                                {
                                    "boundary": m.boundary,
                                    "allocation": m.allocation,
                                    "scrap_method": m.scrap_method,
                                    "electricity_method": m.electricity_method,
                                    "electricity_factor_source": m.electricity_factor_source,
                                    "notes": m.notes,
                                    "created_at": str(m.created_at),
                                }
                            ),
                        )
                    )

                cps = (
                    s.execute(
                        select(CBAMCarbonPricePaid)
                        .where(CBAMCarbonPricePaid.company_id == company_id)
                        .where(CBAMCarbonPricePaid.snapshot_id == int(snapshot.id))
                        .order_by(CBAMCarbonPricePaid.created_at.desc())
                    )
                    .scalars()
                    .all()
                )
                if cps:
                    files.append(
                        (
                            "cbam/carbon_price_paid.json",
                            _json_bytes(
                                {
                                    "items": [
                                        {
                                            "country": c.country,
                                            "instrument": c.instrument,
                                            "amount_per_tco2": c.amount_per_tco2,
                                            "currency": c.currency,
                                            "verified": bool(c.verified),
                                            "evidence_doc_id": c.evidence_doc_id,
                                            "notes": c.notes,
                                            "created_at": str(c.created_at),
                                        }
                                        for c in cps
                                    ]
                                }
                            ),
                        )
                    )

        # ETS evidence extras
        company_id = int(getattr(project, "company_id", 0) or 0)
        year = int(period_year) if period_year else None
        if company_id and year:
            with db() as s:
                mp = (
                    s.execute(
                        select(ETSMonitoringPlan)
                        .where(ETSMonitoringPlan.company_id == company_id)
                        .where(ETSMonitoringPlan.year == year)
                        .where(ETSMonitoringPlan.status == "active")
                        .order_by(ETSMonitoringPlan.version.desc())
                    )
                    .scalars()
                    .first()
                )
                if mp:
                    try:
                        mp_json = json.loads(mp.plan_json or "{}")
                    except Exception:
                        mp_json = {}
                    files.append(("ets/monitoring_plan.json", _json_bytes({"year": year, "version": mp.version, "hash": mp.plan_hash, "plan": mp_json})))

                ua = (
                    s.execute(
                        select(ETSUncertaintyAssessment)
                        .where(ETSUncertaintyAssessment.company_id == company_id)
                        .where(ETSUncertaintyAssessment.year == year)
                        .order_by(ETSUncertaintyAssessment.created_at.desc())
                    )
                    .scalars()
                    .first()
                )
                if ua:
                    try:
                        ua_json = json.loads(ua.assessment_json or "{}")
                    except Exception:
                        ua_json = {}
                    files.append(("ets/uncertainty_assessment.json", _json_bytes({"year": year, "result_percent": ua.result_percent, "method": ua.method, "assessment": ua_json})))

                tj = (
                    s.execute(
                        select(ETSTierJustification)
                        .where(ETSTierJustification.company_id == company_id)
                        .where(ETSTierJustification.year == year)
                        .order_by(ETSTierJustification.created_at.desc())
                    )
                    .scalars()
                    .first()
                )
                if tj:
                    try:
                        tj_json = json.loads(tj.justification_json or "{}")
                    except Exception:
                        tj_json = {}
                    files.append(("ets/tier_justification.json", _json_bytes({"year": year, "justification": tj_json})))

                qs = (
                    s.execute(
                        select(ETSQAQCEvidence)
                        .where(ETSQAQCEvidence.company_id == company_id)
                        .where(ETSQAQCEvidence.year == year)
                        .order_by(ETSQAQCEvidence.created_at.desc())
                    )
                    .scalars()
                    .all()
                )
                if qs:
                    files.append(
                        (
                            "ets/qaqc_evidence.json",
                            _json_bytes(
                                {
                                    "year": year,
                                    "items": [
                                        {
                                            "control_name": q.control_name,
                                            "description": q.description,
                                            "evidence_doc_id": q.evidence_doc_id,
                                            "created_at": str(q.created_at),
                                        }
                                        for q in qs
                                    ],
                                }
                            ),
                        )
                    )

                fs = (
                    s.execute(
                        select(ETSFallbackEvent)
                        .where(ETSFallbackEvent.company_id == company_id)
                        .where(ETSFallbackEvent.year == year)
                        .order_by(ETSFallbackEvent.created_at.desc())
                    )
                    .scalars()
                    .all()
                )
                if fs:
                    files.append(
                        (
                            "ets/missing_data_fallback.json",
                            _json_bytes(
                                {
                                    "year": year,
                                    "events": [
                                        {"reason": f.reason, "method": f.method, "value": f.value, "created_at": str(f.created_at)}
                                        for f in fs
                                    ],
                                }
                            ),
                        )
                    )

    except Exception:
        # Optional enrichment: ignore.
        pass

    # Manifest
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
        z.writestr(
            "signature.json",
            _json_bytes({"algorithm": "HMAC-SHA256", "key_id": "EVIDENCE_PACK_HMAC_KEY", "signature": manifest.get("signature")}),
        )
        for path, bts in sorted(files, key=lambda x: x[0]):
            z.writestr(path, bts or b"")

    return out.getvalue()


def export_evidence_pack(project_id: int, snapshot_id: int) -> Tuple[bytes, Dict[str, Any]]:
    """UI/API compatible wrapper: (zip_bytes, manifest_dict)."""
    zip_bytes = build_evidence_pack(int(snapshot_id))
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as z:
        manifest = json.loads(z.read("manifest.json").decode("utf-8"))
    return zip_bytes, manifest
