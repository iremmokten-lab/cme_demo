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
from src.services.cbam_reporting import cbam_reporting_to_xml
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
        ets_activity = (results.get("ets") or {}).get("verification", {}).get("fuel_rows", []) or []
    except Exception:
        ets_activity = []

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # KPIs
        if kpis:
            pd.DataFrame([kpis]).to_excel(writer, sheet_name="KPIs", index=False)
        else:
            pd.DataFrame([{"note": "No KPIs"}]).to_excel(writer, sheet_name="KPIs", index=False)

        # CBAM Table
        if table:
            pd.DataFrame(table).to_excel(writer, sheet_name="CBAM_Table", index=False)
        else:
            pd.DataFrame([{"note": "No CBAM Table"}]).to_excel(writer, sheet_name="CBAM_Table", index=False)

        # CBAM Goods summary
        if cbam_goods:
            pd.DataFrame(cbam_goods).to_excel(writer, sheet_name="CBAM_Goods_Summary", index=False)
        else:
            pd.DataFrame([{"note": "No CBAM Goods Summary"}]).to_excel(writer, sheet_name="CBAM_Goods_Summary", index=False)

        # ETS Activity
        if ets_activity:
            pd.DataFrame(ets_activity).to_excel(writer, sheet_name="ETS_Activity", index=False)
        else:
            pd.DataFrame([{"note": "No ETS Activity"}]).to_excel(writer, sheet_name="ETS_Activity", index=False)

    return output.getvalue()


def _json_bytes(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str, indent=2).encode("utf-8")


def _sign_manifest(manifest_bytes: bytes, secret: str) -> str:
    mac = hmac.new(secret.encode("utf-8"), manifest_bytes, sha256).digest()
    return base64.b64encode(mac).decode("utf-8")


def build_evidence_pack(snapshot_id: int, secret: str | None = None) -> bytes:
    """
    Evidence Pack ZIP:
      /manifest.json  (signable)
      /report/report.pdf
      /report/report.xlsx
      /report/results.json
      /datasets/*.csv (energy, production, materials)
      /evidence_docs/* (uploads)
      /verification/* (case + findings)
    """
    secret = secret or os.environ.get("EVIDENCE_PACK_SECRET", "demo-secret")

    with db() as s:
        snap = s.get(CalculationSnapshot, int(snapshot_id))
        if not snap:
            raise ValueError("Snapshot bulunamadı.")
        project = s.get(Project, int(snap.project_id))
        if not project:
            raise ValueError("Proje bulunamadı.")

        # uploads
        uploads = (
            s.execute(
                select(DatasetUpload)
                .where(DatasetUpload.project_id == int(project.id))
                .order_by(DatasetUpload.uploaded_at.desc())
            )
            .scalars()
            .all()
        )

        # evidence docs
        ev_docs = (
            s.execute(
                select(EvidenceDocument)
                .where(EvidenceDocument.project_id == int(project.id))
                .order_by(EvidenceDocument.uploaded_at.desc())
            )
            .scalars()
            .all()
        )

        # verification
        vcase = (
            s.execute(
                select(VerificationCase)
                .where(VerificationCase.project_id == int(project.id))
                .order_by(VerificationCase.created_at.desc())
            )
            .scalars()
            .first()
        )
        findings = []
        if vcase:
            findings = (
                s.execute(
                    select(VerificationFinding)
                    .where(VerificationFinding.case_id == int(vcase.id))
                    .order_by(VerificationFinding.created_at.desc())
                )
                .scalars()
                .all()
            )

        # factor refs / methodology for manifest
        methodology = s.get(Methodology, int(snap.methodology_id)) if getattr(snap, "methodology_id", None) else None
        factors = (
            s.execute(select(EmissionFactor).order_by(EmissionFactor.year.desc(), EmissionFactor.version.desc()))
            .scalars()
            .all()
        )

        # results JSON
        res_json = str(getattr(snap, "results_json", "") or "")
        res = json.loads(res_json) if res_json else {}

        # Build report artifacts
        pdf_bytes = build_pdf(res_json, snapshot_id=int(snapshot_id))
        xlsx_bytes = build_xlsx_from_results(res_json)

        # Manifest
        manifest = {
            "schema": "evidence-pack-1.0",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "snapshot": {
                "id": int(snapshot_id),
                "project_id": int(project.id),
                "engine_version": str(getattr(snap, "engine_version", "") or ""),
                "result_hash": str(getattr(snap, "result_hash", "") or ""),
                "created_at": (getattr(snap, "created_at", None).isoformat() if getattr(snap, "created_at", None) else None),
            },
            "project": {
                "id": int(project.id),
                "name": str(getattr(project, "name", "") or ""),
                "facility_id": int(getattr(project, "facility_id", 0) or 0),
                "company_id": int(getattr(project, "company_id", 0) or 0),
            },
            "methodology": (
                {
                    "id": int(methodology.id),
                    "name": str(methodology.name or ""),
                    "version": str(methodology.version or ""),
                    "scope": str(methodology.scope or ""),
                }
                if methodology
                else None
            ),
            "factor_library": [
                {
                    "id": int(f.id),
                    "factor_type": str(f.factor_type),
                    "region": str(f.region),
                    "year": int(f.year) if f.year is not None else None,
                    "version": str(f.version or ""),
                    "value": float(f.value),
                    "unit": str(f.unit or ""),
                    "source": str(f.source or ""),
                }
                for f in factors[:50]
            ],
            "datasets": [],
            "evidence_docs": [],
            "verification": None,
            "checksums": {},
            "signature": None,
        }

        # Build ZIP
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
            # Reports
            z.writestr("report/results.json", _json_bytes(res))
            z.writestr("report/report.pdf", pdf_bytes or b"")

        # CBAM/ETS reporting (FAZ 1)
        try:
            cbam_reporting_obj = (res or {}).get("cbam_reporting") or {}
        except Exception:
            cbam_reporting_obj = {}
        try:
            ets_reporting_obj = (res or {}).get("ets_reporting") or {}
        except Exception:
            ets_reporting_obj = {}
        try:
            allocation_obj = (res or {}).get("allocation") or {}
        except Exception:
            allocation_obj = {}

        z.writestr("report/cbam_report.json", _json_bytes(cbam_reporting_obj if isinstance(cbam_reporting_obj, dict) else {}))
        try:
            cbam_xml = cbam_reporting_to_xml(cbam_reporting_obj if isinstance(cbam_reporting_obj, dict) else {})
        except Exception:
            cbam_xml = ""
        z.writestr("report/cbam_report.xml", (cbam_xml or "").encode("utf-8"))

        z.writestr("report/ets_reporting.json", _json_bytes(ets_reporting_obj if isinstance(ets_reporting_obj, dict) else {}))
        z.writestr("report/allocation.json", _json_bytes(allocation_obj if isinstance(allocation_obj, dict) else {}))

            z.writestr("report/report.xlsx", xlsx_bytes or b"")

            # Datasets
            for u in uploads:
                try:
                    uri = str(getattr(u, "storage_uri", "") or "")
                    if not uri:
                        continue
                    filename = f"datasets/{str(getattr(u, 'dataset_type', 'dataset') or 'dataset')}_{int(getattr(u,'id',0) or 0)}.csv"
                    df = pd.read_csv(uri)
                    csv_bytes = df.to_csv(index=False).encode("utf-8")
                    z.writestr(filename, csv_bytes)

                    manifest["datasets"].append(
                        {
                            "id": int(getattr(u, "id", 0) or 0),
                            "dataset_type": str(getattr(u, "dataset_type", "") or ""),
                            "original_filename": str(getattr(u, "original_filename", "") or ""),
                            "schema_version": str(getattr(u, "schema_version", "") or ""),
                            "sha256": str(getattr(u, "sha256", "") or ""),
                            "zip_path": filename,
                        }
                    )
                    manifest["checksums"][filename] = sha256_bytes(csv_bytes)
                except Exception:
                    continue

            # Evidence docs (uploaded)
            for d in ev_docs:
                try:
                    uri = str(getattr(d, "storage_uri", "") or "")
                    if not uri:
                        continue
                    p = Path(uri)
                    if not p.exists():
                        continue
                    data = p.read_bytes()
                    cat = str(getattr(d, "category", "documents") or "documents")
                    safe_cat = cat if cat in EVIDENCE_DOCS_CATEGORIES else "documents"
                    fn = str(getattr(d, "original_filename", "") or p.name)
                    zip_path = f"evidence_docs/{safe_cat}/{int(getattr(d,'id',0) or 0)}_{fn}"
                    z.writestr(zip_path, data)

                    manifest["evidence_docs"].append(
                        {
                            "id": int(getattr(d, "id", 0) or 0),
                            "category": safe_cat,
                            "original_filename": fn,
                            "uploaded_at": (getattr(d, "uploaded_at", None).isoformat() if getattr(d, "uploaded_at", None) else None),
                            "zip_path": zip_path,
                        }
                    )
                    manifest["checksums"][zip_path] = sha256_bytes(data)
                except Exception:
                    continue

            # Verification
            if vcase:
                v_obj = {
                    "case": {
                        "id": int(vcase.id),
                        "status": str(getattr(vcase, "status", "") or ""),
                        "created_at": (getattr(vcase, "created_at", None).isoformat() if getattr(vcase, "created_at", None) else None),
                    },
                    "findings": [
                        {
                            "id": int(f.id),
                            "severity": str(getattr(f, "severity", "") or ""),
                            "title": str(getattr(f, "title", "") or ""),
                            "description": str(getattr(f, "description", "") or ""),
                            "created_at": (getattr(f, "created_at", None).isoformat() if getattr(f, "created_at", None) else None),
                        }
                        for f in findings
                    ],
                }
                z.writestr("verification/verification.json", _json_bytes(v_obj))
                manifest["verification"] = {"zip_path": "verification/verification.json"}
                manifest["checksums"]["verification/verification.json"] = sha256_bytes(_json_bytes(v_obj))

            # Manifest (signature after checksums)
            manifest_bytes = _json_bytes({k: v for k, v in manifest.items() if k != "signature"})
            signature = _sign_manifest(manifest_bytes, secret)
            manifest["signature"] = signature
            z.writestr("manifest.json", _json_bytes(manifest))
            manifest["checksums"]["manifest.json"] = sha256_bytes(_json_bytes(manifest))

        return buf.getvalue()
