from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Dict, List, Tuple

from sqlalchemy import select

from src.db.models import CalculationSnapshot, DatasetUpload, EvidenceDocument, Project, Report
from src.db.session import db
from src.mrv.lineage import sha256_bytes, sha256_json, build_lineage_graph
from src.services.cbam_reporting import build_cbam_report_json
from src.services.cbam_xml import build_cbam_reporting_xml
from src.services.ets_reporting import build_ets_reporting_dataset
from src.services.tr_ets_reporting import build_tr_ets_reporting
from src.services.validators import validate_cbam_report, validate_ets_reporting
from src.services.signing import sign_manifest_hmac
from src import config as app_config


def _safe_json_loads(s: str, default):
    try:
        return json.loads(s or "")
    except Exception:
        return default


def _hash_bytes(b: bytes) -> str:
    return sha256(b).hexdigest()


def build_evidence_pack_zip(
    *,
    project_id: int,
    snapshot_id: int,
) -> Tuple[bytes, Dict[str, Any]]:
    """
    Evidence Pack içerir:
      - manifest.json
      - signature.json (HMAC key varsa)
      - cbam_report.json
      - cbam_report.xml
      - ets_reporting.json
      - tr_ets_reporting.json (TR_ETS_MODE True ise)
      - compliance_checks.json (validator sonuçları)
      - lineage.json
    """

    with db() as s:
        project = s.get(Project, int(project_id))
        snap = s.get(CalculationSnapshot, int(snapshot_id))
        if not project or not snap:
            raise ValueError("Project/Snapshot bulunamadı.")

        results = _safe_json_loads(snap.results_json, {})
        config = _safe_json_loads(snap.config_json, {})
        input_hashes = _safe_json_loads(snap.input_hashes_json, {})

        uploads = (
            s.execute(select(DatasetUpload).where(DatasetUpload.project_id == int(project_id)).order_by(DatasetUpload.uploaded_at.desc()))
            .scalars()
            .all()
        )
        evidences = (
            s.execute(select(EvidenceDocument).where(EvidenceDocument.project_id == int(project_id)).order_by(EvidenceDocument.uploaded_at.desc()))
            .scalars()
            .all()
        )
        reports = (
            s.execute(select(Report).where(Report.project_id == int(project_id)).order_by(Report.created_at.desc()))
            .scalars()
            .all()
        )

    # Build reporting datasets
    cbam_json = build_cbam_report_json(project_id=project_id, snapshot_id=snapshot_id, results=results, config=config)
    cbam_xml = build_cbam_reporting_xml(cbam_json)

    ets_json = build_ets_reporting_dataset(project_id=project_id, snapshot_id=snapshot_id, results=results, config=config)
    tr_ets_json = None
    if app_config.get_tr_ets_mode():
        tr_ets_json = build_tr_ets_reporting(project_id=project_id, snapshot_id=snapshot_id, results=results, config=config)

    # Validators → compliance checks
    compliance = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_id": int(snapshot_id),
        "input_hash": str(snap.input_hash or ""),
        "result_hash": str(snap.result_hash or ""),
        "cbam": validate_cbam_report(cbam_json),
        "ets": validate_ets_reporting(ets_json),
    }
    if tr_ets_json is not None:
        compliance["tr_ets"] = {"status": "PASS", "notes": "TR ETS reporting dataset üretildi."}

    # Lineage build
    ds_meta: list[dict] = []
    for u in uploads:
        ds_meta.append(
            {
                "id": int(u.id),
                "dataset_type": str(u.dataset_type),
                "uploaded_at": str(u.uploaded_at),
                "sha256": str(u.sha256 or ""),
                "uri": str(u.storage_uri or ""),
                "schema_version": str(getattr(u, "schema_version", "v1") or "v1"),
            }
        )
    ev_meta: list[dict] = []
    for e in evidences:
        ev_meta.append(
            {
                "id": int(e.id),
                "category": str(getattr(e, "category", "") or ""),
                "title": str(getattr(e, "title", "") or ""),
                "sha256": str(e.sha256 or ""),
                "uri": str(e.storage_uri or ""),
            }
        )
    rep_meta: list[dict] = []
    for r in reports:
        rep_meta.append(
            {
                "id": int(r.id),
                "report_type": str(getattr(r, "report_type", "") or ""),
                "uri": str(getattr(r, "storage_uri", "") or ""),
                "sha256": _hash_bytes((getattr(r, "report_json", "") or "").encode("utf-8")),
            }
        )

    factor_refs = []
    try:
        factor_refs = ((results.get("input_bundle") or {}).get("factor_set_ref") or [])
    except Exception:
        factor_refs = []

    lineage = build_lineage_graph(
        snapshot_id=int(snapshot_id),
        project_id=int(project_id),
        input_hash=str(snap.input_hash or ""),
        result_hash=str(snap.result_hash or ""),
        datasets=ds_meta,
        evidence_docs=ev_meta,
        factor_refs=factor_refs if isinstance(factor_refs, list) else [],
        compliance=compliance,
        reports=rep_meta,
    )

    # Package files
    files: Dict[str, bytes] = {}
    files["cbam_report.json"] = json.dumps(cbam_json, ensure_ascii=False, indent=2).encode("utf-8")
    files["cbam_report.xml"] = cbam_xml
    files["ets_reporting.json"] = json.dumps(ets_json, ensure_ascii=False, indent=2).encode("utf-8")
    if tr_ets_json is not None:
        files["tr_ets_reporting.json"] = json.dumps(tr_ets_json, ensure_ascii=False, indent=2).encode("utf-8")

    files["compliance_checks.json"] = json.dumps(compliance, ensure_ascii=False, indent=2).encode("utf-8")
    files["lineage.json"] = json.dumps(lineage, ensure_ascii=False, indent=2).encode("utf-8")

    # Manifest
    manifest = {
        "project_id": int(project_id),
        "snapshot_id": int(snapshot_id),
        "input_hash": str(snap.input_hash or ""),
        "result_hash": str(snap.result_hash or ""),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": [],
    }
    for name, content in sorted(files.items(), key=lambda x: x[0]):
        manifest["files"].append({"path": name, "sha256": sha256_bytes(content), "bytes": len(content)})

    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
    files["manifest.json"] = manifest_bytes

    # Signature (HMAC)
    signature = sign_manifest_hmac(manifest)
    files["signature.json"] = json.dumps(signature, ensure_ascii=False, indent=2).encode("utf-8")

    # zip
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in sorted(files.items(), key=lambda x: x[0]):
            zf.writestr(name, content)

    return out.getvalue(), manifest
