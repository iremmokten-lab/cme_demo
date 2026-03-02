from __future__ import annotations

import io
import zipfile
from datetime import datetime, timezone
from hashlib import sha256
from typing import Dict, Any, Tuple, List

from sqlalchemy import select

from src.db.session import db
from src.db.models import CalculationSnapshot, EvidenceDocument
from src.services.storage_backend import get_storage_backend

def _sha256(b: bytes) -> str:
    return sha256(b).hexdigest()

def build_cbam_portal_package(snapshot_id: int, *, include_evidence: bool = True) -> Tuple[bytes, Dict[str, Any]]:
    with db() as s:
        snap = s.get(CalculationSnapshot, int(snapshot_id))
        if not snap:
            raise ValueError("Snapshot bulunamadı.")
        try:
            res = __import__("json").loads(snap.results_json or "{}")
        except Exception:
            res = {}
        cbam_xml = (((res or {}).get("cbam") or {}).get("cbam_reporting") or "") if isinstance(res, dict) else ""
        if not cbam_xml:
            raise ValueError("CBAM XML boş. (Hard FAIL olabilir ya da CBAM yok.)")

        docs: List[EvidenceDocument] = []
        if include_evidence:
            docs = s.execute(
                select(EvidenceDocument)
                .where(EvidenceDocument.project_id == int(snap.project_id))
                .order_by(EvidenceDocument.created_at.desc())
            ).scalars().all()

    out = io.BytesIO()
    manifest = {
        "snapshot_id": int(snapshot_id),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": [],
        "sha256": {},
    }
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("cbam_report.xml", cbam_xml.encode("utf-8"))
        manifest["files"].append("cbam_report.xml")
        manifest["sha256"]["cbam_report.xml"] = _sha256(cbam_xml.encode("utf-8"))

        backend = get_storage_backend()
        for d in docs:
            # only attach docs marked for CBAM or generic documents
            cat = (d.category or "documents").lower()
            if cat not in ("cbam", "documents"):
                continue
            try:
                b = backend.get_bytes_by_uri(d.uri)
            except Exception:
                continue
            name = (d.filename or f"evidence_{d.id}.bin").replace("..", "_").replace("/", "_")
            path = f"attachments/{name}"
            z.writestr(path, b)
            manifest["files"].append(path)
            manifest["sha256"][path] = _sha256(b)

        z.writestr("manifest.json", __import__("json").dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8"))

    return out.getvalue(), manifest

def store_cbam_portal_package(snapshot_id: int) -> Dict[str, Any]:
    bts, manifest = build_cbam_portal_package(int(snapshot_id))
    backend = get_storage_backend()
    key = f"reports/{int(snapshot_id)}/cbam_portal_package.zip"
    uri = backend.put_bytes(key, bts)
    return {"uri": uri, "sha256": _sha256(bts), "manifest": manifest}
