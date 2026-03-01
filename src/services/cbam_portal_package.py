from __future__ import annotations

"""CBAM portal submission package builder.

Portal workflow (Declarant Portal):
- User uploads a ZIP containing:
  * one quarterly report XML file (must conform to the official XSD)
  * optional binary attachments (pdf/xlsx/docx/jpeg etc.)

This module builds a deterministic ZIP suitable for upload.
"""

import io
import json
import zipfile
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.mrv.lineage import sha256_bytes

from src.services.cbam_portal_xml_v23 import PortalMetaV23, build_qreport_v23
from src.services.cbam_xsd_validator import CBAMXSDValidator


_ALLOWED_ATTACH_EXT = {".pdf", ".xlsx", ".xls", ".doc", ".docx", ".jpeg", ".jpg", ".png"}


def _zip_write_bytes(zf: zipfile.ZipFile, arcname: str, data: bytes) -> None:
    zi = zipfile.ZipInfo(arcname)
    # fixed timestamp for determinism
    zi.date_time = (2020, 1, 1, 0, 0, 0)
    zi.compress_type = zipfile.ZIP_DEFLATED
    zf.writestr(zi, data)


def build_cbam_portal_zip(
    *,
    cbam_reporting_json: Dict[str, Any],
    portal_meta: PortalMetaV23,
    output_zip_path: str,
    xsd_main_path: Optional[str] = None,
    attachments: Optional[List[str]] = None,
    xml_filename: str = "quarterly_report.xml",
) -> Dict[str, Any]:
    """Create portal ZIP. Returns manifest-like info + validation result if XSD provided."""

    attachments = attachments or []
    # filter + stable sort
    cleaned: List[str] = []
    for a in attachments:
        try:
            p = Path(a)
            if not p.exists() or not p.is_file():
                continue
            if p.suffix.lower() not in _ALLOWED_ATTACH_EXT:
                continue
            cleaned.append(str(p))
        except Exception:
            continue
    cleaned = sorted(set(cleaned))

    xml_bytes = build_qreport_v23(report=cbam_reporting_json, meta=portal_meta)

    validation = None
    if xsd_main_path:
        try:
            v = CBAMXSDValidator(xsd_main_path)
            validation = v.validate_xml_bytes(xml_bytes)
        except Exception as e:
            validation = {"valid": False, "errors": [str(e)], "xsd_main": xsd_main_path}

    out_p = Path(output_zip_path)
    out_p.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(out_p, "w") as zf:
        _zip_write_bytes(zf, xml_filename, xml_bytes)

        for ap in cleaned:
            p = Path(ap)
            _zip_write_bytes(zf, p.name, p.read_bytes())

        # add a small deterministic manifest for audit
        manifest = {
            "schema": "cbam_portal_zip_v1",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "xml_filename": xml_filename,
            "xml_sha256": sha256(xml_bytes).hexdigest(),
            "portal_meta": asdict(portal_meta),
            "attachments": [{"name": Path(a).name, "sha256": sha256(Path(a).read_bytes()).hexdigest()} for a in cleaned],
            "xsd_validation": validation,
        }
        _zip_write_bytes(zf, "manifest.json", json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8"))

    return manifest
