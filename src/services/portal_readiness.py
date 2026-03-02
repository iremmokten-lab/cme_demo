from __future__ import annotations

import hashlib
import io
import json
import zipfile
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class PortalReadinessResult:
    ok: bool
    score: int
    errors: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]
    meta: Dict[str, Any]


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def validate_portal_zip_structure(zip_bytes: bytes) -> Tuple[bool, List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """Portal ZIP paketinin temel yapısını kontrol eder.
    Not: Gerçek portalın tüm kurallarını kopyalamaz; üretim için minimum riskleri yakalar.
    """
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    meta: Dict[str, Any] = {"zip_sha256": _sha256_bytes(zip_bytes)}

    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes), "r")
    except Exception as e:
        return False, [{"type": "ZIP", "message": f"ZIP açılamadı: {e}"}], [], meta

    names = [n for n in zf.namelist() if not n.endswith("/") and not n.startswith("__MACOSX")]
    meta["file_count"] = len(names)

    # Heuristics: must contain at least one XML and a manifest-like file
    xmls = [n for n in names if n.lower().endswith(".xml")]
    manifests = [n for n in names if "manifest" in n.lower() and n.lower().endswith(".json")]
    if not xmls:
        errors.append({"type": "STRUCTURE", "message": "ZIP içinde XML bulunamadı."})
    if not manifests:
        warnings.append({"type": "STRUCTURE", "message": "Manifest JSON bulunamadı (önerilir)."})
    if len(xmls) > 1:
        warnings.append({"type": "STRUCTURE", "message": "Birden fazla XML bulundu; portal tek XML bekleyebilir."})

    # Check size limits (soft)
    total_size = 0
    for n in names:
        try:
            total_size += zf.getinfo(n).file_size
        except Exception:
            pass
    meta["total_bytes"] = total_size
    if total_size > 50 * 1024 * 1024:
        warnings.append({"type": "SIZE", "message": "ZIP boyutu çok büyük (>50MB). Portal yükleme sorun çıkarabilir."})

    ok = len(errors) == 0
    return ok, errors, warnings, meta


def compute_readiness_score(xsd_ok: bool, structure_ok: bool, error_count: int, warning_count: int) -> int:
    score = 100
    if not xsd_ok:
        score -= 60
    if not structure_ok:
        score -= 30
    score -= min(20, error_count * 10)
    score -= min(10, warning_count * 2)
    return max(0, min(100, score))
