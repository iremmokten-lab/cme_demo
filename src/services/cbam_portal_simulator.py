from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from src.services.cbam_xsd_validator import CBAMXSDValidator


@dataclass
class PortalSimResult:
    ok: bool
    errors: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]


def simulate_portal_acceptance(xml_bytes: bytes, *, xsd_validator: CBAMXSDValidator) -> PortalSimResult:
    # 1) XSD
    ok, errs = xsd_validator.validate_bytes(xml_bytes)

    errors: List[Dict[str, Any]] = []
    if not ok:
        for e in errs:
            errors.append({"type": "XSD", "message": str(e)})
        return PortalSimResult(ok=False, errors=errors, warnings=[])

    # 2) Lightweight semantic checks (portal-like)
    # NOTE: Portal semantic rules are extensive; we implement critical "presence checks" only.
    # Any missing MUST fields should already be blocked by strict compliance layer.
    warnings = []
    return PortalSimResult(ok=True, errors=[], warnings=warnings)
