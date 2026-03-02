from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from src.services.cbam_xsd_validator import CBAMXSDValidator

@dataclass
class PortalSimResult:
    ok: bool
    errors: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]

def simulate_portal_acceptance(xml_bytes: bytes, *, xsd_validator: CBAMXSDValidator) -> PortalSimResult:
    ok, errs = xsd_validator.validate_bytes(xml_bytes)
    if not ok:
        return PortalSimResult(ok=False, errors=[{"type":"XSD","message":str(e)} for e in errs], warnings=[])
    return PortalSimResult(ok=True, errors=[], warnings=[])
