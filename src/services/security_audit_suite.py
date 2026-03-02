from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class AuditCheck:
    code: str
    title: str
    ok: bool
    detail: str = ""


def build_security_audit_report(*, checks: List[AuditCheck], meta: Dict[str, Any] | None = None) -> Dict[str, Any]:
    meta = meta or {}
    status = "PASS" if all(c.ok for c in checks) else "FAIL"
    return {
        "status": status,
        "meta": meta,
        "checks": [
            {"code": c.code, "title": c.title, "ok": c.ok, "detail": c.detail}
            for c in checks
        ],
    }


def default_security_checks() -> List[AuditCheck]:
    """Repo'ya entegre edildiğinde gerçek DB/RLS testleri eklenebilir.
    Bu iskelet, denetim rapor formatını standardize eder.
    """
    return [
        AuditCheck(code="SEC-RBAC-01", title="Rol bazlı erişim kontrolü (RBAC) tanımlı", ok=True, detail="Uygulama rol sistemi mevcut varsayıldı."),
        AuditCheck(code="SEC-TENANT-01", title="Tenant izolasyonu (RLS) aktif", ok=True, detail="RLS mevcut varsayıldı; production'da otomatik test eklenmeli."),
        AuditCheck(code="SEC-LOG-01", title="Erişim audit logları tutuluyor", ok=True, detail="AccessAuditLog/benzeri kayıtlar mevcut olmalı."),
    ]
