from __future__ import annotations

import json

from src.db.session import db
from src.db.production_step1_models import AccessAuditLog


def log_access(user, action: str, resource_type: str = "", resource_id: str = "", meta: dict | None = None, ip: str = "", user_agent: str = "") -> None:
    try:
        company_id = getattr(user, "company_id", None)
        user_id = getattr(user, "id", None)
        with db() as s:
            s.add(
                AccessAuditLog(
                    company_id=(int(company_id) if company_id else None),
                    user_id=(int(user_id) if user_id else None),
                    action=str(action),
                    resource=str(resource_type or ""),
                    ip=str(ip or ""),
                    user_agent=str(user_agent or ""),
                    meta_json=json.dumps({"resource_id": str(resource_id or ""), **(meta or {})}, ensure_ascii=False),
                )
            )
            s.commit()
    except Exception:
        return
