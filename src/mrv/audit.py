from __future__ import annotations

import json

from src.db.models import AuditEvent, CalculationSnapshot, Project, User
from src.db.session import db


def append_audit(
    event_type: str,
    details: dict | None = None,
    *,
    user_id: int | None = None,
    company_id: int | None = None,
    entity_type: str = "",
    entity_id: int | None = None,
):
    """Audit log.
    Örnek event_type:
      - report_viewed
      - evidence_exported
      - snapshot_viewed
      - snapshot_created
      - snapshot_reused
    """
    try:
        payload = json.dumps(details or {}, ensure_ascii=False)
    except Exception:
        payload = "{}"

    ev = AuditEvent(
        event_type=str(event_type),
        details_json=payload,
        user_id=int(user_id) if user_id is not None else None,
        company_id=int(company_id) if company_id is not None else None,
        entity_type=str(entity_type or ""),
        entity_id=int(entity_id) if entity_id is not None else None,
    )
    with db() as s:
        s.add(ev)
        s.commit()


def infer_company_id_for_user(user: User | None) -> int | None:
    try:
        if user and getattr(user, "company_id", None):
            return int(user.company_id)
    except Exception:
        pass
    return None


def infer_company_id_for_snapshot(snapshot_id: int) -> int | None:
    try:
        with db() as s:
            snap = s.get(CalculationSnapshot, int(snapshot_id))
            if not snap:
                return None
            proj = s.get(Project, int(snap.project_id))
            if not proj:
                return None
            return int(proj.company_id)
    except Exception:
        return None
