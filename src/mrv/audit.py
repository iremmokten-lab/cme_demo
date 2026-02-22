from __future__ import annotations

import json
from typing import Any

from src.db.session import db
from src.db.models import AuditEvent, CalculationSnapshot, Project, User


def _safe_json(d: dict[str, Any] | None) -> str:
    try:
        return json.dumps(d or {}, ensure_ascii=False)
    except Exception:
        return "{}"


def append_audit(
    event_type: str,
    details: dict[str, Any] | None = None,
    *,
    user_id: int | None = None,
    company_id: int | None = None,
    entity_type: str = "",
    entity_id: int | None = None,
) -> None:
    """Audit log kaydı yazar.

    Örnek event_type:
      - login_success
      - login_failed
      - login_blocked_locked
      - logout
      - page_viewed
      - report_exported
      - evidence_exported
      - snapshot_viewed
      - snapshot_compare_viewed
    """
    ev = AuditEvent(
        event_type=str(event_type),
        details_json=_safe_json(details),
        user_id=int(user_id) if user_id is not None else None,
        company_id=int(company_id) if company_id is not None else None,
        entity_type=str(entity_type or ""),
        entity_id=int(entity_id) if entity_id is not None else None,
    )
    try:
        with db() as s:
            s.add(ev)
            s.commit()
    except Exception:
        # Audit kritik değil: uygulama akışını bozmasın
        return


def infer_company_id_for_user(user: User | None) -> int | None:
    try:
        if user and getattr(user, "company_id", None) is not None:
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
