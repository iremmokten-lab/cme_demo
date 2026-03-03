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


def _safe_auditevent_kwargs(**kwargs: Any) -> dict[str, Any]:
    """AuditEvent model şemasındaki farklılıklara karşı dayanıklı kwargs filtresi.

    Deploy/commit farklarında AuditEvent alanları değişmiş olabilir.
    SQLAlchemy declarative constructor, modelde olmayan keyword arg gelirse TypeError atar.
    Bu fonksiyon kwargs'i modelde gerçekten var olan alanlarla sınırlar ve çöküşü engeller.
    """
    try:
        allowed = set(AuditEvent.__mapper__.attrs.keys())
    except Exception:
        # Mapper erişilemezse en güvenli dar set
        allowed = {"event_type", "details_json", "user_id", "company_id", "entity_type", "entity_id"}
    return {k: v for k, v in kwargs.items() if k in allowed}


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

    Önemli: Audit logging hiçbir zaman uygulamanın ana akışını (login vb.) durdurmamalı.
    """
    kwargs = _safe_auditevent_kwargs(
        event_type=str(event_type),
        details_json=_safe_json(details),
        user_id=int(user_id) if user_id is not None else None,
        company_id=int(company_id) if company_id is not None else None,
        entity_type=str(entity_type or ""),
        entity_id=int(entity_id) if entity_id is not None else None,
    )

    try:
        ev = AuditEvent(**kwargs)
    except TypeError:
        # Son çare: minimum alanlarla dene
        ev = AuditEvent(
            **_safe_auditevent_kwargs(
                event_type=str(event_type),
                details_json=_safe_json(details),
            )
        )

    try:
        with db() as s:
            s.add(ev)
            s.commit()
    except Exception:
        # Audit logging hiçbir zaman ana akışı durdurmamalı
        return


def infer_company_id_for_user(user: User | None) -> int | None:
    if not user:
        return None
    try:
        return int(getattr(user, "company_id", None)) if getattr(user, "company_id", None) is not None else None
    except Exception:
        return None


def infer_company_id_for_project(project: Project | None) -> int | None:
    if not project:
        return None
    try:
        return int(getattr(project, "company_id", None)) if getattr(project, "company_id", None) is not None else None
    except Exception:
        return None


def infer_company_id_for_snapshot(snapshot: CalculationSnapshot | None) -> int | None:
    if not snapshot:
        return None
    try:
        return int(getattr(snapshot, "company_id", None)) if getattr(snapshot, "company_id", None) is not None else None
    except Exception:
        return None
