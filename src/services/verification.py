from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from src.db.session import db
from src.db.models import Facility, Project, User, VerificationCase, VerificationFinding
from src.mrv.audit import append_audit, infer_company_id_for_user
from src.services.projects import get_project_for_user, list_company_projects_for_user, require_company_id


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _is_consultant(user: User) -> bool:
    return str(getattr(user, "role", "") or "").lower().startswith("consult")


def _safe_int(x: Any, default: int | None = None) -> int | None:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _safe_str(x: Any, default: str = "") -> str:
    try:
        s = str(x)
        return s
    except Exception:
        return default


def list_cases_for_user(user: User, *, project_id: int | None = None, limit: int = 200) -> List[VerificationCase]:
    """RLS:
    - Consultant: kendi company’si altındaki tüm project’lerdeki caseleri görebilir.
    - Client: sadece kendi company’si altındaki project’lerdeki caseleri görebilir; read-only.
    """
    cid = require_company_id(user)

    with db() as s:
        proj_ids = s.execute(select(Project.id).where(Project.company_id == int(cid))).scalars().all()
        if not proj_ids:
            return []

        q = select(VerificationCase).where(VerificationCase.project_id.in_(proj_ids))
        if project_id is not None:
            q = q.where(VerificationCase.project_id == int(project_id))

        return (
            s.execute(q.order_by(VerificationCase.created_at.desc()).limit(int(limit)))
            .scalars()
            .all()
        )


def read_case_for_user(user: User, case_id: int) -> VerificationCase | None:
    cid = require_company_id(user)
    with db() as s:
        c = s.get(VerificationCase, int(case_id))
        if not c:
            return None
        p = s.get(Project, int(c.project_id))
        if not p or int(p.company_id) != int(cid):
            return None
        # findings eager load (lazy relationship ok)
        _ = list(getattr(c, "findings", []) or [])
        return c


def create_case(
    user: User,
    *,
    project_id: int,
    facility_id: int | None,
    period_year: int,
    verifier_org: str,
) -> VerificationCase:
    if not _is_consultant(user):
        raise PermissionError("Sadece danışman kullanıcılar verification case oluşturabilir.")

    p = get_project_for_user(user, int(project_id))
    if not p:
        raise PermissionError("Projeye erişim yok.")

    with db() as s:
        c = VerificationCase(
            project_id=int(project_id),
            facility_id=_safe_int(facility_id),
            period_year=int(period_year),
            verifier_org=_safe_str(verifier_org),
            status="planning",
            created_by_user_id=_safe_int(getattr(user, "id", None)),
        )
        s.add(c)
        s.commit()
        s.refresh(c)

    append_audit(
        "case_created",
        {
            "case_id": getattr(c, "id", None),
            "project_id": int(project_id),
            "facility_id": _safe_int(facility_id),
            "period_year": int(period_year),
            "verifier_org": _safe_str(verifier_org),
            "created_at": _utcnow_iso(),
        },
        user_id=_safe_int(getattr(user, "id", None)),
        company_id=infer_company_id_for_user(user),
        entity_type="verification_case",
        entity_id=_safe_int(getattr(c, "id", None)),
    )

    return c


def add_finding(
    user: User,
    *,
    case_id: int,
    severity: str,
    description: str,
    corrective_action: str,
    due_date: str,
    status: str = "open",
) -> VerificationFinding:
    if not _is_consultant(user):
        raise PermissionError("Sadece danışman kullanıcılar bulgu ekleyebilir.")

    c = read_case_for_user(user, int(case_id))
    if not c:
        raise PermissionError("Case bulunamadı veya erişim yok.")

    sev = _safe_str(severity).lower().strip() or "minor"
    if sev not in ("minor", "major", "critical"):
        sev = "minor"

    st = _safe_str(status).lower().strip() or "open"
    if st not in ("open", "in_progress", "closed"):
        st = "open"

    with db() as s:
        f = VerificationFinding(
            case_id=int(case_id),
            severity=sev,
            description=_safe_str(description),
            corrective_action=_safe_str(corrective_action),
            due_date=_safe_str(due_date),
            status=st,
        )
        s.add(f)
        s.commit()
        s.refresh(f)

    append_audit(
        "finding_added",
        {
            "case_id": int(case_id),
            "finding_id": getattr(f, "id", None),
            "severity": sev,
            "status": st,
            "due_date": _safe_str(due_date),
            "created_at": _utcnow_iso(),
        },
        user_id=_safe_int(getattr(user, "id", None)),
        company_id=infer_company_id_for_user(user),
        entity_type="verification_finding",
        entity_id=_safe_int(getattr(f, "id", None)),
    )

    return f


def close_finding(user: User, *, finding_id: int) -> VerificationFinding:
    if not _is_consultant(user):
        raise PermissionError("Sadece danışman kullanıcılar bulgu kapatabilir.")

    cid = require_company_id(user)
    with db() as s:
        f = s.get(VerificationFinding, int(finding_id))
        if not f:
            raise ValueError("Bulgu bulunamadı.")
        c = s.get(VerificationCase, int(f.case_id))
        if not c:
            raise ValueError("Case bulunamadı.")
        p = s.get(Project, int(c.project_id))
        if not p or int(p.company_id) != int(cid):
            raise PermissionError("Erişim yok.")

        f.status = "closed"
        f.closed_at = datetime.now(timezone.utc)
        s.add(f)
        s.commit()
        s.refresh(f)

    append_audit(
        "finding_closed",
        {
            "case_id": int(getattr(c, "id", 0) or 0),
            "finding_id": int(getattr(f, "id", 0) or 0),
            "closed_at": _utcnow_iso(),
        },
        user_id=_safe_int(getattr(user, "id", None)),
        company_id=infer_company_id_for_user(user),
        entity_type="verification_finding",
        entity_id=_safe_int(getattr(f, "id", None)),
    )

    return f


def update_case_status(user: User, *, case_id: int, status: str) -> VerificationCase:
    if not _is_consultant(user):
        raise PermissionError("Sadece danışman kullanıcılar case durumunu değiştirebilir.")

    c = read_case_for_user(user, int(case_id))
    if not c:
        raise PermissionError("Case bulunamadı veya erişim yok.")

    st = _safe_str(status).lower().strip() or "planning"
    if st not in ("planning", "fieldwork", "findings", "closed"):
        st = "planning"

    with db() as s:
        c_db = s.get(VerificationCase, int(case_id))
        if not c_db:
            raise ValueError("Case bulunamadı.")
        c_db.status = st
        if st == "closed":
            c_db.closed_at = datetime.now(timezone.utc)
        s.add(c_db)
        s.commit()
        s.refresh(c_db)
        c = c_db

    append_audit(
        "case_closed" if st == "closed" else "case_status_changed",
        {
            "case_id": int(case_id),
            "status": st,
            "updated_at": _utcnow_iso(),
        },
        user_id=_safe_int(getattr(user, "id", None)),
        company_id=infer_company_id_for_user(user),
        entity_type="verification_case",
        entity_id=_safe_int(getattr(c, "id", None)),
    )

    return c


def case_to_json(case: VerificationCase) -> Dict[str, Any]:
    findings = []
    try:
        findings = list(getattr(case, "findings", []) or [])
    except Exception:
        findings = []

    return {
        "id": int(getattr(case, "id", 0) or 0),
        "project_id": int(getattr(case, "project_id", 0) or 0),
        "facility_id": getattr(case, "facility_id", None),
        "period_year": int(getattr(case, "period_year", 0) or 0),
        "verifier_org": str(getattr(case, "verifier_org", "") or ""),
        "status": str(getattr(case, "status", "") or ""),
        "created_at": (getattr(case, "created_at", None).isoformat() if getattr(case, "created_at", None) else None),
        "created_by_user_id": getattr(case, "created_by_user_id", None),
        "closed_at": (getattr(case, "closed_at", None).isoformat() if getattr(case, "closed_at", None) else None),
        "findings": [
            {
                "id": int(getattr(f, "id", 0) or 0),
                "severity": str(getattr(f, "severity", "") or ""),
                "description": str(getattr(f, "description", "") or ""),
                "corrective_action": str(getattr(f, "corrective_action", "") or ""),
                "due_date": str(getattr(f, "due_date", "") or ""),
                "status": str(getattr(f, "status", "") or ""),
                "created_at": (getattr(f, "created_at", None).isoformat() if getattr(f, "created_at", None) else None),
                "closed_at": (getattr(f, "closed_at", None).isoformat() if getattr(f, "closed_at", None) else None),
            }
            for f in sorted(findings, key=lambda x: (str(getattr(x, "status", "")), str(getattr(x, "severity", "")), int(getattr(x, "id", 0) or 0)))
        ],
    }


def list_cases_for_snapshot_payload(user: User, *, project_id: int, period_year: int) -> Dict[str, Any]:
    """Evidence pack için: snapshot’ın project+period’ına uyan caseleri döndürür."""
    cases = list_cases_for_user(user, project_id=int(project_id), limit=500)
    filtered = [c for c in cases if int(getattr(c, "period_year", 0) or 0) == int(period_year)]

    # Facility isimleri için
    facility_names: Dict[int, str] = {}
    try:
        with db() as s:
            fac_ids = {int(getattr(c, "facility_id", 0) or 0) for c in filtered if getattr(c, "facility_id", None)}
            if fac_ids:
                facs = s.execute(select(Facility).where(Facility.id.in_(list(fac_ids)))).scalars().all()
                for f in facs:
                    facility_names[int(f.id)] = str(f.name)
    except Exception:
        facility_names = {}

    payload = {
        "period_year": int(period_year),
        "project_id": int(project_id),
        "cases": [],
    }

    for c in sorted(filtered, key=lambda x: (str(getattr(x, "status", "")), int(getattr(x, "id", 0) or 0))):
        cj = case_to_json(c)
        fid = cj.get("facility_id")
        if fid is not None:
            try:
                cj["facility_name"] = facility_names.get(int(fid), "")
            except Exception:
                cj["facility_name"] = ""
        payload["cases"].append(cj)

    return payload
