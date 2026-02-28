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
    return datetime.now(timezone.utc).isoformat()


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _safe_str(x: Any, default: str = "") -> str:
    try:
        return str(x)
    except Exception:
        return default


def _is_consultant(user) -> bool:
    return str(getattr(user, "role", "") or "").lower().startswith("consult")


def _is_verifier(user) -> bool:
    return str(getattr(user, "role", "") or "").lower().startswith("verifier")


def case_to_json(case: VerificationCase) -> Dict[str, Any]:
    return {
        "id": int(getattr(case, "id", 0) or 0),
        "project_id": int(getattr(case, "project_id", 0) or 0),
        "facility_id": (int(getattr(case, "facility_id", 0) or 0) if getattr(case, "facility_id", None) is not None else None),
        "period_year": int(getattr(case, "period_year", 0) or 0),
        "sampling_notes": str(getattr(case, "sampling_notes", "") or ""),
        "sampling_size": (int(getattr(case, "sampling_size", 0) or 0) if getattr(case, "sampling_size", None) is not None else None),
        "verifier_org": _safe_str(getattr(case, "verifier_org", "")),
        "status": _safe_str(getattr(case, "status", "")),
        "created_by_user_id": (int(getattr(case, "created_by_user_id", 0) or 0) if getattr(case, "created_by_user_id", None) is not None else None),
        "created_at": _safe_str(getattr(case, "created_at", "")),
        "closed_at": _safe_str(getattr(case, "closed_at", "")),
    }


def finding_to_json(f: VerificationFinding) -> Dict[str, Any]:
    return {
        "id": int(getattr(f, "id", 0) or 0),
        "case_id": int(getattr(f, "case_id", 0) or 0),
        "severity": _safe_str(getattr(f, "severity", "")),
        "description": _safe_str(getattr(f, "description", "")),
        "corrective_action": _safe_str(getattr(f, "corrective_action", "")),
        "due_date": _safe_str(getattr(f, "due_date", "")),
        "status": _safe_str(getattr(f, "status", "")),
        "created_at": _safe_str(getattr(f, "created_at", "")),
        "closed_at": _safe_str(getattr(f, "closed_at", "")),
    }


def list_cases_for_user(user: Any, project_id: Optional[int] = None, limit: int = 200) -> List[VerificationCase]:
    cid = require_company_id(user)

    # tenant boundary: sadece kullanıcının şirketindeki projeler üzerinden filtrele
    projects = list_company_projects_for_user(user)
    proj_ids = {int(p.id) for p in (projects or [])}

    with db() as s:
        q = select(VerificationCase)
        if project_id is not None:
            q = q.where(VerificationCase.project_id == int(project_id))
        else:
            if proj_ids:
                q = q.where(VerificationCase.project_id.in_(sorted(list(proj_ids))))
            else:
                return []

        q = q.order_by(VerificationCase.created_at.desc()).limit(int(limit))
        return s.execute(q).scalars().all()


def read_case_for_user(user: Any, case_id: int) -> Dict[str, Any]:
    cid = require_company_id(user)
    with db() as s:
        case = s.get(VerificationCase, int(case_id))
        if not case:
            raise ValueError("Case bulunamadı.")

        p = get_project_for_user(user, int(case.project_id))
        if not p:
            raise PermissionError("Bu case için erişim yok.")

        findings = (
            s.execute(select(VerificationFinding).where(VerificationFinding.case_id == int(case_id)).order_by(VerificationFinding.created_at.asc()))
            .scalars()
            .all()
        )

        payload = {
            "company_id": int(cid),
            "case": case_to_json(case),
            "findings": [finding_to_json(f) for f in findings],
        }

        append_audit(
            "verification_case_viewed",
            {"case_id": int(case_id)},
            user_id=int(getattr(user, "id", 0) or 0) or None,
            company_id=int(cid),
            entity_type="verification_case",
            entity_id=int(case_id),
        )

        return payload


def create_case(
    user: Any,
    project_id: int,
    facility_id: Optional[int],
    period_year: int,
    verifier_org: str = "",
) -> VerificationCase:
    cid = require_company_id(user)
    if not (_is_consultant(user) or _is_verifier(user)):
        raise PermissionError("Case oluşturma yetkiniz yok.")

    p = get_project_for_user(user, int(project_id))
    if not p:
        raise PermissionError("Bu proje için erişim yok.")

    with db() as s:
        case = VerificationCase(
            project_id=int(project_id),
            facility_id=int(facility_id) if facility_id is not None else None,
            period_year=int(period_year),
            verifier_org=_safe_str(verifier_org),
            status="open",
            created_by_user_id=int(getattr(user, "id", 0) or 0) or None,
            created_at=datetime.now(timezone.utc),
        )
        s.add(case)
        s.commit()
        s.refresh(case)

    append_audit(
        "verification_case_created",
        {"case_id": int(case.id), "project_id": int(project_id)},
        user_id=int(getattr(user, "id", 0) or 0) or None,
        company_id=int(cid),
        entity_type="verification_case",
        entity_id=int(case.id),
    )
    return case


def update_case_status(user: Any, case_id: int, status: str) -> VerificationCase:
    cid = require_company_id(user)
    if not (_is_consultant(user) or _is_verifier(user)):
        raise PermissionError("Case durum güncelleme yetkiniz yok.")

    with db() as s:
        case = s.get(VerificationCase, int(case_id))
        if not case:
            raise ValueError("Case bulunamadı.")
        p = get_project_for_user(user, int(case.project_id))
        if not p:
            raise PermissionError("Bu case için erişim yok.")

        case.status = _safe_str(status, "open")
        if case.status == "closed":
            case.closed_at = datetime.now(timezone.utc)
        s.add(case)
        s.commit()
        s.refresh(case)

    append_audit(
        "verification_case_status_updated",
        {"case_id": int(case_id), "status": case.status},
        user_id=int(getattr(user, "id", 0) or 0) or None,
        company_id=int(cid),
        entity_type="verification_case",
        entity_id=int(case_id),
    )
    return case


def update_case_sampling(user: Any, case_id: int, sampling_notes: str = "", sampling_size: Optional[int] = None) -> VerificationCase:
    """Faz 2: sampling notları / örneklem büyüklüğü."""
    cid = require_company_id(user)
    if not (_is_consultant(user) or _is_verifier(user)):
        raise PermissionError("Sampling güncelleme yetkiniz yok.")

    with db() as s:
        case = s.get(VerificationCase, int(case_id))
        if not case:
            raise ValueError("Case bulunamadı.")
        p = get_project_for_user(user, int(case.project_id))
        if not p:
            raise PermissionError("Bu case için erişim yok.")

        case.sampling_notes = _safe_str(sampling_notes, "")
        if sampling_size is None:
            case.sampling_size = None
        else:
            try:
                case.sampling_size = int(sampling_size)
            except Exception:
                case.sampling_size = None

        s.add(case)
        s.commit()
        s.refresh(case)

    append_audit(
        "verification_sampling_updated",
        {"case_id": int(case_id), "sampling_size": sampling_size},
        user_id=int(getattr(user, "id", 0) or 0) or None,
        company_id=int(cid),
        entity_type="verification_case",
        entity_id=int(case_id),
    )
    return case


def add_finding(
    user: Any,
    case_id: int,
    severity: str,
    description: str,
    corrective_action: str = "",
    due_date: str = "",
) -> VerificationFinding:
    cid = require_company_id(user)
    if not (_is_consultant(user) or _is_verifier(user)):
        raise PermissionError("Finding ekleme yetkiniz yok.")

    with db() as s:
        case = s.get(VerificationCase, int(case_id))
        if not case:
            raise ValueError("Case bulunamadı.")
        p = get_project_for_user(user, int(case.project_id))
        if not p:
            raise PermissionError("Bu case için erişim yok.")

        f = VerificationFinding(
            case_id=int(case_id),
            severity=_safe_str(severity, "minor"),
            description=_safe_str(description, ""),
            corrective_action=_safe_str(corrective_action, ""),
            due_date=_safe_str(due_date, ""),
            status="open",
            created_at=datetime.now(timezone.utc),
        )
        s.add(f)
        s.commit()
        s.refresh(f)

    append_audit(
        "verification_finding_added",
        {"case_id": int(case_id), "finding_id": int(f.id), "severity": f.severity},
        user_id=int(getattr(user, "id", 0) or 0) or None,
        company_id=int(cid),
        entity_type="verification_finding",
        entity_id=int(f.id),
    )
    return f


def close_finding(user: Any, finding_id: int) -> VerificationFinding:
    cid = require_company_id(user)
    if not (_is_consultant(user) or _is_verifier(user)):
        raise PermissionError("Finding kapatma yetkiniz yok.")

    with db() as s:
        f = s.get(VerificationFinding, int(finding_id))
        if not f:
            raise ValueError("Finding bulunamadı.")
        case = s.get(VerificationCase, int(f.case_id))
        if not case:
            raise ValueError("Case bulunamadı.")
        p = get_project_for_user(user, int(case.project_id))
        if not p:
            raise PermissionError("Bu finding için erişim yok.")

        f.status = "closed"
        f.closed_at = datetime.now(timezone.utc)
        s.add(f)
        s.commit()
        s.refresh(f)

    append_audit(
        "verification_finding_closed",
        {"finding_id": int(finding_id)},
        user_id=int(getattr(user, "id", 0) or 0) or None,
        company_id=int(cid),
        entity_type="verification_finding",
        entity_id=int(finding_id),
    )
    return f


def build_cases_payload(user: Any, period_year: int, project_id: int) -> Dict[str, Any]:
    """UI için özet payload."""
    cases = list_cases_for_user(user, project_id=project_id, limit=500)
    facility_ids: set[int] = set()
    for c in cases:
        fid = getattr(c, "facility_id", None)
        if fid is not None:
            try:
                facility_ids.add(int(fid))
            except Exception:
                pass

    facility_names: Dict[int, str] = {}
    if facility_ids:
        with db() as s:
            facs = s.execute(select(Facility).where(Facility.id.in_(sorted(list(facility_ids))))).scalars().all()
            for f in facs:
                facility_names[int(f.id)] = str(getattr(f, "name", "") or "")

    filtered = [c for c in cases if int(getattr(c, "period_year", 0) or 0) == int(period_year)]
    payload = {"period_year": int(period_year), "project_id": int(project_id), "cases": []}

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
