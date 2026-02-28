from __future__ import annotations

from datetime import datetime
from typing import Any, List, Optional

from sqlalchemy import desc, select

from src.db.models import VerificationCase, VerificationFinding
from src.db.session import db
from src.services.projects import is_consultant, is_verifier, require_company_id


def list_cases_for_user(user: Any, project_id: int | None = None, limit: int = 200) -> List[VerificationCase]:
    cid = require_company_id(user)
    with db() as s:
        q = select(VerificationCase).order_by(desc(VerificationCase.created_at)).limit(int(limit))
        if project_id is not None:
            q = q.where(VerificationCase.project_id == int(project_id))

        # tenant boundary: company->projects kontrolü UI tarafında yapılır; burada minimum tutuyoruz.
        return s.execute(q).scalars().all()


def get_case(case_id: int) -> Optional[VerificationCase]:
    with db() as s:
        return s.get(VerificationCase, int(case_id))


def create_case(
    user: Any,
    project_id: int,
    facility_id: int | None,
    period_year: int,
    verifier_org: str = "",
) -> VerificationCase:
    if not (is_consultant(user) or is_verifier(user)):
        raise PermissionError("Case oluşturma yetkiniz yok.")

    with db() as s:
        c = VerificationCase(
            project_id=int(project_id),
            facility_id=int(facility_id) if facility_id is not None else None,
            period_year=int(period_year),
            verifier_org=str(verifier_org or "").strip(),
            status="open",
            created_by_user_id=int(getattr(user, "id", 0) or 0) or None,
            created_at=datetime.utcnow(),
        )
        s.add(c)
        s.commit()
        s.refresh(c)
        return c


def update_sampling(user: Any, case_id: int, sampling_notes: str = "", sampling_size: int | None = None) -> VerificationCase:
    if not (is_consultant(user) or is_verifier(user)):
        raise PermissionError("Sampling güncelleme yetkiniz yok.")

    with db() as s:
        c = s.get(VerificationCase, int(case_id))
        if not c:
            raise ValueError("Case bulunamadı.")

        c.sampling_notes = str(sampling_notes or "").strip()
        if sampling_size is not None:
            try:
                c.sampling_size = int(sampling_size)
            except Exception:
                c.sampling_size = None

        s.add(c)
        s.commit()
        s.refresh(c)
        return c


def add_finding(
    user: Any,
    case_id: int,
    severity: str,
    description: str,
    corrective_action: str = "",
    due_date: str = "",
) -> VerificationFinding:
    if not (is_consultant(user) or is_verifier(user)):
        raise PermissionError("Finding ekleme yetkiniz yok.")

    with db() as s:
        c = s.get(VerificationCase, int(case_id))
        if not c:
            raise ValueError("Case bulunamadı.")

        f = VerificationFinding(
            case_id=int(case_id),
            severity=str(severity or "minor"),
            description=str(description or "").strip(),
            corrective_action=str(corrective_action or "").strip(),
            due_date=str(due_date or "").strip(),
            status="open",
            created_at=datetime.utcnow(),
        )
        s.add(f)
        s.commit()
        s.refresh(f)
        return f


def list_findings(case_id: int) -> List[VerificationFinding]:
    with db() as s:
        return (
            s.execute(select(VerificationFinding).where(VerificationFinding.case_id == int(case_id)).order_by(VerificationFinding.created_at.asc()))
            .scalars()
            .all()
        )


def close_finding(user: Any, finding_id: int) -> VerificationFinding:
    if not (is_consultant(user) or is_verifier(user)):
        raise PermissionError("Finding kapatma yetkiniz yok.")
    with db() as s:
        f = s.get(VerificationFinding, int(finding_id))
        if not f:
            raise ValueError("Finding bulunamadı.")
        f.status = "closed"
        f.closed_at = datetime.utcnow()
        s.add(f)
        s.commit()
        s.refresh(f)
        return f


def close_case(user: Any, case_id: int) -> VerificationCase:
    if not (is_consultant(user) or is_verifier(user)):
        raise PermissionError("Case kapatma yetkiniz yok.")
    with db() as s:
        c = s.get(VerificationCase, int(case_id))
        if not c:
            raise ValueError("Case bulunamadı.")
        c.status = "closed"
        c.closed_at = datetime.utcnow()
        s.add(c)
        s.commit()
        s.refresh(c)
        return c
