from __future__ import annotations

from sqlalchemy import select

from src.db.models import Company, Facility, Project, User, CalculationSnapshot
from src.db.session import db


def require_company_id(user: User) -> int:
    cid = getattr(user, "company_id", None)
    if cid is None:
        raise ValueError("Bu kullanıcı bir şirkete bağlı değil.")
    return int(cid)


def list_companies_for_user(user: User):
    """Danışman için birden çok şirket olabilir (demo). Client için kendi company."""
    try:
        role = (getattr(user, "role", "") or "").lower()
    except Exception:
        role = ""

    with db() as s:
        if role.startswith("consult"):
            return s.execute(select(Company).order_by(Company.name.asc())).scalars().all()
        cid = require_company_id(user)
        c = s.get(Company, cid)
        return [c] if c else []


def list_facilities(company_id: int):
    with db() as s:
        return s.execute(select(Facility).where(Facility.company_id == int(company_id)).order_by(Facility.name.asc())).scalars().all()


def create_facility(company_id: int, name: str, country: str = "TR", sector: str = ""):
    f = Facility(company_id=int(company_id), name=(name or "").strip(), country=(country or "TR").strip(), sector=(sector or "").strip())
    with db() as s:
        s.add(f)
        s.commit()
        s.refresh(f)
    return f


def list_projects(company_id: int):
    with db() as s:
        return (
            s.execute(select(Project).where(Project.company_id == int(company_id)).order_by(Project.created_at.desc(), Project.id.desc()))
            .scalars()
            .all()
        )


def create_project(company_id: int, facility_id: int | None, name: str, year: int):
    p = Project(company_id=int(company_id), facility_id=int(facility_id) if facility_id else None, name=(name or "").strip(), year=int(year))
    with db() as s:
        s.add(p)
        s.commit()
        s.refresh(p)
    return p


def get_project_for_user(user: User, project_id: int) -> Project | None:
    """Row-level security: company_id filter."""
    cid = require_company_id(user)
    with db() as s:
        p = s.get(Project, int(project_id))
        if not p:
            return None
        if int(p.company_id) != int(cid) and not (getattr(user, "role", "") or "").lower().startswith("consult"):
            return None
        return p


def list_company_projects_for_user(user: User):
    cid = require_company_id(user)
    with db() as s:
        return s.execute(select(Project).where(Project.company_id == int(cid)).order_by(Project.created_at.desc())).scalars().all()


def list_shared_snapshots_for_user(user: User, *, limit: int = 200):
    """Client portal: sadece shared_with_client olan snapshotlar ve company scope."""
    cid = require_company_id(user)
    with db() as s:
        # join yerine iki aşamalı: projects ids
        proj_ids = s.execute(select(Project.id).where(Project.company_id == int(cid))).scalars().all()
        if not proj_ids:
            return []
        snaps = (
            s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id.in_(proj_ids), CalculationSnapshot.shared_with_client == True)
                .order_by(CalculationSnapshot.created_at.desc())
                .limit(int(limit))
            )
            .scalars()
            .all()
        )
        return snaps


def list_snapshots_for_project(user: User, project_id: int, *, limit: int = 200):
    """Consultant veya client: client ise company filter zorunlu."""
    p = get_project_for_user(user, project_id)
    if not p:
        return []
    with db() as s:
        return (
            s.execute(select(CalculationSnapshot).where(CalculationSnapshot.project_id == int(project_id)).order_by(CalculationSnapshot.created_at.desc()).limit(int(limit)))
            .scalars()
            .all()
        )
