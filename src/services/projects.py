from __future__ import annotations

from typing import Any

from sqlalchemy import select

from src.db.models import CalculationSnapshot, Company, Facility, Project, User
from src.db.session import db


def _get_attr_or_key(obj: Any, key: str, default=None):
    if obj is None:
        return default
    try:
        if hasattr(obj, key):
            return getattr(obj, key)
    except Exception:
        pass
    try:
        if isinstance(obj, dict):
            return obj.get(key, default)
    except Exception:
        pass
    return default


def _role(user: Any) -> str:
    try:
        return str(_get_attr_or_key(user, "role", "") or "").lower()
    except Exception:
        return ""


def _is_consultant(user: Any) -> bool:
    r = _role(user)
    return r.startswith("consultant") or r.startswith("consult")


def ensure_demo_company() -> Company:
    """
    DB'de en az 1 company olmasını garanti eder (demo/bootstrapping).
    Consultant panelin şirket seçimi için gereklidir.
    """
    with db() as s:
        c = s.execute(select(Company).order_by(Company.id.asc()).limit(1)).scalars().first()
        if c:
            return c
        c = Company(name="Demo Company")
        s.add(c)
        s.commit()
        s.refresh(c)
        return c


def require_company_id(user: Any) -> int:
    """
    Client kullanıcılar için company_id zorunlu.
    Consultant kullanıcılar için company_id gerekmez (çoklu şirket görür).
    """
    cid = _get_attr_or_key(user, "company_id", None)
    if cid is None:
        raise ValueError("Bu kullanıcı bir şirkete bağlı değil.")
    return int(cid)


def list_companies_for_user(user: Any) -> list[Company]:
    """
    Danışman için birden çok şirket olabilir (demo). Client için kendi company.
    UI contract: Company ORM objeleri döner (c.name, c.id).
    """
    with db() as s:
        if _is_consultant(user):
            companies = s.execute(select(Company).order_by(Company.name.asc())).scalars().all()
            if not companies:
                companies = [ensure_demo_company()]
            return companies

        cid = require_company_id(user)
        c = s.get(Company, int(cid))
        if c:
            return [c]

        # company_id var ama company yoksa (bozuk demo DB) - panel kırılmasın
        c = ensure_demo_company()
        try:
            if isinstance(user, User):
                user.company_id = c.id
                s.add(user)
                s.commit()
        except Exception:
            pass
        return [c]


def list_facilities(company_id: int) -> list[Facility]:
    with db() as s:
        return (
            s.execute(select(Facility).where(Facility.company_id == int(company_id)).order_by(Facility.name.asc()))
            .scalars()
            .all()
        )


def create_facility(company_id: int, name: str, country: str = "TR", sector: str = "") -> Facility:
    f = Facility(
        company_id=int(company_id),
        name=(name or "").strip(),
        country=(country or "TR").strip(),
        sector=(sector or "").strip(),
    )
    with db() as s:
        s.add(f)
        s.commit()
        s.refresh(f)
    return f


def list_projects(company_id: int) -> list[Project]:
    with db() as s:
        return (
            s.execute(
                select(Project)
                .where(Project.company_id == int(company_id))
                .order_by(Project.created_at.desc(), Project.id.desc())
            )
            .scalars()
            .all()
        )


def create_project(company_id: int, facility_id: int | None, name: str, year: int) -> Project:
    p = Project(
        company_id=int(company_id),
        facility_id=int(facility_id) if facility_id else None,
        name=(name or "").strip(),
        year=int(year),
    )
    with db() as s:
        s.add(p)
        s.commit()
        s.refresh(p)
    return p


def get_project_for_user(user: Any, project_id: int) -> Project | None:
    """
    Row-level security: client ise company filter zorunlu; consultant ise serbest.
    """
    with db() as s:
        p = s.get(Project, int(project_id))
        if not p:
            return None

        if _is_consultant(user):
            return p

        cid = require_company_id(user)
        if int(p.company_id) != int(cid):
            return None
        return p


def list_company_projects_for_user(user: Any) -> list[Project]:
    cid = require_company_id(user)
    with db() as s:
        return (
            s.execute(select(Project).where(Project.company_id == int(cid)).order_by(Project.created_at.desc()))
            .scalars()
            .all()
        )


def list_shared_snapshots_for_user(user: Any, *, limit: int = 200) -> list[CalculationSnapshot]:
    """
    Client portal: sadece shared_with_client olan snapshotlar ve company scope.
    """
    cid = require_company_id(user)
    with db() as s:
        proj_ids = s.execute(select(Project.id).where(Project.company_id == int(cid))).scalars().all()
        if not proj_ids:
            return []
        snaps = (
            s.execute(
                select(CalculationSnapshot)
                .where(
                    CalculationSnapshot.project_id.in_(proj_ids),
                    CalculationSnapshot.shared_with_client == True,  # noqa: E712
                )
                .order_by(CalculationSnapshot.created_at.desc())
                .limit(int(limit))
            )
            .scalars()
            .all()
        )
        return snaps


def list_snapshots_for_project(user: Any, project_id: int, *, limit: int = 200) -> list[CalculationSnapshot]:
    """
    Consultant veya client: client ise company filter zorunlu.
    """
    p = get_project_for_user(user, project_id)
    if not p:
        return []
    with db() as s:
        return (
            s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == int(project_id))
                .order_by(CalculationSnapshot.created_at.desc())
                .limit(int(limit))
            )
            .scalars()
            .all()
        )
