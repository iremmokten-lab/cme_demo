from __future__ import annotations

"""Projects/Facilities service contract.

Bu modül, UI katmanının çağırdığı fonksiyonların tek kaynağıdır.
Amaç:
- Danışman: şirket içindeki tüm veriler (tam erişim)
- Müşteri / Verifier: sadece kendi company kapsamı + snapshot tarafında RLS (shared_with_client)

Not: Streamlit Cloud + SQLite için yazılmıştır.
"""

from typing import Any, List

from sqlalchemy import desc, select

from src.db.models import CalculationSnapshot, Company, Facility, Project
from src.db.session import db


def _get(obj: Any, key: str, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _role(user: Any) -> str:
    return str(_get(user, "role", "") or "").lower().strip()


def is_consultant(user: Any) -> bool:
    return _role(user).startswith("consult")


def is_verifier(user: Any) -> bool:
    return _role(user).startswith("verifier")


def is_client(user: Any) -> bool:
    r = _role(user)
    return (not r.startswith("consult")) and (not r.startswith("verifier"))


def ensure_demo_company() -> Company:
    with db() as s:
        company = s.execute(select(Company).order_by(Company.id).limit(1)).scalars().first()
        if company:
            return company
        company = Company(name="Demo Company")
        s.add(company)
        s.commit()
        s.refresh(company)
        return company


def require_company_id(user: Any) -> int:
    cid = _get(user, "company_id")
    if cid is None:
        c = ensure_demo_company()
        return int(c.id)
    return int(cid)


def list_companies_for_user(user: Any) -> List[Company]:
    with db() as s:
        if is_consultant(user):
            companies = s.execute(select(Company).order_by(Company.id)).scalars().all()
            return companies or [ensure_demo_company()]

        cid = require_company_id(user)
        company = s.get(Company, int(cid))
        return [company] if company else [ensure_demo_company()]


def create_facility(company_id: int, name: str, country_code: str = "TR", sector: str = "") -> Facility:
    nm = (name or "").strip()
    if not nm:
        raise ValueError("Tesis adı boş olamaz.")
    cc = (country_code or "TR").strip().upper()[:2]
    ss = (sector or "").strip()

    with db() as s:
        fac = Facility(company_id=int(company_id), name=nm, country_code=cc, sector=ss)
        s.add(fac)
        s.commit()
        s.refresh(fac)
        return fac


def update_facility(
    company_id: int,
    facility_id: int,
    *,
    name: str | None = None,
    country_code: str | None = None,
    sector: str | None = None,
) -> Facility:
    with db() as s:
        fac = s.get(Facility, int(facility_id))
        if not fac or int(fac.company_id) != int(company_id):
            raise PermissionError("Tesis bulunamadı veya erişim yok.")
        if name is not None:
            fac.name = (name or "").strip() or fac.name
        if country_code is not None:
            fac.country_code = (country_code or fac.country_code or "TR").strip().upper()[:2]
        if sector is not None:
            fac.sector = (sector or "").strip()
        s.add(fac)
        s.commit()
        s.refresh(fac)
        return fac


def get_facility(company_id: int, facility_id: int) -> Facility | None:
    with db() as s:
        fac = s.get(Facility, int(facility_id))
        if not fac:
            return None
        if int(fac.company_id) != int(company_id):
            return None
        return fac


def list_facilities(company_id: int) -> List[Facility]:
    with db() as s:
        return (
            s.execute(select(Facility).where(Facility.company_id == int(company_id)).order_by(Facility.id.desc()))
            .scalars()
            .all()
        )


def create_project(company_id: int, facility_id: int | None, name: str, description: str = "") -> Project:
    nm = (name or "").strip()
    if not nm:
        raise ValueError("Proje adı boş olamaz.")
    desc_txt = (description or "").strip()

    with db() as s:
        if facility_id is not None:
            fac = s.get(Facility, int(facility_id))
            if not fac or int(fac.company_id) != int(company_id):
                raise ValueError("Facility bulunamadı veya bu şirkete ait değil.")
        p = Project(
            company_id=int(company_id),
            facility_id=int(facility_id) if facility_id is not None else None,
            name=nm,
            description=desc_txt,
        )
        s.add(p)
        s.commit()
        s.refresh(p)
        return p


def update_project(
    company_id: int,
    project_id: int,
    *,
    name: str | None = None,
    description: str | None = None,
    facility_id: int | None = None,
) -> Project:
    with db() as s:
        p = s.get(Project, int(project_id))
        if not p or int(p.company_id) != int(company_id):
            raise PermissionError("Proje bulunamadı veya erişim yok.")
        if name is not None:
            p.name = (name or "").strip() or p.name
        if description is not None:
            p.description = (description or "").strip()
        if facility_id is not None:
            if int(facility_id) == 0:
                p.facility_id = None
            else:
                fac = s.get(Facility, int(facility_id))
                if not fac or int(fac.company_id) != int(company_id):
                    raise ValueError("Facility bulunamadı veya bu şirkete ait değil.")
                p.facility_id = int(facility_id)
        s.add(p)
        s.commit()
        s.refresh(p)
        return p


def get_project_for_user(user: Any, project_id: int) -> Project | None:
    cid = require_company_id(user)
    with db() as s:
        p = s.get(Project, int(project_id))
        if not p:
            return None
        if int(p.company_id) != int(cid):
            return None
        return p


def list_projects(company_id: int) -> List[Project]:
    with db() as s:
        return (
            s.execute(select(Project).where(Project.company_id == int(company_id)).order_by(Project.id.desc()))
            .scalars()
            .all()
        )


def list_company_projects_for_user(user: Any) -> List[Project]:
    cid = require_company_id(user)
    return list_projects(int(cid))


def list_snapshots_for_user(user: Any, *, project_id: int | None = None, limit: int = 200) -> List[CalculationSnapshot]:
    """RLS:
    - Consultant: company içindeki tüm snapshot'lar
    - Client / Verifier: sadece shared_with_client=True snapshot'lar
    """

    cid = require_company_id(user)
    with db() as s:
        proj_ids = s.execute(select(Project.id).where(Project.company_id == int(cid))).scalars().all()
        if not proj_ids:
            return []

        q = select(CalculationSnapshot).where(CalculationSnapshot.project_id.in_(proj_ids))
        if project_id is not None:
            q = q.where(CalculationSnapshot.project_id == int(project_id))
        if not is_consultant(user):
            q = q.where(CalculationSnapshot.shared_with_client == True)  # noqa: E712

        return s.execute(q.order_by(desc(CalculationSnapshot.created_at)).limit(int(limit))).scalars().all()


def list_shared_snapshots_for_user(user: Any, limit: int = 200) -> List[CalculationSnapshot]:
    return list_snapshots_for_user(user, project_id=None, limit=int(limit))
