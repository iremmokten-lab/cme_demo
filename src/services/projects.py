from __future__ import annotations

from typing import Any, List, Optional

from sqlalchemy import select

from src.db.models import CalculationSnapshot, Company, Facility, Project, User
from src.db.session import db


def _get(obj: Any, key: str, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _is_consultant(user: Any) -> bool:
    role = str(_get(user, "role", "")).lower()
    return role.startswith("consult")


def ensure_demo_company() -> Company:
    with db() as s:
        company = s.execute(select(Company)).scalars().first()
        if company:
            return company

        company = Company(name="Demo Company")
        s.add(company)
        s.commit()
        s.refresh(company)
        return company


def require_company_id(user: Any) -> int:
    """User.company_id zorunlu kısıt.

    - Streamlit demo modunda bazı kullanıcılar company_id olmadan oluşmuş olabilir.
      Bu durumda demo company oluşturup bağlarız (consultant için).
    """
    cid = _get(user, "company_id")
    if cid:
        return int(cid)

    if _is_consultant(user):
        c = ensure_demo_company()
        # Best-effort: user tablosuna yaz (varsa)
        uid = _get(user, "id")
        if uid:
            try:
                with db() as s:
                    u = s.get(User, int(uid))
                    if u and not u.company_id:
                        u.company_id = int(c.id)
                        s.add(u)
                        s.commit()
            except Exception:
                pass
        return int(c.id)

    raise PermissionError("Kullanıcı company_id bulunamadı.")


def list_companies_for_user(user: Any) -> List[Company]:
    with db() as s:
        if _is_consultant(user):
            companies = s.execute(select(Company)).scalars().all()
            if not companies:
                companies = [ensure_demo_company()]
            return companies

        cid = require_company_id(user)
        company = s.get(Company, int(cid))
        return [company] if company else []


def list_facilities(company_id: int) -> List[Facility]:
    with db() as s:
        return (
            s.execute(select(Facility).where(Facility.company_id == int(company_id)).order_by(Facility.name.asc()))
            .scalars()
            .all()
        )


def list_projects(company_id: int) -> List[Project]:
    with db() as s:
        return (
            s.execute(select(Project).where(Project.company_id == int(company_id)).order_by(Project.created_at.desc()))
            .scalars()
            .all()
        )


def list_company_projects_for_user(user: Any, *, limit: int = 500) -> List[Project]:
    """RLS: user'ın company_id'si altındaki projeler."""
    cid = require_company_id(user)
    with db() as s:
        return (
            s.execute(
                select(Project)
                .where(Project.company_id == int(cid))
                .order_by(Project.year.desc(), Project.created_at.desc())
                .limit(int(limit))
            )
            .scalars()
            .all()
        )


def get_project_for_user(user: Any, project_id: int) -> Optional[Project]:
    """RLS: consultant tüm projeleri görebilir; client sadece kendi company' si."""
    with db() as s:
        p = s.get(Project, int(project_id))
        if not p:
            return None
        if _is_consultant(user):
            return p
        cid = require_company_id(user)
        if int(getattr(p, "company_id", 0) or 0) != int(cid):
            return None
        return p


def create_facility(company_id: int, name: str, country: str = "TR", sector: str = "") -> Facility:
    """Consultant panelinde kullanılan tesis oluşturma."""
    nm = str(name or "").strip()
    if not nm:
        raise ValueError("Tesis adı boş olamaz.")

    cc = str(country or "TR").strip() or "TR"
    sec = str(sector or "").strip()

    with db() as s:
        c = s.get(Company, int(company_id))
        if not c:
            raise ValueError("Company bulunamadı.")

        f = Facility(company_id=int(company_id), name=nm, country=cc, sector=sec)
        s.add(f)
        s.commit()
        s.refresh(f)
        return f


def create_project(company_id: int, facility_id: int | None, name: str, year: int) -> Project:
    """Consultant panelinde kullanılan proje oluşturma."""
    nm = str(name or "").strip()
    if not nm:
        raise ValueError("Proje adı boş olamaz.")

    with db() as s:
        c = s.get(Company, int(company_id))
        if not c:
            raise ValueError("Company bulunamadı.")

        fid = int(facility_id) if facility_id else None
        if fid:
            f = s.get(Facility, int(fid))
            if not f or int(getattr(f, "company_id", 0) or 0) != int(company_id):
                raise ValueError("Seçilen tesis bu şirkete ait değil.")

        p = Project(company_id=int(company_id), facility_id=fid, name=nm, year=int(year))
        s.add(p)
        s.commit()
        s.refresh(p)
        return p


def list_snapshots_for_project(user: Any, project_id: int, *, limit: int = 200) -> List[CalculationSnapshot]:
    """RLS:
    - Consultant: tüm snapshot'lar
    - Client: sadece shared_with_client=True
    """
    p = get_project_for_user(user, int(project_id))
    if not p:
        return []

    with db() as s:
        q = select(CalculationSnapshot).where(CalculationSnapshot.project_id == int(project_id))
        if not _is_consultant(user):
            q = q.where(CalculationSnapshot.shared_with_client.is_(True))
        return s.execute(q.order_by(CalculationSnapshot.created_at.desc()).limit(int(limit))).scalars().all()


def list_shared_snapshots_for_user(user: Any, *, limit: int = 200) -> List[CalculationSnapshot]:
    """Client dashboard için: sadece paylaşılan snapshot'lar (shared_with_client=True)."""
    cid = require_company_id(user)

    with db() as s:
        proj_ids = s.execute(select(Project.id).where(Project.company_id == int(cid))).scalars().all()
        if not proj_ids:
            return []

        q = (
            select(CalculationSnapshot)
            .where(
                CalculationSnapshot.project_id.in_(proj_ids),
                CalculationSnapshot.shared_with_client.is_(True),
            )
            .order_by(CalculationSnapshot.created_at.desc())
            .limit(int(limit))
        )
        return s.execute(q).scalars().all()
