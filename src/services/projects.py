from __future__ import annotations

"""Projects/Facilities service layer (UI contract stabilization).

Bu modül, Streamlit UI katmanının çağırdığı fonksiyonları tek yerde toplar.
Amaç (FAZ 0):
- Consultant Panel akışını kırmadan Facility/Project CRUD (minimum)
- Client portal RLS: sadece shared_with_client=True snapshot listesi
- Compliance checklist + verification workflow ekranlarının ihtiyaç duyduğu yardımcılar

Not: RLS bu demo repo'da uygulama katmanında uygulanır (SQLite).
"""

from typing import Any, List, Optional

from sqlalchemy import select

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


def _is_consultant(user: Any) -> bool:
    return _role(user).startswith("consult")


def require_company_id(user: Any) -> int:
    cid = _get(user, "company_id")
    if cid is None:
        raise ValueError("Kullanıcı company_id bulunamadı.")
    try:
        return int(cid)
    except Exception:
        raise ValueError("Kullanıcı company_id geçersiz.")


def ensure_demo_company() -> Company:
    """Sistemde hiç şirket yoksa Demo Company oluşturur (bootstrap)."""
    with db() as s:
        company = s.execute(select(Company).order_by(Company.id).limit(1)).scalars().first()
        if company:
            return company

        company = Company(name="Demo Company")
        s.add(company)
        s.commit()
        s.refresh(company)
        return company


def list_companies_for_user(user: Any) -> List[Company]:
    """Consultant: tüm şirketler. Client: sadece kendi şirketi."""
    with db() as s:
        if _is_consultant(user):
            companies = s.execute(select(Company).order_by(Company.name)).scalars().all()
            if not companies:
                companies = [ensure_demo_company()]
            return companies

        cid = require_company_id(user)
        company = s.get(Company, cid)
        return [company] if company else []


def list_facilities(company_id: int) -> List[Facility]:
    with db() as s:
        return (
            s.execute(select(Facility).where(Facility.company_id == int(company_id)).order_by(Facility.name))
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


def list_company_projects_for_user(user: Any) -> List[Project]:
    """Compliance checklist / verification UI için.

    RLS:
    - Consultant: kendi company_id altındaki projeler.
    - Client: kendi company_id altındaki projeler.
    """
    cid = require_company_id(user)
    return list_projects(cid)


def create_facility(company_id: int, name: str, country: str = "TR", sector: str = "") -> Facility:
    name = str(name or "").strip()
    if not name:
        raise ValueError("Facility adı zorunludur.")

    country = str(country or "TR").strip() or "TR"
    sector = str(sector or "").strip()

    with db() as s:
        fac = Facility(company_id=int(company_id), name=name, country=country, sector=sector)
        s.add(fac)
        s.commit()
        s.refresh(fac)
        return fac


def create_project(company_id: int, facility_id: Optional[int], name: str, year: int) -> Project:
    name = str(name or "").strip()
    if not name:
        raise ValueError("Proje adı zorunludur.")

    try:
        year_i = int(year)
    except Exception:
        year_i = 2025

    with db() as s:
        # facility doğrulaması
        fac_id = None
        if facility_id is not None:
            try:
                fid = int(facility_id)
                if fid > 0:
                    fac = s.get(Facility, fid)
                    if not fac or int(fac.company_id) != int(company_id):
                        raise PermissionError("Bu facility bu şirkete ait değil.")
                    fac_id = fid
            except Exception:
                fac_id = None

        proj = Project(company_id=int(company_id), facility_id=fac_id, name=name, year=year_i)
        s.add(proj)
        s.commit()
        s.refresh(proj)
        return proj


def get_project_for_user(user: Any, project_id: int) -> Optional[Project]:
    """RLS kontrollü proje okuma."""
    cid = require_company_id(user)
    with db() as s:
        p = s.get(Project, int(project_id))
        if not p:
            return None
        if int(p.company_id) != int(cid):
            return None
        return p


def list_snapshots_for_project(user: Any, project_id: int, *, limit: int = 300) -> List[CalculationSnapshot]:
    """Consultant: projedeki tüm snapshot'lar.

    Client/verifier: sadece shared_with_client=True olanlar.
    """
    p = get_project_for_user(user, int(project_id))
    if not p:
        return []

    with db() as s:
        q = select(CalculationSnapshot).where(CalculationSnapshot.project_id == int(project_id))
        if not _is_consultant(user):
            q = q.where(CalculationSnapshot.shared_with_client == True)  # noqa: E712
        return (
            s.execute(q.order_by(CalculationSnapshot.created_at.desc()).limit(int(limit)))
            .scalars()
            .all()
        )


def list_shared_snapshots_for_user(user: Any, *, limit: int = 300) -> List[CalculationSnapshot]:
    """Client portal RLS (kritik): sadece shared_with_client=True snapshot'lar."""
    cid = require_company_id(user)
    with db() as s:
        proj_ids = s.execute(select(Project.id).where(Project.company_id == int(cid))).scalars().all()
        if not proj_ids:
            return []

        q = (
            select(CalculationSnapshot)
            .where(CalculationSnapshot.project_id.in_(proj_ids))
            .where(CalculationSnapshot.shared_with_client == True)  # noqa: E712
            .order_by(CalculationSnapshot.created_at.desc())
            .limit(int(limit))
        )
        return s.execute(q).scalars().all()


def set_snapshot_shared(user: Any, snapshot_id: int, shared: bool) -> CalculationSnapshot:
    """Consultant action: snapshot'ı müşteri ile paylaş (👁️)."""
    if not _is_consultant(user):
        raise PermissionError("Sadece danışman snapshot paylaşımını değiştirebilir.")

    cid = require_company_id(user)
    with db() as s:
        sn = s.get(CalculationSnapshot, int(snapshot_id))
        if not sn:
            raise ValueError("Snapshot bulunamadı.")
        p = s.get(Project, int(sn.project_id))
        if not p or int(p.company_id) != int(cid):
            raise PermissionError("Erişim yok.")

        sn.shared_with_client = bool(shared)
        s.add(sn)
        s.commit()
        s.refresh(sn)
        return sn


def set_snapshot_locked(user: Any, snapshot_id: int, locked: bool) -> CalculationSnapshot:
    """Consultant action: snapshot'ı kilitle/çöz (audit-ready)."""
    if not _is_consultant(user):
        raise PermissionError("Sadece danışman snapshot kilidini değiştirebilir.")

    from datetime import datetime, timezone

    cid = require_company_id(user)
    with db() as s:
        sn = s.get(CalculationSnapshot, int(snapshot_id))
        if not sn:
            raise ValueError("Snapshot bulunamadı.")
        p = s.get(Project, int(sn.project_id))
        if not p or int(p.company_id) != int(cid):
            raise PermissionError("Erişim yok.")

        sn.locked = bool(locked)
        sn.locked_at = datetime.now(timezone.utc) if locked else None
        try:
            sn.locked_by_user_id = int(_get(user, "id")) if locked and _get(user, "id") is not None else None
        except Exception:
            sn.locked_by_user_id = None

        s.add(sn)
        s.commit()
        s.refresh(sn)
        return sn
