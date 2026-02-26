from __future__ import annotations

from typing import Any

from sqlalchemy import select

from src.db.models import Company, Facility, Project
from src.db.session import db


def _get(obj: Any, key: str, default=None):
    """
    Hem dict hem ORM objeleri destekler
    """
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

        company = Company(
            name="Demo Company"
        )

        s.add(company)
        s.commit()
        s.refresh(company)

        return company


def require_company_id(user: Any) -> int:

    cid = _get(user, "company_id")

    if cid:
        return int(cid)

    raise ValueError("Bu kullanıcı bir şirkete bağlı değil.")


def list_companies_for_user(user: Any) -> list[Company]:

    with db() as s:

        # consultant tüm şirketleri görür
        if _is_consultant(user):

            companies = s.execute(
                select(Company).order_by(Company.name)
            ).scalars().all()

            if not companies:
                companies = [ensure_demo_company()]

            return companies

        # client user
        cid = require_company_id(user)

        company = s.get(Company, cid)

        if company:
            return [company]

        company = ensure_demo_company()

        return [company]


def list_facilities(company_id: int) -> list[Facility]:

    with db() as s:

        facilities = s.execute(
            select(Facility).where(Facility.company_id == company_id)
        ).scalars().all()

        return facilities


def create_facility(company_id: int, name: str) -> Facility:

    facility = Facility(
        company_id=company_id,
        name=name
    )

    with db() as s:
        s.add(facility)
        s.commit()
        s.refresh(facility)

    return facility


def list_projects(company_id: int) -> list[Project]:

    with db() as s:

        projects = s.execute(
            select(Project).where(Project.company_id == company_id)
        ).scalars().all()

        return projects


def create_project(company_id: int, facility_id: int | None, name: str, year: int) -> Project:

    project = Project(
        company_id=company_id,
        facility_id=facility_id,
        name=name,
        year=year
    )

    with db() as s:
        s.add(project)
        s.commit()
        s.refresh(project)

    return project
