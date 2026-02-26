from __future__ import annotations

from typing import Any

from sqlalchemy import select

from src.db.models import Company, Facility, Project
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


def list_companies_for_user(user: Any):

    with db() as s:

        if _is_consultant(user):

            companies = s.execute(select(Company)).scalars().all()

            if not companies:
                companies = [ensure_demo_company()]

            return companies

        cid = _get(user, "company_id")

        if not cid:
            raise ValueError("User company yok")

        company = s.get(Company, cid)

        return [company]


def list_facilities(company_id: int):

    with db() as s:

        return s.execute(
            select(Facility).where(Facility.company_id == company_id)
        ).scalars().all()


def list_projects(company_id: int):

    with db() as s:

        return s.execute(
            select(Project).where(Project.company_id == company_id)
        ).scalars().all()
