from __future__ import annotations

from typing import Any

from sqlalchemy import select

from src.db.models import Project, Facility, CalculationSnapshot, DatasetUpload, EvidenceDocument, Report, VerificationCase
from src.db.session import db


def assert_project_belongs_to_company(project_id: int, company_id: int) -> bool:
    with db() as s:
        p = s.get(Project, int(project_id))
        if not p:
            raise ValueError("Project bulunamadı.")
        if int(p.company_id) != int(company_id):
            raise PermissionError("Tenant ihlali: bu project bu company'ye ait değil.")
        return True


def assert_snapshot_belongs_to_company(snapshot_id: int, company_id: int) -> bool:
    with db() as s:
        snap = s.get(CalculationSnapshot, int(snapshot_id))
        if not snap:
            raise ValueError("Snapshot bulunamadı.")
        p = s.get(Project, int(snap.project_id))
        if not p or int(p.company_id) != int(company_id):
            raise PermissionError("Tenant ihlali: snapshot erişimi engellendi.")
        return True


def tenant_filter_project(query, company_id: int):
    """SQLAlchemy select(...) için company filtresi."""
    return query.where(Project.company_id == int(company_id))


def require_company_id(user: Any) -> int:
    cid = getattr(user, "company_id", None)
    if cid is None:
        raise PermissionError("Kullanıcının company_id alanı yok.")
    return int(cid)
