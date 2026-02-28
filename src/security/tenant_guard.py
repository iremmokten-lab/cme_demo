from __future__ import annotations

from src.db.session import db
from src.db.models import Project, CalculationSnapshot


def assert_project_company(project_id: int, company_id: int) -> None:
    with db() as s:
        p = s.get(Project, int(project_id))
        if not p:
            raise ValueError("Project bulunamadı.")
        if int(p.company_id) != int(company_id):
            raise PermissionError("Tenant ihlali: project bu şirkete ait değil.")


def assert_snapshot_company(snapshot_id: int, company_id: int) -> None:
    with db() as s:
        snap = s.get(CalculationSnapshot, int(snapshot_id))
        if not snap:
            raise ValueError("Snapshot bulunamadı.")
        p = s.get(Project, int(snap.project_id))
        if not p or int(p.company_id) != int(company_id):
            raise PermissionError("Tenant ihlali: snapshot erişimi engellendi.")
