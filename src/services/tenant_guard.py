from __future__ import annotations

from src.db.models import CalculationSnapshot, Project, User


class AccessDenied(Exception):
    """Erişim kontrolü hatası (multi-tenant izolasyon)."""


def can_access_snapshot(*, user: User | None, project: Project | None, snapshot: CalculationSnapshot | None) -> bool:
    """Tenant isolation policy (minimum).

    Kurallar:
    - user.company_id ile project.company_id eşleşmeli
    - snapshot.project_id project.id olmalı
    - Snapshot shared_with_client ise (client/verifier roller için) erişim genişletilebilir;
      bu MVP'de sadece aynı tenant'a izin veriyoruz.
    """
    if not user or not project or not snapshot:
        return False
    try:
        uc = int(getattr(user, "company_id", 0) or 0)
        pc = int(getattr(project, "company_id", 0) or 0)
        if uc <= 0 or pc <= 0:
            return False
        if uc != pc:
            return False
        if int(getattr(snapshot, "project_id", 0) or 0) != int(getattr(project, "id", 0) or 0):
            return False
        return True
    except Exception:
        return False


def require_snapshot_access(*, user: User | None, project: Project | None, snapshot: CalculationSnapshot | None) -> None:
    if not can_access_snapshot(user=user, project=project, snapshot=snapshot):
        raise AccessDenied("Bu kayda erişim yetkiniz yok (tenant izolasyonu).")
