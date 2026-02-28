from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select

from src.db.models import CalculationSnapshot
from src.db.session import db


def lock_snapshot(snapshot_id: int, user_id: int | None = None) -> CalculationSnapshot:
    """Snapshot immutable: kilitle."""
    with db() as s:
        snap = s.get(CalculationSnapshot, int(snapshot_id))
        if not snap:
            raise ValueError("Snapshot bulunamadı.")
        if snap.locked:
            return snap

        snap.locked = True
        snap.locked_at = datetime.now(timezone.utc)
        snap.locked_by_user_id = int(user_id) if user_id is not None else None

        s.commit()
        s.refresh(snap)
        return snap


def ensure_not_locked(snapshot: CalculationSnapshot):
    if snapshot.locked:
        raise ValueError("Bu snapshot kilitli. Değiştirilemez / silinemez.")


def delete_snapshot(snapshot_id: int):
    """Kilitli snapshot silinemez."""
    with db() as s:
        snap = s.get(CalculationSnapshot, int(snapshot_id))
        if not snap:
            return
        if snap.locked:
            raise ValueError("Kilitli snapshot silinemez.")
        s.delete(snap)
        s.commit()


def list_snapshots(project_id: int):
    with db() as s:
        return (
            s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == int(project_id))
                .order_by(CalculationSnapshot.created_at.desc())
            )
            .scalars()
            .all()
        )


def set_shared_with_client(snapshot_id: int, shared: bool) -> CalculationSnapshot:
    with db() as s:
        snap = s.get(CalculationSnapshot, int(snapshot_id))
        if not snap:
            raise ValueError("Snapshot bulunamadı.")
        snap.shared_with_client = bool(shared)
        s.commit()
        s.refresh(snap)
        return snap
