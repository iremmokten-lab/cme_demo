from __future__ import annotations

from datetime import datetime, timezone

from src.db.models import FactorSet
from src.db.session import db


def lock_factor_set(factor_set_id: int, user_id: int | None = None) -> FactorSet:
    with db() as s:
        fs = s.get(FactorSet, int(factor_set_id))
        if not fs:
            raise ValueError("Factor set bulunamadı.")
        if fs.locked:
            return fs
        fs.locked = True
        fs.locked_at = datetime.now(timezone.utc)
        fs.locked_by_user_id = int(user_id) if user_id is not None else None
        s.commit()
        s.refresh(fs)
        return fs


def ensure_factor_set_not_locked(fs: FactorSet):
    if fs.locked:
        raise ValueError("Bu factor set kilitli. Değiştirilemez.")
