from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import select

from src.db.session import db
from src.db.models import EmissionFactor, FactorSet
from src.mrv.lineage import sha256_json


def lock_factor_set(factor_set_id: int) -> None:
    with db() as s:
        fs = s.get(FactorSet, int(factor_set_id))
        if not fs:
            raise ValueError("Factor set bulunamadı.")
        fs.is_locked = True
        s.commit()


def factor_set_ref(project_id: int, factor_set_id: int | None) -> list[dict]:
    if not factor_set_id:
        return []
    with db() as s:
        fs = s.get(FactorSet, int(factor_set_id))
        if not fs or int(fs.project_id) != int(project_id):
            return []
        # include factors snapshot-like reference
        factors = (
            s.execute(select(EmissionFactor).where(EmissionFactor.project_id == int(project_id), EmissionFactor.factor_set_id == int(factor_set_id)))
            .scalars()
            .all()
        )
        payload = [
            {
                "factor_id": int(f.id),
                "name": str(f.name),
                "value": float(f.value),
                "unit": str(f.unit),
                "source": str(f.source),
                "valid_from": str(f.valid_from) if getattr(f, "valid_from", None) else None,
                "valid_to": str(f.valid_to) if getattr(f, "valid_to", None) else None,
            }
            for f in factors
        ]
        return [
            {
                "factor_set_id": int(factor_set_id),
                "name": str(getattr(fs, "name", "") or ""),
                "locked": bool(getattr(fs, "is_locked", False)),
                "hash": sha256_json(payload),
                "count": len(payload),
            }
        ]
