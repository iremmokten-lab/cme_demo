from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from src.db.models import EmissionFactor, FactorSet
from src.db.session import db
from src.mrv.lineage import sha256_json


def list_factor_sets(project_id: int, region: str | None = None) -> list[FactorSet]:
    with db() as s:
        q = select(FactorSet).where(FactorSet.project_id == int(project_id))
        if region:
            q = q.where(FactorSet.region == str(region))
        q = q.order_by(FactorSet.created_at.desc())
        return s.execute(q).scalars().all()


def get_factor_set(project_id: int, factor_set_id: int) -> FactorSet | None:
    with db() as s:
        fs = s.get(FactorSet, int(factor_set_id))
        if not fs:
            return None
        if int(fs.project_id) != int(project_id):
            return None
        return fs


def create_factor_set(
    *,
    project_id: int,
    name: str,
    region: str = "TR",
    year: int | None = None,
    version: str = "v1",
    meta: dict | None = None,
    created_by_user_id: int | None = None,
) -> FactorSet:
    with db() as s:
        fs = FactorSet(
            project_id=int(project_id),
            name=str(name or "Factor Set"),
            region=str(region or "TR"),
            year=int(year) if year is not None else None,
            version=str(version or "v1"),
            meta_json=json.dumps(meta or {}, ensure_ascii=False),
            created_by_user_id=int(created_by_user_id) if created_by_user_id is not None else None,
            locked=False,
        )
        s.add(fs)
        s.commit()
        s.refresh(fs)
        return fs


def add_emission_factor(
    *,
    project_id: int,
    factor_set_id: int | None,
    factor_type: str,
    value: float,
    unit: str = "",
    region: str = "TR",
    year: int | None = None,
    version: str = "v1",
    source: str = "",
    reference: str = "",
    meta: dict | None = None,
    created_by_user_id: int | None = None,
) -> EmissionFactor:
    with db() as s:
        ef = EmissionFactor(
            project_id=int(project_id),
            factor_set_id=int(factor_set_id) if factor_set_id is not None else None,
            factor_type=str(factor_type),
            value=float(value or 0.0),
            unit=str(unit or ""),
            region=str(region or "TR"),
            year=int(year) if year is not None else None,
            version=str(version or "v1"),
            source=str(source or ""),
            reference=str(reference or ""),
            meta_json=json.dumps(meta or {}, ensure_ascii=False),
            created_by_user_id=int(created_by_user_id) if created_by_user_id is not None else None,
        )
        s.add(ef)
        s.commit()
        s.refresh(ef)
        return ef


def list_factors(project_id: int, factor_set_id: int | None = None, region: str | None = None) -> list[dict]:
    with db() as s:
        q = select(EmissionFactor).where(EmissionFactor.project_id == int(project_id))
        if factor_set_id is not None:
            q = q.where(EmissionFactor.factor_set_id == int(factor_set_id))
        if region:
            q = q.where(EmissionFactor.region == str(region))
        rows = s.execute(q).scalars().all()

    out = []
    for r in rows:
        out.append(
            {
                "id": int(r.id),
                "factor_type": str(r.factor_type),
                "value": float(r.value),
                "unit": str(r.unit or ""),
                "region": str(r.region or ""),
                "year": int(r.year) if r.year is not None else None,
                "version": str(r.version or ""),
                "source": str(r.source or ""),
                "reference": str(r.reference or ""),
            }
        )
    out.sort(key=lambda x: (x["factor_type"], x["region"], x.get("year") or 0, x.get("version") or ""))
    return out


def factor_set_lock_payload(project_id: int, factor_set_id: int) -> dict:
    """
    Deterministic factor lock payload.
    Snapshot içine factor_set_ref olarak konabilir.
    """
    fs = get_factor_set(project_id, factor_set_id)
    if not fs:
        raise ValueError("Factor set bulunamadı.")
    factors = list_factors(project_id, factor_set_id=factor_set_id, region=str(fs.region or "TR"))
    payload = {
        "factor_set_id": int(fs.id),
        "name": str(fs.name or ""),
        "region": str(fs.region or ""),
        "year": int(fs.year) if fs.year is not None else None,
        "version": str(fs.version or ""),
        "meta": json.loads(fs.meta_json or "{}") if fs.meta_json else {},
        "factors": factors,
    }
    payload["factor_set_hash"] = sha256_json(payload)
    return payload
