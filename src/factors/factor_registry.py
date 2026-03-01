from __future__ import annotations

import json
from datetime import date
from typing import Any

from sqlalchemy import select

from src.db.session import db
from src.db.models import EmissionFactor, FactorSet, SnapshotFactorLink
from src.mrv.lineage import sha256_json


def _factor_payload_for_hash(
    *,
    factor_type: str,
    region: str,
    year: int | None,
    version: str,
    value: float,
    unit: str,
    source: str,
    reference: str,
    methodology: str,
    valid_from: str,
    valid_to: str,
    meta: dict | None,
) -> dict:
    return {
        "factor_type": str(factor_type or ""),
        "region": str(region or "TR"),
        "year": int(year) if year is not None else None,
        "version": str(version or "v1"),
        "value": float(value or 0.0),
        "unit": str(unit or ""),
        "source": str(source or ""),
        "reference": str(reference or ""),
        "methodology": str(methodology or ""),
        "valid_from": str(valid_from or ""),
        "valid_to": str(valid_to or ""),
        "meta": meta or {},
    }


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
            name=str(name),
            region=str(region),
            year=int(year) if year is not None else None,
            version=str(version),
            meta_json=json.dumps(meta or {}, ensure_ascii=False),
            created_by_user_id=int(created_by_user_id) if created_by_user_id is not None else None,
            locked=False,
        )
        s.add(fs)
        s.commit()
        s.refresh(fs)
        return fs


def add_factor(
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
    methodology: str = "",
    valid_from: str = "",
    valid_to: str = "",
    meta: dict | None = None,
    created_by_user_id: int | None = None,
) -> EmissionFactor:
    """Yeni emisyon faktörü ekle (governance + deterministik hash).

    Notlar:
    - `factor_hash` canonical payload üzerinden sha256 ile doldurulur.
    - locked=True ise daha sonra güncellenmemelidir (UI/API + DB trigger ile korunur).
    """
    payload = _factor_payload_for_hash(
        factor_type=factor_type,
        region=region,
        year=year,
        version=version,
        value=value,
        unit=unit,
        source=source,
        reference=reference,
        methodology=methodology,
        valid_from=valid_from,
        valid_to=valid_to,
        meta=meta,
    )
    fhash = sha256_json(payload)

    with db() as s:
        ef = EmissionFactor(
            project_id=int(project_id),
            factor_set_id=int(factor_set_id) if factor_set_id is not None else None,
            factor_type=str(factor_type),
            value=float(value),
            unit=str(unit or ""),
            region=str(region),
            year=int(year) if year is not None else None,
            version=str(version),
            source=str(source or ""),
            reference=str(reference or ""),
            methodology=str(methodology or ""),
            valid_from=str(valid_from or ""),
            valid_to=str(valid_to or ""),
            locked=False,
            factor_hash=str(fhash),
            meta_json=json.dumps(meta or {}, ensure_ascii=False),
            created_by_user_id=int(created_by_user_id) if created_by_user_id is not None else None,
        )
        s.add(ef)
        s.commit()
        s.refresh(ef)
        return ef


def lock_factor(factor_id: int) -> EmissionFactor:
    """Faktörü kilitle (immutability)."""
    with db() as s:
        ef = s.get(EmissionFactor, int(factor_id))
        if not ef:
            raise ValueError("Faktör bulunamadı.")
        ef.locked = True
        s.commit()
        s.refresh(ef)
        return ef


def list_factors(*, project_id: int, factor_set_id: int | None = None) -> list[dict]:
    with db() as s:
        q = select(EmissionFactor).where(EmissionFactor.project_id == int(project_id))
        if factor_set_id is not None:
            q = q.where(EmissionFactor.factor_set_id == int(factor_set_id))
        rows = s.execute(q).scalars().all()

    out = []
    for r in rows:
        out.append(
            {
                "id": int(r.id),
                "factor_type": str(r.factor_type),
                "value": float(r.value or 0.0),
                "unit": str(r.unit or ""),
                "region": str(r.region or ""),
                "year": int(r.year) if r.year is not None else None,
                "version": str(r.version or ""),
                "source": str(r.source or ""),
                "reference": str(getattr(r, "reference", "") or ""),
                "methodology": str(getattr(r, "methodology", "") or ""),
                "valid_from": str(getattr(r, "valid_from", "") or ""),
                "valid_to": str(getattr(r, "valid_to", "") or ""),
                "locked": bool(getattr(r, "locked", False)),
                "factor_hash": str(getattr(r, "factor_hash", "") or ""),
            }
        )
    out.sort(key=lambda x: (x.get("factor_type", ""), x.get("region", ""), str(x.get("year") or ""), x.get("version", "")))
    return out


def used_in_snapshots(factor_id: int) -> list[dict]:
    """Bu faktör hangi snapshot'larda kullanıldı?"""
    with db() as s:
        links = (
            s.execute(
                select(SnapshotFactorLink)
                .where(SnapshotFactorLink.factor_id == int(factor_id))
                .order_by(SnapshotFactorLink.created_at.desc())
            )
            .scalars()
            .all()
        )
    return [
        {
            "snapshot_id": int(l.snapshot_id),
            "factor_id": int(l.factor_id),
            "factor_type": str(l.factor_type or ""),
            "region": str(l.region or ""),
            "year": int(l.year) if l.year is not None else None,
            "version": str(l.version or ""),
            "factor_hash": str(l.factor_hash or ""),
            "linked_at": str(getattr(l, "created_at", None)) if getattr(l, "created_at", None) else None,
        }
        for l in links
    ]
