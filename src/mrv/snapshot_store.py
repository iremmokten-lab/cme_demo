from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import select

from src.db.session import db
from src.db.models import CalculationSnapshot
from src.mrv.lineage import sha256_json


def _safe_load(s: str | None, default):
    try:
        return json.loads(s or "")
    except Exception:
        return default


def compute_input_hash(
    *,
    engine_version: str,
    config: dict,
    input_hashes: dict,
    scenario: dict,
    methodology_id: int | None,
    factor_set_ref: list[dict] | None,
    monitoring_plan_ref: dict | None,
) -> str:
    """
    Deterministic input-hash:
    Aynı config + input dataset hashleri + senaryo + metodoloji + factor lock => aynı input_hash.
    """
    payload = {
        "engine_version": str(engine_version or ""),
        "config": config or {},
        "input_hashes": input_hashes or {},
        "scenario": scenario or {},
        "methodology_id": int(methodology_id) if methodology_id is not None else None,
        "factor_set_ref": factor_set_ref or [],
        "monitoring_plan_ref": monitoring_plan_ref or None,
    }
    return sha256_json(payload)


def _previous_snapshot_hash(project_id: int) -> str | None:
    with db() as s:
        prev = (
            s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == int(project_id))
                .order_by(CalculationSnapshot.created_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        if not prev:
            return None
        return (str(prev.result_hash or "") or None)


def save_snapshot(
    *,
    project_id: int,
    engine_version: str,
    config: dict,
    input_hashes: dict,
    results: dict,
    input_hash: str,
    result_hash: str,
    methodology_id: int | None = None,
    factor_set_id: int | None = None,
    monitoring_plan_id: int | None = None,
    created_by_user_id: int | None = None,
    shared_with_client: bool = False,
    lock_after_create: bool = False,
) -> CalculationSnapshot:
    """
    DB'ye snapshot kaydeder. lock_after_create=True ise immutable kilitler.
    """
    prev_hash = _previous_snapshot_hash(project_id)

    with db() as s:
        snap = CalculationSnapshot(
            project_id=int(project_id),
            engine_version=str(engine_version or "engine-0.0.0"),
            config_json=json.dumps(config or {}, ensure_ascii=False),
            input_hashes_json=json.dumps(input_hashes or {}, ensure_ascii=False),
            results_json=json.dumps(results or {}, ensure_ascii=False),
            methodology_id=int(methodology_id) if methodology_id is not None else None,
            factor_set_id=int(factor_set_id) if factor_set_id is not None else None,
            monitoring_plan_id=int(monitoring_plan_id) if monitoring_plan_id is not None else None,
            input_hash=str(input_hash or ""),
            result_hash=str(result_hash or ""),
            previous_snapshot_hash=prev_hash,
            created_by_user_id=int(created_by_user_id) if created_by_user_id is not None else None,
            shared_with_client=bool(shared_with_client),
            locked=False,
        )
        s.add(snap)
        s.commit()
        s.refresh(snap)

        if lock_after_create:
            snap.locked = True
            snap.locked_at = datetime.now(timezone.utc)
            snap.locked_by_user_id = int(created_by_user_id) if created_by_user_id is not None else None
            s.commit()
            s.refresh(snap)

        return snap


def get_snapshot(snapshot_id: int) -> CalculationSnapshot | None:
    with db() as s:
        return s.get(CalculationSnapshot, int(snapshot_id))


def snapshot_payload(snapshot_id: int) -> Dict[str, Any]:
    snap = get_snapshot(snapshot_id)
    if not snap:
        raise ValueError("Snapshot bulunamadı.")
    return {
        "snapshot_id": int(snap.id),
        "project_id": int(snap.project_id),
        "created_at": str(snap.created_at),
        "engine_version": str(snap.engine_version or ""),
        "config": _safe_load(snap.config_json, {}),
        "input_hashes": _safe_load(snap.input_hashes_json, {}),
        "results": _safe_load(snap.results_json, {}),
        "input_hash": str(snap.input_hash or ""),
        "result_hash": str(snap.result_hash or ""),
        "locked": bool(snap.locked),
        "shared_with_client": bool(snap.shared_with_client),
    }
