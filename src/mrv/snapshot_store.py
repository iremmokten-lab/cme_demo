from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import select

from src.db.models import CalculationSnapshot
from src.db.session import db
from src.mrv.lineage import sha256_json


def _safe_json_loads(s: str, default):
    try:
        return json.loads(s or "")
    except Exception:
        return default


@dataclass
class SnapshotRecord:
    id: int
    project_id: int
    created_at: str
    engine_version: str
    input_hash: str
    result_hash: str
    locked: bool
    shared_with_client: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "project_id": self.project_id,
            "created_at": self.created_at,
            "engine_version": self.engine_version,
            "input_hash": self.input_hash,
            "result_hash": self.result_hash,
            "locked": self.locked,
            "shared_with_client": self.shared_with_client,
        }


def get_snapshot(snapshot_id: int) -> CalculationSnapshot | None:
    with db() as s:
        return s.get(CalculationSnapshot, int(snapshot_id))


def list_snapshots(project_id: int) -> list[SnapshotRecord]:
    with db() as s:
        rows = (
            s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == int(project_id))
                .order_by(CalculationSnapshot.created_at.desc())
            )
            .scalars()
            .all()
        )

    out: list[SnapshotRecord] = []
    for r in rows:
        out.append(
            SnapshotRecord(
                id=int(r.id),
                project_id=int(r.project_id),
                created_at=str(r.created_at),
                engine_version=str(r.engine_version or ""),
                input_hash=str(r.input_hash or ""),
                result_hash=str(r.result_hash or ""),
                locked=bool(r.locked),
                shared_with_client=bool(r.shared_with_client),
            )
        )
    return out


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
    previous_snapshot_hash: str | None = None,
    created_by_user_id: int | None = None,
    shared_with_client: bool = False,
    lock_after_create: bool = False,
) -> CalculationSnapshot:
    with db() as s:
        snap = CalculationSnapshot(
            project_id=int(project_id),
            engine_version=str(engine_version or ""),
            config_json=json.dumps(config or {}, ensure_ascii=False),
            input_hashes_json=json.dumps(input_hashes or {}, ensure_ascii=False),
            results_json=json.dumps(results or {}, ensure_ascii=False),
            methodology_id=int(methodology_id) if methodology_id is not None else None,
            factor_set_id=int(factor_set_id) if factor_set_id is not None else None,
            monitoring_plan_id=int(monitoring_plan_id) if monitoring_plan_id is not None else None,
            input_hash=str(input_hash or ""),
            result_hash=str(result_hash or ""),
            previous_snapshot_hash=str(previous_snapshot_hash) if previous_snapshot_hash else None,
            created_by_user_id=int(created_by_user_id) if created_by_user_id is not None else None,
            shared_with_client=bool(shared_with_client),
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


def set_shared_with_client(snapshot_id: int, shared: bool) -> CalculationSnapshot:
    with db() as s:
        snap = s.get(CalculationSnapshot, int(snapshot_id))
        if not snap:
            raise ValueError("Snapshot bulunamadı.")
        snap.shared_with_client = bool(shared)
        s.commit()
        s.refresh(snap)
        return snap


def delete_snapshot(snapshot_id: int):
    with db() as s:
        snap = s.get(CalculationSnapshot, int(snapshot_id))
        if not snap:
            return
        if snap.locked:
            raise ValueError("Kilitli snapshot silinemez.")
        s.delete(snap)
        s.commit()


def snapshot_payload(snapshot_id: int) -> Dict[str, Any]:
    """Snapshot içeriğini (config+input_hashes+results) dict olarak verir."""
    snap = get_snapshot(snapshot_id)
    if not snap:
        raise ValueError("Snapshot bulunamadı.")
    return {
        "snapshot_id": int(snap.id),
        "project_id": int(snap.project_id),
        "engine_version": str(snap.engine_version or ""),
        "created_at": str(snap.created_at),
        "config": _safe_json_loads(snap.config_json, {}),
        "input_hashes": _safe_json_loads(snap.input_hashes_json, {}),
        "results": _safe_json_loads(snap.results_json, {}),
        "input_hash": str(snap.input_hash or ""),
        "result_hash": str(snap.result_hash or ""),
        "locked": bool(snap.locked),
        "shared_with_client": bool(snap.shared_with_client),
    }
