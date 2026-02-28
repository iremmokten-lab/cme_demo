from __future__ import annotations

import json
from typing import Any, Dict

import pandas as pd

from src.mrv.snapshot_store import compute_input_hash, save_snapshot
from src.mrv.lineage import sha256_json
from src.mrv.orchestrator import run_orchestrator
from src.services.workflow import latest_upload, load_csv_from_uri, _input_hashes_payload


def _safe_load(s: str | None, default):
    try:
        return json.loads(s or "")
    except Exception:
        return default


def run_calculation_and_snapshot(
    *,
    project_id: int,
    config: dict,
    scenario: dict | None = None,
    methodology_id: int | None = None,
    created_by_user_id: int | None = None,
    shared_with_client: bool = False,
    lock_after_create: bool = False,
) -> dict:
    """
    Tam audit-grade pipeline:
      - latest uploads (energy/production/materials) okunur
      - orchestrator çalışır
      - input_hash ve result_hash deterministik hesaplanır
      - DB snapshot kaydı oluşturulur
    """
    scenario = scenario or {}
    config = config or {}

    energy_u = latest_upload(project_id, "energy")
    prod_u = latest_upload(project_id, "production")
    mat_u = latest_upload(project_id, "materials")

    if not energy_u or not prod_u:
        raise ValueError("Hesaplama için energy + production dataset yüklenmiş olmalı.")

    energy_df = load_csv_from_uri(str(energy_u.storage_uri))
    prod_df = load_csv_from_uri(str(prod_u.storage_uri))
    materials_df = load_csv_from_uri(str(mat_u.storage_uri)) if mat_u and mat_u.storage_uri else None

    activity_snapshot_ref = _input_hashes_payload(project_id, energy_u, prod_u, mat_u)

    input_bundle, result_bundle, legacy = run_orchestrator(
        project_id=int(project_id),
        config=config,
        scenario=scenario,
        methodology_id=methodology_id,
        activity_snapshot_ref=activity_snapshot_ref,
        energy_df=energy_df,
        production_df=prod_df,
        materials_df=materials_df,
    )

    # legacy payload içerisinden lock referansları
    ib = (legacy or {}).get("input_bundle") or {}
    factor_set_ref = ib.get("factor_set_ref") or []
    monitoring_plan_ref = ib.get("monitoring_plan_ref") or None

    input_hash = compute_input_hash(
        engine_version=str((legacy or {}).get("engine_version") or "engine-0.0.0"),
        config=config,
        input_hashes=activity_snapshot_ref,
        scenario=scenario,
        methodology_id=methodology_id,
        factor_set_ref=factor_set_ref if isinstance(factor_set_ref, list) else [],
        monitoring_plan_ref=monitoring_plan_ref if isinstance(monitoring_plan_ref, dict) else None,
    )

    # result_hash: result_bundle varsa onu kullan, yoksa legacy üzerinden üret
    result_hash = ""
    try:
        result_hash = str(getattr(result_bundle, "result_hash", "") or "")
    except Exception:
        result_hash = ""

    if not result_hash:
        result_hash = sha256_json(legacy or {})

    # results_json (DB’ye yazılacak) olarak legacy kullan
    snap = save_snapshot(
        project_id=int(project_id),
        engine_version=str((legacy or {}).get("engine_version") or "engine-0.0.0"),
        config=config,
        input_hashes=activity_snapshot_ref,
        results=legacy or {},
        input_hash=input_hash,
        result_hash=result_hash,
        methodology_id=methodology_id,
        factor_set_id=(config.get("factor_set_id") if isinstance(config, dict) else None),
        monitoring_plan_id=(config.get("monitoring_plan_id") if isinstance(config, dict) else None),
        created_by_user_id=created_by_user_id,
        shared_with_client=shared_with_client,
        lock_after_create=lock_after_create,
    )

    return {
        "snapshot_id": int(snap.id),
        "input_hash": input_hash,
        "result_hash": result_hash,
        "locked": bool(snap.locked),
        "shared_with_client": bool(snap.shared_with_client),
    }
