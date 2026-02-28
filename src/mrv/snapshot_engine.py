from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from sqlalchemy import select

from src.db.models import CalculationSnapshot
from src.db.session import db
from src.mrv.orchestrator import run_orchestrator
from src.mrv.snapshot_store import compute_input_hash, save_snapshot
from src.mrv.lineage import sha256_json
from src.services.workflow import latest_upload, _input_hashes_payload


def _safe_json_loads(s: str, default):
    try:
        return json.loads(s or "")
    except Exception:
        return default


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
        return str(prev.result_hash or "") or None


def run_calculation_and_snapshot(
    *,
    project_id: int,
    config: dict,
    scenario: dict | None = None,
    methodology_id: int | None = None,
    energy_df: pd.DataFrame | None = None,
    production_df: pd.DataFrame | None = None,
    materials_df: pd.DataFrame | None = None,
    created_by_user_id: int | None = None,
    shared_with_client: bool = False,
    lock_after_create: bool = False,
) -> CalculationSnapshot:
    """
    Audit-grade snapshot üretimi (deterministic):

    - Orchestrator çalıştırılır.
    - InputBundle içindeki activity_snapshot_ref + factor lock referansları kullanılır.
    - input_hash ve result_hash hesaplanır ve DB'ye kaydedilir.
    """

    scenario = scenario or {}
    config = config or {}

    # Eğer DF verilmediyse latest upload'tan oku (Streamlit UI için kolaylık)
    if energy_df is None or production_df is None:
        energy_u = latest_upload(project_id, "energy")
        prod_u = latest_upload(project_id, "production")
        mat_u = latest_upload(project_id, "materials")
        if not energy_u or not prod_u:
            raise ValueError("Hesaplama için energy + production dataset yüklenmiş olmalı.")

        # workflow.load_csv_from_uri kullanarak okuyalım
        from src.services.workflow import load_csv_from_uri

        energy_df = load_csv_from_uri(str(energy_u.storage_uri))
        production_df = load_csv_from_uri(str(prod_u.storage_uri))
        materials_df = load_csv_from_uri(str(mat_u.storage_uri)) if mat_u and mat_u.storage_uri else None

        activity_snapshot_ref = _input_hashes_payload(project_id, energy_u, prod_u, mat_u)
    else:
        # DF verildiyse input_hashes referansı config içinden beklenebilir
        activity_snapshot_ref = (config.get("activity_snapshot_ref") or {}) if isinstance(config, dict) else {}

    # Orchestrator
    input_bundle, result_bundle, legacy = run_orchestrator(
        project_id=int(project_id),
        config=config,
        scenario=scenario,
        methodology_id=methodology_id,
        activity_snapshot_ref=activity_snapshot_ref,
        energy_df=energy_df,
        production_df=production_df,
        materials_df=materials_df,
    )

    # input_hash (deterministic key)
    factor_set_ref = (legacy.get("input_bundle") or {}).get("factor_set_ref") or []
    monitoring_plan_ref = (legacy.get("input_bundle") or {}).get("monitoring_plan_ref") or None

    input_hash = compute_input_hash(
        engine_version=str(legacy.get("engine_version") or ""),
        config=config,
        input_hashes=activity_snapshot_ref or {},
        scenario=scenario,
        methodology_id=methodology_id,
        factor_set_ref=factor_set_ref,
        monitoring_plan_ref=monitoring_plan_ref,
    )

    # results payload (deterministic)
    # result_bundle zaten result_hash içeriyor; ayrıca results_json'u saklıyoruz
    results_dict = {}
    try:
        results_dict = json.loads(result_bundle.results_json or "{}")
    except Exception:
        results_dict = legacy if isinstance(legacy, dict) else {}

    result_hash = str(result_bundle.result_hash or sha256_json(results_dict))

    prev_hash = _previous_snapshot_hash(project_id)

    snap = save_snapshot(
        project_id=int(project_id),
        engine_version=str(legacy.get("engine_version") or ""),
        config=config,
        input_hashes=activity_snapshot_ref or {},
        results=results_dict or {},
        input_hash=input_hash,
        result_hash=result_hash,
        methodology_id=methodology_id,
        factor_set_id=(config.get("factor_set_id") if isinstance(config, dict) else None),
        monitoring_plan_id=(config.get("monitoring_plan_id") if isinstance(config, dict) else None),
        previous_snapshot_hash=prev_hash,
        created_by_user_id=created_by_user_id,
        shared_with_client=shared_with_client,
        lock_after_create=lock_after_create,
    )

    return snap
