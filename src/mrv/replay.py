from __future__ import annotations

import json
from typing import Any, Dict, Tuple

from sqlalchemy import select

from src.db.models import CalculationSnapshot
from src.db.session import db
from src.mrv.lineage import sha256_json
from src.mrv.orchestrator import run_orchestrator


def _safe_json_loads(s: str, default):
    try:
        return json.loads(s or "")
    except Exception:
        return default


def replay(snapshot_id: int) -> Dict[str, Any]:
    """Audit-ready snapshot replay.

    Doğrulamalar:
      - input_hash match
      - result_hash match
    """
    with db() as s:
        snap = s.get(CalculationSnapshot, int(snapshot_id))
        if not snap:
            raise ValueError("Snapshot bulunamadı.")

        config = _safe_json_loads(snap.config_json, {})
        input_hashes = _safe_json_loads(snap.input_hashes_json, {})
        results = _safe_json_loads(snap.results_json, {})

    # Orijinal sonuçlardan orchestrator girdilerini çıkar
    # (Bu repo tasarımında input bundle & energy/production dataset referansları results_json içinde bulunuyor.)
    input_bundle = (results or {}).get("input_bundle") or {}
    activity_snapshot_ref = input_bundle.get("activity_snapshot_ref") or input_hashes

    # Datasetleri storage_uri üzerinden tekrar oku
    # workflow.load_csv_from_uri zaten backend okuyor, orchestrator'a df gerekiyor
    from src.services.workflow import load_csv_from_uri

    energy_uri = ((activity_snapshot_ref or {}).get("energy") or {}).get("uri") or ""
    prod_uri = ((activity_snapshot_ref or {}).get("production") or {}).get("uri") or ""
    mat_uri = ((activity_snapshot_ref or {}).get("materials") or {}).get("uri") or ""

    if not energy_uri or not prod_uri:
        raise ValueError("Replay için gerekli dataset URI'ları eksik (energy/production).")

    energy_df = load_csv_from_uri(str(energy_uri))
    prod_df = load_csv_from_uri(str(prod_uri))
    materials_df = load_csv_from_uri(str(mat_uri)) if mat_uri else None

    methodology_id = int(snap.methodology_id) if snap.methodology_id is not None else None

    # Replay çalıştır
    input_bundle2, result_bundle2, legacy2 = run_orchestrator(
        project_id=int(snap.project_id),
        config=config or {},
        scenario=(input_bundle.get("scenario") or {}),
        methodology_id=methodology_id,
        activity_snapshot_ref=activity_snapshot_ref,
        energy_df=energy_df,
        production_df=prod_df,
        materials_df=materials_df,
    )

    # input_hash doğrula (repo mantığında input_hash = determinism candidate key)
    # Biz bunu workflow içinde _compute_result_hash ile üretmiştik, burada aynı payloadı kuruyoruz:
    factor_set_ref = (legacy2.get("input_bundle") or {}).get("factor_set_ref") or []
    monitoring_plan_ref = (legacy2.get("input_bundle") or {}).get("monitoring_plan_ref")

    engine_version = str((legacy2.get("engine_version") or ""))
    candidate_input_hash = sha256_json(
        {
            "engine_version": engine_version,
            "config": config or {},
            "input_hashes": input_hashes or {},
            "scenario": (input_bundle.get("scenario") or {}),
            "methodology_id": methodology_id,
            "factor_set_ref": factor_set_ref,
            "monitoring_plan_ref": monitoring_plan_ref,
        }
    )

    input_hash_match = str(candidate_input_hash) == str(snap.input_hash)
    result_hash_match = str(result_bundle2.result_hash) == str(snap.result_hash)

    return {
        "snapshot_id": int(snapshot_id),
        "input_hash_expected": str(snap.input_hash),
        "input_hash_recomputed": str(candidate_input_hash),
        "input_hash_match": bool(input_hash_match),
        "result_hash_expected": str(snap.result_hash),
        "result_hash_recomputed": str(result_bundle2.result_hash),
        "result_hash_match": bool(result_hash_match),
        "recomputed_results_preview": {
            "total_tco2": ((legacy2.get("energy") or {}).get("total_tco2")),
            "direct_tco2": ((legacy2.get("energy") or {}).get("direct_tco2")),
            "indirect_tco2": ((legacy2.get("energy") or {}).get("indirect_tco2")),
        },
    }
