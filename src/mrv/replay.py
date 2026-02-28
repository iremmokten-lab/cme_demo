from __future__ import annotations

import json
from typing import Any, Dict

from src.db.models import CalculationSnapshot
from src.db.session import db
from src.mrv.orchestrator import run_orchestrator
from src.mrv.snapshot_store import compute_input_hash


def _safe_json_loads(s: str, default):
    try:
        return json.loads(s or "")
    except Exception:
        return default


def replay(snapshot_id: int) -> Dict[str, Any]:
    """
    Snapshot Replay (audit-ready):

    - snapshot içinden config + input_hashes + results alınır
    - dataset uri ile tekrar okunur
    - orchestrator aynı parametrelerle tekrar çalıştırılır
    - input_hash/result_hash doğrulanır
    """

    with db() as s:
        snap = s.get(CalculationSnapshot, int(snapshot_id))
        if not snap:
            raise ValueError("Snapshot bulunamadı.")

        config = _safe_json_loads(snap.config_json, {})
        input_hashes = _safe_json_loads(snap.input_hashes_json, {})
        results = _safe_json_loads(snap.results_json, {})

    input_bundle = (results or {}).get("input_bundle") or {}
    activity_snapshot_ref = input_bundle.get("activity_snapshot_ref") or input_hashes or {}

    # datasetleri uri üzerinden tekrar oku
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
    scenario = (input_bundle.get("scenario") or {}) if isinstance(input_bundle, dict) else {}

    # Replay orchestrator
    input_bundle2, result_bundle2, legacy2 = run_orchestrator(
        project_id=int(snap.project_id),
        config=config or {},
        scenario=scenario or {},
        methodology_id=methodology_id,
        activity_snapshot_ref=activity_snapshot_ref,
        energy_df=energy_df,
        production_df=prod_df,
        materials_df=materials_df,
    )

    factor_set_ref = (legacy2.get("input_bundle") or {}).get("factor_set_ref") or []
    monitoring_plan_ref = (legacy2.get("input_bundle") or {}).get("monitoring_plan_ref") or None

    candidate_input_hash = compute_input_hash(
        engine_version=str(legacy2.get("engine_version") or ""),
        config=config or {},
        input_hashes=activity_snapshot_ref or {},
        scenario=scenario or {},
        methodology_id=methodology_id,
        factor_set_ref=factor_set_ref,
        monitoring_plan_ref=monitoring_plan_ref,
    )

    input_hash_match = str(candidate_input_hash) == str(snap.input_hash or "")
    result_hash_match = str(result_bundle2.result_hash or "") == str(snap.result_hash or "")

    return {
        "snapshot_id": int(snapshot_id),
        "input_hash_expected": str(snap.input_hash or ""),
        "input_hash_recomputed": str(candidate_input_hash),
        "input_hash_match": bool(input_hash_match),
        "result_hash_expected": str(snap.result_hash or ""),
        "result_hash_recomputed": str(result_bundle2.result_hash or ""),
        "result_hash_match": bool(result_hash_match),
        "preview": {
            "total_tco2": ((legacy2.get("energy") or {}).get("total_tco2")),
            "direct_tco2": ((legacy2.get("energy") or {}).get("direct_tco2")),
            "indirect_tco2": ((legacy2.get("energy") or {}).get("indirect_tco2")),
        },
    }
