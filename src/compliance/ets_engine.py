from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from src.compliance.qa_qc import build_qaqc_checks
from src.engine.emissions import energy_emissions
from src.mrv.lineage import sha256_json


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def build_source_streams_from_energy(energy_result: dict) -> list[dict]:
    rows = (energy_result or {}).get("direct_rows") or []
    streams: Dict[str, dict] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        ft = str(r.get("fuel_type") or "").strip() or "unknown"
        streams.setdefault(ft, {"fuel_type": ft, "activity_data": 0.0, "unit": str(r.get("unit") or ""), "emissions_tco2": 0.0})
        streams[ft]["activity_data"] += _to_float(r.get("quantity"), 0.0)
        streams[ft]["emissions_tco2"] += _to_float(r.get("tco2"), 0.0)

    out = []
    for ft, s in sorted(streams.items(), key=lambda x: x[0]):
        out.append(
            {
                "stream_id": f"fuel:{ft}",
                "fuel_type": ft,
                "activity_data": float(s["activity_data"]),
                "unit": s["unit"],
                "emissions_tco2": float(s["emissions_tco2"]),
                "tier": "TIER_2",
                "tier_justification": "Standart hesap motoru + faktör seti (demo).",
                "uncertainty_note": "Belirsizlik notu: tesis ölçüm sistemine göre güncellenmelidir.",
                "qa_qc": [],
            }
        )
    return out


def build_ets_compliance_dataset(
    *,
    project_id: int,
    snapshot_id: int,
    results: dict,
    config: dict,
) -> dict:
    """
    EU ETS / MRR 2018/2066 için minimum zorunlu alanları olan JSON dataset.

    Not: Bu motor fail-fast değil; validator ile birlikte kullanılır.
    """
    energy = (results or {}).get("energy") or {}
    kpis = (results or {}).get("kpis") or {}

    source_streams = build_source_streams_from_energy(energy)
    qaqc = build_qaqc_checks(results=results, config=config)

    # stream QA/QC
    for s in source_streams:
        s["qa_qc"] = qaqc.get("checks", [])

    out = {
        "schema": "ets_reporting.v1",
        "project_id": int(project_id),
        "snapshot_id": int(snapshot_id),
        "period": {
            "year": int(config.get("reporting_year") or 2026),
            "quarter": config.get("reporting_quarter"),
        },
        "totals": {
            "direct_tco2": _to_float(energy.get("direct_tco2"), 0.0),
            "indirect_tco2": _to_float(energy.get("indirect_tco2"), 0.0),
            "total_tco2": _to_float(energy.get("total_tco2"), 0.0),
        },
        "kpis": kpis,
        "source_streams": source_streams,
        "qa_qc": qaqc,
        "audit": {
            "engine_version": str(results.get("engine_version") or ""),
            "input_hash": str(config.get("input_hash") or ""),
            "result_hash": str(config.get("result_hash") or ""),
            "dataset_hash": sha256_json(source_streams),
        },
    }
    out["dataset_hash"] = sha256_json(out)
    return out
