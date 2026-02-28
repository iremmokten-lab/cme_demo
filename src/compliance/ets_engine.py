from __future__ import annotations

from typing import Any, Dict, List

from src.compliance.qa_qc import build_qaqc_checks
from src.mrv.lineage import sha256_json


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def build_ets_reporting_dataset(
    *,
    project_id: int,
    snapshot_id: int,
    results: dict,
    config: dict,
    energy_df=None,
    production_df=None,
) -> dict:
    """
    EU ETS / MRR 2018/2066 için audit-ready ETS reporting dataset.
    (Zorunlu alanlar + QA/QC + tier/uncertainty placeholder)
    """
    energy = (results or {}).get("energy") or {}
    kpis = (results or {}).get("kpis") or {}

    # source streams (fuel bazlı)
    streams = {}
    for r in (energy.get("direct_rows") or []):
        if not isinstance(r, dict):
            continue
        ft = str(r.get("fuel_type") or "unknown").strip().lower()
        streams.setdefault(ft, {"fuel_type": ft, "activity_data": 0.0, "unit": str(r.get("unit") or ""), "emissions_tco2": 0.0})
        streams[ft]["activity_data"] += _f(r.get("quantity"), 0.0)
        streams[ft]["emissions_tco2"] += _f(r.get("tco2"), 0.0)

    source_streams = []
    for ft, v in sorted(streams.items(), key=lambda x: x[0]):
        source_streams.append(
            {
                "stream_id": f"fuel:{ft}",
                "fuel_type": v["fuel_type"],
                "activity_data": float(v["activity_data"]),
                "unit": v["unit"],
                "emissions_tco2": float(v["emissions_tco2"]),
                "tier": "TIER_2",
                "tier_justification": "Standart hesap motoru + versioned factor set (minimum).",
                "uncertainty_note": "Belirsizlik notu: ölçüm ekipmanı, numuneleme ve veri sistemine göre güncellenmelidir.",
                "qa_qc": [],
            }
        )

    qaqc = build_qaqc_checks(energy_df=energy_df, production_df=production_df)
    for s in source_streams:
        s["qa_qc"] = qaqc.get("checks", [])

    out = {
        "schema": "ets_reporting.v1",
        "project_id": int(project_id),
        "snapshot_id": int(snapshot_id),
        "period": {
            "year": int((config or {}).get("reporting_year") or 2026),
            "quarter": (config or {}).get("reporting_quarter"),
        },
        "source_streams": source_streams,
        "totals": {
            "direct_tco2": _f(energy.get("direct_tco2"), 0.0),
            "indirect_tco2": _f(energy.get("indirect_tco2"), 0.0),
            "total_tco2": _f(energy.get("total_tco2"), 0.0),
        },
        "kpis": kpis,
        "qa_qc": qaqc,
    }
    out["dataset_hash"] = sha256_json(out)
    return out
