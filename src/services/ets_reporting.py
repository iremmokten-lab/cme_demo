from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _s(x: Any) -> str:
    return "" if x is None else str(x)


def _to_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def build_ets_reporting_dataset(
    *,
    installation: Dict[str, Any],
    period: Dict[str, Any],
    energy_breakdown: Dict[str, Any],
    methodology: Dict[str, Any] | None,
    config: Dict[str, Any] | None,
    allocation: Dict[str, Any] | None,
    qa_qc: Dict[str, Any] | None,
    tr_ets_mode: bool = False,
) -> Dict[str, Any]:
    """EU ETS / MRR (2018/2066) uyumlu JSON dataset.

    Notlar:
    - Bu dosya regulator'a *sunulabilir* veri seti üretir.
    - PDF rapor (ets_report.pdf) ayrı modülde üretilir (src/services/reporting.py).
    """
    now_utc = datetime.now(timezone.utc).isoformat()

    direct_t = _to_float(energy_breakdown.get("direct_tco2"))
    indirect_t = _to_float(energy_breakdown.get("indirect_tco2"))
    total_t = _to_float(energy_breakdown.get("total_tco2"))

    # Source streams (yakıt kırılımı)
    streams: List[Dict[str, Any]] = []
    for r in (energy_breakdown.get("direct_rows") or []):
        if not isinstance(r, dict):
            continue
        ft = _s(r.get("fuel_type")).strip()
        if not ft:
            continue
        streams.append(
            {
                "stream_type": "fuel",
                "fuel_type": ft,
                "unit": _s(r.get("unit")),
                "month": _s(r.get("month")),
                "quantity": _to_float(r.get("quantity")),
                "gj": _to_float(r.get("gj")),
                "tco2": _to_float(r.get("tco2")),
                "factors": {
                    "ncv_gj_per_unit": _to_float(r.get("ncv_gj_per_unit")),
                    "ef_tco2_per_gj": _to_float(r.get("ef_tco2_per_gj")),
                    "oxidation_factor": _to_float(r.get("oxidation_factor")),
                },
                "factor_sources": r.get("factor_sources") or {},
            }
        )

    for r in (energy_breakdown.get("indirect_rows") or []):
        if not isinstance(r, dict):
            continue
        streams.append(
            {
                "stream_type": "electricity",
                "month": _s(r.get("month")),
                "mwh": _to_float(r.get("mwh")),
                "grid_factor_tco2_per_mwh": _to_float(r.get("grid_factor_tco2_per_mwh")),
                "tco2": _to_float(r.get("tco2")),
                "method": _s(r.get("method")),
                "factor_source": _s(r.get("factor_source")),
            }
        )

    # Tier logic & uncertainty (high level)
    meth_cfg = (methodology or {}).get("config") if isinstance(methodology, dict) else None
    if meth_cfg is None:
        meth_cfg = (methodology or {}).get("config_json") if isinstance(methodology, dict) else None

    tiers = (config or {}).get("tiers") or {}
    uncertainty_notes = (config or {}).get("uncertainty_notes") or []

    # QA/QC
    qaqc = qa_qc or {}
    controls = qaqc.get("controls") or []
    failed = qaqc.get("failed") or []
    passed = qaqc.get("passed") or []

    # Allocation / free allocation
    ets_cfg = (config or {}).get("ets") or {}
    free_alloc = _to_float(ets_cfg.get("free_alloc_t", 0.0))
    banked = _to_float(ets_cfg.get("banked_t", 0.0))
    carbon_price = _to_float(ets_cfg.get("eua_price_eur_per_t", 0.0))

    net_position = total_t - free_alloc - banked
    net_cost = max(net_position, 0.0) * carbon_price

    report = {
        "schema": "ets_reporting.v1",
        "regulation": "TR_ETS" if tr_ets_mode else "EU_ETS_MRR_2018_2066",
        "generated_at_utc": now_utc,
        "period": {
            "year": period.get("year"),
            "from_date": period.get("from_date"),
            "to_date": period.get("to_date"),
        },
        "installation": {
            "facility_id": installation.get("facility_id"),
            "facility_name": installation.get("facility_name", ""),
            "country": installation.get("country", ""),
            "sector": installation.get("sector", ""),
        },
        "methodology": {
            "methodology_id": (methodology or {}).get("id") if isinstance(methodology, dict) else None,
            "name": (methodology or {}).get("name") if isinstance(methodology, dict) else "",
            "regime": (methodology or {}).get("regime") if isinstance(methodology, dict) else "",
            "config": meth_cfg or {},
        },
        "source_streams": streams,
        "activity_data": {
            "notes": _s((config or {}).get("activity_data_notes") or ""),
        },
        "emissions": {
            "direct_tco2": direct_t,
            "indirect_tco2": indirect_t,
            "total_tco2": total_t,
        },
        "tier_logic": tiers,
        "uncertainty_notes": uncertainty_notes,
        "qa_qc": {
            "controls": controls,
            "passed": passed,
            "failed": failed,
        },
        "allocation": allocation or {},
        "free_allocation": {
            "free_alloc_t": free_alloc,
            "banked_t": banked,
        },
        "net_position": {
            "net_t": net_position,
            "eua_price_eur_per_t": carbon_price,
            "net_cost_eur": net_cost,
        },
        "audit_ready": {
            "determinism": "Aynı input + config + factor set + methodology => aynı sonuç.",
            "factor_set_id": energy_breakdown.get("factor_set_id"),
            "used_default_factors": bool(energy_breakdown.get("used_default_factors")),
        },
    }
    return report
