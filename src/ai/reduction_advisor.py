from __future__ import annotations

from typing import Any, Dict, List

from src.engine.advisor import build_reduction_advice
from src.mrv.lineage import sha256_json
from src.services.cbam_liability import compute_cbam_liability


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def build_reduction_recommendations(
    *,
    project_id: int,
    snapshot_id: int,
    results: dict,
    config: dict,
) -> dict:
    """
    AI/Advisor çıktısı:
      - hotspot + aksiyon önerileri
      - her öneri: beklenen azaltım, maliyet etkisi, evidence ihtiyaçları
      - calculation_reference: snapshot + varsayım hash
    """
    energy = (results or {}).get("energy") or {}
    cbam = (results or {}).get("cbam") or {}
    cbam_table = (results or {}).get("cbam_table") or []
    kpis = (results or {}).get("kpis") or {}

    advice = build_reduction_advice(
        project_id=int(project_id),
        snapshot_results={
            "kpis": kpis,
            "breakdown": {"energy": {"fuel_rows": (energy.get("direct_rows") or []), "electricity_rows": (energy.get("indirect_rows") or [])}},
            "cbam": cbam,
            "cbam_table": cbam_table,
        },
        config=config or {},
    )

    year = int((config or {}).get("reporting_year") or 2026)
    eu_ets_price = _f((config or {}).get("eu_ets_price_eur_per_t"), 0.0)
    carbon_paid = _f((config or {}).get("carbon_price_paid_eur_per_t"), 0.0)

    embedded = _f((((cbam or {}).get("totals") or {}).get("embedded_emissions_tco2")), _f(energy.get("total_tco2"), 0.0))
    base_liab = compute_cbam_liability(
        year=year,
        embedded_emissions_tco2=embedded,
        eu_ets_price_eur_per_t=eu_ets_price,
        carbon_price_paid_eur_per_t=carbon_paid,
    ).to_dict()

    recs: List[dict] = []
    for m in (advice or {}).get("measures") or []:
        if not isinstance(m, dict):
            continue
        red_pct = _f(m.get("expected_reduction_pct_of_total"), 0.0) / 100.0
        base_total = _f(energy.get("total_tco2"), 0.0)
        exp_red = max(0.0, base_total * red_pct)
        exp_cost_delta = -exp_red * eu_ets_price

        ref = {
            "project_id": int(project_id),
            "snapshot_id": int(snapshot_id),
            "engine_version": str((results or {}).get("engine_version") or ""),
            "assumptions": m.get("assumptions") or {},
            "baseline_cbam_liability": base_liab,
            "reference_hash": sha256_json({"snapshot_id": snapshot_id, "measure": m}),
        }

        recs.append(
            {
                "id": str(m.get("id") or ""),
                "title": str(m.get("title") or "Öneri"),
                "action_type": str(m.get("category") or "action"),
                "expected_emission_reduction_tco2": float(exp_red),
                "expected_cost_change_eur_per_year": float(exp_cost_delta),
                "evidence_requirements": list(m.get("evidence_needed") or []),
                "calculation_reference": ref,
            }
        )

    return {
        "schema": "ai_reduction_recommendations.v1",
        "project_id": int(project_id),
        "snapshot_id": int(snapshot_id),
        "year": year,
        "recommendations": recs,
        "note_tr": "Öneriler deterministik rule-based + maliyet estimate içerir. Denetim için senaryo snapshot ile doğrulama önerilir.",
    }
