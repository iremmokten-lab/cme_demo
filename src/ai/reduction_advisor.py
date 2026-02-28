from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from src.engine.advisor import build_reduction_advice
from src.services.cbam_liability import compute_cbam_liability
from src.mrv.lineage import sha256_json


@dataclass
class ReductionRecommendation:
    id: str
    title: str
    action_type: str
    expected_emission_reduction_tco2: float
    expected_cost_change_eur_per_year: float
    evidence_requirements: list[str]
    calculation_reference: dict

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "action_type": self.action_type,
            "expected_emission_reduction_tco2": float(self.expected_emission_reduction_tco2),
            "expected_cost_change_eur_per_year": float(self.expected_cost_change_eur_per_year),
            "evidence_requirements": self.evidence_requirements,
            "calculation_reference": self.calculation_reference,
        }


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def build_ai_reduction_recommendations(
    *,
    project_id: int,
    snapshot_id: int,
    results: dict,
    config: dict,
) -> dict:
    """
    Audit-friendly AI öneri motoru:
      - mevcut engine.advisor (heuristic) çıktısını alır
      - her öneriye hesap referansı ekler (snapshot + hashes + varsayımlar)
      - cbam/ets maliyet etkisini kaba şekilde tahmin eder
    """
    energy = (results or {}).get("energy") or {}
    kpis = (results or {}).get("kpis") or {}
    cbam = (results or {}).get("cbam") or {}

    # Build baseline advice (deterministic, rule-based)
    advice = build_reduction_advice(
        project_id=int(project_id),
        snapshot_results={
            "kpis": kpis,
            "breakdown": {"energy": {"fuel_rows": (energy.get("direct_rows") or []), "electricity_rows": (energy.get("indirect_rows") or [])}},
            "cbam": cbam,
            "cbam_table": (results or {}).get("cbam_table") or [],
        },
        config=config or {},
    )

    total_t = _to_float((energy or {}).get("total_tco2"), 0.0)
    year = int((config or {}).get("reporting_year") or 2026)
    eu_ets_price = _to_float((config or {}).get("eu_ets_price_eur_per_t"), 0.0)
    carbon_paid = _to_float((config or {}).get("carbon_price_paid_eur_per_t"), 0.0)

    # baseline cbam liability (estimate)
    base_cbam_liab = compute_cbam_liability(
        year=year,
        embedded_emissions_tco2=_to_float(((cbam or {}).get("totals") or {}).get("embedded_emissions_tco2"), total_t),
        eu_ets_price_eur_per_t=eu_ets_price,
        carbon_price_paid_eur_per_t=carbon_paid,
    )

    recs: List[ReductionRecommendation] = []
    measures = (advice or {}).get("measures") or []
    for m in measures:
        if not isinstance(m, dict):
            continue

        red_pct = _to_float(m.get("expected_reduction_pct_of_total"), 0.0) / 100.0
        exp_red = max(0.0, total_t * red_pct)

        # ETS/CBAM kaba cost tahmini (audit için "estimate" olarak etiketlenir)
        exp_cost_delta = -exp_red * eu_ets_price  # negatif = maliyet düşüşü varsayımı

        ref = {
            "project_id": int(project_id),
            "snapshot_id": int(snapshot_id),
            "engine_version": str((results or {}).get("engine_version") or ""),
            "assumptions": (m.get("assumptions") or {}),
            "baseline_cbam_liability": base_cbam_liab.to_dict(),
            "reference_hash": sha256_json({"snapshot_id": snapshot_id, "measure": m}),
        }

        recs.append(
            ReductionRecommendation(
                id=str(m.get("id") or ""),
                title=str(m.get("title") or "Öneri"),
                action_type=str(m.get("category") or "action"),
                expected_emission_reduction_tco2=float(exp_red),
                expected_cost_change_eur_per_year=float(exp_cost_delta),
                evidence_requirements=list(m.get("evidence_needed") or []),
                calculation_reference=ref,
            )
        )

    return {
        "schema": "ai_reduction_recommendations.v1",
        "project_id": int(project_id),
        "snapshot_id": int(snapshot_id),
        "generated_for_year": year,
        "recommendations": [r.to_dict() for r in recs],
        "note": "Bu öneriler deterministik rule-based + estimate maliyet varsayımı içerir; denetim için senaryo snapshot ile doğrulanması önerilir.",
    }
