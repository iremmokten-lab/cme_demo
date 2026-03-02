from __future__ import annotations
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

@dataclass
class ScenarioInput:
    name: str
    base_emissions_tco2: float
    base_energy_mwh: float
    ets_price_eur_per_t: float
    cbam_price_eur_per_t: float
    fx_try_per_eur: float
    measures: List[Dict[str, Any]]  # each: {"type": "...", "reduction_pct": 0-100, "capex_try": .., "opex_delta_try": ..}

@dataclass
class ScenarioResult:
    name: str
    emissions_before: float
    emissions_after: float
    reduction_tco2: float
    ets_cost_try_before: float
    ets_cost_try_after: float
    cbam_cost_try_before: float
    cbam_cost_try_after: float
    capex_try: float
    opex_delta_try: float
    detail: Dict[str, Any]

def run_scenario(inp: ScenarioInput) -> ScenarioResult:
    total_reduction_pct = 0.0
    capex = 0.0
    opex_delta = 0.0
    for m in inp.measures or []:
        total_reduction_pct += float(m.get("reduction_pct", 0.0))
        capex += float(m.get("capex_try", 0.0))
        opex_delta += float(m.get("opex_delta_try", 0.0))
    total_reduction_pct = max(0.0, min(100.0, total_reduction_pct))
    after = inp.base_emissions_tco2 * (1.0 - total_reduction_pct / 100.0)
    red = inp.base_emissions_tco2 - after

    ets_before = inp.base_emissions_tco2 * inp.ets_price_eur_per_t * inp.fx_try_per_eur
    ets_after = after * inp.ets_price_eur_per_t * inp.fx_try_per_eur

    cbam_before = inp.base_emissions_tco2 * inp.cbam_price_eur_per_t * inp.fx_try_per_eur
    cbam_after = after * inp.cbam_price_eur_per_t * inp.fx_try_per_eur

    return ScenarioResult(
        name=inp.name,
        emissions_before=inp.base_emissions_tco2,
        emissions_after=after,
        reduction_tco2=red,
        ets_cost_try_before=ets_before,
        ets_cost_try_after=ets_after,
        cbam_cost_try_before=cbam_before,
        cbam_cost_try_after=cbam_after,
        capex_try=capex,
        opex_delta_try=opex_delta,
        detail={"total_reduction_pct": total_reduction_pct, "measures": inp.measures},
    )

def to_json(res: ScenarioResult) -> dict:
    return {
        "name": res.name,
        "emissions_before_tco2": res.emissions_before,
        "emissions_after_tco2": res.emissions_after,
        "reduction_tco2": res.reduction_tco2,
        "ets_cost_try_before": res.ets_cost_try_before,
        "ets_cost_try_after": res.ets_cost_try_after,
        "cbam_cost_try_before": res.cbam_cost_try_before,
        "cbam_cost_try_after": res.cbam_cost_try_after,
        "capex_try": res.capex_try,
        "opex_delta_try": res.opex_delta_try,
        "detail": res.detail,
    }
