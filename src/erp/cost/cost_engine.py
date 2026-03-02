from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class CarbonCostInput:
    emissions_tco2: float
    ets_price_eur_per_t: float
    cbam_price_eur_per_t: float
    fx_try_per_eur: float
    free_allocation_tco2: float = 0.0
    carbon_price_paid_eur_per_t: float = 0.0  # for CBAM adjustment

@dataclass
class CarbonCostResult:
    ets_liability_try: float
    cbam_certificates_try: float
    internal_carbon_price_try: float
    detail: Dict[str, Any]

def compute_cost(inp: CarbonCostInput, internal_price_eur_per_t: float | None = None) -> CarbonCostResult:
    e = float(inp.emissions_tco2)
    fx = float(inp.fx_try_per_eur)

    ets_exposed = max(0.0, e - float(inp.free_allocation_tco2))
    ets_cost = ets_exposed * float(inp.ets_price_eur_per_t) * fx

    cbam_adjusted_price = max(0.0, float(inp.cbam_price_eur_per_t) - float(inp.carbon_price_paid_eur_per_t))
    cbam_cost = e * cbam_adjusted_price * fx

    internal_price = float(internal_price_eur_per_t) if internal_price_eur_per_t is not None else float(inp.ets_price_eur_per_t)
    internal_cost = e * internal_price * fx

    return CarbonCostResult(
        ets_liability_try=ets_cost,
        cbam_certificates_try=cbam_cost,
        internal_carbon_price_try=internal_cost,
        detail={
            "ets_exposed_tco2": ets_exposed,
            "cbam_adjusted_price_eur_per_t": cbam_adjusted_price,
            "fx_try_per_eur": fx,
        },
    )
