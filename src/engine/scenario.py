from __future__ import annotations

from typing import Any, Dict, List

from src.mrv.lineage import sha256_json
from src.services.cbam_liability import compute_cbam_liability


def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _pick_year(config: dict, results: dict) -> int:
    # Deterministik: config > results.input_bundle.period.year > 2026
    try:
        y = int((config or {}).get("reporting_year") or 0)
        if y > 0:
            return y
    except Exception:
        pass
    try:
        y = int((((results or {}).get("input_bundle") or {}).get("period") or {}).get("year") or 0)
        if y > 0:
            return y
    except Exception:
        pass
    return 2026


def simulate_cost_scenario(
    *,
    results: dict,
    config: dict,
    portfolio_selected: List[dict],
) -> Dict[str, Any]:
    """
    Faz 4 senaryo simülasyonu.

    Girdi:
      - snapshot results + config
      - optimizer portfolio.selected (option dicts)

    Çıktı:
      - baseline vs scenario: emissions, ETS cost estimate, CBAM liability estimate, deltas
      - deterministic reference hash
    """
    results = results or {}
    config = config or {}

    kpis = (results.get("kpis") or {}) if isinstance(results, dict) else {}
    cbam = (results.get("cbam") or {}) if isinstance(results, dict) else {}

    base_total = _f(kpis.get("total_tco2"), _f((results.get("energy") or {}).get("total_tco2"), 0.0))
    embedded = _f((((cbam or {}).get("totals") or {}).get("embedded_emissions_tco2")), base_total)

    # prices / assumptions (config.ai.prices.*)
    ai_cfg = (config.get("ai") or {}) if isinstance(config, dict) else {}
    prices = (ai_cfg.get("prices") or {}) if isinstance(ai_cfg, dict) else {}

    eu_ets_price = _f(prices.get("eu_ets_price_eur_per_t"), _f(config.get("eu_ets_price_eur_per_t"), 0.0))
    carbon_paid = _f(prices.get("carbon_price_paid_eur_per_t"), _f(config.get("carbon_price_paid_eur_per_t"), 0.0))
    free_alloc = _f(prices.get("ets_free_allocation_tco2"), 0.0)

    year = _pick_year(config, results)

    baseline_ets_cost = max(0.0, base_total - free_alloc) * eu_ets_price
    baseline_cbam = compute_cbam_liability(
        year=int(year),
        embedded_emissions_tco2=float(embedded),
        eu_ets_price_eur_per_t=float(eu_ets_price),
        carbon_price_paid_eur_per_t=float(carbon_paid),
    ).to_dict()

    # portfolio reductions
    red = 0.0
    capex = 0.0
    ann_cost = 0.0
    for o in portfolio_selected or []:
        if not isinstance(o, dict):
            continue
        red += _f(o.get("reduction_tco2"), 0.0)
        capex += _f(o.get("capex_eur"), 0.0)
        ann_cost += _f(o.get("annualized_cost_eur"), 0.0)

    red = max(0.0, red)
    scenario_total = max(0.0, base_total - red)

    # embedded emissions: basit ve deterministik yaklaşım: toplam azaltım oranı ile aynı oranda düşer
    ratio = (scenario_total / base_total) if base_total > 1e-12 else 1.0
    scenario_embedded = max(0.0, embedded * ratio)

    scenario_ets_cost = max(0.0, scenario_total - free_alloc) * eu_ets_price
    scenario_cbam = compute_cbam_liability(
        year=int(year),
        embedded_emissions_tco2=float(scenario_embedded),
        eu_ets_price_eur_per_t=float(eu_ets_price),
        carbon_price_paid_eur_per_t=float(carbon_paid),
    ).to_dict()

    payload = {
        "schema": "ai_scenario_simulation.v1",
        "year": int(year),
        "assumptions": {
            "eu_ets_price_eur_per_t": float(eu_ets_price),
            "carbon_price_paid_eur_per_t": float(carbon_paid),
            "ets_free_allocation_tco2": float(free_alloc),
            "embedded_emissions_scaled_by_total_ratio": True,
        },
        "baseline": {
            "total_emissions_tco2": float(base_total),
            "embedded_emissions_tco2": float(embedded),
            "ets_cost_eur": float(baseline_ets_cost),
            "cbam_liability": baseline_cbam,
        },
        "scenario": {
            "total_emissions_tco2": float(scenario_total),
            "embedded_emissions_tco2": float(scenario_embedded),
            "ets_cost_eur": float(scenario_ets_cost),
            "cbam_liability": scenario_cbam,
            "portfolio": {
                "selected_count": int(len(portfolio_selected or [])),
                "capex_eur": float(capex),
                "annualized_cost_eur": float(ann_cost),
                "reduction_tco2": float(red),
                "selected": list(portfolio_selected or []),
            },
        },
        "delta": {
            "emissions_tco2": float(scenario_total - base_total),
            "ets_cost_eur": float(scenario_ets_cost - baseline_ets_cost),
            "cbam_liability_eur": float(_f(baseline_cbam.get("liability_eur"), 0.0) - _f(scenario_cbam.get("liability_eur"), 0.0)) * -1.0,
        },
    }

    payload["reference_hash"] = sha256_json(payload)
    return payload
