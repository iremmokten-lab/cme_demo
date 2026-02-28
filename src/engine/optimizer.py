from __future__ import annotations

"""Faz 3 — Optimizer (deterministic)

Kapsam:
- Abatement cost curve (MACC) üretimi
- Basit portföy seçimi (greedy):
  - hedef azaltım (% veya tCO2)
  - max CAPEX

Not:
- Harici solver yok (Streamlit Cloud uyumlu).
- Deterministik: stable sort + id tie-break.
"""

from dataclasses import dataclass
from typing import Any, Dict, List


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


@dataclass
class Option:
    id: str
    title: str
    reduction_tco2: float
    capex_eur: float
    opex_delta_eur_per_year: float
    lifetime_years: int
    notes: str = ""

    def annualized_cost_eur(self, discount_rate: float = 0.08) -> float:
        r = float(discount_rate)
        n = max(1, int(self.lifetime_years))
        if self.capex_eur <= 0:
            cap_ann = 0.0
        else:
            af = (r * (1 + r) ** n) / (((1 + r) ** n) - 1) if r > 1e-9 else (1.0 / n)
            cap_ann = float(self.capex_eur) * af
        return cap_ann + float(self.opex_delta_eur_per_year)

    def cost_per_tco2(self, discount_rate: float = 0.08) -> float:
        if self.reduction_tco2 <= 1e-12:
            return float("inf")
        return self.annualized_cost_eur(discount_rate=discount_rate) / float(self.reduction_tco2)

    def to_dict(self, discount_rate: float = 0.08) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "reduction_tco2": float(self.reduction_tco2),
            "capex_eur": float(self.capex_eur),
            "opex_delta_eur_per_year": float(self.opex_delta_eur_per_year),
            "lifetime_years": int(self.lifetime_years),
            "annualized_cost_eur": float(self.annualized_cost_eur(discount_rate=discount_rate)),
            "cost_per_tco2": float(self.cost_per_tco2(discount_rate=discount_rate)),
            "notes": self.notes,
        }


def build_options_from_measures(measures: List[Dict[str, Any]], total_tco2: float) -> List[Option]:
    opts: List[Option] = []
    for m in measures or []:
        if not isinstance(m, dict):
            continue
        pct = _to_float(m.get("expected_reduction_pct_of_total"), 0.0)
        red = (float(total_tco2) * pct / 100.0) if pct > 0 else 0.0
        opts.append(
            Option(
                id=str(m.get("id") or "opt"),
                title=str(m.get("title") or ""),
                reduction_tco2=float(red),
                capex_eur=_to_float(m.get("capex_eur"), 0.0),
                opex_delta_eur_per_year=_to_float(m.get("opex_delta_eur_per_year"), 0.0),
                lifetime_years=int(_to_float(m.get("lifetime_years"), 10) or 10),
                notes=str(m.get("description") or ""),
            )
        )
    return opts


def compute_abatement_curve(options: List[Option], discount_rate: float = 0.08) -> List[Dict[str, Any]]:
    rows = [o.to_dict(discount_rate=discount_rate) for o in options]
    rows.sort(key=lambda r: (float(r.get("cost_per_tco2", 1e18)), str(r.get("id", ""))))

    cum = 0.0
    for r in rows:
        cum += float(r.get("reduction_tco2", 0.0) or 0.0)
        r["cumulative_reduction_tco2"] = float(cum)
    return rows


def optimize_portfolio(
    options: List[Option],
    *,
    target_reduction_tco2: float | None,
    max_capex_eur: float | None,
    discount_rate: float = 0.08,
) -> Dict[str, Any]:
    opts = list(options or [])

    for o in opts:
        o.reduction_tco2 = float(max(0.0, o.reduction_tco2))
        o.capex_eur = float(max(0.0, o.capex_eur))

    opts.sort(key=lambda o: (o.cost_per_tco2(discount_rate=discount_rate), -o.reduction_tco2, str(o.id)))

    target = float(target_reduction_tco2) if target_reduction_tco2 is not None else None
    cap_max = float(max_capex_eur) if max_capex_eur is not None else None

    selected: List[Option] = []
    used_capex = 0.0
    achieved = 0.0

    for o in opts:
        if o.reduction_tco2 <= 0:
            continue
        if cap_max is not None and (used_capex + o.capex_eur) > cap_max + 1e-9:
            continue

        selected.append(o)
        used_capex += o.capex_eur
        achieved += o.reduction_tco2

        if target is not None and achieved >= target - 1e-9:
            break

    portfolio_cost_ann = sum(o.annualized_cost_eur(discount_rate=discount_rate) for o in selected)

    return {
        "target_reduction_tco2": target,
        "max_capex_eur": cap_max,
        "discount_rate": float(discount_rate),
        "selected": [o.to_dict(discount_rate=discount_rate) for o in selected],
        "summary": {
            "selected_count": int(len(selected)),
            "capex_eur": float(used_capex),
            "reduction_tco2": float(achieved),
            "annualized_cost_eur": float(portfolio_cost_ann),
            "avg_cost_per_tco2": float(portfolio_cost_ann / achieved) if achieved > 1e-12 else None,
        },
    }


def build_optimizer_payload(
    *,
    total_tco2: float,
    measures: List[Dict[str, Any]],
    constraints: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    constraints = constraints or {}
    disc = _to_float(constraints.get("discount_rate"), 0.08)
    capex_max = constraints.get("max_capex_eur", None)
    target_pct = constraints.get("target_reduction_pct", None)

    target_t = None
    if target_pct is not None and str(target_pct).strip() != "":
        target_t = float(total_tco2) * _to_float(target_pct, 0.0) / 100.0

    options = build_options_from_measures(measures, total_tco2)
    curve = compute_abatement_curve(options, discount_rate=disc)

    capex_val = None
    if capex_max is not None and str(capex_max).strip() != "":
        capex_val = _to_float(capex_max, 0.0)

    portfolio = optimize_portfolio(
        options,
        target_reduction_tco2=target_t,
        max_capex_eur=capex_val,
        discount_rate=disc,
    )

    return {
        "constraints": {
            "discount_rate": float(disc),
            "target_reduction_pct": target_pct,
            "max_capex_eur": capex_max,
        },
        "abatement_curve": curve,
        "portfolio": portfolio,
    }
