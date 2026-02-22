from __future__ import annotations

from typing import Any


def _to_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def ets_net_and_cost(
    scope1_tco2: float,
    free_alloc_t: float,
    banked_t: float,
    allowance_price_eur_per_t: float,
    fx_tl_per_eur: float,
) -> dict:
    """ETS net & cost (basit finans)."""
    s1 = max(_to_float(scope1_tco2), 0.0)
    free_alloc = max(_to_float(free_alloc_t), 0.0)
    banked = max(_to_float(banked_t), 0.0)

    net = max(s1 - free_alloc - banked, 0.0)
    cost_eur = net * max(_to_float(allowance_price_eur_per_t), 0.0)
    cost_tl = cost_eur * max(_to_float(fx_tl_per_eur), 0.0)

    return {
        "scope1_tco2": s1,
        "free_alloc_tco2": free_alloc,
        "banked_tco2": banked,
        "net_tco2": float(net),
        "cost_eur": float(cost_eur),
        "cost_tl": float(cost_tl),
        "price_eur_per_t": float(allowance_price_eur_per_t),
        "fx_tl_per_eur": float(fx_tl_per_eur),
    }


def ets_verification_payload(
    fuel_rows: list[dict],
    monitoring_plan: dict | None,
    uncertainty_notes: str = "",
) -> dict:
    """ETS MRV yaklaşımı: activity data + QA/QC placeholder + uncertainty.

    fuel_rows: emissions engine'den gelen satırlar.
    monitoring_plan: DB’den okunmuş plan (dict)
    """
    # Activity data summary (fuel-level)
    activity = []
    for r in fuel_rows or []:
        activity.append(
            {
                "fuel_type": r.get("fuel_type"),
                "quantity": r.get("quantity"),
                "unit": r.get("unit"),
                "ncv_gj_per_unit": r.get("ncv_gj_per_unit"),
                "ef_tco2_per_gj": r.get("ef_tco2_per_gj"),
                "oxidation_factor": r.get("oxidation_factor"),
                "tco2": r.get("tco2"),
                "factor_source": r.get("source"),
            }
        )

    return {
        "monitoring_plan": monitoring_plan or {},
        "activity_data": activity,
        "uncertainty": {
            "notes": uncertainty_notes or (
                "Belirsizlik hesapları bu MVP’de placeholder’dır. "
                "Tier metoduna göre ölçüm/aktivite verisi belirsizlikleri eklenmelidir."
            ),
        },
        "qa_qc": {
            "notes": (
                "QA/QC notları bu MVP’de placeholder’dır. "
                "Sayaç kalibrasyonu, fatura mutabakatı, veri onay akışı gibi kontroller eklenmelidir."
            )
        },
    }
