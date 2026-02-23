from __future__ import annotations

from typing import Any, Dict, List


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
    """
    ETS net & cost (finansal görünüm).
    - scope1_tco2: direct emissions (fuel combustion) tCO2
    - free_alloc_t: ücretsiz tahsis tCO2
    - banked_t: devreden / banked tCO2
    """
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
        "price_eur_per_t": float(_to_float(allowance_price_eur_per_t)),
        "fx_tl_per_eur": float(_to_float(fx_tl_per_eur)),
    }


def ets_verification_payload(
    fuel_rows: List[dict],
    monitoring_plan: dict | None,
    uncertainty_notes: str = "",
) -> dict:
    """
    Paket D2: ETS “verification-ready” payload (MVP)

    - Fuel structure (activity data) satır bazında:
        fuel_type, quantity, unit, ncv, ef, of, tco2, factor_source
    - MonitoringPlan zorunlu bağ:
        tier, method, data source, QA procedure, responsible
      (UI tarafında tesis seçimi ile plan DB’den geliyor)
    - Rapora girecek alanlar:
        activity data, uncertainty notes, QA/QC summary
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

    mp = monitoring_plan or {}

    # Monitoring plan minimum fields (verification friendly)
    mp_min = {
        "id": mp.get("id"),
        "facility_id": mp.get("facility_id"),
        "method": mp.get("method") or "standard",
        "tier_level": mp.get("tier_level") or "Tier 2",
        "data_source": mp.get("data_source") or "",
        "qa_procedure": mp.get("qa_procedure") or "",
        "responsible_person": mp.get("responsible_person") or "",
        "updated_at": mp.get("updated_at"),
    }

    uncertainty = {
        "notes": (uncertainty_notes or "").strip()
        or "Belirsizlik notu girilmedi. Verification için tier metoduna göre ölçüm/aktivite belirsizlikleri eklenmelidir.",
        "status": "MVP",
    }

    qa_qc = {
        "notes": (mp_min.get("qa_procedure") or "").strip()
        or (
            "QA/QC prosedürü girilmedi. Verification için sayaç kalibrasyonu, fatura mutabakatı, veri onay akışı ve değişiklik yönetimi tanımlanmalıdır."
        ),
        "status": "MVP",
    }

    return {
        "monitoring_plan": mp_min,
        "activity_data": activity,
        "uncertainty": uncertainty,
        "qa_qc": qa_qc,
        "verification_notes": [
            "Bu çıktı ETS verification-ready formatına yaklaşmak için tasarlanmış MVP’dir.",
            "Resmî raporlama için doğrulama kapsamı, ölçüm sistemleri, belirsizlik hesapları ve kontrol prosedürleri detaylandırılmalıdır.",
        ],
    }
