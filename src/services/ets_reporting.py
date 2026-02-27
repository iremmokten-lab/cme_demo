from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _s(x: Any) -> str:
    return "" if x is None else str(x)


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


def build_ets_reporting_structure(
    *,
    period: dict,
    installation: dict,
    monitoring_plan_ref: dict | None,
    energy_breakdown: dict,
    price_ref: dict | None,
    config: dict,
) -> Dict[str, Any]:
    """EU ETS — MRR (2018/2066) raporlama yapısı (MVP → reg-grade JSON).

    İçerik:
      - activity data (fuel/electricity rows) : hesaplamaya giden temel girdiler
      - source streams summary (fuel types, ölçüm birimleri)
      - uncertainty + tier evidence alanları (monitoring plan + config notları)
    """
    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    fuel_rows = list(energy_breakdown.get("fuel_rows", []) or [])
    elec_rows = list(energy_breakdown.get("electricity_rows", []) or [])

    # source streams summary
    streams = []
    seen = set()
    for r in fuel_rows:
        if not isinstance(r, dict):
            continue
        ft = _s(r.get("fuel_type") or r.get("fuel") or "").strip()
        unit = _s(r.get("unit") or r.get("quantity_unit") or "").strip()
        key = (ft, unit)
        if key in seen:
            continue
        seen.add(key)
        streams.append(
            {
                "stream_type": "fuel",
                "fuel_type": ft,
                "unit": unit,
                "measurement_method": _s(r.get("measurement_method") or ""),
                "factor_sources": {
                    "ncv": _s((r.get("factor_meta") or {}).get("ncv_source") or ""),
                    "ef": _s((r.get("factor_meta") or {}).get("ef_source") or ""),
                },
            }
        )

    if elec_rows:
        streams.append(
            {
                "stream_type": "electricity",
                "measurement_method": _s(((config or {}).get("electricity_method") or "location")),
                "grid_factor": _s(((config or {}).get("market_grid_factor_override") or "")),
            }
        )

    # uncertainty/tier meta
    mp = monitoring_plan_ref or {}
    tier_level = _s(mp.get("tier_level") or "")
    method = _s(mp.get("method") or "")

    uncertainty = {
        "uncertainty_notes": _s((config or {}).get("uncertainty_notes") or ""),
        "tier_level": tier_level,
        "method": method,
        "evidence_expectations": [
            "Sayaç kalibrasyon sertifikaları (meter_readings)",
            "Yakıt satın alma faturaları (invoices)",
            "QA/QC prosedürü (documents)",
        ],
    }

    totals = {
        "direct_tco2": float(energy_breakdown.get("direct_tco2", 0.0) or 0.0),
        "indirect_tco2": float(energy_breakdown.get("indirect_tco2", 0.0) or 0.0),
        "total_tco2": float(energy_breakdown.get("total_tco2", 0.0) or 0.0),
    }

    report = {
        "ets_mrr": "2018/2066",
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
        "monitoring_plan_ref": mp if isinstance(mp, dict) else None,
        "activity_data": {
            "fuel_rows": fuel_rows,
            "electricity_rows": elec_rows,
        },
        "source_streams": streams,
        "uncertainty_and_tiers": uncertainty,
        "price_ref": price_ref if isinstance(price_ref, dict) else None,
        "totals": totals,
        "notes": [
            "Bu çıktı ETS MRR raporlamasına yönelik JSON hazırlar. Resmi ETS rapor formatı için mapping yapılabilir.",
            "Fuel rows energy_emissions() çıktısından gelir; factor_set_ref ile deterministik kilitlenir.",
        ],
    }
    return reportfrom __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _s(x: Any) -> str:
    return "" if x is None else str(x)


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


def build_ets_reporting_structure(
    *,
    period: dict,
    installation: dict,
    monitoring_plan_ref: dict | None,
    energy_breakdown: dict,
    price_ref: dict | None,
    config: dict,
) -> Dict[str, Any]:
    """EU ETS — MRR (2018/2066) raporlama yapısı (MVP → reg-grade JSON).

    İçerik:
      - activity data (fuel/electricity rows) : hesaplamaya giden temel girdiler
      - source streams summary (fuel types, ölçüm birimleri)
      - uncertainty + tier evidence alanları (monitoring plan + config notları)
    """
    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    fuel_rows = list(energy_breakdown.get("fuel_rows", []) or [])
    elec_rows = list(energy_breakdown.get("electricity_rows", []) or [])

    # source streams summary
    streams = []
    seen = set()
    for r in fuel_rows:
        if not isinstance(r, dict):
            continue
        ft = _s(r.get("fuel_type") or r.get("fuel") or "").strip()
        unit = _s(r.get("unit") or r.get("quantity_unit") or "").strip()
        key = (ft, unit)
        if key in seen:
            continue
        seen.add(key)
        streams.append(
            {
                "stream_type": "fuel",
                "fuel_type": ft,
                "unit": unit,
                "measurement_method": _s(r.get("measurement_method") or ""),
                "factor_sources": {
                    "ncv": _s((r.get("factor_meta") or {}).get("ncv_source") or ""),
                    "ef": _s((r.get("factor_meta") or {}).get("ef_source") or ""),
                },
            }
        )

    if elec_rows:
        streams.append(
            {
                "stream_type": "electricity",
                "measurement_method": _s(((config or {}).get("electricity_method") or "location")),
                "grid_factor": _s(((config or {}).get("market_grid_factor_override") or "")),
            }
        )

    # uncertainty/tier meta
    mp = monitoring_plan_ref or {}
    tier_level = _s(mp.get("tier_level") or "")
    method = _s(mp.get("method") or "")

    uncertainty = {
        "uncertainty_notes": _s((config or {}).get("uncertainty_notes") or ""),
        "tier_level": tier_level,
        "method": method,
        "evidence_expectations": [
            "Sayaç kalibrasyon sertifikaları (meter_readings)",
            "Yakıt satın alma faturaları (invoices)",
            "QA/QC prosedürü (documents)",
        ],
    }

    totals = {
        "direct_tco2": float(energy_breakdown.get("direct_tco2", 0.0) or 0.0),
        "indirect_tco2": float(energy_breakdown.get("indirect_tco2", 0.0) or 0.0),
        "total_tco2": float(energy_breakdown.get("total_tco2", 0.0) or 0.0),
    }

    report = {
        "ets_mrr": "2018/2066",
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
        "monitoring_plan_ref": mp if isinstance(mp, dict) else None,
        "activity_data": {
            "fuel_rows": fuel_rows,
            "electricity_rows": elec_rows,
        },
        "source_streams": streams,
        "uncertainty_and_tiers": uncertainty,
        "price_ref": price_ref if isinstance(price_ref, dict) else None,
        "totals": totals,
        "notes": [
            "Bu çıktı ETS MRR raporlamasına yönelik JSON hazırlar. Resmi ETS rapor formatı için mapping yapılabilir.",
            "Fuel rows energy_emissions() çıktısından gelir; factor_set_ref ile deterministik kilitlenir.",
        ],
    }
    return report
