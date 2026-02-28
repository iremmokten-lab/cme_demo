from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional


@dataclass
class TRETSReporting:
    schema: str
    year: int
    facility: Dict[str, Any]
    scope: Dict[str, Any]
    activity_data: Dict[str, Any]
    emission_sources: List[Dict[str, Any]]
    factors: List[Dict[str, Any]]
    totals: Dict[str, Any]
    allocation: Dict[str, Any]
    compliance: Dict[str, Any]
    verification: Dict[str, Any]
    qa_qc: Dict[str, Any]
    references: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema": self.schema,
            "year": self.year,
            "facility": self.facility,
            "scope": self.scope,
            "activity_data": self.activity_data,
            "emission_sources": self.emission_sources,
            "factors": self.factors,
            "totals": self.totals,
            "allocation": self.allocation,
            "compliance": self.compliance,
            "verification": self.verification,
            "qa_qc": self.qa_qc,
            "references": self.references,
        }


def _sum(rows: List[Dict[str, Any]], key: str) -> float:
    s = 0.0
    for r in rows or []:
        try:
            s += float(r.get(key) or 0.0)
        except Exception:
            s += 0.0
    return float(s)


def build_tr_ets_reporting(
    *,
    year: int,
    facility: Dict[str, Any],
    energy_breakdown_rows: List[Dict[str, Any]],
    electricity_rows: List[Dict[str, Any]],
    factor_refs: List[Dict[str, Any]],
    verified_total_tco2: float,
    in_scope_threshold_tco2: float = 50000.0,
    pilot_start_year: int = 2026,
    pilot_end_year: int = 2027,
) -> TRETSReporting:
    """TR ETS reporting dataset (draft-aligned).

    Notes:
    - TR ETS is designed to be integrated with MRV. This dataset mirrors MRV content:
      monitoring plan -> activity data -> factors -> totals -> verification.
    - In pilot period (default 2026-2027) draft summary indicates 100% free allocation by benchmarking;
      we model free_allocation_t = verified_total_tco2 for pilot years, else 0 unless provided externally.
    """
    y = int(year)
    total_direct = _sum(energy_breakdown_rows, "tco2")
    total_indirect = _sum(electricity_rows, "tco2")
    total = float(verified_total_tco2 or (total_direct + total_indirect))

    # scope heuristic: capacity threshold is an eligibility criterion; we use emissions as proxy unless capacity is known.
    in_scope = total >= float(in_scope_threshold_tco2 or 50000.0)

    # allocation
    is_pilot = pilot_start_year <= y <= pilot_end_year
    free_allocation_t = total if is_pilot and in_scope else 0.0

    required_surrender_t = total if in_scope else 0.0
    net_purchase_t = max(0.0, required_surrender_t - free_allocation_t)

    emission_sources = []
    for r in energy_breakdown_rows or []:
        emission_sources.append(
            {
                "type": "combustion",
                "fuel_type": r.get("fuel_type"),
                "month": r.get("month"),
                "quantity": r.get("quantity"),
                "unit": r.get("unit"),
                "tco2": r.get("tco2"),
                "gj": r.get("gj"),
            }
        )
    for r in electricity_rows or []:
        emission_sources.append(
            {
                "type": "electricity",
                "method": r.get("method"),
                "month": r.get("month"),
                "mwh": r.get("mwh"),
                "grid_factor_tco2_per_mwh": r.get("grid_factor_tco2_per_mwh"),
                "tco2": r.get("tco2"),
            }
        )

    reporting = TRETSReporting(
        schema="tr_ets_reporting.v1",
        year=y,
        facility=facility or {},
        scope={
            "tr_ets_mode": True,
            "in_scope": in_scope,
            "threshold_tco2": float(in_scope_threshold_tco2 or 50000.0),
            "pilot_period": {"start_year": int(pilot_start_year), "end_year": int(pilot_end_year)},
            "is_pilot_year": bool(is_pilot),
        },
        activity_data={
            "energy_rows_count": len(energy_breakdown_rows or []),
            "electricity_rows_count": len(electricity_rows or []),
        },
        emission_sources=emission_sources,
        factors=factor_refs or [],
        totals={
            "direct_tco2": float(total_direct),
            "indirect_tco2": float(total_indirect),
            "verified_total_tco2": float(total),
        },
        allocation={
            "method": "benchmarking",
            "free_allocation_t": float(free_allocation_t),
            "notes": "Pilot dönemde (2026-2027) taslak özetine göre %100 ücretsiz tahsisat varsayılmıştır.",
        },
        compliance={
            "required_surrender_t": float(required_surrender_t),
            "net_purchase_t": float(net_purchase_t),
            "status": "in_scope" if in_scope else "out_of_scope",
        },
        verification={
            "status": "required" if in_scope else "not_required",
            "channel": "MEDAS",
            "evidence_required": True if in_scope else False,
        },
        qa_qc={
            "qa_checks": [],
            "qc_notes": "",
        },
        references={
            "mrp_mrv_reference": "Sera Gazı Emisyonlarının Takibi Hakkında Yönetmelik (17.05.2014 / RG 29003)",
            "verification_reference": "MEDAS (Merkezi Elektronik Doğrulayıcı Kuruluş Atama Sistemi)",
            "tr_ets_draft_reference": "Türkiye Emisyon Ticaret Sistemi Yönetmeliği Taslağı (İklim Değişikliği Başkanlığı, 22.07.2025 duyurusu)",
        },
    )
    return reporting
src/services/tr_ets.py
