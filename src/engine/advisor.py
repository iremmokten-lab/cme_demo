from __future__ import annotations

"""Phase 3 — Reduction Advisor (heuristic, evidence-aware)

Amaç:
- Hotspot analizi: emisyonların en büyük kaynakları
- Öneri seti: reduction measures + gerekçe + hangi evidence beklenir

Not:
- Bu demo sürümünde öneriler deterministik, rule-based.
- Faz 3.2'de "evidence-backed" yaklaşım için: evidence categories + gaps.
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


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


@dataclass
class Measure:
    id: str
    title: str
    description: str
    category: str
    expected_reduction_pct_of_total: float
    capex_eur: float
    opex_delta_eur_per_year: float
    evidence_needed: List[str]
    assumptions: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "category": self.category,
            "expected_reduction_pct_of_total": self.expected_reduction_pct_of_total,
            "capex_eur": self.capex_eur,
            "opex_delta_eur_per_year": self.opex_delta_eur_per_year,
            "evidence_needed": self.evidence_needed,
            "assumptions": self.assumptions,
        }


def _hotspots_from_energy(energy_breakdown: Dict[str, Any]) -> Dict[str, Any]:
    fuel_rows = (energy_breakdown or {}).get("fuel_rows") or []
    elec_rows = (energy_breakdown or {}).get("electricity_rows") or []

    by_fuel: Dict[str, float] = {}
    for r in fuel_rows:
        if not isinstance(r, dict):
            continue
        ft = _norm(r.get("fuel_type")) or "other"
        by_fuel[ft] = by_fuel.get(ft, 0.0) + _to_float(r.get("tco2"), 0.0)

    elec_t = 0.0
    for r in elec_rows:
        if not isinstance(r, dict):
            continue
        elec_t += _to_float(r.get("tco2"), 0.0)

    total_direct = sum(by_fuel.values())
    total = total_direct + elec_t

    fuels_sorted = sorted(by_fuel.items(), key=lambda x: x[1], reverse=True)

    return {
        "direct_total_tco2": float(total_direct),
        "indirect_total_tco2": float(elec_t),
        "total_tco2": float(total),
        "by_fuel_tco2": [{"fuel_type": k, "tco2": float(v)} for k, v in fuels_sorted],
    }


def build_reduction_advice(
    *,
    kpis: Dict[str, Any],
    energy_breakdown: Dict[str, Any],
    cbam: Dict[str, Any],
    evidence_categories_present: List[str] | None = None,
) -> Dict[str, Any]:
    """Snapshot results -> advisor payload."""

    total_tco2 = _to_float((kpis or {}).get("total_tco2", 0.0), 0.0)
    if total_tco2 <= 0:
        return {
            "hotspots": {},
            "measures": [],
            "notes": ["Toplam emisyon 0; öneriler üretilemedi."],
        }

    hotspots = _hotspots_from_energy(energy_breakdown or {})

    # evidence gap awareness
    ev = set([_norm(x) for x in (evidence_categories_present or []) if x])

    def ev_need(*cats: str) -> List[str]:
        return [c for c in cats]

    measures: List[Measure] = []

    # Determine dominant source
    direct = hotspots.get("direct_total_tco2", 0.0) or 0.0
    indirect = hotspots.get("indirect_total_tco2", 0.0) or 0.0
    direct_share = float(direct) / float(total_tco2) if total_tco2 > 0 else 0.0
    indirect_share = float(indirect) / float(total_tco2) if total_tco2 > 0 else 0.0

    # Electricity efficiency
    if indirect_share >= 0.15:
        measures.append(
            Measure(
                id="m_elec_eff",
                title="Elektrik verimliliği (motor/kompresör/VFD/kaçak)",
                description="Elektrik tüketimini süreç bazlı analiz edip verimlilik yatırımları ile azaltın.",
                category="energy_efficiency",
                expected_reduction_pct_of_total=6.0 if indirect_share < 0.35 else 10.0,
                capex_eur=25000.0,
                opex_delta_eur_per_year=-8000.0,
                evidence_needed=ev_need("energy_bills", "metering", "equipment_specs"),
                assumptions={"applies_if_indirect_share_ge": 0.15},
            )
        )

    # Heat recovery / boiler optimization
    if direct_share >= 0.20:
        measures.append(
            Measure(
                id="m_boiler_opt",
                title="Kazan/yanma optimizasyonu + ısı geri kazanım",
                description="Yanma ayarı, economizer, kondens geri dönüşü ve ısı geri kazanım ile yakıt tüketimini düşürün.",
                category="process_efficiency",
                expected_reduction_pct_of_total=8.0 if direct_share < 0.45 else 12.0,
                capex_eur=60000.0,
                opex_delta_eur_per_year=-12000.0,
                evidence_needed=ev_need("fuel_invoices", "boiler_logs", "maintenance"),
                assumptions={"applies_if_direct_share_ge": 0.20},
            )
        )

    # Fuel switch (demo)
    by_fuel = hotspots.get("by_fuel_tco2") or []
    top_fuels = [r.get("fuel_type") for r in by_fuel[:2] if isinstance(r, dict)]
    if any(_norm(f) in ("coal", "komur", "kömür", "lignite", "linyit") for f in top_fuels):
        measures.append(
            Measure(
                id="m_fuel_switch",
                title="Kömürden düşük karbonlu yakıta geçiş",
                description="Kömür/linyit kullanımını azaltıp doğal gaz/biomass/elektrifikasyon gibi seçenekleri değerlendirin.",
                category="fuel_switch",
                expected_reduction_pct_of_total=15.0,
                capex_eur=180000.0,
                opex_delta_eur_per_year=20000.0,
                evidence_needed=ev_need("fuel_invoices", "process_diagrams", "capex_quotes"),
                assumptions={"detected_top_fuels": top_fuels},
            )
        )

    # CBAM-related: precursor emissions
    cbam_totals = (cbam or {}).get("totals") or {}
    precursor = _to_float(cbam_totals.get("precursor_tco2", 0.0), 0.0)
    if precursor > 0 and precursor / total_tco2 >= 0.10:
        measures.append(
            Measure(
                id="m_precursor_supplier",
                title="Precursor/Supplier emisyon düşürme",
                description="Tedarikçilerden ürün bazlı EPD/verification talep ederek precursor embedded emissions azaltın.",
                category="supply_chain",
                expected_reduction_pct_of_total=5.0,
                capex_eur=10000.0,
                opex_delta_eur_per_year=0.0,
                evidence_needed=ev_need("supplier_epd", "contracts", "materials"),
                assumptions={"precursor_share_ge": 0.10},
            )
        )

    # Generic measure
    measures.append(
        Measure(
            id="m_data_improve",
            title="Ölçüm ve veri kalitesi iyileştirme",
            description="Ölçüm noktaları/metering iyileştirmesi ve veri doğrulama süreçleri ile belirsizliği azaltın.",
            category="mrv",
            expected_reduction_pct_of_total=0.0,
            capex_eur=15000.0,
            opex_delta_eur_per_year=2000.0,
            evidence_needed=ev_need("metering", "calibration", "procedures"),
            assumptions={},
        )
    )

    # Evidence gap hints
    missing = []
    if ev:
        needed = set()
        for m in measures:
            needed.update([_norm(x) for x in (m.evidence_needed or [])])
        missing = sorted([x for x in needed if x and x not in ev])

    return {
        "hotspots": hotspots,
        "measures": [m.to_dict() for m in measures],
        "evidence_missing_categories": missing,
        "notes": [],
    }
