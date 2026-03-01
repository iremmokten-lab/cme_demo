from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Tuple

from src.services.cbam_liability import compute_cbam_liability
from src.config import get_eu_ets_reference_price_eur_per_t


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _to_int(x: Any, default: int | None = None) -> int | None:
    try:
        return int(x)
    except Exception:
        return default


def _period_year(results: Dict[str, Any], config: Dict[str, Any]) -> int | None:
    # Önce results.input_bundle.period.year
    try:
        y = (((results or {}).get("input_bundle") or {}).get("period") or {}).get("year")
        yy = _to_int(y, None)
        if yy:
            return yy
    except Exception:
        pass

    # Sonra config.period.year veya config.year
    try:
        y = (((config or {}).get("period") or {}).get("year"))
        yy = _to_int(y, None)
        if yy:
            return yy
    except Exception:
        pass
    try:
        yy = _to_int((config or {}).get("year"), None)
        if yy:
            return yy
    except Exception:
        pass
    return None


def _fx_tl_per_eur(results: Dict[str, Any], config: Dict[str, Any]) -> float:
    # ETS net_and_cost.fx_tl_per_eur tercih edilir
    try:
        v = (((results or {}).get("cost_outputs") or {}).get("ets") or {}).get("fx_tl_per_eur")
        fx = _to_float(v, 0.0)
        if fx > 0:
            return fx
    except Exception:
        pass

    # Config içinde price.fx_tl_per_eur veya fx_tl_per_eur
    try:
        fx = _to_float((((config or {}).get("price") or {}).get("fx_tl_per_eur")), 0.0)
        if fx > 0:
            return fx
    except Exception:
        pass
    try:
        fx = _to_float((config or {}).get("fx_tl_per_eur"), 0.0)
        if fx > 0:
            return fx
    except Exception:
        pass
    return 0.0


def _eua_price_eur_per_t(results: Dict[str, Any], config: Dict[str, Any]) -> float:
    # CBAM fiyatı için önce results.cbam.eua_price_eur_per_t, sonra ets price, sonra config, sonra default
    try:
        v = (((results or {}).get("cbam") or {}).get("eua_price_eur_per_t"))
        p = _to_float(v, 0.0)
        if p > 0:
            return p
    except Exception:
        pass
    try:
        v = (((results or {}).get("cost_outputs") or {}).get("ets") or {}).get("price_eur_per_t")
        p = _to_float(v, 0.0)
        if p > 0:
            return p
    except Exception:
        pass
    try:
        p = _to_float((((config or {}).get("price") or {}).get("eua_price_eur_per_t")), 0.0)
        if p > 0:
            return p
    except Exception:
        pass
    try:
        p = _to_float((config or {}).get("eua_price_eur_per_t"), 0.0)
        if p > 0:
            return p
    except Exception:
        pass
    return float(get_eu_ets_reference_price_eur_per_t() or 0.0)


def _carbon_price_paid_eur_per_t(results: Dict[str, Any], config: Dict[str, Any]) -> float:
    # CBAM için: config.cbam.carbon_price_paid_eur_per_t veya config.carbon_price_paid_eur_per_t
    try:
        v = (((config or {}).get("cbam") or {}).get("carbon_price_paid_eur_per_t"))
        return max(0.0, _to_float(v, 0.0))
    except Exception:
        pass
    try:
        return max(0.0, _to_float((config or {}).get("carbon_price_paid_eur_per_t"), 0.0))
    except Exception:
        return 0.0


def _safe_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}


@dataclass(frozen=True)
class CarbonCostReport:
    schema: str
    snapshot_id: int
    project_id: int
    year: int | None

    ets: Dict[str, Any]
    cbam: Dict[str, Any]
    totals: Dict[str, Any]
    assumptions: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema": self.schema,
            "snapshot_id": self.snapshot_id,
            "project_id": self.project_id,
            "year": self.year,
            "ets": self.ets,
            "cbam": self.cbam,
            "totals": self.totals,
            "assumptions": self.assumptions,
        }


def compute_carbon_cost_report(
    *,
    snapshot_id: int,
    project_id: int,
    results_json: Dict[str, Any],
    config: Dict[str, Any],
) -> CarbonCostReport:
    """Faz 2: Karbon maliyeti hesap raporu (deterministik).

    Kaynaklar:
    - results_json.cost_outputs.ets: ETS net + cost
    - results_json.cbam.liability: CBAM sertifika gereksinimi ve tahmini ödeme
    - config.cbam.carbon_price_paid_eur_per_t: düşüş için
    - config.price.fx_tl_per_eur: TL karşılığı için
    """
    res = results_json or {}
    cfg = config or {}

    year = _period_year(res, cfg)

    fx = _fx_tl_per_eur(res, cfg)
    eua = _eua_price_eur_per_t(res, cfg)
    paid = _carbon_price_paid_eur_per_t(res, cfg)

    ets_cost = _safe_dict((((res.get("cost_outputs") or {}).get("ets") or {})))
    # Bazı senaryolarda orchestrator ETS üretmemiş olabilir; yine de default
    ets_cost_eur = _to_float(ets_cost.get("cost_eur"), 0.0)
    ets_cost_tl = _to_float(ets_cost.get("cost_tl"), ets_cost_eur * fx if fx > 0 else 0.0)

    cbam_block = _safe_dict(res.get("cbam") or {})
    liab = _safe_dict(cbam_block.get("liability") or {})

    embedded = _to_float(liab.get("embedded_emissions_tco2"), _to_float(cbam_block.get("embedded_emissions_tco2e"), 0.0))
    liab_year = _to_int(liab.get("year"), year)
    liab_year = liab_year if liab_year is not None else year

    # Eğer liability yoksa yeniden hesapla (embedded ve fiyatlardan)
    if not liab and embedded > 0 and (liab_year is not None):
        liab = compute_cbam_liability(
            year=int(liab_year),
            embedded_emissions_tco2=float(embedded),
            eu_ets_price_eur_per_t=float(eua),
            carbon_price_paid_eur_per_t=float(paid),
        ).to_dict()

    cbam_amount_eur = _to_float(liab.get("estimated_payable_amount_eur"), 0.0)
    cbam_amount_tl = cbam_amount_eur * fx if fx > 0 else 0.0
    certs = _to_float(liab.get("certificates_required"), 0.0)

    totals_eur = float(ets_cost_eur + cbam_amount_eur)
    totals_tl = float(ets_cost_tl + cbam_amount_tl)

    assumptions = {
        "eua_price_eur_per_t": float(eua),
        "fx_tl_per_eur": float(fx),
        "carbon_price_paid_eur_per_t": float(paid),
        "notes_tr": [
            "ETS maliyeti, scope1 emisyon - ücretsiz tahsis - banked üzerinden net hesaplanır.",
            "CBAM (2026+) için payable share kuralı uygulanır; ödenmiş karbon fiyatı varsa düşüş yapılır.",
            "Bu rapor tahmini maliyet raporudur; resmi ödeme süreçleri otorite kurallarına tabidir.",
        ],
    }

    cbam_out = {
        "liability": liab,
        "certificates_required": float(certs),
        "estimated_payable_amount_eur": float(cbam_amount_eur),
        "estimated_payable_amount_tl": float(cbam_amount_tl),
    }

    ets_out = dict(ets_cost)
    ets_out.setdefault("cost_eur", float(ets_cost_eur))
    ets_out.setdefault("cost_tl", float(ets_cost_tl))

    return CarbonCostReport(
        schema="carbon_cost_report.v1",
        snapshot_id=int(snapshot_id),
        project_id=int(project_id),
        year=int(year) if year is not None else None,
        ets=ets_out,
        cbam=cbam_out,
        totals={"total_cost_eur": totals_eur, "total_cost_tl": totals_tl},
        assumptions=assumptions,
    )


def compare_carbon_cost(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """İki carbon cost raporu arasındaki farkı verir: B - A."""
    a = a or {}
    b = b or {}

    def gf(d: Dict[str, Any], path: Tuple[str, ...]) -> float:
        cur: Any = d
        for p in path:
            if not isinstance(cur, dict):
                return 0.0
            cur = cur.get(p)
        return _to_float(cur, 0.0)

    out = {
        "schema": "carbon_cost_compare.v1",
        "a_snapshot_id": a.get("snapshot_id"),
        "b_snapshot_id": b.get("snapshot_id"),
        "diff": {
            "ets_cost_eur": gf(b, ("ets", "cost_eur")) - gf(a, ("ets", "cost_eur")),
            "cbam_cost_eur": gf(b, ("cbam", "estimated_payable_amount_eur")) - gf(a, ("cbam", "estimated_payable_amount_eur")),
            "total_cost_eur": gf(b, ("totals", "total_cost_eur")) - gf(a, ("totals", "total_cost_eur")),
            "ets_cost_tl": gf(b, ("ets", "cost_tl")) - gf(a, ("ets", "cost_tl")),
            "cbam_cost_tl": gf(b, ("cbam", "estimated_payable_amount_tl")) - gf(a, ("cbam", "estimated_payable_amount_tl")),
            "total_cost_tl": gf(b, ("totals", "total_cost_tl")) - gf(a, ("totals", "total_cost_tl")),
        },
    }
    return out
