from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List


def _s(x: Any) -> str:
    return "" if x is None else str(x)


def _f(x: Any, digits: int = 6) -> str:
    try:
        return f"{float(x):.{digits}f}"
    except Exception:
        return "0.000000"


def build_cbam_reporting_structure(
    *,
    period: dict,
    declarant: dict,
    installation: dict,
    cbam_table_rows: List[dict],
    allocation_meta: dict | None = None,
    methodology_note_tr: str = "",
) -> Dict[str, Any]:
    """
    CBAM Transitional (EU 2023/1773) için XML-ready rapor yapısı.

    Step-3 ile:
      - data_type_flag (ACTUAL/DEFAULT)
      - intensity alanları
      - default_value_evidence_hash
      - definitive regime tahmin alanları (certificates_required vb.)
    eklenir.

    Bu JSON yapısı deterministik olarak XML'e dönüştürülebilir.
    """
    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    alloc_method = None
    alloc_hash = None
    if isinstance(allocation_meta, dict):
        alloc_method = allocation_meta.get("allocation_method")
        alloc_hash = allocation_meta.get("allocation_hash")

    goods: List[Dict[str, Any]] = []
    for r in cbam_table_rows or []:
        if not isinstance(r, dict):
            continue
        qty_unit = _s(r.get("quantity_unit") or "t")
        goods.append(
            {
                "sku": _s(r.get("sku")),
                "cn_code": _s(r.get("cn_code")),
                "goods_description": _s(r.get("cbam_good") or r.get("goods") or ""),
                "cbam_good_key": _s(r.get("cbam_good_key") or ""),
                "cbam_covered": bool(r.get("cbam_covered")) if r.get("cbam_covered") is not None else False,
                "produced_quantity": float(r.get("quantity") or 0.0),
                "produced_quantity_unit": qty_unit,
                "eu_import_quantity": float(r.get("export_to_eu_quantity") or 0.0),
                "eu_import_unit": qty_unit,
                # emissions (explicit naming)
                "direct_emissions_tco2e": float(r.get("direct_emissions_tco2e") or r.get("direct_alloc_tco2") or 0.0),
                "indirect_emissions_tco2e": float(r.get("indirect_emissions_tco2e") or r.get("indirect_alloc_tco2") or 0.0),
                "precursor_emissions_tco2e": float(r.get("precursor_tco2e") or r.get("precursor_tco2") or 0.0),
                "embedded_emissions_tco2e": float(r.get("embedded_emissions_tco2e") or r.get("embedded_tco2") or 0.0),
                # intensity (per unit)
                "direct_intensity_tco2e_per_unit": float(r.get("direct_intensity_tco2_per_unit") or 0.0),
                "indirect_intensity_tco2e_per_unit": float(r.get("indirect_intensity_tco2_per_unit") or 0.0),
                "embedded_intensity_tco2e_per_unit": float(r.get("embedded_intensity_tco2_per_unit") or 0.0),
                # reporting flag
                "data_type_flag": _s(r.get("data_type_flag") or r.get("method_flag") or "ACTUAL"),
                # default evidence pin
                "default_value_evidence_hash": _s(r.get("default_value_evidence_hash") or ""),
                # indicative cost / liability fields
                "export_share": float(r.get("export_share") or 0.0),
                "cbam_cost_signal_eur": float(r.get("cbam_cost_eur") or r.get("cbam_cost_signal_eur") or 0.0),
                "carbon_price_paid_eur_per_t": float(r.get("carbon_price_paid_eur_per_t") or 0.0),
                "certificates_required": float(r.get("certificates_required") or 0.0),
                "estimated_payable_amount_eur": float(r.get("estimated_payable_amount_eur") or 0.0),
                # provenance
                "mapping_rule": _s(r.get("mapping_rule") or ""),
                "allocation_method": _s(r.get("allocation_method") or alloc_method or ""),
                "allocation_hash": _s(r.get("allocation_hash") or alloc_hash or ""),
            }
        )

    # deterministik sıralama
    goods.sort(key=lambda x: (x.get("cn_code", ""), x.get("sku", ""), x.get("goods_description", "")))

    totals = {
        "direct_emissions_tco2e": float(sum(float(g.get("direct_emissions_tco2e") or 0.0) for g in goods)),
        "indirect_emissions_tco2e": float(sum(float(g.get("indirect_emissions_tco2e") or 0.0) for g in goods)),
        "precursor_emissions_tco2e": float(sum(float(g.get("precursor_emissions_tco2e") or 0.0) for g in goods)),
        "embedded_emissions_tco2e": float(sum(float(g.get("embedded_emissions_tco2e") or 0.0) for g in goods)),
        "cbam_cost_signal_eur": float(sum(float(g.get("cbam_cost_signal_eur") or 0.0) for g in goods)),
        "certificates_required": float(sum(float(g.get("certificates_required") or 0.0) for g in goods)),
        "estimated_payable_amount_eur": float(sum(float(g.get("estimated_payable_amount_eur") or 0.0) for g in goods)),
        "allocation_method": _s(alloc_method or ""),
        "allocation_hash": _s(alloc_hash or ""),
    }

    report = {
        "schema": "cbam_reporting_structure_v2",
        "generated_at_utc": now_utc,
        "period": period or {},
        "declarant": declarant or {},
        "installation": installation or {},
        "methodology_note_tr": methodology_note_tr or "",
        "goods": goods,
        "totals": totals,
    }
    return report
