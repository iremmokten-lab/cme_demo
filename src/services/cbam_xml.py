from __future__ import annotations

"""CBAM Transitional (EU 2023/1773) — XML-ready reporting (deterministic).

Bu modül:
  - results_json.cbam_reporting altında XML'e çevrilebilir deterministik bir JSON structure üretir
  - JSON -> XML dönüşümünü yapar (XML exporter)

Önemli:
  - Repo içinde resmi CBAM XML şeması (XSD) olmadığı için bu çıktı 'mapping-ready' tasarlanmıştır.
  - XSD doğrulaması ve resmi şema birebir uyumu Step-4'te eklenir.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List
import xml.etree.ElementTree as ET


def _s(x: Any) -> str:
    return "" if x is None else str(x)


def _f(x: Any, digits: int = 6) -> str:
    try:
        return f"{float(x):.{digits}f}"
    except Exception:
        return f"{0.0:.{digits}f}"


def build_cbam_reporting(
    *,
    period: dict,
    declarant: dict,
    installation: dict,
    cbam_table: List[dict],
    methodology_note_tr: str = "",
) -> Dict[str, Any]:
    """
    Step-3 CBAM engine çıktısını CBAM reporting JSON formatına getirir.
    """
    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    goods: List[Dict[str, Any]] = []
    for r in cbam_table or []:
        if not isinstance(r, dict):
            continue
        qty_unit = _s(r.get("quantity_unit") or "t")
        goods.append(
            {
                "sku": _s(r.get("sku")),
                "cn_code": _s(r.get("cn_code")),
                "goods_description": _s(r.get("cbam_good")),
                "cbam_good_key": _s(r.get("cbam_good_key") or ""),
                "cbam_covered": bool(r.get("cbam_covered")) if r.get("cbam_covered") is not None else False,
                "produced_quantity": float(r.get("quantity") or 0.0),
                "produced_quantity_unit": qty_unit,
                "eu_import_quantity": float(r.get("export_to_eu_quantity") or 0.0),
                "eu_import_unit": qty_unit,
                "direct_emissions_tco2e": float(r.get("direct_emissions_tco2e") or r.get("direct_alloc_tco2") or 0.0),
                "indirect_emissions_tco2e": float(r.get("indirect_emissions_tco2e") or r.get("indirect_alloc_tco2") or 0.0),
                "precursor_emissions_tco2e": float(r.get("precursor_tco2e") or r.get("precursor_tco2") or 0.0),
                "embedded_emissions_tco2e": float(r.get("embedded_emissions_tco2e") or r.get("embedded_tco2") or 0.0),
                "direct_intensity_tco2e_per_unit": float(r.get("direct_intensity_tco2_per_unit") or 0.0),
                "indirect_intensity_tco2e_per_unit": float(r.get("indirect_intensity_tco2_per_unit") or 0.0),
                "embedded_intensity_tco2e_per_unit": float(r.get("embedded_intensity_tco2_per_unit") or 0.0),
                "data_type_flag": _s(r.get("data_type_flag") or "ACTUAL"),
                "default_value_evidence_hash": _s(r.get("default_value_evidence_hash") or ""),
                "export_share": float(r.get("export_share") or 0.0),
                "cbam_cost_signal_eur": float(r.get("cbam_cost_eur") or 0.0),
                "carbon_price_paid_eur_per_t": float(r.get("carbon_price_paid_eur_per_t") or 0.0),
                "certificates_required": float(r.get("certificates_required") or 0.0),
                "estimated_payable_amount_eur": float(r.get("estimated_payable_amount_eur") or 0.0),
                "mapping_rule": _s(r.get("mapping_rule") or ""),
                "allocation_method": _s(r.get("allocation_method") or ""),
                "allocation_hash": _s(r.get("allocation_hash") or ""),
            }
        )

    goods.sort(key=lambda x: (x.get("cn_code", ""), x.get("sku", "")))

    report = {
        "schema": "cbam_reporting_v2",
        "generated_at_utc": now_utc,
        "period": period or {},
        "declarant": declarant or {},
        "installation": installation or {},
        "methodology_note_tr": methodology_note_tr or "",
        "goods": goods,
    }
    return report


def cbam_reporting_json_to_xml(report: Dict[str, Any]) -> str:
    """
    Deterministic JSON -> XML export (mapping-ready).
    Step-4'te XSD uyumu için element/namespace yapısı resmi şemaya göre güncellenecek.
    """
    report = report or {}
    root = ET.Element("CBAMReport")
    root.set("schema", _s(report.get("schema") or "cbam_reporting_v2"))
    root.set("generated_at_utc", _s(report.get("generated_at_utc") or ""))

    period = report.get("period") or {}
    period_el = ET.SubElement(root, "Period")
    for k in ("year", "quarter", "start_date", "end_date"):
        if k in period and period.get(k) is not None:
            ET.SubElement(period_el, k).text = _s(period.get(k))

    decl = report.get("declarant") or {}
    decl_el = ET.SubElement(root, "Declarant")
    for k, v in sorted(decl.items(), key=lambda x: x[0]):
        ET.SubElement(decl_el, k).text = _s(v)

    inst = report.get("installation") or {}
    inst_el = ET.SubElement(root, "Installation")
    for k, v in sorted(inst.items(), key=lambda x: x[0]):
        ET.SubElement(inst_el, k).text = _s(v)

    if report.get("methodology_note_tr"):
        ET.SubElement(root, "MethodologyNoteTR").text = _s(report.get("methodology_note_tr"))

    goods_el = ET.SubElement(root, "GoodsList")
    for g in report.get("goods") or []:
        ge = ET.SubElement(goods_el, "Goods")
        # deterministic key order
        keys = [
            "sku",
            "cn_code",
            "goods_description",
            "cbam_good_key",
            "cbam_covered",
            "produced_quantity",
            "produced_quantity_unit",
            "eu_import_quantity",
            "eu_import_unit",
            "direct_emissions_tco2e",
            "indirect_emissions_tco2e",
            "precursor_emissions_tco2e",
            "embedded_emissions_tco2e",
            "direct_intensity_tco2e_per_unit",
            "indirect_intensity_tco2e_per_unit",
            "embedded_intensity_tco2e_per_unit",
            "data_type_flag",
            "default_value_evidence_hash",
            "export_share",
            "cbam_cost_signal_eur",
            "carbon_price_paid_eur_per_t",
            "certificates_required",
            "estimated_payable_amount_eur",
            "mapping_rule",
            "allocation_method",
            "allocation_hash",
        ]
        for k in keys:
            if k not in g:
                continue
            v = g.get(k)
            if isinstance(v, bool):
                ET.SubElement(ge, k).text = "true" if v else "false"
            elif isinstance(v, (int, float)):
                ET.SubElement(ge, k).text = _f(v, 6)
            else:
                ET.SubElement(ge, k).text = _s(v)

    xml_bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    return xml_bytes.decode("utf-8")
