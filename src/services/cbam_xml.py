from __future__ import annotations

"""CBAM Transitional (2023/1773) — XML-ready reporting.

Bu modül:
  - results_json.cbam_reporting altında XML'e çevrilebilir deterministik bir JSON structure üretir
  - JSON -> XML dönüşümünü yapar (XML exporter)

Not:
  - Repo içinde resmi CBAM XML şeması (XSD) olmadığı için bu çıktı 'mapping-ready' tasarlanmıştır.
  - Evidence pack export içinde cbam_report.json + cbam_report.xml olarak paketlenir.
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
    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    goods: List[Dict[str, Any]] = []
    for r in cbam_table or []:
        if not isinstance(r, dict):
            continue
        goods.append(
            {
                "sku": _s(r.get("sku")),
                "cn_code": _s(r.get("cn_code")),
                "goods_description": _s(r.get("cbam_good")),
                "cbam_covered": bool(r.get("cbam_covered")) if r.get("cbam_covered") is not None else False,
                "produced_quantity": float(r.get("quantity") or 0.0),
                "produced_quantity_unit": _s(r.get("quantity_unit") or "t"),
                "eu_import_quantity": float(r.get("export_to_eu_quantity") or 0.0),
                "eu_import_unit": _s(r.get("quantity_unit") or "t"),
                "direct_emissions_tco2": float(r.get("direct_alloc_tco2") or 0.0),
                "indirect_emissions_tco2": float(r.get("indirect_alloc_tco2") or 0.0),
                "precursor_emissions_tco2": float(r.get("precursor_tco2") or 0.0),
                                "embedded_emissions_tco2": float(r.get("embedded_tco2") or 0.0),
                "embedded_intensity_tco2_per_unit": float(r.get("embedded_intensity_tco2_per_unit") or 0.0),
                "direct_intensity_tco2_per_unit": float(r.get("direct_intensity_tco2_per_unit") or 0.0),
                "indirect_intensity_tco2_per_unit": float(r.get("indirect_intensity_tco2_per_unit") or 0.0),
                "carbon_price_paid_eur_per_t": float(r.get("carbon_price_paid_eur_per_t") or 0.0),
                "carbon_price_paid_amount_eur": float(r.get("carbon_price_paid_amount_eur") or 0.0),
                "carbon_price_paid_currency": _s(r.get("carbon_price_paid_currency") or "EUR"),
                "data_type_flag": _s(r.get("data_type_flag") or "actual"),  # actual/default
                "mapping_rule": _s(r.get("mapping_rule") or ""),
                "allocation_method": _s(r.get("allocation_method") or ""),
                "allocation_hash": _s(r.get("allocation_hash") or ""),
            }
        )

    goods.sort(key=lambda x: (x.get("cn_code", ""), x.get("sku", ""), x.get("goods_description", "")))

    totals = {
        "direct_emissions_tco2": sum(float(g.get("direct_emissions_tco2") or 0.0) for g in goods),
        "indirect_emissions_tco2": sum(float(g.get("indirect_emissions_tco2") or 0.0) for g in goods),
        "precursor_emissions_tco2": sum(float(g.get("precursor_emissions_tco2") or 0.0) for g in goods),
        "embedded_emissions_tco2": sum(float(g.get("embedded_emissions_tco2") or 0.0) for g in goods),
        "eu_import_quantity_total": sum(float(g.get("eu_import_quantity") or 0.0) for g in goods),
    }

    return {
        "cbam_transitional_ir": "2023/1773",
        "generated_at_utc": now_utc,
        "period": {
            "year": period.get("year"),
            "quarter": period.get("quarter"),
            "from_date": period.get("from_date"),
            "to_date": period.get("to_date"),
        },
        "declarant": {
            "company_name": declarant.get("company_name", ""),
            "company_id": declarant.get("company_id"),
            "eori": declarant.get("eori", ""),
            "country": declarant.get("country", ""),
            "contact_email": declarant.get("contact_email", ""),
        },
        "installation": {
            "facility_id": installation.get("facility_id"),
            "facility_name": installation.get("facility_name", ""),
            "country": installation.get("country", ""),
            "sector": installation.get("sector", ""),
        },
        "methodology_note_tr": methodology_note_tr or "",
        "goods": goods,
        "carbon_price_paid": {
            "currency": "EUR",
            "total_amount_eur": sum(float(g.get("carbon_price_paid_amount_eur") or 0.0) for g in goods),
            "paid_eur_per_t_reference": (max(float(g.get("carbon_price_paid_eur_per_t") or 0.0) for g in goods) if goods else 0.0),
        },
        "totals": totals,
        "notes": [
            "Bu çıktı XML-ready bir yapıdır. Resmi CBAM XML formatına mapping yapılabilir.",
            "data_type_flag: actual/default (MVP varsayılan: actual).",
            "allocation_hash: ürün bazlı deterministik allocation kilidi (varsa).",
        ],
    }


def cbam_reporting_to_xml(cbam_reporting: Dict[str, Any]) -> str:
    r = cbam_reporting or {}

    root = ET.Element("CbamTransitionalReport")
    root.set("regulation", _s(r.get("cbam_transitional_ir", "2023/1773")))
    root.set("generated_at_utc", _s(r.get("generated_at_utc")))

    p = r.get("period") or {}
    pe = ET.SubElement(root, "Period")
    ET.SubElement(pe, "Year").text = _s(p.get("year"))
    if _s(p.get("quarter")):
        ET.SubElement(pe, "Quarter").text = _s(p.get("quarter"))
    if _s(p.get("from_date")):
        ET.SubElement(pe, "FromDate").text = _s(p.get("from_date"))
    if _s(p.get("to_date")):
        ET.SubElement(pe, "ToDate").text = _s(p.get("to_date"))

    d = r.get("declarant") or {}
    de = ET.SubElement(root, "Declarant")
    ET.SubElement(de, "CompanyName").text = _s(d.get("company_name"))
    ET.SubElement(de, "CompanyId").text = _s(d.get("company_id"))
    ET.SubElement(de, "EORI").text = _s(d.get("eori"))
    ET.SubElement(de, "Country").text = _s(d.get("country"))
    ET.SubElement(de, "ContactEmail").text = _s(d.get("contact_email"))

    i = r.get("installation") or {}
    ie = ET.SubElement(root, "Installation")
    ET.SubElement(ie, "FacilityId").text = _s(i.get("facility_id"))
    ET.SubElement(ie, "FacilityName").text = _s(i.get("facility_name"))
    ET.SubElement(ie, "Country").text = _s(i.get("country"))
    ET.SubElement(ie, "Sector").text = _s(i.get("sector"))

    if _s(r.get("methodology_note_tr")):
        ET.SubElement(root, "MethodologyNoteTR").text = _s(r.get("methodology_note_tr"))

    t = r.get("totals") or {}
    te = ET.SubElement(root, "Totals")
    ET.SubElement(te, "DirectEmissionsTCO2").text = _f(t.get("direct_emissions_tco2"))
    ET.SubElement(te, "IndirectEmissionsTCO2").text = _f(t.get("indirect_emissions_tco2"))
    ET.SubElement(te, "PrecursorEmissionsTCO2").text = _f(t.get("precursor_emissions_tco2"))
    ET.SubElement(te, "EmbeddedEmissionsTCO2").text = _f(t.get("embedded_emissions_tco2"))
    ET.SubElement(te, "EUImportQuantityTotal").text = _f(t.get("eu_import_quantity_total"), 6)

    ge = ET.SubElement(root, "GoodsList")
    for g in (r.get("goods") or []):
        if not isinstance(g, dict):
            continue
        g1 = ET.SubElement(ge, "Good")
        ET.SubElement(g1, "SKU").text = _s(g.get("sku"))
        ET.SubElement(g1, "CNCode").text = _s(g.get("cn_code"))
        ET.SubElement(g1, "Description").text = _s(g.get("goods_description"))
        ET.SubElement(g1, "CbamCovered").text = "true" if bool(g.get("cbam_covered")) else "false"

        q = ET.SubElement(g1, "Quantities")
        ET.SubElement(q, "ProducedQuantity").text = _f(g.get("produced_quantity"), 6)
        ET.SubElement(q, "ProducedUnit").text = _s(g.get("produced_quantity_unit"))
        ET.SubElement(q, "EUImportQuantity").text = _f(g.get("eu_import_quantity"), 6)
        ET.SubElement(q, "EUImportUnit").text = _s(g.get("eu_import_unit"))

        em = ET.SubElement(g1, "Emissions")
        ET.SubElement(em, "DirectEmissionsTCO2").text = _f(g.get("direct_emissions_tco2"))
        ET.SubElement(em, "IndirectEmissionsTCO2").text = _f(g.get("indirect_emissions_tco2"))
        ET.SubElement(em, "PrecursorEmissionsTCO2").text = _f(g.get("precursor_emissions_tco2"))
        ET.SubElement(em, "EmbeddedEmissionsTCO2").text = _f(g.get("embedded_emissions_tco2"))

        ET.SubElement(g1, "DataTypeFlag").text = _s(g.get("data_type_flag"))
        ET.SubElement(g1, "MappingRule").text = _s(g.get("mapping_rule"))
        if _s(g.get("allocation_hash")):
            al = ET.SubElement(g1, "Allocation")
            ET.SubElement(al, "Method").text = _s(g.get("allocation_method"))
            ET.SubElement(al, "Hash").text = _s(g.get("allocation_hash"))

    xml_bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    return xml_bytes.decode("utf-8")
