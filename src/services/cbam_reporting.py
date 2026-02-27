from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET


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
    """CBAM Transitional (2023/1773) için XML-ready rapor yapısı.

    Amaç:
      - `results_json.cbam_reporting` altında raporlanabilir bir JSON oluşturmak.
      - Bu JSON deterministik şekilde XML'e dönüştürülebilir.

    Not:
      - Bu repo MVP olduğundan resmi XSD şemasını birebir hedeflemez.
      - Alanlar mapping-ready şekilde isimlendirilmiştir.
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
        goods.append(
            {
                "sku": _s(r.get("sku")),
                "cn_code": _s(r.get("cn_code")),
                "goods_description": _s(r.get("cbam_good") or r.get("goods") or ""),
                "cbam_covered": bool(r.get("cbam_covered")) if r.get("cbam_covered") is not None else False,
                "quantity": float(r.get("quantity") or 0.0),
                "quantity_unit": _s(r.get("quantity_unit") or "t"),  # MVP default
                "export_to_eu_quantity": float(r.get("export_to_eu_quantity") or 0.0),
                "direct_alloc_tco2": float(r.get("direct_alloc_tco2") or 0.0),
                "indirect_alloc_tco2": float(r.get("indirect_alloc_tco2") or 0.0),
                "precursor_tco2": float(r.get("precursor_tco2") or 0.0),
                "embedded_tco2": float(r.get("embedded_tco2") or 0.0),
                # Transitional reporting: actual/default flag
                # MVP: Eğer satırda 'data_type_flag' yoksa 'actual' varsayılır, ancak compliance engine bunu warn edebilir.
                "data_type_flag": _s(r.get("data_type_flag") or r.get("method_flag") or "actual"),
                "mapping_rule": _s(r.get("mapping_rule") or ""),
                "allocation_method": _s(r.get("allocation_method") or alloc_method or ""),
                "allocation_hash": _s(r.get("allocation_hash") or alloc_hash or ""),
            }
        )

    # deterministik sıralama
    goods.sort(key=lambda x: (x.get("cn_code", ""), x.get("sku", ""), x.get("goods_description", "")))

    totals = {
        "direct_tco2": sum(float(g.get("direct_alloc_tco2") or 0.0) for g in goods),
        "indirect_tco2": sum(float(g.get("indirect_alloc_tco2") or 0.0) for g in goods),
        "precursor_tco2": sum(float(g.get("precursor_tco2") or 0.0) for g in goods),
        "embedded_tco2": sum(float(g.get("embedded_tco2") or 0.0) for g in goods),
    }

    report = {
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
        "allocation": {
            "method": _s(alloc_method or ""),
            "hash": _s(alloc_hash or ""),
        },
        "methodology_note_tr": methodology_note_tr or "",
        "goods": goods,
        "totals": totals,
        "notes": [
            "Bu çıktı XML-ready bir yapıdır. Resmi CBAM XML şemasına uyarlamak için mapping yapılabilir.",
            "data_type_flag: actual/default (MVP varsayılan: actual).",
        ],
    }
    return report


def cbam_reporting_to_xml(cbam_reporting: Dict[str, Any]) -> str:
    """CBAM JSON → deterministik XML.

    Not: Resmi CBAM XSD hedeflenmiyor; ancak XML exporter gereksinimi için stable bir format sağlar.
    """
    r = cbam_reporting or {}

    root = ET.Element("CbamTransitionalReport")
    root.set("regulation", _s(r.get("cbam_transitional_ir", "2023/1773")))
    root.set("generated_at_utc", _s(r.get("generated_at_utc")))

    period = ET.SubElement(root, "Period")
    p = r.get("period") or {}
    ET.SubElement(period, "Year").text = _s(p.get("year"))
    if p.get("quarter") is not None:
        ET.SubElement(period, "Quarter").text = _s(p.get("quarter"))
    if p.get("from_date"):
        ET.SubElement(period, "FromDate").text = _s(p.get("from_date"))
    if p.get("to_date"):
        ET.SubElement(period, "ToDate").text = _s(p.get("to_date"))

    declarant = ET.SubElement(root, "Declarant")
    d = r.get("declarant") or {}
    ET.SubElement(declarant, "CompanyName").text = _s(d.get("company_name"))
    ET.SubElement(declarant, "CompanyId").text = _s(d.get("company_id"))
    ET.SubElement(declarant, "EORI").text = _s(d.get("eori"))
    ET.SubElement(declarant, "Country").text = _s(d.get("country"))
    ET.SubElement(declarant, "ContactEmail").text = _s(d.get("contact_email"))

    inst = ET.SubElement(root, "Installation")
    i = r.get("installation") or {}
    ET.SubElement(inst, "FacilityId").text = _s(i.get("facility_id"))
    ET.SubElement(inst, "FacilityName").text = _s(i.get("facility_name"))
    ET.SubElement(inst, "Country").text = _s(i.get("country"))
    ET.SubElement(inst, "Sector").text = _s(i.get("sector"))

    alloc = ET.SubElement(root, "Allocation")
    a = r.get("allocation") or {}
    ET.SubElement(alloc, "Method").text = _s(a.get("method"))
    ET.SubElement(alloc, "Hash").text = _s(a.get("hash"))

    totals = ET.SubElement(root, "Totals")
    t = r.get("totals") or {}
    ET.SubElement(totals, "DirectTCO2").text = _f(t.get("direct_tco2"))
    ET.SubElement(totals, "IndirectTCO2").text = _f(t.get("indirect_tco2"))
    ET.SubElement(totals, "PrecursorTCO2").text = _f(t.get("precursor_tco2"))
    ET.SubElement(totals, "EmbeddedTCO2").text = _f(t.get("embedded_tco2"))

    goods_el = ET.SubElement(root, "GoodsList")
    for g in (r.get("goods") or []):
        if not isinstance(g, dict):
            continue
        ge = ET.SubElement(goods_el, "Good")
        ET.SubElement(ge, "SKU").text = _s(g.get("sku"))
        ET.SubElement(ge, "CNCode").text = _s(g.get("cn_code"))
        ET.SubElement(ge, "Description").text = _s(g.get("goods_description"))
        ET.SubElement(ge, "CbamCovered").text = "true" if bool(g.get("cbam_covered")) else "false"
        ET.SubElement(ge, "Quantity").text = _f(g.get("quantity"), 6)
        ET.SubElement(ge, "QuantityUnit").text = _s(g.get("quantity_unit"))
        ET.SubElement(ge, "ExportToEUQuantity").text = _f(g.get("export_to_eu_quantity"), 6)

        em = ET.SubElement(ge, "Emissions")
        ET.SubElement(em, "DirectAllocTCO2").text = _f(g.get("direct_alloc_tco2"))
        ET.SubElement(em, "IndirectAllocTCO2").text = _f(g.get("indirect_alloc_tco2"))
        ET.SubElement(em, "PrecursorTCO2").text = _f(g.get("precursor_tco2"))
        ET.SubElement(em, "EmbeddedTCO2").text = _f(g.get("embedded_tco2"))

        ET.SubElement(ge, "DataTypeFlag").text = _s(g.get("data_type_flag"))
        ET.SubElement(ge, "MappingRule").text = _s(g.get("mapping_rule"))
        ET.SubElement(ge, "AllocationMethod").text = _s(g.get("allocation_method"))
        ET.SubElement(ge, "AllocationHash").text = _s(g.get("allocation_hash"))

    if r.get("methodology_note_tr"):
        mn = ET.SubElement(root, "MethodologyNoteTR")
        mn.text = _s(r.get("methodology_note_tr"))

    # Deterministik pretty-print olmadan (minified) döner; evidence pack için idealdir.
    xml_bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    return xml_bytes.decode("utf-8")
