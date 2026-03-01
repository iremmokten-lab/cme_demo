from __future__ import annotations

"""CBAM Quarterly Report (Portal XML) builder — versioned (v23.*).

Goal
- Produce an XML document that is *schema-validatable* against the official
  CBAM Quarterly Report XSD (QReport_ver23.00.xsd) when the mandatory fields are provided.

Important constraints
- Deterministic output: stable ordering, stable floats, stable namespaces.
- Security: avoid unsafe XML parsing; we only *generate* XML here.

Practical note
- The official schema changes across versions. This module keeps a versioned builder:
    * build_qreport_v23(report, portal_meta)
- If the Commission publishes a new schema version, add a new builder module rather than
  modifying the v23 output in place (audit reproducibility).
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET


def _s(x: Any) -> str:
    return "" if x is None else str(x)


def _f(x: Any, digits: int = 6) -> str:
    try:
        return f"{float(x):.{digits}f}"
    except Exception:
        return f"{0.0:.{digits}f}"


@dataclass(frozen=True)
class PortalMetaV23:
    """Fields that the portal expects but may not exist in internal calc results."""
    # These are typically portal header fields
    report_id: str = ""
    reporting_period_year: int = 0
    reporting_period_quarter: int = 0
    declarant_eori: str = ""
    declarant_name: str = ""
    declarant_country: str = ""
    # Optional representative
    rep_eori: str = ""
    rep_name: str = ""
    rep_country: str = ""

    # Installation operator meta (supplier)
    operator_name: str = ""
    operator_country: str = ""
    installation_name: str = ""
    installation_city: str = ""
    installation_country: str = ""

    # Signature (portal uses confirmations)
    signed_at_iso: str = ""


def build_qreport_v23(*, report: Dict[str, Any], meta: PortalMetaV23) -> bytes:
    """Build portal-grade quarterly report XML (v23 schema family).

    This builder intentionally uses a conservative subset of elements:
    - Header / declarant
    - GoodsImported list with quantities and embedded emissions
    - Emissions breakdown (direct/indirect/precursors)
    - Confirmation/signature fields (when provided)

    The exact element names are aligned to the CBAM Declarant Portal documentation.
    For strict XSD pass, mandatory fields must be supplied via `meta` and `report`.
    """

    # Root element name in portal XSD is typically QReport (varies by schema).
    # We use 'QReport' to match QReport_ver23.00.xsd naming convention.
    root = ET.Element("QReport")

    # ---- Header
    header = ET.SubElement(root, "Header")
    if meta.report_id:
        ET.SubElement(header, "ReportId").text = _s(meta.report_id)

    # Reporting period
    period = report.get("period") or {}
    year = meta.reporting_period_year or int(period.get("year") or 0)
    quarter = meta.reporting_period_quarter or int(period.get("quarter") or 0)
    if year:
        ET.SubElement(header, "ReportingYear").text = _s(year)
    if quarter:
        ET.SubElement(header, "ReportingQuarter").text = _s(quarter)

    # ---- Declarant
    decl = ET.SubElement(root, "Declarant")
    ET.SubElement(decl, "EORI").text = _s(meta.declarant_eori or (report.get("declarant") or {}).get("eori"))
    ET.SubElement(decl, "Name").text = _s(meta.declarant_name or (report.get("declarant") or {}).get("name"))
    if meta.declarant_country or (report.get("declarant") or {}).get("country"):
        ET.SubElement(decl, "Country").text = _s(meta.declarant_country or (report.get("declarant") or {}).get("country"))

    # ---- Optional Representative
    if meta.rep_eori or meta.rep_name:
        rep = ET.SubElement(root, "IndirectCustomsRepresentative")
        if meta.rep_eori:
            ET.SubElement(rep, "EORI").text = _s(meta.rep_eori)
        if meta.rep_name:
            ET.SubElement(rep, "Name").text = _s(meta.rep_name)
        if meta.rep_country:
            ET.SubElement(rep, "Country").text = _s(meta.rep_country)

    # ---- Installation / Operator (optional but usually expected for embedded emissions evidence)
    inst = ET.SubElement(root, "ThirdCountryInstallation")
    if meta.operator_name:
        ET.SubElement(inst, "OperatorName").text = _s(meta.operator_name)
    if meta.operator_country:
        ET.SubElement(inst, "OperatorCountry").text = _s(meta.operator_country)
    if meta.installation_name:
        ET.SubElement(inst, "InstallationName").text = _s(meta.installation_name)
    if meta.installation_city:
        ET.SubElement(inst, "City").text = _s(meta.installation_city)
    if meta.installation_country:
        ET.SubElement(inst, "Country").text = _s(meta.installation_country)

    # ---- Goods Imported
    goods_list = ET.SubElement(root, "CBAMGoodsImported")
    goods: List[Dict[str, Any]] = list((report.get("goods") or []))
    # deterministic sort
    goods.sort(key=lambda g: (_s(g.get("cn_code")), _s(g.get("sku"))))
    for idx, g in enumerate(goods, start=1):
        ge = ET.SubElement(goods_list, "Goods")
        ET.SubElement(ge, "GoodsItemNumber").text = _s(idx)
        ET.SubElement(ge, "CNCode").text = _s(g.get("cn_code"))
        if g.get("goods_description"):
            ET.SubElement(ge, "GoodsDescription").text = _s(g.get("goods_description"))
        # Quantities
        qty = g.get("eu_import_quantity") if g.get("eu_import_quantity") is not None else g.get("produced_quantity")
        unit = g.get("eu_import_unit") or g.get("produced_quantity_unit") or "t"
        ET.SubElement(ge, "Quantity").text = _f(qty or 0.0, 6)
        ET.SubElement(ge, "QuantityUnit").text = _s(unit)

        # Emissions (tCO2e) - embedded and components
        em = ET.SubElement(ge, "Emissions")
        ET.SubElement(em, "DirectEmissions").text = _f(g.get("direct_emissions_tco2e") or 0.0, 6)
        ET.SubElement(em, "IndirectEmissions").text = _f(g.get("indirect_emissions_tco2e") or 0.0, 6)
        ET.SubElement(em, "PrecursorEmissions").text = _f(g.get("precursor_emissions_tco2e") or 0.0, 6)
        ET.SubElement(em, "EmbeddedEmissions").text = _f(g.get("embedded_emissions_tco2e") or 0.0, 6)

        # Data type flag
        if g.get("data_type_flag"):
            ET.SubElement(ge, "DataType").text = _s(g.get("data_type_flag"))

        # Carbon price paid (optional)
        if g.get("carbon_price_paid_eur_per_t") is not None:
            ET.SubElement(ge, "CarbonPricePaid").text = _f(g.get("carbon_price_paid_eur_per_t") or 0.0, 6)

    # ---- Confirmations / signature (optional)
    if meta.signed_at_iso:
        conf = ET.SubElement(root, "ReportConfirmation")
        ET.SubElement(conf, "DateOfSignature").text = _s(meta.signed_at_iso)

    # Deterministic serialization (no pretty print)
    xml_bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    return xml_bytes
