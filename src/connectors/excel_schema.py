
# -*- coding: utf-8 -*-
"""Excel/CSV Connector schema definitions (Excel-first, ERP-agnostic)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    required: bool = True
    description_tr: str = ""
    allowed_values: Optional[List[str]] = None


FACILITY_SCHEMA: List[ColumnSpec] = [
    ColumnSpec("facility_id", True, "Tesis benzersiz ID (string)"),
    ColumnSpec("facility_name", True, "Tesis adı"),
    ColumnSpec("country", True, "Ülke (örn. TR)"),
    ColumnSpec("sector", True, "Sektör (cement, iron_steel, aluminium, fertilisers, electricity, hydrogen)"),
]

ENERGY_SCHEMA: List[ColumnSpec] = [
    ColumnSpec("facility_id", True, "Tesis ID"),
    ColumnSpec("period", True, "Dönem (YYYY-MM). Örn: 2025-01"),
    ColumnSpec("fuel_type", True, "Yakıt türü (örn. natural_gas, coal, petcoke, diesel)"),
    ColumnSpec("quantity", True, "Miktar"),
    ColumnSpec("unit", True, "Birim (örn. Nm3, ton, kg, MWh, GJ)"),
]

ELECTRICITY_SCHEMA: List[ColumnSpec] = [
    ColumnSpec("facility_id", True, "Tesis ID"),
    ColumnSpec("period", True, "Dönem (YYYY-MM)"),
    ColumnSpec("quantity_mwh", True, "Elektrik tüketimi (MWh)"),
]

PRODUCTION_SCHEMA: List[ColumnSpec] = [
    ColumnSpec("facility_id", True, "Tesis ID"),
    ColumnSpec("period", True, "Dönem (YYYY-MM)"),
    ColumnSpec("product_sku", True, "Ürün kodu/SKU"),
    ColumnSpec("product_name", True, "Ürün adı"),
    ColumnSpec("quantity", True, "Üretim miktarı"),
    ColumnSpec("unit", True, "Birim (örn. ton, kg, MWh)"),
]

CBAM_PRODUCTS_SCHEMA: List[ColumnSpec] = [
    ColumnSpec("facility_id", True, "Tesis ID"),
    ColumnSpec("period", True, "Dönem (YYYY-MM)"),
    ColumnSpec("product_sku", True, "Ürün kodu/SKU (production ile aynı olmalı)"),
    ColumnSpec("cn_code", True, "CN kodu (8 hane önerilir)"),
    ColumnSpec("goods_category", True, "CBAM kategorisi (cement, iron_steel, aluminium, fertilisers, electricity, hydrogen)"),
    ColumnSpec("actual_default_flag", True, "ACTUAL veya DEFAULT"),
]

BOM_PRECURSORS_SCHEMA: List[ColumnSpec] = [
    ColumnSpec("facility_id", True, "Tesis ID"),
    ColumnSpec("product_sku", True, "Ürün SKU"),
    ColumnSpec("precursor_sku", True, "Öncül/precursor SKU"),
    ColumnSpec("share", True, "Pay (0-1 arası). Örn: 0.25"),
]

SCHEMAS: Dict[str, List[ColumnSpec]] = {
    "facility": FACILITY_SCHEMA,
    "energy": ENERGY_SCHEMA,
    "electricity": ELECTRICITY_SCHEMA,
    "production": PRODUCTION_SCHEMA,
    "cbam_products": CBAM_PRODUCTS_SCHEMA,
    "bom_precursors": BOM_PRECURSORS_SCHEMA,
}
