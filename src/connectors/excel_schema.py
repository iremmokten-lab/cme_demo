
# -*- coding: utf-8 -*-
"""Excel Connector şemaları (Excel-first, ERP-agnostic).

⚠️ Bu Faz-0 tasarımı özellikle Excel/CSV ile başlamak içindir.
İleride SAP/Logo/Netsis/Oracle gibi ERP'ler için aynı şemaya map eden adapter'lar eklenecektir.

Notlar:
- MRV çekirdek katmanı (src/services/ingestion.validate_csv) enerji/üretim için 'month' kolonunu bekler.
- Bu yüzden burada 'month' zorunludur (YYYY-MM).
"""

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
    ColumnSpec("month", True, "Dönem (YYYY-MM). Örn: 2025-01"),
    ColumnSpec("facility_id", True, "Tesis ID"),
    ColumnSpec("fuel_type", True, "Yakıt türü (örn. natural_gas, coal, petcoke, diesel)"),
    ColumnSpec("fuel_quantity", True, "Yakıt miktarı"),
    ColumnSpec("fuel_unit", True, "Yakıt birimi (Nm3, ton, kg, MWh, GJ vb.)"),
]

PRODUCTION_SCHEMA: List[ColumnSpec] = [
    ColumnSpec("month", True, "Dönem (YYYY-MM)"),
    ColumnSpec("facility_id", True, "Tesis ID"),
    ColumnSpec("product_code", True, "Ürün kodu/SKU"),
    ColumnSpec("quantity", True, "Üretim miktarı"),
    ColumnSpec("unit", True, "Birim (ton, kg vb.)"),
]

# CBAM ürün master listesi (Faz-0: Excel-first)
CBAM_PRODUCTS_SCHEMA: List[ColumnSpec] = [
    ColumnSpec("month", True, "Dönem (YYYY-MM)"),
    ColumnSpec("facility_id", True, "Tesis ID"),
    ColumnSpec("product_code", True, "Ürün kodu/SKU (production.product_code ile eşleşmeli)"),
    ColumnSpec("cn_code", True, "CN kodu (8 hane önerilir)"),
    ColumnSpec("goods_category", True, "CBAM kategorisi (cement, iron_steel, aluminium, fertilisers, electricity, hydrogen)"),
    ColumnSpec("actual_default_flag", True, "ACTUAL veya DEFAULT"),
]

# Öncelik/precursor ilişkisi (Faz-0: basit BOM)
BOM_PRECURSORS_SCHEMA: List[ColumnSpec] = [
    ColumnSpec("facility_id", True, "Tesis ID"),
    ColumnSpec("product_code", True, "Ürün kodu/SKU"),
    ColumnSpec("precursor_code", True, "Öncül/precursor SKU"),
    ColumnSpec("share", True, "Pay (0-1 arası). Örn: 0.25"),
]

SCHEMAS: Dict[str, List[ColumnSpec]] = {
    "facility": FACILITY_SCHEMA,
    "energy": ENERGY_SCHEMA,
    "production": PRODUCTION_SCHEMA,
    "cbam_products": CBAM_PRODUCTS_SCHEMA,
    "bom_precursors": BOM_PRECURSORS_SCHEMA,
}
