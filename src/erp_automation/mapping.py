from __future__ import annotations
from typing import Any, Dict, List

CANONICAL_SCHEMAS: Dict[str, List[str]] = {
    # Platformun beklediği kolon adları: sade (gerekirse genişletilir)
    "energy": ["facility_code", "period", "fuel_type", "consumption_value", "unit"],
    "production": ["facility_code", "period", "product_sku", "quantity", "unit"],
    "cost": ["facility_code", "period", "cost_center", "amount", "currency"],
}

def apply_mapping(records: List[Dict[str, Any]], mapping: Dict[str, str], *, dataset_type: str) -> List[Dict[str, Any]]:
    if dataset_type not in CANONICAL_SCHEMAS:
        raise ValueError(f"Bilinmeyen dataset_type: {dataset_type}")
    out=[]
    for rec in records:
        row={}
        for ext, val in rec.items():
            internal = mapping.get(ext)
            if internal:
                row[internal]=val
        # ensure keys exist (missing -> None)
        for k in CANONICAL_SCHEMAS[dataset_type]:
            row.setdefault(k, None)
        out.append(row)
    return out
