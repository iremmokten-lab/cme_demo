
# -*- coding: utf-8 -*-
"""Excel Connector (deterministic, Streamlit Cloud compatible).

Bu modül:
- Excel'den okur
- Kolon isimlerini normalize eder (lower + underscore)
- Şema zorunlu kolonlarını kontrol eder
- Deterministik dataset_hash üretir (canonical JSON + stable floats + row/col ordering)
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Tuple

import pandas as pd

from .excel_schema import SCHEMAS, ColumnSpec


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2.columns = [_norm(c) for c in df2.columns]
    return df2


def _quantize_number(x: Any) -> Any:
    if pd.isna(x):
        return None
    if isinstance(x, bool):
        return bool(x)
    if isinstance(x, int):
        return int(x)
    if isinstance(x, float):
        # Stable float representation (6 decimals)
        return float(f"{x:.6f}")
    return x


def canonical_json_records(records: List[Dict[str, Any]]) -> str:
    normalized = []
    for r in records:
        nr = {k: _quantize_number(v) for k, v in r.items()}
        normalized.append(nr)
    return json.dumps(normalized, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def compute_dataset_hash(df: pd.DataFrame) -> str:
    df2 = df.copy()
    df2 = normalize_headers(df2)

    # deterministic column ordering
    df2 = df2.reindex(sorted(df2.columns), axis=1)

    # deterministic row ordering
    sort_keys = [c for c in ["month", "facility_id", "product_code", "cn_code", "fuel_type"] if c in df2.columns]
    if sort_keys:
        df2 = df2.sort_values(by=sort_keys, kind="mergesort").reset_index(drop=True)

    records = df2.to_dict(orient="records")
    payload = canonical_json_records(records)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_schema(df: pd.DataFrame, schema: List[ColumnSpec]) -> Tuple[bool, List[str]]:
    cols = set(normalize_headers(df).columns)
    missing = [c.name for c in schema if c.required and c.name not in cols]
    return (len(missing) == 0, missing)


def load_excel(file, dataset_type: str) -> Dict[str, Any]:
    if dataset_type not in SCHEMAS:
        raise ValueError(f"Bilinmeyen dataset türü: {dataset_type}")

    schema = SCHEMAS[dataset_type]
    df = pd.read_excel(file)
    df = normalize_headers(df)

    ok, missing = validate_schema(df, schema)
    if not ok:
        raise ValueError(f"Eksik zorunlu kolonlar: {missing}")

    dataset_hash = compute_dataset_hash(df)

    return {"dataset_type": dataset_type, "hash": dataset_hash, "dataframe": df}
