from __future__ import annotations

import io
from typing import Dict, Tuple

import pandas as pd


def read_dataset(file_bytes: bytes, filename: str) -> Dict[str, pd.DataFrame]:
    """CSV veya XLSX oku. XLSX ise sheet bazında döner."""
    name = (filename or "").lower().strip()
    if name.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(file_bytes))
        return {"_single": df}

    if name.endswith(".xlsx") or name.endswith(".xlsm") or name.endswith(".xls"):
        xls = pd.ExcelFile(io.BytesIO(file_bytes))
        out: Dict[str, pd.DataFrame] = {}
        for sh in xls.sheet_names:
            try:
                out[sh] = pd.read_excel(xls, sheet_name=sh)
            except Exception:
                out[sh] = pd.DataFrame()
        return out

    raise ValueError("Desteklenmeyen dosya türü. CSV veya XLSX yükleyin.")


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d.columns = [str(c).strip().lower().replace(" ", "_") for c in d.columns]
    return d


def apply_mapping(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """mapping: {source_col: target_col}"""
    d = normalize_headers(df)
    mp = {str(k).strip().lower(): str(v).strip().lower() for k, v in (mapping or {}).items() if k and v}
    cols = list(d.columns)
    rename = {}
    for src, tgt in mp.items():
        if src in cols and tgt:
            rename[src] = tgt
    return d.rename(columns=rename)


def guess_mapping(df: pd.DataFrame, target_schema: Tuple[str, ...]) -> Dict[str, str]:
    """Basit auto-mapping: benzer isimler eşleştirir."""
    d = normalize_headers(df)
    src_cols = set(d.columns)
    mapping: Dict[str, str] = {}
    for tgt in target_schema:
        t = str(tgt).strip().lower()
        if t in src_cols:
            mapping[t] = t
            continue
        # synonyms
        synonyms = {
            "cn_code": ["cn", "cncode"],
            "export_to_eu_quantity": ["eu_export", "export_eu", "eu_qty"],
            "fuel_quantity": ["quantity", "amount", "qty"],
            "fuel_unit": ["unit"],
        }
        for s in synonyms.get(t, []):
            if s in src_cols:
                mapping[s] = t
                break
    return mapping
