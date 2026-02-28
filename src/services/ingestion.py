from __future__ import annotations

import json
from typing import Any

import pandas as pd


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


def _has_cols(df: pd.DataFrame, required: set[str]) -> bool:
    cols = {_norm(c) for c in df.columns}
    return required.issubset(cols)


def _is_numeric_series(s: pd.Series) -> bool:
    try:
        pd.to_numeric(s.dropna().head(50), errors="raise")
        return True
    except Exception:
        return False


def validate_csv(dataset_type: str, df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    dtype = _norm(dataset_type)

    if df is None or len(df) == 0:
        return ["Dosya boş görünüyor."]

    cols = {_norm(c) for c in df.columns}

    if dtype == "energy":
        required_row = {"month", "facility_id", "fuel_type", "fuel_quantity", "fuel_unit"}
        legacy_min = {"month", "facility_id"}
        legacy_any = {"natural_gas_m3", "electricity_kwh", "diesel_l", "coal_kg"}

        if _has_cols(df, required_row):
            col = [c for c in df.columns if _norm(c) == "fuel_quantity"][0]
            if not _is_numeric_series(df[col]):
                errors.append("energy.csv: fuel_quantity sayısal olmalı.")
            return errors

        if _has_cols(df, legacy_min) and any(c in cols for c in legacy_any):
            return errors

        errors.append(
            "energy.csv şeması tanınmadı. Beklenen kolonlar:\n"
            "- Yeni şema: month, facility_id, fuel_type, fuel_quantity, fuel_unit\n"
            "- Legacy: month, facility_id ve (natural_gas_m3 veya electricity_kwh vb.)"
        )
        return errors

    if dtype == "production":
        required = {"month", "facility_id", "sku", "cn_code", "quantity", "unit", "export_to_eu_quantity"}
        missing = sorted(list(required - cols))
        if missing:
            errors.append(f"production.csv eksik kolon(lar): {', '.join(missing)}")

        if "quantity" in cols:
            col = [c for c in df.columns if _norm(c) == "quantity"][0]
            if not _is_numeric_series(df[col]):
                errors.append("production.csv: quantity sayısal olmalı.")
        if "export_to_eu_quantity" in cols:
            col = [c for c in df.columns if _norm(c) == "export_to_eu_quantity"][0]
            if not _is_numeric_series(df[col]):
                errors.append("production.csv: export_to_eu_quantity sayısal olmalı.")
        return errors

    if dtype == "materials":
        required = {"sku", "material_name", "material_quantity", "material_unit", "emission_factor"}
        missing = sorted(list(required - cols))
        if missing:
            errors.append(f"materials.csv eksik kolon(lar): {', '.join(missing)}")

        if "material_quantity" in cols:
            col = [c for c in df.columns if _norm(c) == "material_quantity"][0]
            if not _is_numeric_series(df[col]):
                errors.append("materials.csv: material_quantity sayısal olmalı.")
        if "emission_factor" in cols:
            col = [c for c in df.columns if _norm(c) == "emission_factor"][0]
            if not _is_numeric_series(df[col]):
                errors.append("materials.csv: emission_factor sayısal olmalı.")
        return errors

    return [f"Dataset tipi tanınmadı: {dataset_type}"]


def data_quality_assess(dataset_type: str, df: pd.DataFrame) -> tuple[int, dict]:
    """Basit data quality skoru:
    - Eksik değer oranı
    - Tip uygunluğu
    - Basit outlier IQR
    """
    dtype = _norm(dataset_type)
    report: dict = {"dataset_type": dtype, "checks": []}

    if df is None or len(df) == 0:
        report["checks"].append({"id": "non_empty", "status": "fail", "details": {"rows": 0}})
        return 0, report

    n_rows = len(df)
    penalties = 0

    def add_check(cid: str, status: str, details: dict, penalty: int = 0):
        nonlocal penalties
        report["checks"].append({"id": cid, "status": status, "details": details})
        penalties += int(penalty)

    # Missingness
    miss_ratio = float(df.isna().mean().mean()) if n_rows > 0 else 1.0
    if miss_ratio > 0.20:
        add_check("missingness", "warn", {"missing_ratio": miss_ratio}, penalty=15)
    elif miss_ratio > 0.05:
        add_check("missingness", "warn", {"missing_ratio": miss_ratio}, penalty=8)
    else:
        add_check("missingness", "pass", {"missing_ratio": miss_ratio}, penalty=0)

    # Schema validation
    errs = validate_csv(dtype, df)
    if errs:
        add_check("schema", "fail", {"errors": errs[:10]}, penalty=35)
    else:
        add_check("schema", "pass", {"errors": []}, penalty=0)

    # Simple outlier check for numeric columns
    df2 = df.copy()
    numeric_cols = []
    for c in df2.columns:
        try:
            s = df2[c]
            if _is_numeric_series(s):
                numeric_cols.append(c)
        except Exception:
            continue

    outlier_count = 0
    for c in numeric_cols[:8]:
        vals = pd.to_numeric(df2[c], errors="coerce").dropna()
        if len(vals) < 20:
            continue
        q1 = vals.quantile(0.25)
        q3 = vals.quantile(0.75)
        iqr = q3 - q1
        if iqr <= 0:
            continue
        lower = q1 - 3.0 * iqr
        upper = q3 + 3.0 * iqr
        outlier_count += int(((vals < lower) | (vals > upper)).sum())
    if outlier_count > 0:
        add_check("outliers_iqr", "warn", {"outlier_count": outlier_count}, penalty=min(10, 3 + outlier_count // 10))
    else:
        add_check("outliers_iqr", "pass", {"outlier_count": 0}, penalty=0)

    score = max(0, min(100, 100 - penalties))
    report["score"] = int(score)
    report["rows"] = int(n_rows)
    report["numeric_columns_checked"] = numeric_cols[:8]

    return int(score), report


# --- Faz 2: Excel ingestion helpers ---

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return df
    df2 = df.copy()
    df2.columns = [_norm(c) for c in df2.columns]
    return df2


def read_xlsx_sheets(xlsx_bytes: bytes) -> dict[str, pd.DataFrame]:
    """Beklenen sheet isimleri: energy, production, materials"""
    import io

    xf = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    out: dict[str, pd.DataFrame] = {}
    for name in xf.sheet_names:
        key = _norm(name)
        if key in ("energy", "production", "materials"):
            df = pd.read_excel(xf, sheet_name=name)
            out[key] = df
    return out
