from __future__ import annotations

import json
from typing import Any

import pandas as pd

from src.mrv.data_quality_engine import anomaly_checks, completeness_checks


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
        legacy_min = {"month", "facility_id", "fuel", "quantity"}
        if not (required_row.issubset(cols) or legacy_min.issubset(cols)):
            errors.append("Energy şeması bekleniyor: month, facility_id, fuel_type/fuel, fuel_quantity/quantity, fuel_unit.")
    elif dtype == "production":
        required_row = {"month", "facility_id", "product_code", "quantity", "unit"}
        legacy_min = {"month", "facility_id", "product", "quantity"}
        if not (required_row.issubset(cols) or legacy_min.issubset(cols)):
            errors.append("Production şeması bekleniyor: month, facility_id, product_code/product, quantity, unit.")
    elif dtype == "materials":
        required_row = {"month", "facility_id", "material", "quantity", "unit"}
        if not required_row.issubset(cols):
            errors.append("Materials şeması bekleniyor: month, facility_id, material, quantity, unit.")
    else:
        # esnek
        if "month" not in cols:
            errors.append("Şema kontrolü: 'month' kolonu bulunamadı.")

    return errors


def data_quality_assess(dataset_type: str, df: pd.DataFrame) -> tuple[int, dict]:
    """Regülasyon-grade Data Quality skorlaması (baseline).

    Kapsam:
      - missingness
      - schema
      - outliers (IQR)
      - completeness checks (missing months/products/fuels)
      - anomaly checks (numeric spikes/outliers)

    Not: Cross-check (production vs energy) hesaplaması upload bazında tek dataset ile sınırlı,
    tam cross-check orchestrator/compliance safhasında yapılır.
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

    # Schema
    errs = validate_csv(dtype, df)
    if errs:
        add_check("schema", "fail", {"errors": errs[:10]}, penalty=35)
    else:
        add_check("schema", "pass", {"errors": []}, penalty=0)

    # Numeric outliers (IQR)
    df2 = df.copy()
    numeric_cols = []
    for c in df2.columns:
        try:
            if _is_numeric_series(df2[c]):
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

    # Completeness + anomalies
    for chk in completeness_checks(dtype, df):
        stt = str(chk.get("status") or "pass")
        pen = 25 if stt == "fail" else (10 if stt == "warn" else 0)
        add_check(str(chk.get("id") or "completeness"), stt, chk.get("details") or {}, penalty=pen)

    for chk in anomaly_checks(dtype, df):
        stt = str(chk.get("status") or "pass")
        pen = 15 if stt == "fail" else (5 if stt == "warn" else 0)
        add_check(str(chk.get("id") or "anomaly"), stt, chk.get("details") or {}, penalty=pen)

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
