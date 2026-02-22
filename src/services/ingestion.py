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
                errors.append("materials.csv: emission_factor sayısal olmalı (kgCO2e / material_unit varsayımı).")

        return errors

    errors.append("Bilinmeyen dataset_type. Beklenen: energy / production / materials")
    return errors


def _to_float(x: Any) -> float:
    try:
        if pd.isna(x):
            return 0.0
    except Exception:
        pass
    try:
        return float(x)
    except Exception:
        return 0.0


def _safe_json(d: dict) -> str:
    try:
        return json.dumps(d, ensure_ascii=False)
    except Exception:
        return "{}"


def data_quality_assess(dataset_type: str, df: pd.DataFrame) -> tuple[int, dict]:
    """Paket B: Data Quality Engine (0-100)

    Kontroller:
    - negatif değer
    - boş değer
    - export > production (production.csv)
    - duplicate month (energy/production)
    - birim hatası (kısıtlı, basic)
    - sıra dışı değer (IQR tabanlı, numeric alanlar)
    """
    dtype = _norm(dataset_type)
    report: dict = {"dataset_type": dtype, "checks": [], "warnings": []}

    if df is None or len(df) == 0:
        return 0, {"dataset_type": dtype, "checks": [{"check": "empty_file", "status": "fail"}], "warnings": []}

    df2 = df.copy()
    df2.columns = [_norm(c) for c in df2.columns]
    n_rows = len(df2)

    penalties = 0

    def add_check(name: str, status: str, details: dict | None = None, penalty: int = 0):
        nonlocal penalties
        report["checks"].append({"check": name, "status": status, "details": details or {}})
        penalties += int(penalty)

    # Missing / empty values
    missing_ratio = float(df2.isna().sum().sum()) / max(float(n_rows * max(len(df2.columns), 1)), 1.0)
    if missing_ratio > 0.02:
        add_check("missing_values", "warn", {"missing_ratio": missing_ratio}, penalty=10 if missing_ratio > 0.10 else 5)
    else:
        add_check("missing_values", "pass", {"missing_ratio": missing_ratio}, penalty=0)

    # Negative values (numeric columns)
    numeric_cols = []
    for c in df2.columns:
        if c in ("month", "facility_id", "sku", "cn_code", "fuel_type", "fuel_unit", "unit", "material_name", "material_unit"):
            continue
        # heuristic numeric detection
        try:
            pd.to_numeric(df2[c].dropna().head(30), errors="raise")
            numeric_cols.append(c)
        except Exception:
            continue

    neg_found = 0
    for c in numeric_cols:
        vals = pd.to_numeric(df2[c], errors="coerce").fillna(0.0)
        neg_found += int((vals < 0).sum())
    if neg_found > 0:
        add_check("negative_values", "fail", {"negative_count": neg_found}, penalty=min(20, 5 + neg_found))
    else:
        add_check("negative_values", "pass", {"negative_count": 0}, penalty=0)

    # Duplicate month (for energy/production)
    if dtype in ("energy", "production"):
        if "month" in df2.columns and "facility_id" in df2.columns:
            dup = df2.duplicated(subset=["month", "facility_id"]).sum()
            if int(dup) > 0:
                add_check("duplicate_month_facility", "warn", {"duplicate_rows": int(dup)}, penalty=min(15, 5 + int(dup)))
            else:
                add_check("duplicate_month_facility", "pass", {"duplicate_rows": 0}, penalty=0)
        else:
            add_check("duplicate_month_facility", "warn", {"note": "month/facility_id yok"}, penalty=2)

    # Unit mismatch (basic)
    if dtype == "energy":
        # fuel_unit beklenen: kWh/MWh veya Nm3/L/kg vb
        if "fuel_unit" in df2.columns:
            u = df2["fuel_unit"].astype(str).str.lower().str.strip()
            # aşırı boş veya tek değer çok şüpheli
            empty = int((u == "").sum())
            if empty > 0:
                add_check("unit_missing", "warn", {"empty_unit_rows": empty}, penalty=min(10, 3 + empty))
            else:
                add_check("unit_missing", "pass", {"empty_unit_rows": 0}, penalty=0)
        else:
            add_check("unit_missing", "warn", {"note": "fuel_unit yok"}, penalty=3)

    if dtype == "production":
        if "unit" in df2.columns:
            u = df2["unit"].astype(str).str.lower().str.strip()
            empty = int((u == "").sum())
            if empty > 0:
                add_check("unit_missing", "warn", {"empty_unit_rows": empty}, penalty=min(10, 3 + empty))
            else:
                add_check("unit_missing", "pass", {"empty_unit_rows": 0}, penalty=0)

        # export > production
        if "export_to_eu_quantity" in df2.columns and "quantity" in df2.columns:
            exp = pd.to_numeric(df2["export_to_eu_quantity"], errors="coerce").fillna(0.0)
            qty = pd.to_numeric(df2["quantity"], errors="coerce").fillna(0.0)
            bad = int((exp > qty).sum())
            if bad > 0:
                add_check("export_gt_production", "fail", {"rows": bad}, penalty=min(25, 10 + bad))
            else:
                add_check("export_gt_production", "pass", {"rows": 0}, penalty=0)

    # Outliers (IQR) on numeric cols: warn only
    outlier_count = 0
    for c in numeric_cols[:8]:  # limit
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

    # Final score
    score = max(0, min(100, 100 - penalties))
    report["score"] = int(score)
    report["rows"] = int(n_rows)
    report["numeric_columns_checked"] = numeric_cols[:8]

    return int(score), report
