from __future__ import annotations

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
    """CSV şema/doğrulama kontrolleri (Paket A).

    Dönen: hata mesajları listesi (boş ise OK).
    """
    errors: list[str] = []
    dtype = _norm(dataset_type)

    if df is None or len(df) == 0:
        return ["Dosya boş görünüyor."]

    cols = {_norm(c) for c in df.columns}

    if dtype == "energy":
        # Paket A hedef şema: row-based
        required_row = {"month", "facility_id", "fuel_type", "fuel_quantity", "fuel_unit"}
        # Legacy wide şema: minimum month/facility + one of common
        legacy_min = {"month", "facility_id"}
        legacy_any = {"natural_gas_m3", "electricity_kwh", "diesel_l", "coal_kg"}

        if _has_cols(df, required_row):
            # row-based kontroller
            if not _is_numeric_series(df[[c for c in df.columns if _norm(c) == "fuel_quantity"][0]]):
                errors.append("energy.csv: fuel_quantity sayısal olmalı.")
            # elektrik satırları için unit kontrolü (kWh/MWh önerilir)
            # burada hard error değil, uyarı gibi davranalım
            return errors

        if _has_cols(df, legacy_min) and any(c in cols for c in legacy_any):
            # legacy kabul
            return errors

        errors.append(
            "energy.csv şeması tanınmadı. Beklenen kolonlar:\n"
            "- Yeni şema: month, facility_id, fuel_type, fuel_quantity, fuel_unit\n"
            "- Legacy: month, facility_id ve (natural_gas_m3 veya electricity_kwh vb.)"
        )
        return errors

    if dtype == "production":
        # Paket A: CN code + CBAM exposure + export qty
        required = {"month", "facility_id", "sku", "cn_code", "quantity", "unit", "export_to_eu_quantity"}
        missing = sorted(list(required - cols))
        if missing:
            errors.append(f"production.csv eksik kolon(lar): {', '.join(missing)}")

        # numeric checks (best effort)
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

    # Bilinmeyen dataset
    errors.append("Bilinmeyen dataset_type. Beklenen: energy / production / materials")
    return errors
