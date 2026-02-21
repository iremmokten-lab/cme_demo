import pandas as pd
import numpy as np

def safe_float(x):
    try:
        if pd.isna(x):
            return np.nan
        return float(x)
    except Exception:
        return np.nan

def kg_to_t(x_kg: float) -> float:
    return float(x_kg) / 1000.0

def validate_required_columns(df: pd.DataFrame, required: list, df_name: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"'{df_name}' dosyasında eksik kolon(lar): " + ", ".join(missing))

def validate_nonnegative(df: pd.DataFrame, cols: list, df_name: str):
    problems = []
    for c in cols:
        if c not in df.columns:
            continue
        s = df[c].apply(safe_float)
        bad = s.isna() | (s < 0)
        if bad.any():
            i = int(np.where(bad.to_numpy())[0][0])
            excel_row = i + 2
            val = df.iloc[i][c]
            problems.append(f"{df_name}: kolon '{c}' satır {excel_row} geçersiz (boş/negatif): {val}")
    if problems:
        raise ValueError(" | ".join(problems))

def compute_energy_emissions(energy_df: pd.DataFrame):
    required = ["energy_carrier", "scope", "activity_amount", "emission_factor_kgco2_per_unit"]
    validate_required_columns(energy_df, required, "energy.csv")

    df = energy_df.copy()
    df["scope"] = df["scope"].apply(safe_float)
    df["activity_amount"] = df["activity_amount"].apply(safe_float)
    df["emission_factor_kgco2_per_unit"] = df["emission_factor_kgco2_per_unit"].apply(safe_float)

    validate_nonnegative(df, ["activity_amount", "emission_factor_kgco2_per_unit"], "energy.csv")

    bad_scope = ~df["scope"].isin([1, 2])
    if bad_scope.any():
        i = int(np.where(bad_scope.to_numpy())[0][0])
        excel_row = i + 2
        raise ValueError(f"energy.csv: kolon 'scope' satır {excel_row} sadece 1 veya 2 olmalı.")

    df["scope"] = df["scope"].astype(int)
    df["emissions_kgco2"] = df["activity_amount"] * df["emission_factor_kgco2_per_unit"]

    total_kg = float(df["emissions_kgco2"].sum()) if len(df) else 0.0
    scope1_kg = float(df.loc[df["scope"] == 1, "emissions_kgco2"].sum()) if len(df) else 0.0
    scope2_kg = float(df.loc[df["scope"] == 2, "emissions_kgco2"].sum()) if len(df) else 0.0

    summary = {
        "total_kgco2": total_kg,
        "scope1_kgco2": scope1_kg,
        "scope2_kgco2": scope2_kg,
        "total_tco2": kg_to_t(total_kg),
        "scope1_tco2": kg_to_t(scope1_kg),
        "scope2_tco2": kg_to_t(scope2_kg),
    }
    return df, summary
