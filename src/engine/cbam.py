import pandas as pd

def allocate_energy_to_skus(prod_df: pd.DataFrame, total_energy_kg: float) -> pd.DataFrame:
    df = prod_df.copy()
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0.0)
    total_qty = float(df["quantity"].sum())
    if total_qty <= 0:
        df["alloc_energy_kgco2"] = 0.0
        df["alloc_energy_kgco2_per_unit"] = 0.0
        return df
    df["alloc_energy_kgco2"] = (df["quantity"] / total_qty) * float(total_energy_kg)
    df["alloc_energy_kgco2_per_unit"] = df.apply(
        lambda r: (r["alloc_energy_kgco2"] / r["quantity"]) if r["quantity"] > 0 else 0.0, axis=1
    )
    return df

def cbam_cost(prod_df: pd.DataFrame, total_energy_kg: float, eua_price_eur_per_t: float) -> tuple[pd.DataFrame, dict]:
    df = prod_df.copy()
    if "cbam_covered" not in df.columns:
        df["cbam_covered"] = 1

    df["export_to_eu_quantity"] = pd.to_numeric(df["export_to_eu_quantity"], errors="coerce").fillna(0.0)
    df["input_emission_factor_kg_per_unit"] = pd.to_numeric(df["input_emission_factor_kg_per_unit"], errors="coerce").fillna(0.0)
    df["cbam_covered"] = pd.to_numeric(df["cbam_covered"], errors="coerce").fillna(1).astype(int)

    df = allocate_energy_to_skus(df, total_energy_kg)

    df["export_for_cbam"] = df.apply(lambda r: r["export_to_eu_quantity"] if r["cbam_covered"] == 1 else 0.0, axis=1)
    df["total_factor_kg_per_unit"] = df["alloc_energy_kgco2_per_unit"] + df["input_emission_factor_kg_per_unit"]

    df["embedded_kg"] = df["export_for_cbam"] * df["total_factor_kg_per_unit"]
    df["embedded_t"] = df["embedded_kg"] / 1000.0
    df["cbam_cost_eur"] = df["embedded_t"] * float(eua_price_eur_per_t)

    totals = {"embedded_tco2": float(df["embedded_t"].sum()), "cbam_cost_eur": float(df["cbam_cost_eur"].sum())}
    return df, totals
