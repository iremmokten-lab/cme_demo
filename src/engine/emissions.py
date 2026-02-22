import pandas as pd

def energy_emissions_kg(energy_df: pd.DataFrame) -> dict:
    df = energy_df.copy()
    df["activity_amount"] = pd.to_numeric(df["activity_amount"], errors="coerce").fillna(0.0)
    df["emission_factor_kgco2_per_unit"] = pd.to_numeric(df["emission_factor_kgco2_per_unit"], errors="coerce").fillna(0.0)
    df["scope"] = pd.to_numeric(df["scope"], errors="coerce").fillna(0.0)

    df["emissions_kgco2"] = df["activity_amount"] * df["emission_factor_kgco2_per_unit"]

    total = float(df["emissions_kgco2"].sum())
    s1 = float(df.loc[df["scope"] == 1, "emissions_kgco2"].sum())
    s2 = float(df.loc[df["scope"] == 2, "emissions_kgco2"].sum())

    return {"total_kg": total, "scope1_kg": s1, "scope2_kg": s2}
