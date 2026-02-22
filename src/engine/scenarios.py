import pandas as pd

def apply_scenarios(
    energy_df: pd.DataFrame,
    prod_df: pd.DataFrame,
    renewable_share: float,
    energy_reduction_pct: float,
    supplier_factor_multiplier: float,
    export_mix_multiplier: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    renewable_share: 0..1 -> scope2'yi (1-renewable_share) ile çarpar
    energy_reduction_pct: 0..1 -> activity_amount'u (1 - pct) ile çarpar
    supplier_factor_multiplier: üretim input_emission_factor_kg_per_unit çarpanı
    export_mix_multiplier: export_to_eu_quantity çarpanı (örn 0.8 = EU ihracatını azalt)
    """
    e = energy_df.copy()
    p = prod_df.copy()

    # Energy reduction
    if "activity_amount" in e.columns:
        e["activity_amount"] = pd.to_numeric(e["activity_amount"], errors="coerce").fillna(0.0) * (1.0 - float(energy_reduction_pct))

    # Renewable share -> Scope2 EF etkisi
    # scope==2 satırlarında emission factor'ü azalt
    if "scope" in e.columns and "emission_factor_kgco2_per_unit" in e.columns:
        e["scope"] = pd.to_numeric(e["scope"], errors="coerce").fillna(0.0)
        ef = pd.to_numeric(e["emission_factor_kgco2_per_unit"], errors="coerce").fillna(0.0)
        e.loc[e["scope"] == 2, "emission_factor_kgco2_per_unit"] = ef.loc[e["scope"] == 2] * (1.0 - float(renewable_share))

    # Supplier factor change
    if "input_emission_factor_kg_per_unit" in p.columns:
        p["input_emission_factor_kg_per_unit"] = pd.to_numeric(p["input_emission_factor_kg_per_unit"], errors="coerce").fillna(0.0) * float(supplier_factor_multiplier)

    # Export mix shift
    if "export_to_eu_quantity" in p.columns:
        p["export_to_eu_quantity"] = pd.to_numeric(p["export_to_eu_quantity"], errors="coerce").fillna(0.0) * float(export_mix_multiplier)

    return e, p
