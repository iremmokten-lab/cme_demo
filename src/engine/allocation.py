import pandas as pd
import numpy as np
from .emissions import safe_float, validate_required_columns, validate_nonnegative

def allocate_energy_to_skus(production_df: pd.DataFrame, total_energy_kgco2: float):
    validate_required_columns(production_df, ["sku", "quantity"], "production.csv")
    df = production_df.copy()
    df["quantity"] = df["quantity"].apply(safe_float)
    validate_nonnegative(df, ["quantity"], "production.csv")

    total_qty = float(df["quantity"].sum()) if len(df) else 0.0
    if total_qty <= 0:
        df["alloc_energy_kgco2"] = 0.0
        df["alloc_energy_kgco2_per_unit"] = 0.0
        return df

    df["qty_share"] = df["quantity"] / total_qty
    df["alloc_energy_kgco2"] = df["qty_share"] * float(total_energy_kgco2)
    df["alloc_energy_kgco2_per_unit"] = np.where(
        df["quantity"] > 0,
        df["alloc_energy_kgco2"] / df["quantity"],
        0.0
    )
    return df
