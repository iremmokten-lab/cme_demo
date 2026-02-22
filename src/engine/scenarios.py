from __future__ import annotations

from typing import Any

import pandas as pd


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


def apply_scenarios(
    energy_df: pd.DataFrame,
    prod_df: pd.DataFrame,
    renewable_share: float = 0.0,
    energy_reduction_pct: float = 0.0,
    supplier_factor_multiplier: float = 1.0,
    export_mix_multiplier: float = 1.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Senaryo uygulaması (Paket A uyumlu).

    - energy_df: row-based fuel/electricity veya legacy wide olabilir.
      - Enerji azaltımı: tüm fuel_quantity / electricity değerlerine uygulanır.
      - Renewable share: elektrik satırlarını azaltır gibi değil; grid factor metoduna Paket C'de taşınır.
        Paket A'da renewable_share sadece "market based" seçiminde daha düşük grid factor seçimi ile yapılır.
        Bu fonksiyon elektrik satırını değiştirmez.

    - prod_df:
      - export_to_eu_quantity: export_mix_multiplier ile çarpılır.
    """
    e = energy_df.copy() if energy_df is not None else pd.DataFrame()
    p = prod_df.copy() if prod_df is not None else pd.DataFrame()

    # Energy reduction
    red = max(min(_to_float(energy_reduction_pct), 1.0), 0.0)
    if red > 0.0 and len(e) > 0:
        cols = {str(c).strip().lower(): c for c in e.columns}
        if "fuel_quantity" in cols:
            c = cols["fuel_quantity"]
            e[c] = e[c].apply(_to_float) * (1.0 - red)
        else:
            # legacy wide: try common columns
            for key in ("natural_gas_m3", "electricity_kwh", "diesel_l", "coal_kg"):
                if key in cols:
                    c = cols[key]
                    e[c] = e[c].apply(_to_float) * (1.0 - red)

    # Export mix
    mult = max(_to_float(export_mix_multiplier), 0.0)
    if len(p) > 0:
        cols = {str(c).strip().lower(): c for c in p.columns}
        if "export_to_eu_quantity" in cols:
            c = cols["export_to_eu_quantity"]
            p[c] = p[c].apply(_to_float) * mult

    # supplier_factor_multiplier: Paket A'da materials.csv tarafında uygulanacak; burada dokunmuyoruz.
    # renewable_share: Paket A'da grid method seçimine bağlanacak; burada dokunmuyoruz.

    return e, p
