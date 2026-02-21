import pandas as pd
from src.engine.emissions import compute_energy_emissions
from src.engine.cbam import compute_cbam
from src.engine.ets import compute_ets

def run_calculation(
    energy_df: pd.DataFrame,
    production_df: pd.DataFrame | None,
    eua_price: float,
    fx: float,
    free_allocation: float,
    banked_allowances: float,
):
    energy_calc_df, energy_summary = compute_energy_emissions(energy_df)

    ets_summary = compute_ets(
        scope1_tco2=float(energy_summary["scope1_tco2"]),
        free_allocation_tco2=float(free_allocation),
        banked_tco2=float(banked_allowances),
        eua_price=float(eua_price),
        fx_tl_per_eur=float(fx),
    )

    cbam_df = None
    cbam_totals = None
    cbam_warning = None
    if production_df is not None:
        cbam_df, cbam_totals, cbam_warning = compute_cbam(
            production_df,
            eua_price_eur_per_t=float(eua_price),
            total_energy_kgco2=float(energy_summary["total_kgco2"]),
        )

    return {
        "energy_calc_df": energy_calc_df,
        "energy_summary": energy_summary,
        "ets_summary": ets_summary,
        "cbam_df": cbam_df,
        "cbam_totals": cbam_totals,
        "cbam_warning": cbam_warning,
    }
