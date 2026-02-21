def compute_ets(scope1_tco2: float, free_allocation_tco2: float, banked_tco2: float, eua_price: float, fx_tl_per_eur: float):
    scope1_tco2 = float(scope1_tco2)
    free_allocation_tco2 = float(free_allocation_tco2)
    banked_tco2 = float(banked_tco2)
    eua_price = float(eua_price)
    fx_tl_per_eur = float(fx_tl_per_eur)

    net_req = max(0.0, scope1_tco2 - free_allocation_tco2 - banked_tco2)
    cost_tl = net_req * eua_price * fx_tl_per_eur

    return {
        "scope1_tco2": scope1_tco2,
        "free_allocation_tco2": free_allocation_tco2,
        "banked_allowances_tco2": banked_tco2,
        "net_eua_requirement_tco2": net_req,
        "eua_price_eur_per_tco2": eua_price,
        "fx_tl_per_eur": fx_tl_per_eur,
        "ets_cost_tl": cost_tl,
    }
