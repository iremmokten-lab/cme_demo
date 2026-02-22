def ets_cost_tl(scope1_tco2: float, free_alloc_t: float, banked_t: float, allowance_price_eur_per_t: float, fx_tl_per_eur: float):
    net = max(0.0, float(scope1_tco2) - float(free_alloc_t) - float(banked_t))
    cost = net * float(allowance_price_eur_per_t) * float(fx_tl_per_eur)
    return {"net_tco2": net, "cost_tl": cost}
