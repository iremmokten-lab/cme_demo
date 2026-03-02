from __future__ import annotations
import json
import streamlit as st
from src.services.authz import current_user, login_view, logout_button
from src.erp.cost.cost_engine import CarbonCostInput, compute_cost

st.set_page_config(page_title="Carbon ERP • Karbon Maliyeti", layout="wide")
user=current_user()
if not user:
    login_view(); st.stop()
with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

st.title("💸 Carbon ERP • Karbon Maliyeti")
st.caption("ETS yükümlülüğü + CBAM sertifika maliyeti + iç karbon fiyatı hesaplar.")

c1,c2,c3=st.columns(3)
with c1: emissions=st.number_input("Emisyon (tCO2)", min_value=0.0, value=100000.0, step=1000.0)
with c2: ets=st.number_input("ETS fiyatı (€/t)", min_value=0.0, value=60.0, step=1.0)
with c3: cbam=st.number_input("CBAM fiyatı (€/t)", min_value=0.0, value=60.0, step=1.0)

c4,c5,c6=st.columns(3)
with c4: fx=st.number_input("Kur (TL/€)", min_value=0.0, value=35.0, step=0.5)
with c5: free_alloc=st.number_input("Ücretsiz tahsis (tCO2)", min_value=0.0, value=0.0, step=1000.0)
with c6: paid=st.number_input("Ödenen karbon fiyatı (€/t) (CBAM)", min_value=0.0, value=0.0, step=1.0)

internal = st.number_input("İç karbon fiyatı (€/t) (opsiyonel)", min_value=0.0, value=0.0, step=1.0)

if st.button("Hesapla", type="primary"):
    inp=CarbonCostInput(emissions_tco2=float(emissions), ets_price_eur_per_t=float(ets), cbam_price_eur_per_t=float(cbam), fx_try_per_eur=float(fx), free_allocation_tco2=float(free_alloc), carbon_price_paid_eur_per_t=float(paid))
    res=compute_cost(inp, internal_price_eur_per_t=(float(internal) if internal>0 else None))
    out={"ets_liability_try":res.ets_liability_try, "cbam_certificates_try":res.cbam_certificates_try, "internal_carbon_price_try":res.internal_carbon_price_try, "detail":res.detail}
    st.success("Tamamlandı.")
    st.json(out)
    st.download_button("carbon_cost.json indir", data=json.dumps(out, ensure_ascii=False, indent=2), file_name="carbon_cost.json", mime="application/json")
