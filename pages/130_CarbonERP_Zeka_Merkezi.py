from __future__ import annotations
import streamlit as st
from src.services.authz import current_user, login_view, logout_button
from src.erp.intelligence.abatement_library import default_library
from src.erp.intelligence.benchmark_engine import simple_benchmark

st.set_page_config(page_title="Carbon ERP • Zeka Merkezi", layout="wide")
user=current_user()
if not user:
    login_view(); st.stop()
with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

st.title("🧠 Carbon ERP • Zeka Merkezi")
st.caption("Abatement (azaltım) kütüphanesi + basit benchmark hesapları.")

tab1,tab2=st.tabs(["Azaltım Kütüphanesi","Benchmark"])

with tab1:
    opts=default_library()
    st.dataframe([{"code":o.code,"title":o.title,"typical_reduction_pct":o.typical_reduction_pct,"capex_try":o.capex_try,"opex_delta_try":o.opex_delta_try,"notes":o.notes} for o in opts], use_container_width=True)

with tab2:
    em=st.number_input("Emisyon (tCO2)", min_value=0.0, value=100000.0, step=1000.0)
    prod=st.number_input("Üretim (ton)", min_value=0.0, value=50000.0, step=1000.0)
    if st.button("Benchmark hesapla", type="primary"):
        res=simple_benchmark(em, prod)
        st.success("Hesaplandı.")
        st.write(f"Yoğunluk: **{res.intensity:.4f} {res.unit}**")
        st.caption(res.percentile_note)
