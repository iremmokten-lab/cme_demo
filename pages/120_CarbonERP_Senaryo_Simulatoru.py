from __future__ import annotations
import json
import streamlit as st
from src.services.authz import current_user, login_view, logout_button
from src.services.projects import list_company_projects_for_user
from src.erp.scenario.scenario_engine import ScenarioInput, run_scenario, to_json

st.set_page_config(page_title="Carbon ERP • Senaryo", layout="wide")
user=current_user()
if not user:
    login_view(); st.stop()
with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

st.title("📈 Carbon ERP • Senaryo Simülatörü")
st.caption("Yakıt değişimi / verimlilik / elektrikleşme gibi önlemlerin emisyon ve maliyet etkisini hızlıca simüle eder.")

projects=list_company_projects_for_user(user)
if not projects:
    st.warning("Proje bulunamadı."); st.stop()
proj_map={f"{p.name} (#{p.id})": int(p.id) for p in projects}
proj_label=st.selectbox("Proje seç", list(proj_map.keys()))
project_id=proj_map[proj_label]

c1,c2,c3=st.columns(3)
with c1: base_em=st.number_input("Baz emisyon (tCO2)", min_value=0.0, value=100000.0, step=1000.0)
with c2: ets_price=st.number_input("ETS fiyatı (€/t)", min_value=0.0, value=60.0, step=1.0)
with c3: cbam_price=st.number_input("CBAM fiyatı (€/t)", min_value=0.0, value=60.0, step=1.0)

fx=st.number_input("Kur (TL/€)", min_value=0.0, value=35.0, step=0.5)
base_energy=st.number_input("Baz enerji (MWh)", min_value=0.0, value=250000.0, step=1000.0)

st.subheader("Önlemler")
st.caption("Her önlem: azaltım yüzdesi + CAPEX + OPEX değişimi. (Basit model)")
measures=[]
for i in range(1,4):
    with st.expander(f"Önlem {i}"):
        mtype=st.selectbox("Tür", ["fuel_switch","efficiency","electrification","ccs","other"], key=f"t{i}")
        red=st.number_input("Azaltım (%)", min_value=0.0, max_value=100.0, value=0.0, key=f"r{i}")
        capex=st.number_input("CAPEX (TL)", min_value=0.0, value=0.0, key=f"c{i}")
        opex=st.number_input("OPEX değişimi (TL/yıl)", value=0.0, key=f"o{i}")
        if red>0 or capex>0 or opex!=0:
            measures.append({"type":mtype, "reduction_pct": float(red), "capex_try": float(capex), "opex_delta_try": float(opex)})

name=st.text_input("Senaryo adı", value="Senaryo-1")
if st.button("Senaryoyu çalıştır", type="primary"):
    inp=ScenarioInput(name=name, base_emissions_tco2=float(base_em), base_energy_mwh=float(base_energy), ets_price_eur_per_t=float(ets_price), cbam_price_eur_per_t=float(cbam_price), fx_try_per_eur=float(fx), measures=measures)
    res=run_scenario(inp)
    out=to_json(res)
    st.success("Tamamlandı.")
    st.json(out)
    st.download_button("scenario_result.json indir", data=json.dumps(out, ensure_ascii=False, indent=2), file_name="scenario_result.json", mime="application/json")
