from __future__ import annotations
import streamlit as st
from src.services.authz import current_user, login_view, logout_button
from src.services.projects import list_company_projects_for_user
from src.services.verification import list_cases_for_project
from src.services.verifier_workspace import (
    ensure_case_state, add_sampling_item, list_sampling,
    add_finding, list_findings, add_corrective_action, list_corrective_actions
)

st.set_page_config(page_title="Doğrulayıcı Workspace Pro", layout="wide")
u=current_user()
if not u:
    login_view(); st.stop()
with st.sidebar:
    st.write(f"👤 {u.email}")
    st.caption(f"Rol: {u.role}")
    logout_button()

st.title("🕵️ Doğrulayıcı Workspace Pro")
st.caption("Sampling plan + bulgular + düzeltici aksiyonlar (DB persist).")

projects=list_company_projects_for_user(u)
if not projects:
    st.warning("Proje yok."); st.stop()
pmap={f"{p.name} (#{p.id})": int(p.id) for p in projects}
pl=st.selectbox("Proje", list(pmap.keys()))
project_id=pmap[pl]

cases=list_cases_for_project(project_id)
if not cases:
    st.info("Bu proje için verification case yok (önce case oluştur).")
    st.stop()
cmap={f"#{c.id} • {c.title} • {c.status}": int(c.id) for c in cases}
cl=st.selectbox("Verification Case", list(cmap.keys()))
case_id=cmap[cl]
ensure_case_state(case_id)

tab1,tab2,tab3=st.tabs(["Sampling Plan","Bulgular","Düzeltici Aksiyon"])

with tab1:
    st.subheader("Sampling Plan")
    r=st.text_input("Record ref", value="energy/2025-01")
    reason=st.text_input("Sebep", value="yüksek tüketim")
    if st.button("Sampling ekle", type="primary"):
        add_sampling_item(case_id, r, reason)
        st.success("Eklendi."); st.rerun()
    st.dataframe([{"id":x.id,"record_ref":x.record_ref,"reason":x.reason} for x in list_sampling(case_id)], use_container_width=True)

with tab2:
    st.subheader("Bulgular")
    code=st.text_input("Kod", value="F-01")
    sev=st.selectbox("Seviye", ["low","medium","high"], index=2)
    desc=st.text_area("Açıklama", value="Kalibrasyon sertifikası eksik", height=90)
    if st.button("Bulgu ekle", type="primary"):
        add_finding(case_id, code, sev, desc)
        st.success("Eklendi."); st.rerun()
    finds=list_findings(case_id)
    st.dataframe([{"id":f.id,"code":f.code,"severity":f.severity,"status":f.status,"description":f.description} for f in finds], use_container_width=True)

with tab3:
    st.subheader("Düzeltici Aksiyonlar")
    finds=list_findings(case_id)
    if not finds:
        st.info("Önce bulgu ekle.")
    else:
        fmap={f"#{f.id} {f.code} ({f.severity})": int(f.id) for f in finds}
        fl=st.selectbox("Bulgu seç", list(fmap.keys()))
        fid=fmap[fl]
        owner=st.text_input("Sorumlu", value=u.email)
        act=st.text_area("Aksiyon", value="Sertifikayı yükle ve referans ekle", height=90)
        due=st.text_input("Termin", value="2026-03-31")
        if st.button("Aksiyon ekle", type="primary"):
            add_corrective_action(fid, owner, act, due_date=due)
            st.success("Eklendi."); st.rerun()
        st.dataframe([{"id":a.id,"owner":a.owner,"due_date":a.due_date,"status":a.status,"action":a.action} for a in list_corrective_actions(fid)], use_container_width=True)
