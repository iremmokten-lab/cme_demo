from __future__ import annotations
import json
import streamlit as st
from src.services.authz import current_user, login_view, logout_button
from src.services.projects import list_company_projects_for_user
from src.services.cbam_reporting import build_cbam_xml_for_project_quarter
from src.services.storage import write_bytes, storage_path_for_project
from src.services.cbam_portal_workflow import mark_ready, submit_to_portal, refresh_status, get_or_create
from src.services.portal_readiness import validate_portal_zip_structure, compute_readiness_score

st.set_page_config(page_title="CBAM Portal Submission", layout="wide")
u=current_user()
if not u:
    login_view(); st.stop()
with st.sidebar:
    st.write(f"👤 {u.email}")
    st.caption(f"Rol: {u.role}")
    logout_button()

st.title("📤 CBAM Portal Submission Workflow")
st.caption("Resmi portal/sandbox entegrasyonu için: hazırla → kontrol et → gönder → durum izle.")

projects=list_company_projects_for_user(u)
if not projects:
    st.warning("Proje yok."); st.stop()
pmap={f"{p.name} (#{p.id})": int(p.id) for p in projects}
pl=st.selectbox("Proje", list(pmap.keys()))
project_id=pmap[pl]

c1,c2=st.columns(2)
with c1: year=st.number_input("Yıl", min_value=2023, max_value=2100, value=2025, step=1)
with c2: quarter=st.selectbox("Çeyrek", [1,2,3,4], index=0)

sub = get_or_create(project_id, int(year), int(quarter))
st.info(f"Mevcut durum: **{sub.status}** | Portal ref: {sub.portal_reference or '-'}")

st.subheader("1) XML üret")
if st.button("XML üret", type="primary"):
    try:
        xml_bytes = build_cbam_xml_for_project_quarter(project_id=int(project_id), period_year=int(year), period_quarter=int(quarter))
        xml_uri = write_bytes(storage_path_for_project(project_id, f"cbam/xml/{year}Q{quarter}.xml"), xml_bytes)
        st.success(f"XML kaydedildi: {xml_uri}")
        st.session_state["cbam_xml_bytes"]=xml_bytes
        st.session_state["cbam_xml_uri"]=xml_uri
    except Exception as e:
        st.error(str(e))

st.subheader("2) Portal ZIP kontrol")
zip_up = st.file_uploader("Portal'a göndereceğin ZIP'i yükle", type=["zip"])
if zip_up is not None:
    zb = zip_up.read()
    ok, errs, warns, meta = validate_portal_zip_structure(zb)
    score = compute_readiness_score(True, ok, len(errs), len(warns))
    st.metric("Readiness Score", score)
    if errs: st.json(errs)
    if warns: st.json(warns)
    st.session_state["portal_zip_bytes"]=zb

st.subheader("3) Ready olarak işaretle")
schema_version = st.text_input("Schema version etiketi", value="official")
if st.button("Ready işaretle"):
    if "portal_zip_bytes" not in st.session_state:
        st.error("Önce ZIP yükle.")
    elif "cbam_xml_uri" not in st.session_state:
        st.error("Önce XML üret.")
    else:
        z_uri = write_bytes(storage_path_for_project(project_id, f"cbam/portal_zip/{year}Q{quarter}.zip"), st.session_state["portal_zip_bytes"])
        mark_ready(project_id,int(year),int(quarter), portal_zip_uri=z_uri, cbam_xml_uri=st.session_state["cbam_xml_uri"], schema_version=schema_version)
        st.success("Ready.")
        st.rerun()

st.subheader("4) Portala gönder")
if st.button("Gönder (Submit)"):
    if "portal_zip_bytes" not in st.session_state:
        st.error("ZIP yok.")
    else:
        resp = submit_to_portal(project_id,int(year),int(quarter), zip_bytes=st.session_state["portal_zip_bytes"], filename=f"cbam_{year}Q{quarter}.zip")
        if resp.ok:
            st.success(f"Gönderildi. ref={resp.reference}")
        else:
            st.error(f"Gönderim hatası: {resp.error}")
        st.rerun()

st.subheader("5) Durum güncelle")
if st.button("DURUM SORGULA"):
    rep = refresh_status(project_id,int(year),int(quarter))
    st.json(rep)
