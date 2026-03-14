from __future__ import annotations
from src.db.session import init_db
import json
import streamlit as st
from src.services.authz import current_user, login_view, logout_button
from src.services.projects import list_company_projects_for_user
from src.services.integrations_registry import create_connection, list_connections

st.set_page_config(page_title="Integrations Admin", layout="wide")
init_db()
u=current_user()
if not u:
    login_view(); st.stop()
with st.sidebar:
    st.write(f"👤 {u.email}")
    st.caption(f"Rol: {u.role}")
    logout_button()

st.title("🔌 Entegrasyonlar (ERP/SCADA)")
st.caption("Bağlantı kayıtları: SAP/Logo/Netsis/Generic REST/OData. (İlk adım: registry)")

projects=list_company_projects_for_user(u)
if not projects:
    st.warning("Proje yok."); st.stop()
pmap={f"{p.name} (#{p.id})": int(p.id) for p in projects}
pl=st.selectbox("Proje", list(pmap.keys()))
project_id=pmap[pl]

with st.expander("➕ Yeni bağlantı"):
    name=st.text_input("Ad", value="SAP")
    kind=st.selectbox("Tür", ["odata","rest","file","generic"], index=0)
    base=st.text_input("Base URL", value="")
    auth=st.text_area("Auth JSON", value=json.dumps({"token":""}, ensure_ascii=False, indent=2), height=120)
    cfg=st.text_area("Config JSON", value=json.dumps({"paths":{}}, ensure_ascii=False, indent=2), height=120)
    if st.button("Kaydet", type="primary"):
        try:
            c=create_connection(project_id, name, kind=kind, base_url=base, auth=json.loads(auth), config=json.loads(cfg))
            st.success(f"Eklendi #{c.id}")
            st.rerun()
        except Exception as e:
            st.error(str(e))

conns=list_connections(project_id)
st.dataframe([{"id":c.id,"name":c.name,"kind":c.kind,"base_url":c.base_url,"status":c.status} for c in conns], use_container_width=True)
