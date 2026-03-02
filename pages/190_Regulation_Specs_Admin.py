from __future__ import annotations
import streamlit as st
from src.services.authz import current_user, login_view, logout_button
from src.services.regulation_spec_registry import register_spec, list_specs

st.set_page_config(page_title="Regulation Specs", layout="wide")
u=current_user()
if not u:
    login_view(); st.stop()
with st.sidebar:
    st.write(f"👤 {u.email}")
    st.caption(f"Rol: {u.role}")
    logout_button()

st.title("📚 Regülasyon Sürüm Kayıtları")
st.caption("Regülasyon/spec değişim yönetimi: code + version + sha256.")

with st.expander("➕ Yeni kayıt"):
    code=st.selectbox("Code", ["CBAM","ETS","TR_ETS","OTHER"], index=0)
    ver=st.text_input("Version", value="2025-01")
    url=st.text_input("Source URL", value="")
    notes=st.text_area("Not", value="")
    if st.button("Kaydet", type="primary"):
        try:
            s=register_spec(code, ver, url, notes=notes)
            st.success(f"Kaydedildi #{s.id} sha={s.sha256[:12]}")
            st.rerun()
        except Exception as e:
            st.error(str(e))

specs=list_specs()
st.dataframe([{"id":s.id,"code":s.code,"version":s.version,"sha256":(s.sha256 or "")[:16],"url":s.source_url} for s in specs], use_container_width=True)
