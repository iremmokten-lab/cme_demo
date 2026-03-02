from __future__ import annotations

import streamlit as st

from src.services.authz import current_user, login_view, logout_button
from src.services.security_audit import log_access
from src.services.regulation_updates import fetch_and_register, active_spec
from src.services.erp_connectors import create_connection, list_connections

st.set_page_config(page_title="Enterprise Admin Center", layout="wide")

user = current_user()
if not user:
    login_view()
    st.stop()

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

role = str(getattr(user, "role", "") or "").lower()
if not role.startswith("consultant_admin"):
    st.error("Bu sayfa sadece consultant_admin rolüne açıktır.")
    st.stop()

log_access(user, action="page_viewed", resource_type="page", resource_id="enterprise_admin_center")

st.title("🏢 Enterprise Admin Center")
st.caption("Regülasyon sürümleri + ERP bağlantıları + erişim/audit kayıtları yönetimi.")

tab1, tab2 = st.tabs(["📜 Regülasyon Sürümleri", "🔌 ERP Bağlantıları"])

with tab1:
    st.subheader("Regülasyon Spec Sürümleri")
    st.caption("Örn: CBAM_XSD, ETS_MRR, CBAM_ANNEX mapping dosyaları için sürüm kaydı.")

    spec_name = st.selectbox("Spec", ["CBAM_XSD", "ETS_MRR", "CBAM_ANNEX"], index=0)
    version_label = st.text_input("Version label", value="2025-04-01")
    url = st.text_input("Source URL", value="")

    if st.button("Fetch + Register", type="primary"):
        try:
            obj = fetch_and_register(spec_name, version_label, url)
            st.success(f"Kaydedildi: {obj.spec_name} {obj.version_label} sha={obj.sha256[:12]}...")
        except Exception as e:
            st.error(f"Başarısız: {e}")

    act = active_spec(spec_name)
    if act:
        st.info(f"Aktif: {act.spec_name} {act.version_label} sha={act.sha256[:12]}...")
    else:
        st.warning("Aktif sürüm yok.")

with tab2:
    st.subheader("ERP Bağlantıları")
    st.caption("OData/CSV/API türünde bağlantı tanımla. (Token secret Streamlit secrets ile yönetilebilir.)")

    company_id = int(getattr(user, "company_id", 0) or 0)
    conns = list_connections(company_id)
    if conns:
        st.dataframe([{"id":c.id, "name":c.name, "kind":c.kind, "base_url":c.base_url, "active":c.is_active} for c in conns], use_container_width=True)
    else:
        st.info("Henüz bağlantı yok.")

    with st.expander("➕ Yeni bağlantı ekle"):
        name = st.text_input("Bağlantı adı", value="SAP OData")
        kind = st.selectbox("Tür", ["odata","csv","api"], index=0)
        base_url = st.text_input("Base URL", value="")
        token = st.text_input("Token secret", value="", type="password")
        if st.button("Kaydet"):
            try:
                c = create_connection(company_id, name, kind, base_url, token, config={})
                st.success(f"Bağlantı eklendi: #{c.id}")
                st.rerun()
            except Exception as e:
                st.error(f"Eklenemedi: {e}")
