from __future__ import annotations

import streamlit as st

from src.db.session import init_db
from src.services.authz import current_user, ensure_bootstrap_admin, login_view, logout_button
from src.ui.regulatory_mapping import regulatory_mapping_page

st.set_page_config(page_title="Regülasyon Eşleme", layout="wide")

init_db()
ensure_bootstrap_admin()

user = current_user()
if not user:
    login_view()
    st.stop()

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    st.divider()
    st.info("ETS/CBAM resmi şablon alan eşlemesi ve veri sözlüğü.", icon="📘")
    st.divider()
    logout_button()

regulatory_mapping_page()
