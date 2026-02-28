from __future__ import annotations

import streamlit as st

from src.db.session import init_db
from src.services.authz import current_user, ensure_bootstrap_admin, login_view, logout_button
from src.ui.compliance_checklist import compliance_checklist_page

st.set_page_config(page_title="Uyum Kontrolleri", layout="wide")

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
    st.info("ETS/CBAM/TR-ETS zorunlu alanlar ve kontroller.", icon="✅")
    st.divider()
    logout_button()

compliance_checklist_page(user)
