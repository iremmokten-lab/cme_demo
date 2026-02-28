from __future__ import annotations

import streamlit as st

from src.db.session import init_db
from src.services.authz import current_user, ensure_bootstrap_admin, login_view, logout_button
from src.ui.client import client_app

st.set_page_config(page_title="Müşteri Paneli", layout="wide")

init_db()
ensure_bootstrap_admin()

user = current_user()
if not user:
    login_view()
    st.stop()

role = str(getattr(user, "role", "") or "").lower()
if role.startswith("consultant") or role.startswith("verifier"):
    st.error("Bu sayfa müşteri kullanıcıları içindir.")
    st.stop()

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    st.divider()
    st.markdown("## Bilgi")
    st.caption("Müşteri sadece paylaşılan snapshotları (shared_with_client=True) görür.")
    st.divider()
    logout_button()

client_app(user)
