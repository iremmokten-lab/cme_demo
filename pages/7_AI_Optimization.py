from __future__ import annotations

import streamlit as st

from src.db.session import init_db
from src.services.authz import current_user, ensure_bootstrap_admin, login_view, logout_button
from src.ui.ai_optimization import ai_optimization_page

st.set_page_config(page_title="AI & Optimizasyon", layout="wide")

init_db()
ensure_bootstrap_admin()

user = current_user()
if not user:
    login_view()
    st.stop()

if not str(getattr(user, "role", "") or "").lower().startswith("consultant"):
    st.error("Bu sayfa sadece danışman kullanıcılar içindir.")
    st.stop()

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    st.divider()
    st.info("Senaryo motoru + AI öneriler (hesap referanslı).", icon="🤖")
    st.divider()
    logout_button()

ai_optimization_page(user)
