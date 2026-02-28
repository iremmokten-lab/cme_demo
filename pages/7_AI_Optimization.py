from __future__ import annotations

import streamlit as st

from src.db.session import init_db
from src.mrv.audit import append_audit, infer_company_id_for_user
from src.services.authz import current_user, ensure_bootstrap_admin, login_view, logout_button
from src.ui.ai_optimization import ai_optimization_page

st.set_page_config(page_title="AI & Optimization", layout="wide")

init_db()
ensure_bootstrap_admin()

user = current_user()
if not user:
    login_view()
    st.stop()

# Yetki kontrolü
if not str(user.role).startswith("consultant"):
    st.error("Bu sayfa sadece danışman kullanıcıları içindir.")
    st.stop()

append_audit(
    "page_viewed",
    {"page": "ai_optimization"},
    user_id=getattr(user, "id", None),
    company_id=infer_company_id_for_user(user),
    entity_type="page",
    entity_id=None,
)

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

ai_optimization_page(user)
