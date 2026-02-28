from __future__ import annotations

import streamlit as st

from src.db.session import init_db
from src.services.authz import current_user, login_view
from src.ui.ai_optimization import ai_optimization_page

st.set_page_config(page_title="AI & Optimizasyon", layout="wide")

init_db()

user = current_user()
if not user:
    login_view()
    st.stop()

ai_optimization_page(user)
