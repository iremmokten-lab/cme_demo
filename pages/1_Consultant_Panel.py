import streamlit as st

from src.db.session import init_db
from src.services.authz import ensure_bootstrap_admin, current_user, login_view
from src.ui.consultant import consultant_app

st.set_page_config(page_title="Danışman Paneli", layout="wide")

# DB init + bootstrap
init_db()
ensure_bootstrap_admin()

user = current_user()
if not user:
    login_view()
    st.stop()

# Yetki kontrolü
if not str(user.role).startswith("consultant"):
    st.error("Bu sayfa sadece danışman kullanıcılar içindir.")
    st.stop()

consultant_app(user)
