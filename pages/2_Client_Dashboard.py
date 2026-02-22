import streamlit as st

from src.db.session import init_db
from src.services.authz import ensure_bootstrap_admin, current_user, login_view
from src.ui.client import client_app

st.set_page_config(page_title="Müşteri Paneli", layout="wide")

# DB init + bootstrap
init_db()
ensure_bootstrap_admin()

user = current_user()
if not user:
    login_view()
    st.stop()

# Yetki kontrolü
if str(user.role).startswith("consultant"):
    st.error("Bu sayfa müşteri kullanıcıları içindir. (Danışman olarak Danışman Paneli’ni kullanın.)")
    st.stop()

client_app(user)
