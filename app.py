import streamlit as st

from src.db.session import init_db
from src.services.authz import (
    ensure_bootstrap_admin,
    login_view,
    current_user,
    logout_button,
)
from src.ui.consultant import consultant_app
from src.ui.client import client_app

st.set_page_config(page_title="CME Platform (Demo)", layout="wide")

# DB bootstrap
init_db()
ensure_bootstrap_admin()

# Auth
user = current_user()
if not user:
    # authz.login_view() şu an repo'da "Login" başlıklı; dosya talebiniz app.py olduğu için burada ekstra dokunmuyoruz.
    login_view()
    st.stop()

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

# Routing
if str(user.role).startswith("consultant"):
    consultant_app(user)
else:
    client_app(user)
