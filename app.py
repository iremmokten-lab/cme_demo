import streamlit as st

from src.db.session import init_db
from src.services.authz import ensure_bootstrap_admin, login_view, current_user, logout_button
from src.ui.consultant import consultant_app
from src.ui.client import client_app

st.set_page_config(page_title="CME Platform", layout="wide")

def main():
    init_db()
    ensure_bootstrap_admin()

    user = current_user()
    if not user:
        login_view()
        return

    with st.sidebar:
        st.write(f"👤 **{user.email}**")
        st.caption(f"Rol: `{user.role}`")
        logout_button()

    if user.role.startswith("consultant"):
        consultant_app(user)
    else:
        client_app(user)

if __name__ == "__main__":
    main()
