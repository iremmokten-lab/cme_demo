from __future__ import annotations

import streamlit as st

from src.db.session import init_db
from src.services.authz import get_or_create_demo_user
from src.ui.consultant import consultant_app

st.set_page_config(page_title="Consultant Panel", layout="wide")

init_db()

user = get_or_create_demo_user()

consultant_app(user)
