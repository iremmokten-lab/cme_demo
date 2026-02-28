from __future__ import annotations

import streamlit as st

from src.db.session import init_db
from src.services.authz import get_or_create_demo_user
from src.ui.consultant import consultant_app

st.set_page_config(page_title="Danışman Paneli", layout="wide")

init_db()

user = get_or_create_demo_user()

with st.sidebar:
    st.markdown("## ⚡ Hızlı Aksiyonlar")
    st.caption("Audit-ready akış: Yükle → Hesapla → Snapshot → Replay → Evidence Pack")
    st.info("Bu panel, MRV/ETS/CBAM hesaplama ve raporlama için ana çalışma alanıdır.", icon="ℹ️")

consultant_app(user)
