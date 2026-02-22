import streamlit as st

from src.db.session import init_db
from src.services.authz import ensure_bootstrap_admin, current_user, login_view, logout_button

st.set_page_config(page_title="CME Demo", layout="wide")

init_db()
ensure_bootstrap_admin()

user = current_user()
if not user:
    login_view()
    st.stop()

st.title("CME Demo")

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

st.success("Giriş başarılı ✅")
st.markdown(
    """
Sol menüden sayfa seçin:

- **Consultant Panel** → veri yükleme / hesaplama / senaryo / raporlar  
- **Client Dashboard** → KPI + trend + rapor indirme (müşteri görünümü)

> Not: Eğer yanlış role ile sayfa açarsanız, sistem izin vermez.
"""
)
