from __future__ import annotations

import streamlit as st

from src.db.session import init_db
from src.mrv.audit import append_audit, infer_company_id_for_user
from src.services.authz import current_user, ensure_bootstrap_admin, login_view, logout_button

st.set_page_config(page_title="Carbon Compliance Platform", layout="wide")

# DB init + bootstrap
init_db()
ensure_bootstrap_admin()

user = current_user()
if not user:
    login_view()
    st.stop()

# Audit: home view
append_audit(
    "page_viewed",
    {"page": "home"},
    user_id=getattr(user, "id", None),
    company_id=infer_company_id_for_user(user),
    entity_type="page",
    entity_id=None,
)

st.title("Carbon Compliance Platform — CBAM + EU ETS + MRV")

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

st.success("Giriş başarılı ✅")

role = str(getattr(user, "role", "") or "").lower()

st.markdown(
    """
Bu demo uygulama **CBAM + ETS uyumluluğuna yaklaşan** bir Carbon MRV platformudur.

Sol menüden sayfa seçin:
- **Consultant Panel** → veri yükleme / hesaplama / senaryo / raporlar / evidence
- **Client Dashboard** → KPI + trend + snapshot karşılaştırma + rapor/evidence indirme
"""
)

st.divider()

if role.startswith("consultant"):
    st.info("Danışman rolündesiniz. Sol menüden **Consultant Panel** sayfasına gidin.")
else:
    st.info("Müşteri rolündesiniz. Sol menüden **Client Dashboard** sayfasına gidin.")

st.caption(
    "Not: Eğer yanlış role ile sayfa açarsanız, sistem erişimi engeller. "
    "Paylaşım için snapshot üzerinde 👁️ (shared_with_client) açılmalıdır."
)
