from __future__ import annotations
import streamlit as st
from src.services.authz import current_user, login_view, logout_button
from src.erp.verification.workspace import new_workspace

st.set_page_config(page_title="Doğrulayıcı Çalışma Alanı", layout="wide")
user=current_user()
if not user:
    login_view(); st.stop()
with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

st.title("🕵️ Doğrulayıcı Çalışma Alanı (Gelişmiş)")
st.caption("Sampling plan + doküman inceleme + bulgu notları için temel çalışma alanı.")

ws = new_workspace()
st.info("Bu ekran, doğrulayıcı deneyimi için temel iskelet sağlar. Sonraki adımda DB'ye bağlanıp gerçek case/finding kayıtları ile tamamlanır.")

st.subheader("Sampling Plan")
st.write("Şimdilik örnek gösterim.")
st.dataframe([{"record_ref":"energy/2025-01", "reason":"yüksek tüketim"}])

st.subheader("Bulgular")
st.dataframe([{"code":"F-01","description":"Kalibrasyon sertifikası eksik","severity":"high"}])

st.subheader("Notlar")
st.text_area("Denetçi notu", value="...")
