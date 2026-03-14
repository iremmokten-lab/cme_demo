from __future__ import annotations
from src.db.session import init_db
import streamlit as st
from src.services.authz import current_user, login_view, logout_button
from src.services.projects import list_company_projects_for_user
from src.services.support_bundle import build_support_bundle

st.set_page_config(page_title="Support Bundle", layout="wide")
init_db()
u=current_user()
if not u:
    login_view(); st.stop()
with st.sidebar:
    st.write(f"👤 {u.email}")
    st.caption(f"Rol: {u.role}")
    logout_button()

st.title("🧰 Support Bundle (Tek Dosya)")
st.caption("Müşteri destek/denetim için: compliance + snapshot list + evidence pack özetini ZIP indir.")

projects=list_company_projects_for_user(u)
if not projects:
    st.warning("Proje yok."); st.stop()
pmap={f"{p.name} (#{p.id})": int(p.id) for p in projects}
pl=st.selectbox("Proje", list(pmap.keys()))
project_id=pmap[pl]

if st.button("Support Bundle üret", type="primary"):
    try:
        b=build_support_bundle(int(project_id))
        st.success("Hazır.")
        st.download_button("support_bundle.zip indir", data=b, file_name="support_bundle.zip", mime="application/zip")
    except Exception as e:
        st.error(str(e))
