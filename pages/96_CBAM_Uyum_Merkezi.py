from __future__ import annotations

import streamlit as st
import pandas as pd

from sqlalchemy import select

from src.db.session import db, init_db
from src.services.authz import current_user, login_view, logout_button
from src.mrv.audit import append_audit, infer_company_id_for_user
from src.db.cbam_compliance_models import CBAMProducer, CBAMProducerAttestation, CBAMMethodologyEvidence, CBAMCarbonPricePaid, CBAMQuarterlySubmission
from src.services.cbam_portal_package import build_cbam_portal_package, store_cbam_portal_package
from src.services.cbam_schema_registry import fetch_and_cache_official_cbam_xsd_zip, get_latest_cbam_xsd

st.set_page_config(page_title="CBAM Uyum Merkezi", layout="wide")
init_db()

u = current_user()
if not u:
    login_view()
    st.stop()

company_id = infer_company_id_for_user(u)
with st.sidebar:
    st.write(f"👤 {u.email}")
    st.caption(f"Rol: {u.role}")
    logout_button()

st.title("CBAM Uyum Merkezi (Adım 1)")
st.caption("Producer doğrulama, metodoloji kanıtı, carbon price paid ve quarterly submission yönetimi.")

tabs = st.tabs(["Üreticiler", "Attestation", "Metodoloji Kanıtı", "Carbon Price Paid", "Quarterly Submission", "Portal Paketi & XSD"])

with db() as s:
    # Producers
    with tabs[0]:
        st.subheader("Üretici Kaydı")
        rows = s.execute(select(CBAMProducer).where(CBAMProducer.company_id==company_id)).scalars().all()
        st.dataframe(pd.DataFrame([{"id":r.id,"producer_code":r.producer_code,"legal_name":r.legal_name,"country":r.country,"active":r.is_active} for r in rows]), use_container_width=True)

        st.markdown("### Yeni üretici ekle")
        code = st.text_input("Producer code / id", value="")
        name = st.text_input("Legal name", value="")
        country = st.text_input("Country", value="")
        if st.button("Kaydet (Üretici)", type="primary"):
            s.add(CBAMProducer(company_id=company_id, producer_code=code.strip(), legal_name=name.strip(), country=country.strip()))
            s.commit()
            st.success("Kaydedildi")
            st.rerun()

    with tabs[4]:
        st.subheader("Quarterly Submission")
        subs = s.execute(select(CBAMQuarterlySubmission).where(CBAMQuarterlySubmission.company_id==company_id).order_by(CBAMQuarterlySubmission.year.desc(), CBAMQuarterlySubmission.quarter.desc())).scalars().all()
        st.dataframe(pd.DataFrame([{"id":x.id,"year":x.year,"quarter":x.quarter,"status":x.status,"snapshot_id":x.snapshot_id,"sha256":x.portal_package_sha256} for x in subs]), use_container_width=True)

    with tabs[5]:
        st.subheader("Portal Paketi & XSD")
        if st.button("Resmi XSD indir & cache'le"):
            info = fetch_and_cache_official_cbam_xsd_zip()
            st.success(f"Kaydedildi: {info.spec_version} ({info.spec_hash[:12]})")

        latest = get_latest_cbam_xsd()
        st.write("Mevcut XSD:", latest.spec_version if latest else "Yok")

        snap_id = st.number_input("Snapshot ID", min_value=1, step=1, value=1)
        if st.button("Portal paketi üret (ZIP)"):
            bts, manifest = build_cbam_portal_package(int(snap_id))
            st.download_button("İndir: cbam_portal_package.zip", data=bts, file_name="cbam_portal_package.zip")
            st.json(manifest)

        if st.button("Portal paketi üret ve storage'a kaydet"):
            obj = store_cbam_portal_package(int(snap_id))
            st.success("Kaydedildi")
            st.json(obj)
