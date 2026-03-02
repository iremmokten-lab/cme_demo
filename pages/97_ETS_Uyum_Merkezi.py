from __future__ import annotations

import streamlit as st
import pandas as pd
import json

from sqlalchemy import select

from src.db.session import db, init_db
from src.services.authz import current_user, login_view, logout_button
from src.mrv.audit import infer_company_id_for_user
from src.db.ets_compliance_models import ETSMonitoringPlan, ETSUncertaintyAssessment, ETSQAQCEvidence, ETSTierJustification, ETSFallbackEvent
from src.services.ets_monitoring_plan import upsert_monitoring_plan
from src.services.ets_uncertainty import record_uncertainty

st.set_page_config(page_title="ETS Uyum Merkezi", layout="wide")
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

st.title("ETS Uyum Merkezi (Adım 2)")
st.caption("Monitoring plan, tier gerekçesi, belirsizlik, QA/QC kanıtı ve fallback kayıtları.")

tabs = st.tabs(["Monitoring Plan", "Uncertainty", "Tier Justification", "QA/QC Evidence", "Fallback Events"])

with db() as s:
    with tabs[0]:
        st.subheader("Monitoring Plan")
        year = st.number_input("Yıl", min_value=2005, max_value=2100, value=2025, step=1)
        plans = s.execute(select(ETSMonitoringPlan).where(ETSMonitoringPlan.company_id==company_id, ETSMonitoringPlan.year==int(year)).order_by(ETSMonitoringPlan.version.desc())).scalars().all()
        st.dataframe(pd.DataFrame([{"id":p.id,"version":p.version,"status":p.status,"hash":p.plan_hash[:12]} for p in plans]), use_container_width=True)
        st.markdown("### Yeni plan (JSON)")
        plan_text = st.text_area("Plan JSON", value='{"source_streams":[],"tiers":{},"qaqc":[]}', height=160)
        if st.button("Kaydet (Yeni Versiyon)", type="primary"):
            plan = json.loads(plan_text)
            row = upsert_monitoring_plan(company_id, int(year), plan)
            st.success(f"Kaydedildi: v{row.version}")
            st.rerun()

    with tabs[1]:
        st.subheader("Uncertainty Assessment")
        year2 = st.number_input("Yıl ", min_value=2005, max_value=2100, value=2025, step=1, key="y2")
        rows = s.execute(select(ETSUncertaintyAssessment).where(ETSUncertaintyAssessment.company_id==company_id, ETSUncertaintyAssessment.year==int(year2)).order_by(ETSUncertaintyAssessment.created_at.desc())).scalars().all()
        st.dataframe(pd.DataFrame([{"id":r.id,"percent":r.result_percent,"created_at":r.created_at} for r in rows]), use_container_width=True)
        errs = st.text_input("Error listesi (virgüllü)", value="1.2,0.8,0.3")
        if st.button("Hesapla & Kaydet", type="primary"):
            errors = [float(x.strip()) for x in errs.split(",") if x.strip()]
            row = record_uncertainty(company_id, int(year2), errors)
            st.success(f"Kaydedildi: {row.result_percent:.4f}")
            st.rerun()
