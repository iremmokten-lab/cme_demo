import json
import streamlit as st
from sqlalchemy import select

from src.db.session import db
from src.db.models import Project, CalculationSnapshot, Report
from src.ui.components import h2

def client_app(user):
    st.title("Client Dashboard")

    if not user.company_id:
        st.error("Bu kullanıcıya company atanmadı.")
        return

    with db() as s:
        projects = s.execute(select(Project).where(Project.company_id == user.company_id).order_by(Project.created_at.desc())).scalars().all()

    if not projects:
        st.info("Henüz proje yok. Danışman proje oluşturmalı.")
        return

    proj_label = [f"{p.name} / {p.year} (id:{p.id})" for p in projects]
    psel = st.selectbox("Project", proj_label)
    project = projects[proj_label.index(psel)]

    with db() as s:
        snaps = s.execute(select(CalculationSnapshot).where(CalculationSnapshot.project_id == project.id).order_by(CalculationSnapshot.created_at.desc())).scalars().all()

    if not snaps:
        st.info("Henüz snapshot yok.")
        return

    latest = snaps[0]
    results = json.loads(latest.results_json)
    kpis = results.get("kpis", {})

    h2("KPI Dashboard")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Energy tCO2", f'{kpis.get("energy_total_tco2", 0):.3f}')
    c2.metric("Scope1 tCO2", f'{kpis.get("energy_scope1_tco2", 0):.3f}')
    c3.metric("CBAM €", f'{kpis.get("cbam_cost_eur", 0):.2f}')
    c4.metric("ETS TL", f'{kpis.get("ets_cost_tl", 0):.2f}')

    h2("Reports")
    with db() as s:
        reports = s.execute(
            select(Report).join(CalculationSnapshot, Report.snapshot_id == CalculationSnapshot.id)
            .where(CalculationSnapshot.project_id == project.id)
            .order_by(Report.created_at.desc())
        ).scalars().all()

    if reports:
        st.dataframe([{"id": r.id, "type": r.report_type, "uri": r.storage_uri, "sha256": r.sha256, "at": r.created_at} for r in reports],
                     use_container_width=True)
    else:
        st.info("Henüz PDF rapor yok. Danışman üretmeli.")
