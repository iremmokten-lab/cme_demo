import json
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.session import db
from src.db.models import Project, CalculationSnapshot, Report
from src.services.exports import build_zip, build_xlsx_from_results
from src.services.mailer import send_pdf_mail
from src.ui.components import h2


def _fmt(x, d=2):
    try:
        return f"{float(x):,.{d}f}"
    except:
        return "0"


def client_app(user):
    st.title("Müşteri Paneli")

    if not user.company_id:
        st.error("Bu kullanıcıya şirket atanmadı.")
        return

    with db() as s:
        projects = s.execute(
            select(Project)
            .where(Project.company_id == user.company_id)
            .order_by(Project.created_at.desc())
        ).scalars().all()

    if not projects:
        st.info("Henüz proje yok.")
        return

    proj_label = [f"{p.name} / {p.year}" for p in projects]
    psel = st.selectbox("Proje seç", proj_label)
    project = projects[proj_label.index(psel)]

    with db() as s:
        snaps = s.execute(
            select(CalculationSnapshot)
            .where(CalculationSnapshot.project_id == project.id)
            .order_by(CalculationSnapshot.created_at.desc())
        ).scalars().all()

    if not snaps:
        st.info("Henüz hesaplama yapılmamış.")
        return

    latest = snaps[0]

    try:
        results = json.loads(latest.results_json)
    except:
        results = {}

    kpis = results.get("kpis", {})

    h2("KPI Dashboard")

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Toplam Emisyon", _fmt(kpis.get("energy_total_tco2"), 3))
    c2.metric("Scope-1", _fmt(kpis.get("energy_scope1_tco2"), 3))
    c3.metric("CBAM €", _fmt(kpis.get("cbam_cost_eur")))
    c4.metric("ETS TL", _fmt(kpis.get("ets_cost_tl")))

    st.divider()

    # TREND
    rows = []
    for s in snaps[:20]:
        try:
            r = json.loads(s.results_json)
            k = r.get("kpis", {})
        except:
            k = {}

        rows.append(
            {
                "date": s.created_at,
                "emission": k.get("energy_total_tco2", 0),
                "cbam": k.get("cbam_cost_eur", 0),
                "ets": k.get("ets_cost_tl", 0),
            }
        )

    if rows:
        df = pd.DataFrame(rows).sort_values("date")
        st.line_chart(df.set_index("date"))

    st.divider()

    h2("Raporlar")

    with db() as s:
        reports = s.execute(
            select(Report)
            .join(CalculationSnapshot, Report.snapshot_id == CalculationSnapshot.id)
            .where(CalculationSnapshot.project_id == project.id)
            .order_by(Report.created_at.desc())
        ).scalars().all()

    if not reports:
        st.info("Henüz PDF rapor yok.")
        return

    for r in reports:

        col1, col2 = st.columns([3, 1])

        with col1:
            st.write(f"PDF • {r.created_at}")

        with col2:
            p = Path(r.storage_uri)
            if p.exists():
                data = p.read_bytes()

                st.download_button(
                    "İndir",
                    data=data,
                    file_name=p.name,
                    mime="application/pdf",
                    key=f"pdf_{r.id}",
                )

    st.divider()

    st.subheader("Export")

    if st.button("ZIP indir"):
        st.download_button(
            "ZIP",
            build_zip(latest.id, latest.results_json),
            file_name="export.zip",
        )

    if st.button("Excel indir"):
        st.download_button(
            "Excel",
            build_xlsx_from_results(latest.results_json),
            file_name="export.xlsx",
        )

    if st.button("JSON indir"):
        st.download_button(
            "JSON",
            latest.results_json.encode(),
            file_name="export.json",
        )

    st.divider()

    st.subheader("PDF Mail Gönder")

    email = st.text_input("Email")

    if st.button("Gönder"):

        if not email:
            st.warning("Email girin")
        else:
            p = Path(reports[0].storage_uri)

            if p.exists():
                send_pdf_mail(email, p.read_bytes(), p.name)
                st.success("Mail gönderildi")
