import json
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.session import db
from src.db.models import DatasetUpload, CalculationSnapshot, Report
from src.services.workflow import run_full
from src.services.reporting import build_pdf
from src.services.exports import build_zip
from src.services.storage import UPLOAD_DIR, write_bytes
from src.services.ingestion import validate_csv
from src.services import projects as prj
from src.mrv.lineage import sha256_bytes


def consultant_app(user):

    st.title("Danışman Paneli")

    companies = prj.list_companies_for_user(user)

    if not companies:
        st.error("Company bulunamadı")
        return

    with st.sidebar:

        company_map = {c.name: c.id for c in companies}
        cname = st.selectbox("Company", list(company_map.keys()))
        company_id = company_map[cname]

        st.divider()

        facilities = prj.list_facilities(company_id)
        fac_map = {"(yok)": None}
        for f in facilities:
            fac_map[f.name] = f.id

        fname = st.selectbox("Facility", list(fac_map.keys()))
        facility_id = fac_map[fname]

        st.divider()

        projects = prj.list_projects(company_id)

        proj_map = {"(yeni)": None}
        for p in projects:
            proj_map[f"{p.name} / {p.year}"] = p.id

        pname = st.selectbox("Project", list(proj_map.keys()))

        if pname == "(yeni)":

            new_name = st.text_input("Project name")
            new_year = st.number_input("Year", 2020, 2100, 2025)

            if st.button("Create project"):

                if new_name.strip() == "":
                    st.warning("Project name boş olamaz")

                else:
                    prj.create_project(company_id, facility_id, new_name, int(new_year))
                    st.success("Project oluşturuldu")
                    st.rerun()

            st.stop()

        project_id = proj_map[pname]

        st.divider()

        eua = st.slider("EUA €", 0.0, 200.0, 80.0)
        fx = st.number_input("FX TL/EUR", value=35.0)
        free_alloc = st.number_input("Free allocation", value=0.0)
        banked = st.number_input("Banked", value=0.0)

    tabs = st.tabs(
        ["Upload", "Run", "History", "Reports / Export"]
    )

    # ------------------------
    # UPLOAD
    # ------------------------

    with tabs[0]:

        st.subheader("CSV Upload")

        col1, col2 = st.columns(2)

        with col1:
            energy = st.file_uploader("energy.csv", type=["csv"])

        with col2:
            production = st.file_uploader("production.csv", type=["csv"])

        if energy:

            df = pd.read_csv(energy)
            errors = validate_csv("energy", df)

            if errors:
                st.error(errors)

            else:

                b = energy.getvalue()
                sha = sha256_bytes(b)

                path = UPLOAD_DIR / f"{project_id}_energy_{sha}.csv"
                write_bytes(path, b)

                with db() as s:
                    u = DatasetUpload(
                        project_id=project_id,
                        dataset_type="energy",
                        original_filename=energy.name,
                        sha256=sha,
                        storage_uri=str(path),
                    )
                    s.add(u)
                    s.commit()

                st.success("energy.csv yüklendi")

        if production:

            df = pd.read_csv(production)
            errors = validate_csv("production", df)

            if errors:
                st.error(errors)

            else:

                b = production.getvalue()
                sha = sha256_bytes(b)

                path = UPLOAD_DIR / f"{project_id}_production_{sha}.csv"
                write_bytes(path, b)

                with db() as s:
                    u = DatasetUpload(
                        project_id=project_id,
                        dataset_type="production",
                        original_filename=production.name,
                        sha256=sha,
                        storage_uri=str(path),
                    )
                    s.add(u)
                    s.commit()

                st.success("production.csv yüklendi")

    # ------------------------
    # RUN
    # ------------------------

    with tabs[1]:

        st.subheader("Run Calculation")

        config = {
            "eua_price_eur": float(eua),
            "fx_tl_per_eur": float(fx),
            "free_alloc_t": float(free_alloc),
            "banked_t": float(banked),
        }

        if st.button("Run baseline"):

            snap = run_full(project_id, config=config, scenario=None)

            st.success(f"Snapshot: {snap.id} hash={snap.result_hash[:10]}")

    # ------------------------
    # HISTORY
    # ------------------------

    with tabs[2]:

        st.subheader("Uploads")

        with db() as s:

            uploads = s.execute(
                select(DatasetUpload)
                .where(DatasetUpload.project_id == project_id)
            ).scalars().all()

        if uploads:

            st.dataframe(
                [
                    {
                        "id": u.id,
                        "type": u.dataset_type,
                        "file": u.original_filename,
                        "sha": u.sha256,
                    }
                    for u in uploads
                ]
            )

        else:
            st.info("Upload yok")

        st.divider()

        st.subheader("Snapshots")

        with db() as s:

            snaps = s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == project_id)
            ).scalars().all()

        if snaps:

            st.dataframe(
                [
                    {
                        "id": s.id,
                        "hash": s.result_hash,
                        "date": s.created_at,
                    }
                    for s in snaps
                ]
            )

        else:
            st.info("Snapshot yok")

    # ------------------------
    # REPORTS
    # ------------------------

    with tabs[3]:

        st.subheader("PDF / Export")

        with db() as s:
            snaps = s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == project_id)
            ).scalars().all()

        if not snaps:
            st.info("Önce snapshot üret")
            st.stop()

        snap_map = {f"id:{s.id}": s for s in snaps}
        chosen = st.selectbox("Snapshot", list(snap_map.keys()))
        snap = snap_map[chosen]

        results = json.loads(snap.results_json)

        if st.button("Generate PDF"):

            pdf_uri, sha = build_pdf(
                snap.id,
                "CME Report",
                results.get("kpis", {}),
            )

            with db() as s:
                r = Report(
                    snapshot_id=snap.id,
                    report_type="pdf",
                    storage_uri=pdf_uri,
                    sha256=sha,
                )
                s.add(r)
                s.commit()

            st.success("PDF oluşturuldu")

        st.divider()

        with db() as s:
            reports = s.execute(
                select(Report)
                .where(Report.snapshot_id == snap.id)
            ).scalars().all()

        if reports:

            for r in reports:

                path = Path(r.storage_uri)

                if path.exists():

                    st.download_button(
                        "PDF indir",
                        data=path.read_bytes(),
                        file_name=path.name,
                        mime="application/pdf",
                        key=f"pdf{r.id}",
                    )

        else:
            st.info("Rapor yok")
