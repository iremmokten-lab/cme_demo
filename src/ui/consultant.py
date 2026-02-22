import json
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.ui.components import h2
from src.services import projects as prj
from src.db.session import db
from src.db.models import DatasetUpload, CalculationSnapshot, Report
from src.services.ingestion import validate_csv
from src.services.storage import UPLOAD_DIR, write_bytes
from src.mrv.lineage import sha256_bytes
from src.mrv.audit import append_audit
from src.services.workflow import run_full
from src.services.reporting import build_pdf
from src.services.exports import build_zip, build_xlsx_from_results


def _safe_name(name: str) -> str:
    return (name or "").replace("/", "_").replace("\\", "_").strip()


def _save_upload(project_id: int, dataset_type: str, file_name: str, file_bytes: bytes, user_id: int | None):
    """
    DEDUP:
    Aynı project + dataset_type + sha256 zaten varsa yeni kayıt açmaz.
    """
    sha = sha256_bytes(file_bytes)
    safe = _safe_name(file_name) or f"{dataset_type}.csv"
    fp = UPLOAD_DIR / f"project_{project_id}" / dataset_type / f"{sha}_{safe}"

    # 1) Aynı hash ile zaten kayıt var mı?
    with db() as s:
        existing = s.execute(
            select(DatasetUpload).where(
                DatasetUpload.project_id == project_id,
                DatasetUpload.dataset_type == dataset_type,
                DatasetUpload.sha256 == sha,
            )
        ).scalar_one_or_none()

        if existing:
            # zaten var -> yeniden yazma/kayıt açma
            return existing.sha256

    # 2) Yoksa dosyayı yaz + DB kaydı oluştur
    write_bytes(fp, file_bytes)

    u = DatasetUpload(
        project_id=project_id,
        dataset_type=dataset_type,
        original_filename=safe,
        sha256=sha,
        schema_version="v1",
        storage_uri=str(fp),
        uploaded_by_user_id=user_id,
    )
    with db() as s:
        s.add(u)
        s.commit()

    append_audit("upload_saved", {"project_id": project_id, "type": dataset_type, "sha": sha, "uri": str(fp)})
    return sha


def consultant_app(user):
    st.title("Danışman Paneli")

    companies = prj.list_companies_for_user(user)
    if not companies:
        st.warning("Company yok.")
        return

    if "selected_project_id" not in st.session_state:
        st.session_state["selected_project_id"] = None

    with st.sidebar:
        st.markdown("### Şirket")
        company_map = {c.name: c.id for c in companies}
        company_name = st.selectbox("Seç", list(company_map.keys()), index=0)
        company_id = company_map[company_name]

        st.markdown("### Tesis")
        facilities = prj.list_facilities(company_id)
        fac_opts = {"(yok)": None}
        for f in facilities:
            fac_opts[f"{f.name} ({f.country})"] = f.id
        fac_label = st.selectbox("Tesis", list(fac_opts.keys()), index=0)
        facility_id = fac_opts[fac_label]

        with st.expander("Tesis oluştur"):
            fn = st.text_input("Tesis adı")
            cc = st.text_input("Ülke", value="TR")
            ss = st.text_input("Sektör", value="")
            if st.button("Tesis ekle"):
                try:
                    if not fn.strip():
                        st.warning("Tesis adı boş olamaz.")
                    else:
                        prj.create_facility(company_id, fn, cc, ss)
                        st.success("Tesis oluşturuldu.")
                        st.rerun()
                except Exception as e:
                    st.exception(e)

        st.markdown("### Proje")
        projects = prj.list_projects(company_id)
        proj_opts = {"(yeni)": None}
        for p in projects:
            proj_opts[f"{p.name} / {p.year} (id:{p.id})"] = p.id

        if st.session_state["selected_project_id"] not in proj_opts.values():
            st.session_state["selected_project_id"] = None

       
