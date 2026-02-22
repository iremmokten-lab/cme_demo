import json
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.services import projects as prj
from src.db.session import db
from src.db.models import DatasetUpload, CalculationSnapshot, Report
from src.services.ingestion import validate_csv
from src.services.storage import UPLOAD_DIR, write_bytes
from src.mrv.lineage import sha256_bytes
from src.services.workflow import run_full
from src.services.reporting import build_pdf
from src.services.exports import build_zip, build_xlsx_from_results


def _safe_name(name: str) -> str:
    return (name or "").replace("/", "_").replace("\\", "_").strip()


def _save_upload_dedup(project_id: int, dataset_type: str, file_name: str, file_bytes: bytes, user_id: int | None):
    sha = sha256_bytes(file_bytes)
    safe = _safe_name(file_name) or f"{dataset_type}.csv"
    fp = UPLOAD_DIR / f"project_{project_id}" / dataset_type / f"{sha}_{safe}"

    with db() as s:
        existing = s.execute(
            select(DatasetUpload).where(
                DatasetUpload.project_id == project_id,
                DatasetUpload.dataset_type == dataset_type,
                DatasetUpload.sha256 == sha,
            )
        ).scalar_one_or_none()
        if existing:
            return existing.sha256

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

    return sha


def consultant_app(user):
    st.title("Danışman Paneli")

    # ---- SIDE BAR ----
    try:
        companies = prj.list_companies_for_user(user)
    except Exception as e:
        st.error("Company listesi alınamadı.")
        st.exception(e)
        return

    if not companies:
        st.warning("Hiç company yok.")
        return

    with st.sidebar:
        st.markdown("### Şirket")
        company_map = {c.name: c.id for c in companies}
        company_name = st.selectbox("Seç", list(company_map.keys()), index=0)
        company_id = company_map[company_name]

        st.markdown("### Tesis")
        try:
            facilities = prj.list_facilities(company_id)
        except Exception as e:
            st.error("Facility listesi alınamadı.")
            st.exception(e)
            return

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
        try:
            projects = prj.list_projects(company_id)
        except Exception as e:
            st.error("Project listesi alınamadı.")
            st.exception(e)
            return

        proj_opts = {"(yeni proje oluştur)": None}
        for p in projects:
            proj_opts[f"{p.name} / {p.year} (id:{p.id})"] = p.id

        psel = st.selectbox("Proje seç", list(proj_opts.keys()), index=0)

        if psel == "(yeni proje oluştur)":
            pn = st.text_input("Proje adı")
            py = st.number_input("Yıl", 2000, 2100, 2025)
            if st.button("Proje oluştur", type="primary"):
                try:
                    if not pn.strip():
                        st.warning("Proje adı boş olamaz.")
                    else:
                        newp = prj.create_project(company_id, facility_id, pn, int(py))
                        st.success(f"Proje oluşturuldu: id={newp.id}")
                        st.rerun()
                except Exception as e:
                    st.exception(e)
            st.info("Devam etmek için proje oluştur veya mevcut proje seç.")
            st.stop()

        project_id = proj_opts[psel]

        st.divider()
        st.markdown("### Parametreler")
        eua = st.slider("EUA (€/t)", 0.0, 200.0, 80.0)
        fx = st.number_input("FX (TL/€)", value=35.0)
        free_alloc = st.number_input("Free allocation (tCO2)", value=0.0)
        banked = st.number_input("Banked (tCO2)", value=0.0)

    # ---- TABS ----
    tabs = st.tabs(["Yükleme", "Hesapla", "Geçmiş", "Raporlar / İndir", "Senaryolar"])

    # Upload
    with tabs[0]:
        st.subheader("CSV Yükleme")
        col1, col2 = st.columns(2)
        with col1:
            up_energy = st.file_uploader("energy.csv", type=["csv"], key=f"energy_{project_id}")
        with col2:
            up_prod = st.file_uploader("production.csv", type=["csv"], key=f"prod_{project_id}")

        if up_energy is not None:
            try:
                b = up_energy.getvalue()
                df = pd.read_csv(up_energy)
                errs = validate_csv("energy", df)
                if errs:
                    st.error(" | ".join(errs))
                else:
                    sha = _save_upload_dedup(project_id, "energy", up_energy.name, b, user.id)
                    st.success(f"energy.csv yüklendi ✅ sha={sha[:10]}…")
            except Exception as e:
                st.exception(e)

        if up_prod is not None:
            try:
                b = up_prod.getvalue()
                df = pd.read_csv(up_prod)
                errs = validate_csv("production", df)
                if errs:
                    st.error(" | ".join(errs))
                else:
                    sha = _save_upload_dedup(project_id, "production", up_prod.name, b, user.id)
                    st.success(f"production.csv yüklendi ✅ sha={sha[:10]}…")
            except Exception as e:
                st.exception(e)

    # Run
    with tabs[1]:
        st.subheader("Hesaplama")
        config = {
            "eua_price_eur": float(eua),
            "fx_tl_per_eur": float(fx),
            "free_alloc_t": float(free_alloc),
            "banked_t": float(banked),
        }
        if st.button("Baseline Çalıştır", type="primary"):
            try:
                snap = run_full(project_id, config=config, scenario=None)
                st.success(f"Snapshot: {snap.id}  hash={snap.result_hash[:10]}…")
            except Exception as e:
                st.exception(e)

    # History
    with tabs[2]:
        st.subheader("Geçmiş")
        try:
            with db() as s:
                uploads = s.execute(
                    select(DatasetUpload)
                    .where(DatasetUpload.project_id == project_id)
                    .order_by(DatasetUpload.uploaded_at.desc())
                ).scalars().all()
                snaps = s.execute(
                    select(CalculationSnapshot)
                    .where(CalculationSnapshot.project_id == project_id)
                    .order_by(CalculationSnapshot.created_at.desc())
                ).scalars().all()
        except Exception as e:
            st.exception(e)
            return

        st.markdown("#### Uploads")
        st.dataframe([{
            "id": u.id,
            "type": u.dataset_type,
            "file": u.original_filename,
            "sha256": (u.sha256[:12] + "…") if u.sha256 else "",
            "at": u.uploaded_at,
        } for u in uploads], use_container_width=True)

        st.markdown("#### Snapshots")
        st.dataframe([{
            "id": sn.id,
            "hash": (sn.result_hash[:12] + "…") if sn.result_hash else "",
            "at": sn.created_at,
        } for sn in snaps], use_container_width=True)

    # Reports/Export
    with tabs[3]:
        st.subheader("Raporlar / İndir")
        with db() as s:
            snaps = s.execute(
                select(CalculationSnapshot)
                .where(CalculationSnapshot.project_id == project_id)
                .order_by(CalculationSnapshot.created_at.desc())
            ).scalars().all()

        if not snaps:
            st.info("Önce snapshot üret.")
            st.stop()

        options = [f"id:{sn.id}  {sn.created_at}" for sn in snaps]
        sel = st.selectbox("Snapshot seç", options)
        sn = snaps[options.index(sel)]

        results = json.loads(sn.results_json)
        kpis = results.get("kpis", {})
        config = json.loads(sn.config_json) if sn.config_json else {}

        colA, colB, colC, colD = st.columns(4)

        with colA:
            if st.button("PDF Üret", type="primary"):
                pdf_uri, pdf_sha = build_pdf(
                    sn.id,
                    "CME Demo Raporu — CBAM + ETS (Tahmini)",
                    {"kpis": kpis, "config": config, "top_skus": []},
                )
                with db() as s:
                    r = Report(snapshot_id=sn.id, report_type="pdf", storage_uri=pdf_uri, sha256=pdf_sha)
                    s.add(r)
                    s.commit()
                st.success("PDF üretildi ✅")

        with colB:
            zip_bytes = build_zip(sn.id, sn.results_json)
            st.download_button("ZIP indir (JSON+XLSX)", data=zip_bytes, file_name=f"snapshot_{sn.id}.zip")

        with colC:
            xlsx_bytes = build_xlsx_from_results(sn.results_json)
            st.download_button("XLSX indir", data=xlsx_bytes, file_name=f"snapshot_{sn.id}.xlsx")

        with colD:
            st.download_button("JSON indir", data=sn.results_json.encode("utf-8"), file_name=f"snapshot_{sn.id}.json")

        st.divider()

        with db() as s:
            reports = s.execute(
                select(Report).where(Report.snapshot_id == sn.id).order_by(Report.created_at.desc())
            ).scalars().all()

        st.markdown("#### PDF İndirme")
        for r in reports:
            p = Path(r.storage_uri)
            if p.exists():
                st.download_button(
                    "PDF indir",
                    data=p.read_bytes(),
                    file_name=p.name,
                    mime="application/pdf",
                    key=f"dl_pdf_{r.id}",
                )
            else:
                st.warning("PDF bulunamadı (reboot sonrası silinmiş olabilir).")

    # Scenarios (mevcut engine çalışıyorsa dokunmuyoruz)
    with tabs[4]:
        st.subheader("Senaryolar")
        st.info("Senaryo ekranın şu an çalışıyor — bunu bozmamak için burada dokunmadım.")
