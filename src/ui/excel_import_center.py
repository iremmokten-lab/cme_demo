# -*- coding: utf-8 -*-
from __future__ import annotations

import streamlit as st

from src.connectors.excel_connector import load_excel
from src.connectors.excel_schema import SCHEMAS
from src.services import projects as prj
from src.services.excel_ingestion_service import save_excel_dataset
from src.services.workflow import run_full


def render_excel_import_center(user):
    st.title("Excel Veri Yükleme Merkezi (Faz-0)")
    st.caption("Excel ile başlayın; ileride SAP/Logo/Netsis gibi ERP sistemleri aynı standarda adapte edilecektir.")

    projs = prj.list_company_projects_for_user(user)
    if not projs:
        st.warning("Bu kullanıcı için proje bulunamadı. Önce bir tesis/proje oluşturun.")
        return

    pmap = {f"{p.name} (#{p.id})": int(p.id) for p in projs}
    plabel = st.selectbox("Proje seç", list(pmap.keys()), key="excel_import_project")
    project_id = pmap[plabel]

    st.divider()

    dataset_type = st.selectbox(
        "Dataset türü seçin",
        options=list(SCHEMAS.keys()),
        help="Bu katmanda yapısal kolon kontrolü yapılır. Regülasyon zorunlu alanlar (HARD FAIL) validator katmanındadır.",
        key="excel_import_dataset_type",
    )

    with st.expander("Beklenen kolonlar (zorunlu)", expanded=True):
        for c in SCHEMAS[dataset_type]:
            if c.required:
                st.write(f"- **{c.name}** — {c.description_tr}")

    uploaded = st.file_uploader("Excel dosyasını yükleyin (.xlsx)", type=["xlsx"], key="excel_import_uploader")

    if uploaded is None:
        st.info("Excel şablonları: repo içinde `spec/excel_templates/` klasöründe.")
        return

    try:
        result = load_excel(uploaded, dataset_type)
        st.success("✅ Dosya doğrulandı. Deterministik dataset hash üretildi.")
        st.write("Dataset Hash (deterministik):")
        st.code(result["hash"])
        st.dataframe(result["dataframe"].head(200), use_container_width=True)
    except Exception as e:
        st.error(f"🚫 Dosya doğrulama başarısız: {e}")
        return

    st.divider()

    col1, col2 = st.columns([1, 1])
    with col1:
        do_save = st.button("İçe Aktar ve Kaydet", type="primary", use_container_width=True)
    with col2:
        do_save_and_snapshot = st.button("Kaydet + Snapshot Üret", type="secondary", use_container_width=True)

    if do_save or do_save_and_snapshot:
        try:
            imp = save_excel_dataset(
                project_id=int(project_id),
                dataset_type=str(dataset_type),
                df=result["dataframe"],
                original_filename=str(getattr(uploaded, "name", f"{dataset_type}.xlsx")),
                user=user,
            )
            st.success("✅ Dataset kaydedildi.")
            st.json(
                {
                    "upload_id": imp.upload_id,
                    "dataset_type": imp.dataset_type,
                    "storage_uri": imp.storage_uri,
                    "sha256_file": imp.sha256_file,
                    "dataset_hash": imp.dataset_hash,
                    "data_quality_score": imp.data_quality_score,
                }
            )

            with st.expander("Data Quality raporu", expanded=False):
                st.json(imp.data_quality_report)

            if do_save_and_snapshot:
                snap_id = run_full(project_id=int(project_id), created_by_user_id=getattr(user, "id", None))
                st.success(f"✅ Snapshot üretildi: #{snap_id}")
        except Exception as e:
            st.error(f"🚫 Kayıt / snapshot başarısız: {e}")
            st.exception(e)
