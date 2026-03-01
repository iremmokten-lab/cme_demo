
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path

import streamlit as st
from sqlalchemy import select

from src.db.models import Project
from src.db.session import db
from src.services.excel_ingestion_service import ingest_excel_to_datasetupload
from src.connectors.excel_schema import SCHEMAS


SPEC_DIR = Path("./spec")  # kullanıcı ekran görüntüsünde bu klasörü kullandı


def _load_template_bytes(template_name: str) -> tuple[bytes | None, str]:
    fp = SPEC_DIR / template_name
    if fp.exists() and fp.is_file():
        return fp.read_bytes(), fp.name
    return None, template_name


def render_excel_import_center(user):
    st.title("Excel Veri Yükleme Merkezi (Faz-0)")
    st.caption("Excel ile başla → CSV formatına deterministik dönüştür → storage'a kaydet → dataset kayıtları oluşsun.")

    # Project selection
    with db() as s:
        projects = s.execute(select(Project).order_by(Project.created_at.desc())).scalars().all()

    if not projects:
        st.warning("Henüz proje yok. Önce Danışman Paneli > Kurulum sekmesinden proje oluşturun.")
        return

    proj_map = {f"#{p.id} — {p.name}": p.id for p in projects}
    proj_label = st.selectbox("Proje seçin", options=list(proj_map.keys()))
    project_id = int(proj_map[proj_label])

    st.markdown("### 1) Şablon indir (opsiyonel)")
    st.write("Şablonlar repo içinde `spec/` klasöründe durur. İndirmek için aşağıdan tıklayın:")

    tpl_cols = st.columns(3)
    templates = [
        ("facility", "facility_template.xlsx"),
        ("energy", "energy_template.xlsx"),
        ("production", "production_template.xlsx"),
        ("cbam_products", "cbam_products_template.xlsx"),
        ("bom_precursors", "bom_precursors_template.xlsx"),
    ]

    for i, (dtype, fname) in enumerate(templates):
        with tpl_cols[i % 3]:
            b, real_name = _load_template_bytes(fname)
            if b is None:
                st.warning(f"Şablon bulunamadı: spec/{fname}")
            else:
                st.download_button(
                    label=f"📥 {dtype} şablonu indir",
                    data=b,
                    file_name=real_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )

    st.markdown("---")
    st.markdown("### 2) Excel yükle → Kaydet")

    dataset_type = st.selectbox("Dataset türü", options=list(SCHEMAS.keys()))

    with st.expander("Beklenen kolonlar (zorunlu)", expanded=True):
        for col in SCHEMAS[dataset_type]:
            if col.required:
                st.write(f"- **{col.name}** — {col.description_tr}")

    uploaded = st.file_uploader("Excel dosyası (.xlsx)", type=["xlsx"], key="excel_upload_center")

    if uploaded is not None:
        xbytes = uploaded.getvalue()
        st.write(f"Dosya: **{uploaded.name}** ({len(xbytes)} byte)")

        if st.button("✅ Kaydet (DatasetUpload oluştur)", type="primary", use_container_width=True):
            try:
                res = ingest_excel_to_datasetupload(
                    project_id=project_id,
                    dataset_type=dataset_type,
                    xlsx_bytes=xbytes,
                    original_filename=uploaded.name,
                    uploaded_by_user_id=getattr(user, "id", None),
                )

                st.success("Kaydedildi ✅")
                st.write("DatasetUpload ID:", res["dataset_upload_id"])
                st.write("storage_uri:", res["storage_uri"])
                st.code(f"sha256: {res['sha256']}\ncontent_hash: {res['content_hash']}")

                if not res["validated"]:
                    st.error("⚠️ Core CSV validator FAIL (energy/production/materials için).")
                    st.json(res["core_validation_errors"])

                st.write("Data Quality Skoru:", res["data_quality_score"])
                with st.expander("Data Quality raporu", expanded=False):
                    st.json(res["data_quality_report"])

            except Exception as e:
                st.error("Kaydetme başarısız")
                st.exception(e)
