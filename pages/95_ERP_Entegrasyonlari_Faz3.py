from __future__ import annotations

import json
import streamlit as st
from sqlalchemy import select

from src.db.models import Project
from src.db.session import db, init_db
from src.services.authz import current_user, login_view, logout_button
from src.mrv.audit import append_audit, infer_company_id_for_user
from src.services.erp_sync_service import ERPSyncService


st.set_page_config(page_title="ERP Entegrasyonları (Faz 3)", layout="wide")

init_db()


def _rerun():
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()
        except Exception:
            return


user = current_user()
if not user:
    login_view()
    st.stop()

role = str(getattr(user, "role", "") or "").lower()
if not role.startswith("consultant"):
    st.error("Bu sayfa sadece danışman (consultant) rolüne açıktır.")
    st.stop()

company_id = infer_company_id_for_user(user)
if not company_id:
    st.error("Şirket bilgisi bulunamadı.")
    st.stop()

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    logout_button()

append_audit(
    "page_viewed",
    {"page": "erp_integrations"},
    user_id=getattr(user, "id", None),
    company_id=company_id,
    entity_type="page",
    entity_id=None,
)


st.title("Faz 3 — ERP Entegrasyonları")
st.caption(
    "Sade anlatım: ERP'den veri çekmek için bağlantı tanımlarsın → veri dosyası yüklersin veya REST ile çekersin → "
    "sistem bunu DatasetUpload'a çevirir. Böylece snapshot ve raporlar ERP verisiyle çalışır."
)


with db() as s:
    svc = ERPSyncService(s, company_id=company_id, user_id=getattr(user, "id", None))

    # Project selection
    projects = s.execute(select(Project).order_by(Project.created_at.desc())).scalars().all()
    if not projects:
        st.warning("Henüz proje yok. Önce Danışman Paneli > Kurulum sekmesinden proje oluşturun.")
        st.stop()

    proj_map = {f"#{p.id} — {p.name}": p.id for p in projects}
    proj_label = st.selectbox("Proje seçin", options=list(proj_map.keys()))
    project_id = int(proj_map[proj_label])

    tabs = st.tabs(["🔌 Bağlantılar", "🧩 Mapping", "⬆️ Dosya ile Sync", "🌐 REST ile Sync", "🧾 Çalıştırma Kayıtları"])

    # -----------------------------
    # Connections
    # -----------------------------
    with tabs[0]:
        st.subheader("Bağlantılar")
        st.info(
            "Güvenlik: Secret (token/şifre) DB'ye yazılmaz. 'secret_ref' girersin, sonra ortam değişkenine eklersin: "
            "ERP_SECRET_<secret_ref>=... (Streamlit Cloud secrets / env)."
        )

        conns = svc.list_connections()
        if conns:
            st.dataframe(
                [
                    {
                        "id": c.id,
                        "name": c.name,
                        "vendor": c.vendor,
                        "mode": c.mode,
                        "base_url": c.base_url,
                        "auth_type": c.auth_type,
                        "secret_ref": c.secret_ref,
                        "active": c.is_active,
                        "updated_at": c.updated_at,
                    }
                    for c in conns
                ],
                use_container_width=True,
            )
        else:
            st.warning("Henüz bağlantı yok.")

        st.divider()
        st.markdown("### Yeni bağlantı ekle / güncelle")

        conn_ids = [None] + [int(c.id) for c in conns]
        selected_id = st.selectbox("Güncellenecek bağlantı (boş = yeni)", options=conn_ids, index=0)
        existing = svc.get_connection(selected_id) if selected_id else None

        col1, col2 = st.columns(2)
        with col1:
            name = st.text_input("Bağlantı adı", value=(existing.name if existing else ""))
            vendor = st.selectbox(
                "ERP türü",
                options=["SAP", "LOGO", "NETSIS", "CUSTOM"],
                index=["SAP", "LOGO", "NETSIS", "CUSTOM"].index(existing.vendor) if existing else 3,
            )
            mode = st.selectbox(
                "Bağlantı modu",
                options=["csv_upload", "rest"],
                index=["csv_upload", "rest"].index(existing.mode) if existing else 0,
                help="CSV upload: ERP'den export alıp yükle. REST: API/OData ile çek.",
            )
            is_active = st.checkbox("Aktif", value=(bool(existing.is_active) if existing else True))

        with col2:
            base_url = st.text_input("base_url (REST için)", value=(existing.base_url if existing else ""))
            auth_type = st.selectbox(
                "auth_type",
                options=["none", "api_key", "bearer", "basic"],
                index=["none", "api_key", "bearer", "basic"].index(existing.auth_type) if existing else 0,
            )
            secret_ref = st.text_input(
                "secret_ref (DB'ye secret yazılmaz)",
                value=(existing.secret_ref if existing else ""),
                help="Örn: DEMO1 → ERP_SECRET_DEMO1 ortam değişkeni okunur.",
            )
            description = st.text_area("Açıklama", value=(existing.description if existing else ""), height=90)

        if st.button("Kaydet (Bağlantı)", type="primary"):
            try:
                svc.upsert_connection(
                    connection_id=selected_id,
                    name=name,
                    vendor=vendor,
                    mode=mode,
                    base_url=base_url,
                    auth_type=auth_type,
                    secret_ref=secret_ref,
                    description=description,
                    is_active=is_active,
                )
                s.commit()
                st.success("Kaydedildi ✅")
                _rerun()
            except Exception as e:
                s.rollback()
                st.error(str(e))

    # -----------------------------
    # Mapping
    # -----------------------------
    with tabs[1]:
        st.subheader("Mapping (ERP kolonları → Platform kolonları)")
        st.write(
            "Sade anlatım: ERP dosyanın kolon isimleri farklıysa burada eşleştirme yaparsın. "
            "Örnek: 'qty' → 'fuel_quantity'."
        )

        conns = svc.list_connections()
        if not conns:
            st.warning("Önce bağlantı oluşturun.")
            st.stop()

        conn = st.selectbox("Bağlantı seç", options=conns, format_func=lambda c: f"#{c.id} {c.name} ({c.vendor})")
        dataset_type = st.selectbox(
            "Dataset türü",
            options=["energy", "production", "materials", "cbam_products", "bom_precursors"],
        )

        m = svc.get_mapping(conn.id, dataset_type)
        default_mapping = m.mapping_json if m else "{}"
        default_transform = m.transform_json if m else "{}"
        enabled = bool(m.enabled) if m else True

        col1, col2 = st.columns(2)
        with col1:
            mapping_json = st.text_area(
                "Mapping JSON",
                value=default_mapping,
                height=220,
                help="Format: {\"kaynak_kolon\": \"hedef_kolon\"}",
            )
        with col2:
            transform_json = st.text_area(
                "Transform JSON (opsiyonel)",
                value=default_transform,
                height=220,
                help="Örn: {\"set_defaults\": {\"fuel_unit\": \"m3\"}, \"multiply\": {\"fuel_quantity\": 1000}}",
            )

        enabled = st.checkbox("Bu mapping aktif", value=enabled)

        if st.button("Kaydet (Mapping)", type="primary"):
            try:
                svc.upsert_mapping(
                    connection_id=int(conn.id),
                    dataset_type=dataset_type,
                    mapping_json=mapping_json,
                    transform_json=transform_json,
                    enabled=enabled,
                )
                s.commit()
                st.success("Mapping kaydedildi ✅")
                _rerun()
            except Exception as e:
                s.rollback()
                st.error(str(e))

        st.divider()
        st.caption("İpucu: İlk başta mapping boş bırakılabilir; sistem kolonları olduğu gibi alır.")
        try:
            st.json({"mapping": json.loads(mapping_json or "{}"), "transform": json.loads(transform_json or "{}")})
        except Exception:
            pass

    # -----------------------------
    # Upload sync
    # -----------------------------
    with tabs[2]:
        st.subheader("Dosya ile Sync (CSV/JSON)")
        conns = svc.list_connections()
        if not conns:
            st.warning("Önce bağlantı oluşturun.")
            st.stop()

        conn = st.selectbox("Bağlantı seç", options=conns, format_func=lambda c: f"#{c.id} {c.name} ({c.vendor})", key="upl_conn")
        dataset_type = st.selectbox(
            "Dataset türü",
            options=["energy", "production", "materials", "cbam_products", "bom_precursors"],
            key="upl_dtype",
        )
        file_format = st.selectbox("Dosya formatı", options=["csv", "json"], key="upl_fmt")
        up = st.file_uploader("ERP export dosyası", type=["csv", "json"], key="erp_file")

        if up is not None:
            b = up.getvalue()
            st.write(f"Dosya: **{up.name}** ({len(b)} byte)")
            if st.button("Sync başlat", type="primary"):
                res = svc.sync_from_upload(
                    connection_id=int(conn.id),
                    project_id=int(project_id),
                    dataset_type=dataset_type,
                    file_name=up.name,
                    file_bytes=b,
                    file_format=file_format,
                )
                s.commit()
                if res.status == "success":
                    st.success(f"Sync başarılı ✅ (job_id={res.job_run_id})")
                    st.json(res.datasetuploads)
                else:
                    st.error(f"Sync başarısız ❌ (job_id={res.job_run_id})")
                    st.write(res.error)

    # -----------------------------
    # REST sync
    # -----------------------------
    with tabs[3]:
        st.subheader("REST ile Sync (API/OData)")
        st.write("Bu mod REST/OData endpoint'inden JSON çekip DatasetUpload'a çevirir.")

        conns = [c for c in svc.list_connections() if (c.mode or "").lower() == "rest"]
        if not conns:
            st.warning("REST modunda bağlantı yok. Bağlantı ekleyip mode=rest seçin.")
            st.stop()

        conn = st.selectbox("REST bağlantı seç", options=conns, format_func=lambda c: f"#{c.id} {c.name} ({c.vendor})", key="rest_conn")
        dataset_type = st.selectbox(
            "Dataset türü",
            options=["energy", "production", "materials", "cbam_products", "bom_precursors"],
            key="rest_dtype",
        )
        endpoint = st.text_input("endpoint_path (örn: /api/energy)", value="")
        params_json = st.text_area("params (JSON, opsiyonel)", value="{}", height=120)

        st.caption(
            "Not: Auth gerekiyorsa bağlantıda auth_type + secret_ref ayarlayın. "
            "Örn bearer için ERP_SECRET_<ref> içine token koyun."
        )

        if st.button("REST Sync başlat", type="primary"):
            res = svc.sync_from_rest(
                connection_id=int(conn.id),
                project_id=int(project_id),
                dataset_type=dataset_type,
                endpoint_path=endpoint,
                params_json=params_json,
            )
            s.commit()
            if res.status == "success":
                st.success(f"REST Sync başarılı ✅ (job_id={res.job_run_id})")
                st.json(res.datasetuploads)
            else:
                st.error(f"REST Sync başarısız ❌ (job_id={res.job_run_id})")
                st.write(res.error)

    # -----------------------------
    # Job runs
    # -----------------------------
    with tabs[4]:
        st.subheader("Çalıştırma Kayıtları")
        conns = svc.list_connections()
        conn_ids = ["Tümü"] + [f"#{c.id} {c.name}" for c in conns]
        sel = st.selectbox("Filtre", options=conn_ids)
        connection_id = None
        if sel != "Tümü":
            connection_id = int(sel.split(" ", 1)[0].replace("#", ""))

        runs = svc.list_job_runs(connection_id=connection_id, limit=200)
        if not runs:
            st.info("Kayıt yok.")
        else:
            st.dataframe(
                [
                    {
                        "id": r.id,
                        "connection_id": r.connection_id,
                        "project_id": r.project_id,
                        "status": r.status,
                        "started_at": r.started_at,
                        "finished_at": r.finished_at,
                        "error": (r.error_text[:120] + "...") if r.error_text and len(r.error_text) > 120 else r.error_text,
                    }
                    for r in runs
                ],
                use_container_width=True,
            )
