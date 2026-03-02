from __future__ import annotations
import json
import streamlit as st

from src.services.authz import current_user, login_view, logout_button
from src.services.projects import list_company_projects_for_user
from src.erp_automation.orchestrator import (
    create_connection, list_connections,
    ensure_default_mapping, upsert_mapping, approve_mapping,
    run_ingestion, list_runs, get_latest_mapping,
)
from src.erp_automation.job_queue import enqueue, list_jobs
from src.erp_automation.worker import register, run_once

st.set_page_config(page_title="ERP Otomasyon Merkezi", layout="wide")
u = current_user()
if not u:
    login_view(); st.stop()

with st.sidebar:
    st.write(f"👤 {u.email}")
    st.caption(f"Rol: {u.role}")
    logout_button()

st.title("🔁 ERP Otomasyon Merkezi")
st.caption("SAP/SCADA/IoT yerine ilk aşamada: API / OData / Dosya-drop ile otomatik veri çekme + mapping + ingestion + job kuyruğu.")

projects = list_company_projects_for_user(u)
if not projects:
    st.warning("Proje yok."); st.stop()
pmap = {f"{p.name} (#{p.id})": int(p.id) for p in projects}
pl = st.selectbox("Proje", list(pmap.keys()))
project_id = pmap[pl]

tab1, tab2, tab3, tab4 = st.tabs(["Bağlantılar", "Mapping", "Ingestion", "Jobs & Loglar"])

with tab1:
    st.subheader("Bağlantılar (Connection Registry)")
    with st.expander("➕ Yeni bağlantı ekle"):
        name = st.text_input("Ad", value="SAP")
        kind = st.selectbox("Tür", ["odata", "rest", "file"], index=0)
        base_url = st.text_input("Base URL (file ise boş olabilir)", value="")
        auth = st.text_area("Auth JSON", value=json.dumps({"token": ""}, ensure_ascii=False, indent=2), height=120)
        cfg_default = {"endpoints": {"energy": "/energy", "production": "/production", "cost": "/cost"}, "health_path": "/health"}
        if kind == "odata":
            cfg_default = {"endpoints": {"energy": "EnergySet", "production": "ProductionSet", "cost": "CostSet"}, "since_filter": "ChangedAt ge datetime'{since}'"}
        if kind == "file":
            cfg_default = {"folder": "./storage/erp_drop"}
        cfg = st.text_area("Config JSON", value=json.dumps(cfg_default, ensure_ascii=False, indent=2), height=160)

        if st.button("Kaydet", type="primary"):
            try:
                c = create_connection(project_id, name=name, kind=kind, base_url=base_url, auth=json.loads(auth), config=json.loads(cfg))
                st.success(f"Eklendi: #{c.id}")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    conns = list_connections(project_id)
    st.dataframe([{"id":c.id, "name":c.name, "kind":c.kind, "base_url":c.base_url, "status":c.status} for c in conns], use_container_width=True)

with tab2:
    st.subheader("Mapping (ERP alan adı → Platform alan adı)")
    dataset_type = st.selectbox("Dataset type", ["energy", "production", "cost"], index=0)
    m = get_latest_mapping(project_id, dataset_type)
    if not m:
        m = ensure_default_mapping(project_id, dataset_type)

    st.info(f"Mapping v{m.version} • durum={m.status}")
    try:
        mapping = json.loads(m.mapping_json or "{}")
    except Exception:
        mapping = {}
    mapping_text = st.text_area("Mapping JSON (external_field -> internal_field)", value=json.dumps(mapping, ensure_ascii=False, indent=2), height=240)
    notes = st.text_input("Not", value=m.notes or "")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Kaydet (draft)"):
            try:
                upsert_mapping(project_id, dataset_type, m.version, json.loads(mapping_text), status="draft", notes=notes)
                st.success("Kaydedildi.")
                st.rerun()
            except Exception as e:
                st.error(str(e))
    with c2:
        if st.button("Onayla (approved)", type="primary"):
            try:
                upsert_mapping(project_id, dataset_type, m.version, json.loads(mapping_text), status="approved", notes=notes)
                st.success("Onaylandı.")
                st.rerun()
            except Exception as e:
                st.error(str(e))
    with c3:
        if st.button("Yeni versiyon aç"):
            try:
                # next version
                new_v = int(m.version) + 1
                upsert_mapping(project_id, dataset_type, new_v, {}, status="draft", notes="")
                st.success(f"Yeni mapping v{new_v} açıldı.")
                st.rerun()
            except Exception as e:
                st.error(str(e))

with tab3:
    st.subheader("Ingestion (Veri çek → normalize et → dataset upload oluştur)")
    conns = list_connections(project_id)
    if not conns:
        st.warning("Önce bağlantı ekle."); st.stop()
    cmap = {f"#{c.id} {c.name} ({c.kind})": int(c.id) for c in conns}
    conn_label = st.selectbox("Connection", list(cmap.keys()))
    conn_id = cmap[conn_label]
    dataset_type = st.selectbox("Dataset", ["energy","production","cost"], index=0, key="ing_dataset")

    since = st.text_input("since (opsiyonel)", value="")
    until = st.text_input("until (opsiyonel)", value="")

    if st.button("Hemen çalıştır (sync)", type="primary"):
        try:
            run_id, upload_id, dlq = run_ingestion(project_id, conn_id, dataset_type, since=(since or None), until=(until or None))
            st.success(f"Run #{run_id} tamamlandı. Upload #{upload_id}. DLQ={dlq}")
        except Exception as e:
            st.error(str(e))

    st.caption("İstersen job kuyruğuna da atabilirsin (uzun işler için).")
    if st.button("Job olarak kuyruğa al"):
        j = enqueue("erp_ingest", {"project_id": project_id, "connection_id": conn_id, "dataset_type": dataset_type, "since": since or None, "until": until or None}, project_id=project_id)
        st.success(f"Enqueued job #{j.id}")

    st.subheader("Son ingestion run'ları")
    runs = list_runs(project_id, 30)
    st.dataframe([{"id":r.id,"dataset":r.dataset_type,"status":r.status,"raw":r.raw_count,"normalized":r.normalized_count,"upload_id":r.output_upload_id,"error":(r.error or "")[:120]} for r in runs], use_container_width=True)

with tab4:
    st.subheader("Job kuyruğu ve worker")
    # Register handler
    def _handler(payload: dict) -> dict:
        project_id = int(payload["project_id"])
        connection_id = int(payload["connection_id"])
        dataset_type = str(payload["dataset_type"])
        run_id, upload_id, dlq = run_ingestion(project_id, connection_id, dataset_type, since=payload.get("since"), until=payload.get("until"))
        return {"run_id": run_id, "upload_id": upload_id, "dlq": dlq}

    register("erp_ingest", _handler)

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Worker: 1 job çalıştır"):
            did = run_once()
            st.info("Çalıştı" if did else "Kuyruk boş")
    with c2:
        st.caption("Streamlit Cloud'da gerçek background yok. Bu butonla manuel çalıştırılır.")

    jobs = list_jobs(200)
    st.dataframe([{"id":j.id,"kind":j.kind,"status":j.status,"error":(j.error or "")[:120]} for j in jobs], use_container_width=True)
