from __future__ import annotations

import json

import streamlit as st

from src.db.session import db
from src.services.authz import current_user
from src.services.projects import get_project_for_user, list_company_projects_for_user
from src.services.regulatory_specs import assess_project_against_spec, list_specs, load_data_dictionary


def _badge(status: str) -> str:
    s = (status or "").upper()
    if s == "OK":
        return "✅ OK"
    if s == "MISSING":
        return "❌ EKSİK"
    return "⚠️ BELİRSİZ"


def _req_badge(req: str) -> str:
    r = (req or "").upper()
    if r == "MUST":
        return "ZORUNLU"
    if r == "CONDITIONAL":
        return "KOŞULLU"
    if r == "DEFINTIVE_ONLY":
        return "2026+"
    return "ÖNERİLEN"


def regulatory_mapping_page() -> None:
    st.title("Regülasyon Şablon Eşlemesi & Veri Sözlüğü")

    user = current_user()
    if not user:
        st.error("Giriş yapılmadı.")
        return

    projects = list_company_projects_for_user(user)
    if not projects:
        st.info("Henüz proje yok. Önce bir proje oluşturun.")
        return

    project_labels = {p.id: f"{p.name} (ID: {p.id})" for p in projects}
    pid = st.selectbox("Proje Seç", options=list(project_labels.keys()), format_func=lambda x: project_labels[x])

    project = get_project_for_user(user, int(pid))
    if not project:
        st.error("Bu projeye erişim yok.")
        return

    specs = list_specs()
    if not specs:
        st.warning("Spec dosyaları bulunamadı: ./spec/*.yaml")
        return

    tab_titles = []
    spec_ids = []
    for s in specs:
        sid = str(s.get("spec_id", "")).strip()
        if sid:
            tab_titles.append(str(s.get("title_tr", sid)))
            spec_ids.append(sid)

    tab_titles.append("Veri Sözlüğü")
    tabs = st.tabs(tab_titles)

    with db() as session:
        for i, sid in enumerate(spec_ids):
            with tabs[i]:
                spec = next((x for x in specs if str(x.get("spec_id", "")).strip() == sid), {})
                st.subheader(str(spec.get("title_tr", sid)))

                checks = assess_project_against_spec(session, int(pid), sid)

                must_missing = [c for c in checks if c.required == "MUST" and c.status == "MISSING"]
                if must_missing:
                    st.error(f"ZORUNLU eksikler var: {len(must_missing)} alan.")
                else:
                    st.success("ZORUNLU alanlarda kritik eksik görünmüyor (Adım-1 yapısal kontrol).")

                col1, col2, col3 = st.columns(3)
                col1.metric("Toplam Alan", len(checks))
                col2.metric("Eksik", sum(1 for c in checks if c.status == "MISSING"))
                col3.metric("Belirsiz", sum(1 for c in checks if c.status == "UNKNOWN"))

                rows = []
                for c in checks:
                    rows.append(
                        {
                            "Durum": _badge(c.status),
                            "Öncelik": _req_badge(c.required),
                            "Alan": c.field_key,
                            "Açıklama": c.label_tr,
                            "Kaynak": c.source,
                            "Beklenen Yol": c.internal_path,
                            "Not": c.reason_tr,
                        }
                    )
                st.dataframe(rows, use_container_width=True, hide_index=True)

                with st.expander("JSON çıktı (kopyala)"):
                    st.code(json.dumps(rows, ensure_ascii=False, indent=2), language="json")

    with tabs[-1]:
        st.subheader("Veri Sözlüğü")
        dd = load_data_dictionary()
        if "markdown" in dd:
            st.markdown(dd["markdown"])
        else:
            st.caption(f"Sürüm: {dd.get('version','')}")
            for ds in dd.get("datasets", []):
                st.markdown(f"### {ds.get('dataset_type')} (`{ds.get('file_name')}`)")
                st.write(ds.get("purpose_tr", ""))
                st.write("Gerekli:", ", ".join(ds.get("required_for", [])))
                st.dataframe(ds.get("columns", []), use_container_width=True, hide_index=True)
                if ds.get("notes_tr"):
                    st.info(ds["notes_tr"])
            st.markdown("### Config Anahtarları")
            st.dataframe(dd.get("config_keys", []), use_container_width=True, hide_index=True)
