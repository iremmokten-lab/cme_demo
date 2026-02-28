from __future__ import annotations

from typing import Any, Dict, List

import streamlit as st

from src.services.projects import list_company_projects_for_user
from src.services.verification import (
    add_finding,
    close_finding,
    create_case,
    list_cases_for_user,
    read_case_for_user,
    update_case_status,
    update_case_sampling,
)


def _is_consultant(user) -> bool:
    return str(getattr(user, "role", "") or "").lower().startswith("consult")


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def verification_workflow_page(user) -> None:
    st.header("Verification Workflow")

    projects = list_company_projects_for_user(user)
    if not projects:
        st.info("Önce en az bir proje oluşturmalısınız.")
        return

    p_map = {f"{p.id} • {p.name}": int(p.id) for p in projects}
    p_label = st.selectbox("Proje", list(p_map.keys()))
    project_id = p_map[p_label]

    st.subheader("Cases")
    cases = list_cases_for_user(user, project_id=project_id, limit=200)

    if cases:
        case_labels = [f"#{c.id} • {c.status} • {c.period_year} • {c.verifier_org or ''}" for c in cases]
        idx = st.selectbox("Case seç", list(range(len(case_labels))), format_func=lambda i: case_labels[i])
        case_id = int(cases[idx].id)
        case_payload = read_case_for_user(user, case_id=case_id)
        case = case_payload["case"]
        findings = case_payload["findings"]

        cols = st.columns(4)
        cols[0].metric("Case ID", str(case.get("id")))
        cols[1].metric("Durum", str(case.get("status")))
        cols[2].metric("Yıl", str(case.get("period_year")))
        cols[3].metric("Verifier Org", str(case.get("verifier_org") or "-"))

        if _is_consultant(user) or str(getattr(user, "role", "") or "").lower().startswith("verifier"):
            with st.form("case_status"):
                new_status = st.selectbox("Case durumu", ["open", "in_review", "closed"], index=["open", "in_review", "closed"].index(case.get("status") or "open"))
                save = st.form_submit_button("Durumu Güncelle", type="primary")
            if save:
                try:
                    update_case_status(user, case_id=int(case_id), status=new_status)
                    st.success("Case durumu güncellendi.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Case durumu güncellenemedi: {e}")

        st.divider()

        st.subheader("Sampling / Örnekleme Notları")
        with st.form(f"sampling_{case_id}"):
            sampling_notes = st.text_area("Sampling Notları", value=str(case.get("sampling_notes", "") or ""), height=120)
            sampling_size = st.number_input("Örneklem Büyüklüğü (opsiyonel)", min_value=0, step=1, value=int(case.get("sampling_size") or 0))
            save_sampling = st.form_submit_button("Sampling'i Kaydet", type="primary")
        if save_sampling:
            try:
                update_case_sampling(user, int(case_id), sampling_notes=sampling_notes, sampling_size=int(sampling_size) if sampling_size else None)
                st.success("Sampling güncellendi.")
                st.rerun()
            except Exception as e:
                st.error(f"Sampling güncellenemedi: {e}")

        st.subheader("Bulgular")

        if findings:
            for f in findings:
                with st.expander(f"#{f['id']} • {f['severity']} • {f['status']}", expanded=False):
                    st.write("**Açıklama**")
                    st.write(f.get("description") or "-")
                    st.write("**Düzeltici Aksiyon**")
                    st.write(f.get("corrective_action") or "-")
                    st.write("**Due date**")
                    st.write(f.get("due_date") or "-")

                    if f.get("status") != "closed":
                        if st.button("Finding kapat", key=f"close_{f['id']}"):
                            try:
                                close_finding(user, finding_id=int(f["id"]))
                                st.success("Finding kapatıldı.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Finding kapatılamadı: {e}")
        else:
            st.caption("Henüz finding yok.")

        st.divider()
        st.subheader("Yeni Finding Ekle")
        with st.form("add_finding"):
            severity = st.selectbox("Severity", ["minor", "major", "critical"], index=0)
            description = st.text_area("Açıklama", height=120)
            corrective_action = st.text_area("Düzeltici aksiyon", height=100)
            due_date = st.text_input("Due date (opsiyonel)")
            ok = st.form_submit_button("Finding ekle", type="primary")
        if ok:
            try:
                add_finding(
                    user,
                    case_id=int(case_id),
                    severity=severity,
                    description=description,
                    corrective_action=corrective_action,
                    due_date=due_date,
                )
                st.success("Finding eklendi.")
                st.rerun()
            except Exception as e:
                st.error(f"Finding eklenemedi: {e}")

    else:
        st.info("Bu proje için case yok.")

    st.divider()
    st.subheader("Yeni Verification Case Oluştur")
    with st.form("create_case"):
        period_year = st.number_input("Period year", min_value=2020, max_value=2100, value=2025, step=1)
        facility_id = st.number_input("Facility ID (opsiyonel)", min_value=0, value=0, step=1)
        verifier_org = st.text_input("Verifier org (opsiyonel)", value="")
        create = st.form_submit_button("Case oluştur", type="primary")

    if create:
        try:
            create_case(
                user,
                project_id=int(project_id),
                facility_id=(int(facility_id) if int(facility_id) != 0 else None),
                period_year=int(period_year),
                verifier_org=verifier_org,
            )
            st.success("Verification case oluşturuldu.")
            st.rerun()
        except Exception as e:
            st.error(f"Case oluşturulamadı: {e}")
