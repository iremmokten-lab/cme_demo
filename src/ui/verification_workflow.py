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
)


def _is_consultant(user) -> bool:
    return str(getattr(user, "role", "") or "").lower().startswith("consult")


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def verification_workflow_page(user) -> None:
    st.title("🧾 Verification Workflow (MVP)")
    st.caption("Verification case + bulgular (findings). Danışman oluşturur/yönetir; müşteri read-only görüntüler.")

    consultant = _is_consultant(user)

    projects = list_company_projects_for_user(user)
    if not projects:
        st.warning("Bu şirkete bağlı proje bulunamadı.")
        return

    proj_label_to_id = {f"{p.name} (#{p.id})": int(p.id) for p in projects}
    proj_label = st.selectbox("Proje seç", options=list(proj_label_to_id.keys()))
    project_id = proj_label_to_id[proj_label]

    cases = list_cases_for_user(user, project_id=project_id)

    st.divider()

    if consultant:
        with st.expander("➕ Yeni Verification Case Oluştur", expanded=False):
            c1, c2, c3 = st.columns(3)
            with c1:
                period_year = st.number_input("Dönem (Yıl)", min_value=2000, max_value=2100, value=2025, step=1)
            with c2:
                facility_id = st.number_input("Facility ID (opsiyonel)", min_value=0, value=0, step=1)
            with c3:
                verifier_org = st.text_input("Verifier Organizasyon", value="")

            if st.button("Case oluştur", type="primary"):
                try:
                    created = create_case(
                        user,
                        project_id=project_id,
                        facility_id=(int(facility_id) if int(facility_id) > 0 else None),
                        period_year=int(period_year),
                        verifier_org=str(verifier_org),
                    )
                    st.success(f"Case oluşturuldu. (#{created.id})")
                    st.rerun()
                except Exception as e:
                    st.error(f"Case oluşturulamadı: {e}")
    else:
        st.info("Müşteri rolü: case oluşturma/düzenleme kapalı (read-only).")

    if not cases:
        st.warning("Bu proje için verification case bulunamadı.")
        return

    # Case seçimi
    label_to_id = {}
    for c in cases:
        label_to_id[f"Case #{c.id} — {c.period_year} — {c.status} — {c.verifier_org}".strip()] = int(c.id)

    case_label = st.selectbox("Case seç", options=list(label_to_id.keys()))
    case_id = label_to_id[case_label]

    case = read_case_for_user(user, case_id)
    if not case:
        st.error("Case bulunamadı veya erişim yok.")
        return

    st.subheader(f"Case #{case.id}")
    st.write(f"**Proje:** #{case.project_id}")
    st.write(f"**Tesis:** {case.facility_id if case.facility_id else '-'}")
    st.write(f"**Dönem:** {case.period_year}")
    st.write(f"**Verifier Org:** {case.verifier_org or '-'}")
    st.write(f"**Durum:** {case.status}")

    if consultant:
        st.divider()
        st.subheader("Case Durumu")
        new_status = st.selectbox(
            "Durum güncelle",
            options=["planning", "fieldwork", "findings", "closed"],
            index=["planning", "fieldwork", "findings", "closed"].index(case.status) if case.status in ["planning", "fieldwork", "findings", "closed"] else 0,
        )
        if st.button("Durumu kaydet"):
            try:
                update_case_status(user, case_id=case.id, status=new_status)
                st.success("Durum güncellendi.")
                st.rerun()
            except Exception as e:
                st.error(f"Durum güncellenemedi: {e}")

    st.divider()

    # Findings
    findings = list(getattr(case, "findings", []) or [])
    open_findings = [f for f in findings if str(getattr(f, "status", "")) != "closed"]
    closed_findings = [f for f in findings if str(getattr(f, "status", "")) == "closed"]

    c1, c2, c3 = st.columns(3)
    c1.metric("Açık", len(open_findings))
    c2.metric("Kapalı", len(closed_findings))
    c3.metric("Toplam", len(findings))

    if consultant:
        with st.expander("➕ Bulgu Ekle", expanded=False):
            sev = st.selectbox("Severity", options=["minor", "major", "critical"], index=0)
            desc = st.text_area("Açıklama", value="", height=120)
            ca = st.text_area("Düzeltici Aksiyon", value="", height=120)
            due = st.text_input("Due Date (YYYY-MM-DD)", value="")
            stt = st.selectbox("Status", options=["open", "in_progress", "closed"], index=0)

            if st.button("Bulgu ekle", type="primary"):
                try:
                    add_finding(
                        user,
                        case_id=case.id,
                        severity=sev,
                        description=desc,
                        corrective_action=ca,
                        due_date=due,
                        status=stt,
                    )
                    st.success("Bulgu eklendi.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Bulgu eklenemedi: {e}")

    st.subheader("Bulgular")

    if not findings:
        st.info("Henüz bulgu yok.")
        return

    # Display findings
    for f in sorted(findings, key=lambda x: (str(getattr(x, "status", "")) != "closed", str(getattr(x, "severity", "")), int(getattr(x, "id", 0) or 0)), reverse=True):
        is_closed = str(getattr(f, "status", "")) == "closed"
        head = f"Bulgu #{f.id} — {f.severity.upper()} — {f.status}"
        with st.expander(head, expanded=False):
            st.write(f"**Açıklama:** {f.description}")
            st.write(f"**Düzeltici Aksiyon:** {f.corrective_action}")
            st.write(f"**Due Date:** {f.due_date or '-'}")
            if consultant and not is_closed:
                if st.button(f"Bulgu #{f.id} kapat", key=f"close_{f.id}"):
                    try:
                        close_finding(user, finding_id=f.id)
                        st.success("Bulgu kapatıldı.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Bulgu kapatılamadı: {e}")
            elif not consultant:
                st.caption("Müşteri rolü: read-only")

    st.caption("Audit log event’leri: case_created, finding_added, finding_closed, case_closed / case_status_changed")
