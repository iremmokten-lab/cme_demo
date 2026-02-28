from __future__ import annotations

import streamlit as st

from src.services.projects import list_company_projects_for_user
from src.services.verification import (
    add_finding,
    close_finding,
    create_case,
    list_cases_for_user,
    read_case_for_user,
    update_case_sampling,
    update_case_status,
    case_to_json,
)


def _role(user) -> str:
    return str(getattr(user, "role", "") or "").lower()


def _can_write(user) -> bool:
    r = _role(user)
    return r.startswith("consult") or r.startswith("verifier")


def verification_workflow_page(user) -> None:
    st.title("🧾 Verification Workflow")
    st.caption("Case + findings + sampling notları. Danışman/verifier yönetir; müşteri read-only.")

    can_write = _can_write(user)

    projects = list_company_projects_for_user(user)
    if not projects:
        st.warning("Bu şirkete bağlı proje bulunamadı.")
        return

    proj_label_to_id = {f"{p.name} (#{p.id})": int(p.id) for p in projects}
    proj_label = st.selectbox("Proje seç", options=list(proj_label_to_id.keys()))
    project_id = proj_label_to_id[proj_label]

    cases = list_cases_for_user(user, project_id=project_id)

    st.divider()

    if can_write:
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
        st.info("Read-only mod: müşteri rolü (case oluşturma/düzenleme kapalı).")

    if not cases:
        st.warning("Bu proje için verification case yok.")
        return

    case_labels = [f"#{c.id} • {c.status} • {c.period_year} • {c.verifier_org or ''}" for c in cases]
    sel = st.selectbox("Case seç", list(range(len(case_labels))), format_func=lambda i: case_labels[i])
    case_id = int(cases[sel].id)

    c_obj = read_case_for_user(user, case_id)
    if not c_obj:
        st.error("Case okunamadı.")
        return

    c = case_to_json(c_obj)

    cols = st.columns(4)
    cols[0].metric("Case", f"#{c['id']}")
    cols[1].metric("Durum", c.get("status", ""))
    cols[2].metric("Yıl", str(c.get("period_year", "")))
    cols[3].metric("Verifier Org", c.get("verifier_org", "") or "-")

    st.subheader("Sampling / Örnekleme")
    if can_write:
        with st.form("sampling_form"):
            notes = st.text_area("Sampling notları", value=str(c.get("sampling_notes", "") or ""), height=120)
            size = st.number_input("Örneklem büyüklüğü (opsiyonel)", min_value=0, step=1, value=int(c.get("sampling_size") or 0))
            ok = st.form_submit_button("Kaydet", type="primary")
        if ok:
            try:
                update_case_sampling(user, case_id=case_id, sampling_notes=notes, sampling_size=(int(size) if int(size) > 0 else None))
                st.success("Sampling güncellendi.")
                st.rerun()
            except Exception as e:
                st.error(f"Sampling güncellenemedi: {e}")
    else:
        st.write(c.get("sampling_notes") or "—")
        st.caption(f"Örneklem büyüklüğü: {c.get('sampling_size') or '—'}")

    st.subheader("Case Durumu")
    if can_write:
        with st.form("status_form"):
            stt = st.selectbox("Durum", ["planning", "fieldwork", "findings", "closed"], index=["planning", "fieldwork", "findings", "closed"].index(c.get("status") or "planning"))
            ok = st.form_submit_button("Durumu Güncelle", type="primary")
        if ok:
            try:
                update_case_status(user, case_id=case_id, status=stt)
                st.success("Durum güncellendi.")
                st.rerun()
            except Exception as e:
                st.error(f"Durum güncellenemedi: {e}")

    st.divider()
    st.subheader("Findings (Bulgular)")

    findings = c.get("findings", []) or []
    if not findings:
        st.caption("Henüz finding yok.")
    else:
        for f in findings:
            with st.expander(f"#{f['id']} • {f['severity']} • {f['status']}", expanded=False):
                st.write("**Açıklama**")
                st.write(f.get("description") or "—")
                st.write("**Düzeltici Aksiyon**")
                st.write(f.get("corrective_action") or "—")
                st.write("**Due date**")
                st.write(f.get("due_date") or "—")

                if can_write and f.get("status") != "closed":
                    if st.button("Finding kapat", key=f"close_{f['id']}"):
                        try:
                            close_finding(user, finding_id=int(f["id"]))
                            st.success("Finding kapatıldı.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Finding kapatılamadı: {e}")

    st.divider()
    st.subheader("Yeni Finding Ekle")
    if can_write:
        with st.form("add_finding_form"):
            sev = st.selectbox("Severity", ["minor", "major", "critical"], index=0)
            desc = st.text_area("Açıklama", height=120)
            ca = st.text_area("Düzeltici aksiyon", height=100)
            due = st.text_input("Due date (opsiyonel, YYYY-MM-DD)")
            ok = st.form_submit_button("Finding ekle", type="primary")
        if ok:
            try:
                add_finding(
                    user,
                    case_id=case_id,
                    severity=sev,
                    description=desc,
                    corrective_action=ca,
                    due_date=due,
                )
                st.success("Finding eklendi.")
                st.rerun()
            except Exception as e:
                st.error(f"Finding eklenemedi: {e}")
    else:
        st.info("Read-only mod: finding ekleme kapalı.")
