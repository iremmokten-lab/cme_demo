from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

import pandas as pd
import streamlit as st

from src.db.models import CalculationSnapshot
from src.services.projects import list_company_projects_for_user, list_shared_snapshots_for_user, list_snapshots_for_project
from src.services.authz import current_user


def _is_consultant(user) -> bool:
    return str(getattr(user, "role", "") or "").lower().startswith("consult")


def _safe_json_load(s: str) -> dict:
    try:
        return json.loads(s or "{}")
    except Exception:
        return {}


def _extract_checks(snapshot: CalculationSnapshot) -> Tuple[List[dict], List[dict], dict]:
    res = _safe_json_load(getattr(snapshot, "results_json", "{}") or "{}")

    checks = []
    qa = []
    try:
        checks = res.get("compliance_checks", []) or []
    except Exception:
        checks = []
    try:
        qa = res.get("qa_flags", []) or []
    except Exception:
        qa = []

    deterministic = {}
    try:
        deterministic = res.get("deterministic", {}) or {}
    except Exception:
        deterministic = {}

    if not isinstance(checks, list):
        checks = []
    if not isinstance(qa, list):
        qa = []
    if not isinstance(deterministic, dict):
        deterministic = {}

    # Stabilize: dict list
    checks = [c for c in checks if isinstance(c, dict)]
    qa = [q for q in qa if isinstance(q, dict)]

    return checks, qa, deterministic


def _checks_df(checks: List[dict]) -> pd.DataFrame:
    rows = []
    for c in checks:
        rows.append(
            {
                "Durum": str(c.get("status", "")),
                "Seviye": str(c.get("severity", "")),
                "Mevzuat": str(c.get("reg_reference", "")),
                "Kural": str(c.get("rule_id", "")),
                "Mesaj": str(c.get("message_tr", "")),
                "Aksiyon": str(c.get("remediation_tr", "")),
                "Kanıt Gereksinimleri": ", ".join(list(c.get("evidence_requirements", []) or [])),
            }
        )
    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    order = {"fail": 0, "warn": 1, "pass": 2, "info": 3, "": 9}
    df["_o"] = df["Durum"].map(lambda x: order.get(str(x).lower(), 9))
    df = df.sort_values(by=["_o", "Mevzuat", "Seviye", "Kural"], ascending=[True, True, True, True]).drop(columns=["_o"])
    return df


def _qa_df(qa: List[dict]) -> pd.DataFrame:
    rows = []
    for q in qa:
        rows.append(
            {
                "Seviye": str(q.get("severity", "")),
                "Flag": str(q.get("flag_id", "")),
                "Mesaj": str(q.get("message_tr", "")),
            }
        )
    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    order = {"fail": 0, "warn": 1, "info": 2, "": 9}
    df["_o"] = df["Seviye"].map(lambda x: order.get(str(x).lower(), 9))
    df = df.sort_values(by=["_o", "Flag"], ascending=[True, True]).drop(columns=["_o"])
    return df


def compliance_checklist_page(user) -> None:
    st.title("✅ Uyum Kontrolleri (Checklist)")
    st.caption("Snapshot sonuçlarındaki compliance_checks ve qa_flags otomatik olarak listelenir. Müşteri rolü sadece paylaşılan snapshot’ları görür.")

    consultant = _is_consultant(user)

    # Snapshot seçimi
    if consultant:
        projects = list_company_projects_for_user(user)
        if not projects:
            st.warning("Bu şirkete bağlı proje bulunamadı.")
            return

        proj_label_to_id = {f"{p.name} (#{p.id})": int(p.id) for p in projects}
        proj_label = st.selectbox("Proje seç", options=list(proj_label_to_id.keys()))
        project_id = proj_label_to_id[proj_label]

        snaps = list_snapshots_for_project(user, project_id)
    else:
        st.info("Müşteri görünümü: sadece danışman tarafından paylaşılan snapshot’lar listelenir.")
        snaps = list_shared_snapshots_for_user(user)

    if not snaps:
        st.warning("Snapshot bulunamadı.")
        return

    snap_label_to_obj = {}
    for sn in snaps:
        tag = "👁️" if bool(getattr(sn, "shared_with_client", False)) else ""
        snap_label_to_obj[f"{tag} Snapshot #{sn.id} — {getattr(sn, 'created_at', '')}"] = sn

    snap_label = st.selectbox("Snapshot seç", options=list(snap_label_to_obj.keys()))
    snap = snap_label_to_obj[snap_label]

    checks, qa, det = _extract_checks(snap)

    # Header metrics
    c_fail = len([c for c in checks if str(c.get("status", "")).lower() == "fail"])
    c_warn = len([c for c in checks if str(c.get("status", "")).lower() == "warn"])
    c_pass = len([c for c in checks if str(c.get("status", "")).lower() == "pass"])

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Fail", c_fail)
    m2.metric("Warn", c_warn)
    m3.metric("Pass", c_pass)
    m4.metric("Toplam", len(checks))

    with st.expander("Deterministik Kilit (Hash)", expanded=False):
        st.code(json.dumps(det or {}, ensure_ascii=False, indent=2), language="json")

    st.divider()

    # Filtre
    st.subheader("Kontrol Tablosu")
    status_filter = st.multiselect(
        "Durum filtresi",
        options=["pass", "warn", "fail"],
        default=["warn", "fail"],
        help="Varsayılan: warn + fail (aksiyon gerektirenler)",
    )

    reg_filter = st.multiselect(
        "Mevzuat filtresi",
        options=sorted(list({str(c.get("reg_reference", "")) for c in checks if isinstance(c, dict)})),
        default=[],
    )

    df = _checks_df(checks)
    if len(df) == 0:
        st.info("Bu snapshot’ta compliance_checks bulunamadı. (Engine A paketinde compliance üretildiğinde burada görünür.)")
    else:
        if status_filter:
            df = df[df["Durum"].str.lower().isin([s.lower() for s in status_filter])]
        if reg_filter:
            df = df[df["Mevzuat"].isin(reg_filter)]

        st.dataframe(df, use_container_width=True, hide_index=True)

        # Fail detayları
        fail_rows = [c for c in checks if str(c.get("status", "")).lower() == "fail"]
        if fail_rows:
            st.subheader("🚨 Fail Detayları")
            for c in sorted(fail_rows, key=lambda x: (str(x.get("reg_reference", "")), str(x.get("rule_id", "")))):
                st.error(f"{c.get('reg_reference','')} / {c.get('rule_id','')}: {c.get('message_tr','')}")
                st.write("**Önerilen Aksiyon (Remediation)**")
                st.write(str(c.get("remediation_tr", "")))
                req = list(c.get("evidence_requirements", []) or [])
                if req:
                    st.write("**Kanıt Gereksinimleri (Evidence)**")
                    st.write(", ".join(req))
                st.caption("Bu kontrol çıktısı evidence pack içine JSON olarak dahil edilebilir.")
                st.divider()

    st.divider()

    st.subheader("QA Flags")
    qdf = _qa_df(qa)
    if len(qdf) == 0:
        st.info("QA flag bulunamadı.")
    else:
        st.dataframe(qdf, use_container_width=True, hide_index=True)

    st.caption("Not: Müşteri rolü bu sayfada sadece okuma modundadır; değişiklikler danışman panelinden yapılır.")
