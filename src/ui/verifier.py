from __future__ import annotations

import json

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.models import CalculationSnapshot, Project
from src.db.session import db
from src.mrv.audit import append_audit, infer_company_id_for_user
from src.services import projects as prj
from src.services.alerts import list_open_alerts_for_user
from src.services.exports import build_evidence_pack
from src.ui.verification_workflow import verification_workflow_page


def _read_results(snapshot: CalculationSnapshot) -> dict:
    try:
        return json.loads(snapshot.results_json) if snapshot.results_json else {}
    except Exception:
        return {}


def verifier_portal(user) -> None:
    st.title("🔍 Verifier Portal")
    st.caption("Read-only snapshot inceleme + evidence pack + verification workflow + sampling notları.")

    if not prj.is_verifier(user):
        st.error("Bu sayfa sadece verifier rolü içindir.")
        return

    company_id = prj.require_company_id(user)

    append_audit(
        "verifier_portal_viewed",
        {},
        user_id=getattr(user, "id", None),
        company_id=company_id,
        entity_type="page",
        entity_id=None,
    )

    alerts = list_open_alerts_for_user(user, limit=50)
    if alerts:
        with st.expander(f"🚨 Açık Uyarılar ({len(alerts)})", expanded=False):
            rows = []
            for a in alerts:
                rows.append(
                    {
                        "severity": getattr(a, "severity", ""),
                        "başlık": getattr(a, "title", ""),
                        "mesaj": getattr(a, "message", ""),
                        "snapshot_id": getattr(a, "snapshot_id", None),
                    }
                )
            st.dataframe(pd.DataFrame(rows), use_container_width=True)

    st.divider()
    st.subheader("Paylaşılan Snapshot İnceleme (shared_with_client=True)")

    snaps = prj.list_shared_snapshots_for_user(user, limit=400)
    if not snaps:
        st.info("Henüz paylaşılmış snapshot yok.")
    else:
        labels = []
        ids = []
        for sn in snaps[:200]:
            lock_tag = "🔒" if getattr(sn, "locked", False) else ""
            labels.append(f"{lock_tag} ID:{sn.id} • Proje:{sn.project_id} • {sn.created_at}")
            ids.append(int(sn.id))

        sel = st.selectbox("Snapshot seç", options=labels, index=0)
        sid = ids[labels.index(sel)]

        with db() as s:
            sn = s.get(CalculationSnapshot, int(sid))
            p = s.get(Project, int(sn.project_id)) if sn else None

        if sn:
            append_audit(
                "verifier_snapshot_viewed",
                {"snapshot_id": int(sn.id)},
                user_id=getattr(user, "id", None),
                company_id=company_id,
                entity_type="snapshot",
                entity_id=int(sn.id),
            )

            st.write(f"**Snapshot ID:** {sn.id}")
            st.write(f"**Project:** {sn.project_id} • {getattr(p, 'name', '-') if p else '-'}")
            st.write(f"**Engine:** {sn.engine_version}")
            st.write(f"**Locked:** {bool(getattr(sn, 'locked', False))}")
            st.write(f"**Shared:** {bool(getattr(sn, 'shared_with_client', False))}")

            results = _read_results(sn)
            with st.expander("Sonuç Özeti", expanded=False):
                st.json(
                    {
                        "kpis": results.get("kpis", {}),
                        "compliance_checks": (results.get("compliance_checks", []) or [])[:50],
                        "qa_flags": (results.get("qa_flags", []) or [])[:50],
                    }
                )

            st.divider()
            if st.button("Evidence Pack Oluştur/İndir", type="primary"):
                data = build_evidence_pack(int(sn.id))
                st.download_button(
                    "Evidence Pack ZIP indir",
                    data=data,
                    file_name=f"evidence_pack_snapshot_{sn.id}.zip",
                    mime="application/zip",
                    use_container_width=True,
                )

    st.divider()
    verification_workflow_page(user)
