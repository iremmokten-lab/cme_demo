from __future__ import annotations

import json

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.models import CalculationSnapshot, Project
from src.db.session import db
from src.mrv.audit import append_audit, infer_company_id_for_user
from src.services.alerts import list_open_alerts_for_user
from src.services.exports import build_evidence_pack, build_xlsx_from_results
from src.services.projects import list_shared_snapshots_for_user, require_company_id
from src.services.reporting import build_pdf
from src.ui.verification_workflow import verification_workflow_page


def _read_results(snapshot: CalculationSnapshot) -> dict:
    try:
        return json.loads(snapshot.results_json) if snapshot.results_json else {}
    except Exception:
        return {}


def verifier_portal(user) -> None:
    st.title("🔍 Verifier Portal")
    st.caption("Read-only snapshot inceleme + export + verification workflow + sampling notları.")

    role = str(getattr(user, "role", "") or "").lower()
    if not role.startswith("verifier"):
        st.error("Bu sayfa sadece verifier rolü içindir.")
        return

    require_company_id(user)

    append_audit(
        "verifier_portal_viewed",
        {},
        user_id=getattr(user, "id", None),
        company_id=infer_company_id_for_user(user),
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

    st.subheader("Paylaşılan Snapshotlar (shared_with_client=True)")

    snaps = list_shared_snapshots_for_user(user, limit=300)
    if not snaps:
        st.info("Henüz paylaşılmış snapshot yok.")
    else:
        labels = []
        ids = []
        for sn in snaps[:200]:
            lock_tag = "🔒" if getattr(sn, "locked", False) else ""
            labels.append(f"{lock_tag} ID:{sn.id} • Proje:{sn.project_id} • {str(sn.created_at)[:19]}")
            ids.append(int(sn.id))

        sel = st.selectbox("Snapshot seç", options=labels, index=0)
        sid = ids[labels.index(sel)]

        with db() as s:
            sn = s.get(CalculationSnapshot, int(sid))
            p = s.get(Project, int(sn.project_id)) if sn else None

        if sn:
            res = _read_results(sn)
            st.write(f"**Snapshot:** #{sn.id}")
            st.write(f"**Proje:** {sn.project_id} • {getattr(p, 'name', '-') if p else '-'}")
            st.write(f"**Engine:** {sn.engine_version}")
            st.write(f"**Locked:** {bool(getattr(sn, 'locked', False))}")
            st.write(f"**Shared:** {bool(getattr(sn, 'shared_with_client', False))}")

            with st.expander("Sonuç Özeti", expanded=False):
                st.json(
                    {
                        "kpis": res.get("kpis", {}),
                        "compliance_checks": (res.get("compliance_checks", []) or [])[:80],
                        "qa_flags": (res.get("qa_flags", []) or [])[:80],
                    }
                )

            st.divider()
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("Evidence Pack indir", type="primary"):
                    data = build_evidence_pack(int(sn.id))
                    st.download_button(
                        "Evidence Pack ZIP indir",
                        data=data,
                        file_name=f"evidence_pack_snapshot_{sn.id}.zip",
                        mime="application/zip",
                        use_container_width=True,
                    )
            with c2:
                if st.button("XLSX indir"):
                    x = build_xlsx_from_results(int(sn.id))
                    st.download_button(
                        "XLSX indir",
                        data=x,
                        file_name=f"results_snapshot_{sn.id}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )
            with c3:
                if st.button("PDF indir"):
                    pdf = build_pdf(int(sn.id))
                    st.download_button(
                        "PDF indir",
                        data=pdf,
                        file_name=f"report_snapshot_{sn.id}.pdf",
                        mime="application/pdf",
                        use_container_width=True,
                    )

    st.divider()
    st.subheader("Verification Workflow")
    verification_workflow_page(user)
