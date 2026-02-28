from __future__ import annotations

import json

import pandas as pd
import streamlit as st

from src.db.models import CalculationSnapshot
from src.db.session import db
from src.mrv.audit import append_audit
from src.services import projects as prj
from src.services.alerts import list_open_alerts_for_user
from src.services.exports import build_evidence_pack
from src.ui.verification_workflow import verification_workflow_page


def verifier_portal(user) -> None:
    st.title("🔍 Verifier Portal (Read-only + Workflow)")
    st.caption("Verifier rolü: paylaşılan snapshot inceleme + evidence pack + case/finding + sampling notları.")

    if not prj.is_verifier(user):
        st.error("Bu sayfa sadece verifier rolü içindir.")
        return

    # Alerts
    alerts = list_open_alerts_for_user(user, limit=50)
    if alerts:
        with st.expander(f"🚨 Açık Uyarılar ({len(alerts)})", expanded=False):
            rows = []
            for a in alerts:
                rows.append({"severity": a.severity, "başlık": a.title, "mesaj": a.message, "snapshot": a.snapshot_id})
            st.dataframe(pd.DataFrame(rows), use_container_width=True)

    st.divider()
    st.subheader("Paylaşılan Snapshot İnceleme")

    snaps = prj.list_shared_snapshots_for_user(user, limit=200)
    if not snaps:
        st.info("Henüz paylaşılan snapshot yok.")
    else:
        labels = []
        ids = []
        for sn in snaps:
            lock_tag = "🔒" if getattr(sn, "locked", False) else ""
            labels.append(f"{lock_tag} ID:{sn.id} • Proje:{sn.project_id} • {sn.created_at}")
            ids.append(int(sn.id))
        sel = st.selectbox("Snapshot seç", options=labels, index=0)
        sid = ids[labels.index(sel)]
        with db() as s:
            sn = s.get(CalculationSnapshot, int(sid))

        if sn:
            append_audit(
                "verifier_snapshot_viewed",
                {"snapshot_id": sn.id},
                user_id=getattr(user, "id", None),
                company_id=prj.require_company_id(user),
                entity_type="snapshot",
                entity_id=sn.id,
            )

            try:
                results = json.loads(sn.results_json or "{}")
            except Exception:
                results = {}

            st.write(f"**Snapshot ID:** {sn.id}")
            st.write(f"**Project ID:** {sn.project_id}")
            st.write(f"**Engine:** {sn.engine_version}")
            st.write(f"**Locked:** {bool(getattr(sn, 'locked', False))}")
            st.write(f"**Shared:** {bool(getattr(sn, 'shared_with_client', False))}")

            with st.expander("Sonuç Özeti (JSON)", expanded=False):
                st.json({
                    "kpis": results.get("kpis", {}),
                    "compliance_checks": results.get("compliance_checks", [])[:50],
                    "qa_flags": results.get("qa_flags", [])[:50],
                })

            # Faz 3 AI çıktıları (read-only)
            ai = (results.get("ai") or {}) if isinstance(results, dict) else {}
            if isinstance(ai, dict) and ai:
                with st.expander("🤖 AI çıktıları (Benchmark / Advisor / Optimizer)", expanded=False):
                    bench = ai.get("benchmark") or {}
                    if isinstance(bench, dict) and bench:
                        st.write("**Benchmark**")
                        st.json({
                            "facility": bench.get("facility"),
                            "outliers": (bench.get("outliers") or [])[:50],
                        })

                    adv = ai.get("advisor") or {}
                    if isinstance(adv, dict) and adv:
                        st.write("**Advisor**")
                        st.json({
                            "hotspots": adv.get("hotspots"),
                            "evidence_missing_categories": adv.get("evidence_missing_categories"),
                            "measures": (adv.get("measures") or [])[:20],
                        })

                    opt = ai.get("optimizer") or {}
                    if isinstance(opt, dict) and opt:
                        st.write("**Optimizer**")
                        port = (opt.get("portfolio") or {}) if isinstance(opt, dict) else {}
                        st.json({
                            "constraints": opt.get("constraints"),
                            "portfolio_summary": (port.get("summary") or {}) if isinstance(port, dict) else {},
                            "selected": (port.get("selected") or [])[:20] if isinstance(port, dict) else [],
                        })

            if getattr(sn, "locked", False):
                if st.button("Evidence Pack indir", type="primary"):
                    ep = build_evidence_pack(sn.id)
                    st.download_button(
                        "Evidence Pack ZIP indir",
                        data=ep,
                        file_name=f"evidence_pack_snapshot_{sn.id}.zip",
                        mime="application/zip",
                        use_container_width=True,
                    )
            else:
                st.info("Evidence Pack için snapshot'ın kilitli (🔒) olması önerilir.")

    st.divider()
    verification_workflow_page(user)
