from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.models import CalculationSnapshot, Project
from src.db.session import db
from src.mrv.audit import append_audit, infer_company_id_for_user, infer_company_id_for_snapshot
from src.services import projects as prj
from src.services.exports import build_evidence_pack, build_xlsx_from_results
from src.services.reporting import build_pdf
from src.services.alerts import list_open_alerts_for_user


def _read_results(snapshot: CalculationSnapshot) -> dict:
    try:
        return json.loads(snapshot.results_json) if snapshot.results_json else {}
    except Exception:
        return {}


def _fmt_tr(x, digits=2) -> str:
    try:
        s = f"{float(x):,.{digits}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "0"


def _snapshot_kpis(snap: CalculationSnapshot) -> dict:
    r = _read_results(snap)
    k = (r.get("kpis") or {}) if isinstance(r, dict) else {}
    cbam = (r.get("cbam") or {}) if isinstance(r, dict) else {}
    totals = (cbam.get("totals") or {}) if isinstance(cbam, dict) else {}
    return {
        "direct_tco2": float(k.get("direct_tco2", 0.0) or 0.0),
        "indirect_tco2": float(k.get("indirect_tco2", 0.0) or 0.0),
        "total_tco2": float(k.get("total_tco2", 0.0) or 0.0),
        "cbam_cost_eur": float(k.get("cbam_cost_eur", 0.0) or 0.0),
        "ets_cost_tl": float(k.get("ets_cost_tl", 0.0) or 0.0),
        "precursor_tco2": float(totals.get("precursor_tco2", 0.0) or 0.0),
    }


def _get_project(project_id: int) -> Project | None:
    with db() as s:
        return s.get(Project, int(project_id))


def _facility_name_for_snapshot(snapshot: CalculationSnapshot) -> str:
    try:
        p = _get_project(snapshot.project_id)
        if not p:
            return "-"
        if not getattr(p, "facility_id", None):
            return "(tesis yok)"
        # facility adı projects service üzerinden daha iyi; burada basit fetch
        from src.db.models import Facility

        with db() as s:
            f = s.get(Facility, int(p.facility_id))
        if not f:
            return "-"
        return f.name
    except Exception:
        return "-"


def _trend_dataframe(shared_snaps: list[CalculationSnapshot]) -> pd.DataFrame:
    rows = []
    for sn in shared_snaps:
        k = _snapshot_kpis(sn)
        rows.append(
            {
                "tarih": sn.created_at,
                "snapshot_id": sn.id,
                "direct_tco2": k["direct_tco2"],
                "indirect_tco2": k["indirect_tco2"],
                "total_tco2": k["total_tco2"],
                "cbam_cost_eur": k["cbam_cost_eur"],
                "ets_cost_tl": k["ets_cost_tl"],
            }
        )
    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    df = df.sort_values("tarih")
    df["tarih_str"] = df["tarih"].apply(lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x))
    return df


def client_app(user):
    st.title("Müşteri Paneli")

    company_id = prj.require_company_id(user)

    snaps = prj.list_shared_snapshots_for_user(user, limit=400)

    append_audit(
        "client_dashboard_viewed",
        {"snapshots_visible": len(snaps)},
        user_id=getattr(user, "id", None),
        company_id=company_id,
        entity_type="dashboard",
        entity_id=None,
    )

    if not snaps:
        st.info("Henüz paylaşılmış (👁️) snapshot yok. Danışmanınız paylaştığında burada görünecek.")
        return

    # Alerts (Faz 2)
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

    latest = snaps[0]
    latest_k = _snapshot_kpis(latest)

    st.subheader("Şirket Dashboard")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Toplam Emisyon (tCO2)", _fmt_tr(latest_k["total_tco2"], 3))
    c2.metric("Direct (tCO2)", _fmt_tr(latest_k["direct_tco2"], 3))
    c3.metric("Indirect (tCO2)", _fmt_tr(latest_k["indirect_tco2"], 3))
    c4.metric("CBAM Exposure (€)", _fmt_tr(latest_k["cbam_cost_eur"], 2))
    c5.metric("ETS Exposure (TL)", _fmt_tr(latest_k["ets_cost_tl"], 2))

    st.divider()

    st.subheader("Trend")
    df = _trend_dataframe(snaps)
    if len(df) >= 2:
        metric = st.selectbox("Gösterge", ["total_tco2", "cbam_cost_eur", "ets_cost_tl", "direct_tco2", "indirect_tco2"], index=0)
        chart_df = df[["tarih_str", metric]].set_index("tarih_str")
        st.line_chart(chart_df)
    else:
        st.caption("Trend için en az 2 paylaşılmış snapshot gerekir.")

    st.divider()

    st.subheader("Tesis Bazlı Görünüm")
    by_fac = defaultdict(list)
    for sn in snaps:
        by_fac[_facility_name_for_snapshot(sn)].append(sn)

    fac_rows = []
    for fac, items in by_fac.items():
        items_sorted = sorted(items, key=lambda x: x.created_at, reverse=True)
        k = _snapshot_kpis(items_sorted[0])
        fac_rows.append(
            {
                "tesis": fac,
                "son_snapshot_id": items_sorted[0].id,
                "total_tco2": k["total_tco2"],
                "cbam_cost_eur": k["cbam_cost_eur"],
                "ets_cost_tl": k["ets_cost_tl"],
            }
        )
    fac_df = pd.DataFrame(fac_rows).sort_values(["cbam_cost_eur", "ets_cost_tl", "total_tco2"], ascending=False)
    st.dataframe(fac_df, use_container_width=True)

    st.divider()

    st.subheader("Snapshot Karşılaştırma")
    labels = []
    id_map = []
    for sn in snaps[:200]:
        r = _read_results(sn)
        tag = (r.get("scenario") or {}).get("name") if isinstance(r, dict) else ""
        tag = f" • {tag}" if tag else ""
        labels.append(f"#{sn.id}{tag} • {str(sn.created_at)[:19]}")
        id_map.append(int(sn.id))

    colA, colB = st.columns(2)
    with colA:
        left = st.selectbox("Baseline", options=labels, index=0)
    with colB:
        right = st.selectbox("Scenario", options=labels, index=min(1, len(labels) - 1) if len(labels) > 1 else 0)

    sid_left = id_map[labels.index(left)]
    sid_right = id_map[labels.index(right)]

    with db() as s:
        s_left = s.get(CalculationSnapshot, int(sid_left))
        s_right = s.get(CalculationSnapshot, int(sid_right))

    if s_left and s_right:
        kL = _snapshot_kpis(s_left)
        kR = _snapshot_kpis(s_right)

        comp = pd.DataFrame(
            [
                {"metrik": "total_tco2", "baseline": kL["total_tco2"], "scenario": kR["total_tco2"], "delta": kR["total_tco2"] - kL["total_tco2"]},
                {"metrik": "cbam_cost_eur", "baseline": kL["cbam_cost_eur"], "scenario": kR["cbam_cost_eur"], "delta": kR["cbam_cost_eur"] - kL["cbam_cost_eur"]},
                {"metrik": "ets_cost_tl", "baseline": kL["ets_cost_tl"], "scenario": kR["ets_cost_tl"], "delta": kR["ets_cost_tl"] - kL["ets_cost_tl"]},
            ]
        )
        st.dataframe(comp, use_container_width=True)

        st.divider()
        st.subheader("Raporlama / Export")
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("Evidence Pack indir", type="primary"):
                data = build_evidence_pack(int(s_left.id))
                st.download_button(
                    "Evidence Pack ZIP indir",
                    data=data,
                    file_name=f"evidence_pack_snapshot_{s_left.id}.zip",
                    mime="application/zip",
                    use_container_width=True,
                )
        with c2:
            if st.button("XLSX indir"):
                x = build_xlsx_from_results(int(s_left.id))
                st.download_button(
                    "XLSX indir",
                    data=x,
                    file_name=f"results_snapshot_{s_left.id}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
        with c3:
            if st.button("PDF indir"):
                pdf = build_pdf(int(s_left.id))
                st.download_button(
                    "PDF indir",
                    data=pdf,
                    file_name=f"report_snapshot_{s_left.id}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )
