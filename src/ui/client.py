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
    # tarih stringleştirme (grafikler için)
    df["tarih_str"] = df["tarih"].apply(lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x))
    return df


def client_app(user):
    st.title("Müşteri Paneli")

    company_id = prj.require_company_id(user)

    # Only shared snapshots
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

    # Company KPIs (latest snapshot totals)
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

    # Trend
    st.subheader("Trend")
    df = _trend_dataframe(snaps)
    if len(df) >= 2:
        metric = st.selectbox("Gösterge", ["total_tco2", "cbam_cost_eur", "ets_cost_tl", "direct_tco2", "indirect_tco2"], index=0)
        chart_df = df[["tarih_str", metric]].set_index("tarih_str")
        st.line_chart(chart_df)
    else:
        st.caption("Trend için en az 2 paylaşılmış snapshot gerekir.")

    st.divider()

    # Multi-facility view
    st.subheader("Tesis Bazlı Görünüm ve Risk Sıralaması")
    by_fac = defaultdict(list)
    for sn in snaps:
        by_fac[_facility_name_for_snapshot(sn)].append(sn)

    fac_rows = []
    for fac, items in by_fac.items():
        # latest per facility
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

    # Snapshot list & compare
    st.subheader("Snapshot Karşılaştırma (Baseline vs Senaryo)")
    labels = []
    id_map = []
    for sn in snaps[:200]:
        r = _read_results(sn)
        scen = (r.get("scenario") or {}) if isinstance(r, dict) else {}
        kind = "Senaryo" if scen else "Baseline"
        name = scen.get("name") if scen else ""
        lock_tag = "🔒" if getattr(sn, "locked", False) else ""
        chain_tag = "⛓️" if getattr(sn, "previous_snapshot_hash", None) else ""
        fac = _facility_name_for_snapshot(sn)
        labels.append(f"{lock_tag}{chain_tag} ID:{sn.id} • {fac} • {kind}{(' — ' + name) if name else ''} • {sn.created_at}")
        id_map.append(sn.id)

    a, b = st.columns(2)
    with a:
        left_sel = st.selectbox("1. Snapshot", labels, index=0, key="cmp_left")
    with b:
        right_sel = st.selectbox("2. Snapshot", labels, index=min(1, len(labels) - 1), key="cmp_right")

    left_id = id_map[labels.index(left_sel)]
    right_id = id_map[labels.index(right_sel)]

    with db() as s:
        left_snap = s.get(CalculationSnapshot, int(left_id))
        right_snap = s.get(CalculationSnapshot, int(right_id))

    if left_snap and right_snap:
        append_audit(
            "snapshot_compare_viewed",
            {"left": left_snap.id, "right": right_snap.id},
            user_id=getattr(user, "id", None),
            company_id=company_id,
            entity_type="snapshot_compare",
            entity_id=None,
        )

        lk = _snapshot_kpis(left_snap)
        rk = _snapshot_kpis(right_snap)

        comp_rows = []
        for key, label in [
            ("total_tco2", "Toplam Emisyon (tCO2)"),
            ("direct_tco2", "Direct (tCO2)"),
            ("indirect_tco2", "Indirect (tCO2)"),
            ("precursor_tco2", "Precursor (tCO2)"),
            ("cbam_cost_eur", "CBAM (€)"),
            ("ets_cost_tl", "ETS (TL)"),
        ]:
            comp_rows.append(
                {
                    "metrik": label,
                    "A": lk[key],
                    "B": rk[key],
                    "fark (B-A)": rk[key] - lk[key],
                }
            )
        st.dataframe(pd.DataFrame(comp_rows), use_container_width=True)

        st.divider()

        st.subheader("Raporlar")
        rcol1, rcol2, rcol3 = st.columns(3)

        def _download_pdf_for_snapshot(sn: CalculationSnapshot, title_suffix: str):
            results = _read_results(sn)
            try:
                cfg = json.loads(sn.config_json or "{}")
            except Exception:
                cfg = {}
            payload = {
                "kpis": (results.get("kpis") or {}) if isinstance(results, dict) else {},
                "config": cfg,
                "cbam_table": results.get("cbam_table", []),
                "scenario": results.get("scenario", {}),
                "methodology": results.get("methodology", None),
                "data_sources": [
                    "energy.csv (yüklenen dosya)",
                    "production.csv (yüklenen dosya)",
                    "materials.csv (opsiyonel, precursor)",
                    "EmissionFactor Library (DB)",
                    "Monitoring Plan (DB, facility bazlı)",
                ],
                "formulas": [
                    "Direct: fuel_quantity × NCV × EF × OF",
                    "Indirect: electricity_kwh × grid_factor (location/market)",
                    "Precursor: materials.material_quantity × materials.emission_factor",
                ],
            }
            title = f"Rapor — {title_suffix}"
            uri, sha = build_pdf(sn.id, title, payload)
            # build_pdf returns uri; read from disk
            from pathlib import Path

            p = Path(str(uri))
            data = p.read_bytes() if p.exists() else b""
            return data, sha

        with rcol1:
            if st.button("A için PDF indir", type="primary", key="dl_pdf_a"):
                data, sha = _download_pdf_for_snapshot(left_snap, f"Snapshot {left_snap.id}")
                append_audit(
                    "report_exported",
                    {"snapshot_id": left_snap.id, "sha256": sha},
                    user_id=getattr(user, "id", None),
                    company_id=infer_company_id_for_snapshot(left_snap.id) or company_id,
                    entity_type="report",
                    entity_id=left_snap.id,
                )
                st.download_button("PDF indir (A)", data=data, file_name=f"snapshot_{left_snap.id}.pdf", mime="application/pdf", use_container_width=True)

        with rcol2:
            if st.button("B için PDF indir", type="primary", key="dl_pdf_b"):
                data, sha = _download_pdf_for_snapshot(right_snap, f"Snapshot {right_snap.id}")
                append_audit(
                    "report_exported",
                    {"snapshot_id": right_snap.id, "sha256": sha},
                    user_id=getattr(user, "id", None),
                    company_id=infer_company_id_for_snapshot(right_snap.id) or company_id,
                    entity_type="report",
                    entity_id=right_snap.id,
                )
                st.download_button("PDF indir (B)", data=data, file_name=f"snapshot_{right_snap.id}.pdf", mime="application/pdf", use_container_width=True)

        with rcol3:
            st.caption("Evidence Pack yalnızca kilitli (🔒) snapshot’lar için önerilir.")
            target = left_snap if getattr(left_snap, "locked", False) else right_snap
            if st.button(f"Evidence Pack indir (ID:{target.id})", type="primary" if getattr(target, "locked", False) else "secondary", key="dl_ep"):
                ep = build_evidence_pack(target.id)
                append_audit(
                    "evidence_exported",
                    {"snapshot_id": target.id},
                    user_id=getattr(user, "id", None),
                    company_id=infer_company_id_for_snapshot(target.id) or company_id,
                    entity_type="evidence_pack",
                    entity_id=target.id,
                )
                st.download_button(
                    "Evidence Pack ZIP indir",
                    data=ep,
                    file_name=f"evidence_pack_snapshot_{target.id}.zip",
                    mime="application/zip",
                    use_container_width=True,
                    type="primary" if getattr(target, "locked", False) else "secondary",
                )

        st.divider()
        st.subheader("Excel")
        x1, x2 = st.columns(2)
        with x1:
            xlsx_a = build_xlsx_from_results(left_snap.results_json or "{}")
            if st.download_button("XLSX indir (A)", data=xlsx_a, file_name=f"snapshot_{left_snap.id}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True):
                append_audit(
                    "xlsx_exported",
                    {"snapshot_id": left_snap.id},
                    user_id=getattr(user, "id", None),
                    company_id=infer_company_id_for_snapshot(left_snap.id) or company_id,
                    entity_type="xlsx",
                    entity_id=left_snap.id,
                )
        with x2:
            xlsx_b = build_xlsx_from_results(right_snap.results_json or "{}")
            if st.download_button("XLSX indir (B)", data=xlsx_b, file_name=f"snapshot_{right_snap.id}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True):
                append_audit(
                    "xlsx_exported",
                    {"snapshot_id": right_snap.id},
                    user_id=getattr(user, "id", None),
                    company_id=infer_company_id_for_snapshot(right_snap.id) or company_id,
                    entity_type="xlsx",
                    entity_id=right_snap.id,
                )
