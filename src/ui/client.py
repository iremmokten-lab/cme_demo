from __future__ import annotations

import json
from collections import defaultdict

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.models import CalculationSnapshot, Facility, Project
from src.db.session import db
from src.mrv.audit import append_audit, infer_company_id_for_snapshot
from src.services import projects as prj
from src.services.alerts import list_open_alerts_for_user
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


def _facility_name_for_project(project_id: int) -> str:
    try:
        with db() as s:
            p = s.get(Project, int(project_id))
            if not p or not getattr(p, "facility_id", None):
                return "(tesis yok)"
            f = s.get(Facility, int(p.facility_id))
            return f.name if f else "-"
    except Exception:
        return "-"


def _trend_dataframe(snaps: list[CalculationSnapshot]) -> pd.DataFrame:
    rows = []
    for sn in snaps:
        k = _snapshot_kpis(sn)
        rows.append(
            {
                "tarih": sn.created_at,
                "tarih_str": sn.created_at.strftime("%Y-%m-%d") if hasattr(sn.created_at, "strftime") else str(sn.created_at),
                "snapshot_id": sn.id,
                "project_id": sn.project_id,
                "tesis": _facility_name_for_project(sn.project_id),
                **k,
            }
        )
    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    return df.sort_values("tarih")


def _product_intensity_table(results: dict) -> pd.DataFrame:
    """CBAM table'dan ürün yoğunluğu/CBAM exposure çıkar.

    Motor çıktısı değişken olabilir; olası anahtarları tolere eder.
    """
    rows = []
    cbam_table = results.get("cbam_table") or results.get("cbam") or []
    if isinstance(cbam_table, dict):
        cbam_table = cbam_table.get("table") or cbam_table.get("rows") or []
    if not isinstance(cbam_table, list):
        cbam_table = []

    for r in cbam_table:
        if not isinstance(r, dict):
            continue
        sku = str(r.get("sku") or r.get("product") or "")
        cn = str(r.get("cn_code") or r.get("cn") or "")
        qty = float(r.get("quantity") or r.get("qty") or 0.0)
        exp = float(r.get("export_to_eu_quantity") or r.get("export_qty") or 0.0)
        emb = float(r.get("embedded_emissions_tco2") or r.get("embedded_tco2") or r.get("embedded") or 0.0)
        intensity = float(r.get("intensity_tco2_per_unit") or r.get("intensity") or 0.0)
        cost = float(r.get("cbam_cost_eur") or r.get("cost_eur") or 0.0)
        rows.append(
            {
                "sku": sku,
                "cn_code": cn,
                "üretim": qty,
                "AB ihracat": exp,
                "embedded (tCO2)": emb,
                "intensity": intensity,
                "CBAM (€)": cost,
            }
        )

    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    # intensity boşsa emb/exp ile türet
    if (df["intensity"].fillna(0.0) == 0.0).all():
        denom = df["AB ihracat"].replace(0, pd.NA)
        df["intensity"] = (df["embedded (tCO2)"] / denom).fillna(0.0)
    return df.sort_values(["CBAM (€)", "embedded (tCO2)", "AB ihracat"], ascending=False)


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

    # Alerts
    alerts = list_open_alerts_for_user(user, limit=50)
    if alerts:
        with st.expander(f"🚨 Açık Uyarılar ({len(alerts)})", expanded=True):
            rows = []
            for a in alerts:
                rows.append(
                    {
                        "severity": a.severity,
                        "başlık": a.title,
                        "mesaj": a.message,
                        "snapshot_id": a.snapshot_id,
                        "created_at": a.created_at,
                    }
                )
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
    else:
        st.success("Açık uyarı yok ✅")

    st.divider()

    # Filters
    df_all = _trend_dataframe(snaps)
    facs = sorted(list(set(df_all["tesis"].tolist())))
    years = sorted(list(set([int(getattr(s.created_at, "year", 0) or 0) for s in snaps if getattr(s, "created_at", None)])))
    years = [y for y in years if y > 0]

    fcol1, fcol2, fcol3 = st.columns([2, 1, 1])
    with fcol1:
        fac_sel = st.selectbox("Tesis filtresi", options=["(tümü)"] + facs, index=0)
    with fcol2:
        year_sel = st.selectbox("Yıl", options=["(tümü)"] + [str(y) for y in years], index=0)
    with fcol3:
        metric = st.selectbox(
            "Trend metriği",
            ["total_tco2", "cbam_cost_eur", "ets_cost_tl", "direct_tco2", "indirect_tco2"],
            index=0,
        )

    df_f = df_all.copy()
    if fac_sel != "(tümü)":
        df_f = df_f[df_f["tesis"] == fac_sel]
    if year_sel != "(tümü)":
        try:
            y = int(year_sel)
            df_f = df_f[df_f["tarih"].dt.year == y]
        except Exception:
            pass

    # KPI cards (latest of filtered)
    latest_snap = None
    if len(df_f) > 0:
        latest_id = int(df_f.sort_values("tarih", ascending=False).iloc[0]["snapshot_id"])
        with db() as s:
            latest_snap = s.get(CalculationSnapshot, latest_id)
    if latest_snap is None:
        latest_snap = snaps[0]

    latest_k = _snapshot_kpis(latest_snap)

    st.subheader("Dashboard")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Toplam Emisyon (tCO2)", _fmt_tr(latest_k["total_tco2"], 3))
    c2.metric("Direct (tCO2)", _fmt_tr(latest_k["direct_tco2"], 3))
    c3.metric("Indirect (tCO2)", _fmt_tr(latest_k["indirect_tco2"], 3))
    c4.metric("CBAM Exposure (€)", _fmt_tr(latest_k["cbam_cost_eur"], 2))
    c5.metric("ETS Exposure (TL)", _fmt_tr(latest_k["ets_cost_tl"], 2))

    st.divider()

    st.subheader("Trend")
    if len(df_f) >= 2:
        chart_df = df_f[["tarih_str", metric]].set_index("tarih_str")
        st.line_chart(chart_df)
    else:
        st.caption("Trend için en az 2 paylaşılmış snapshot gerekir (seçili filtre içinde).")

    st.divider()

    st.subheader("Tesis Bazlı Risk Sıralaması")
    by_fac = defaultdict(list)
    for sn in snaps:
        by_fac[_facility_name_for_project(sn.project_id)].append(sn)

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

    st.subheader("Ürün Yoğunluğu & CBAM")
    pr_df = _product_intensity_table(_read_results(latest_snap))
    if len(pr_df) == 0:
        st.info("Bu snapshot'ta ürün tablosu bulunamadı (cbam_table).")
    else:
        st.dataframe(pr_df, use_container_width=True)

    st.subheader("🤖 AI Özet (Faz 3)")
    ai = {}
    try:
        r_latest = _read_results(latest_snap)
        ai = (r_latest.get("ai") or {}) if isinstance(r_latest, dict) else {}
    except Exception:
        ai = {}

    if not isinstance(ai, dict) or not ai:
        st.info("Bu snapshot'ta AI çıktısı yok. Danışmanınız yeni snapshot üretince burada görünecek.")
    else:
        with st.expander("Benchmark & Outlier", expanded=False):
            bench = ai.get("benchmark") or {}
            if isinstance(bench, dict) and bench:
                fac = bench.get("facility") or {}
                st.write(
                    {
                        "tesis_intensity_tco2_per_ton": fac.get("intensity_tco2_per_ton"),
                        "benchmark_tco2_per_ton": fac.get("benchmark_tco2_per_ton"),
                        "ratio_to_benchmark": fac.get("ratio_to_benchmark"),
                        "sector": fac.get("sector"),
                    }
                )
                outs = bench.get("outliers") or []
                if isinstance(outs, list) and outs:
                    st.warning(f"Outlier/Anomali: {len(outs)}")
                    st.dataframe(pd.DataFrame(outs), use_container_width=True)
                prows = bench.get("products") or []
                if isinstance(prows, list) and prows:
                    dfp = pd.DataFrame(prows)
                    cols = [c for c in ["sku", "quantity_ton", "embedded_tco2", "intensity_tco2_per_ton", "benchmark_tco2_per_ton", "ratio_to_benchmark"] if c in dfp.columns]
                    st.dataframe(dfp[cols], use_container_width=True)

        with st.expander("Hotspot & Reduction Advisor", expanded=False):
            adv = ai.get("advisor") or {}
            if isinstance(adv, dict) and adv:
                hs = adv.get("hotspots") or {}
                c1, c2, c3 = st.columns(3)
                c1.metric("Direct (tCO2)", _fmt_tr(hs.get("direct_total_tco2", 0.0), 3))
                c2.metric("Indirect (tCO2)", _fmt_tr(hs.get("indirect_total_tco2", 0.0), 3))
                c3.metric("Toplam (tCO2)", _fmt_tr(hs.get("total_tco2", 0.0), 3))

                fuels = hs.get("by_fuel_tco2") or []
                if isinstance(fuels, list) and fuels:
                    st.dataframe(pd.DataFrame(fuels[:10]), use_container_width=True)

                measures = adv.get("measures") or []
                if isinstance(measures, list) and measures:
                    dfm = pd.DataFrame(measures)
                    cols = [c for c in ["title", "category", "expected_reduction_pct_of_total", "capex_eur", "opex_delta_eur_per_year"] if c in dfm.columns]
                    st.dataframe(dfm[cols], use_container_width=True)

                miss = adv.get("evidence_missing_categories") or []
                if isinstance(miss, list) and miss:
                    st.warning("Eksik evidence kategorileri: " + ", ".join([str(x) for x in miss]))

        with st.expander("Abatement Cost Curve & Portfolio", expanded=False):
            opt = ai.get("optimizer") or {}
            if isinstance(opt, dict) and opt:
                port = opt.get("portfolio") or {}
                summ = (port.get("summary") or {}) if isinstance(port, dict) else {}
                sc1, sc2, sc3, sc4 = st.columns(4)
                sc1.metric("Seçili önlem", str(summ.get("selected_count", 0)))
                sc2.metric("CAPEX (€)", _fmt_tr(summ.get("capex_eur", 0.0), 0))
                sc3.metric("Azaltım (tCO2)", _fmt_tr(summ.get("reduction_tco2", 0.0), 2))
                sc4.metric("Ort. €/tCO2", _fmt_tr(summ.get("avg_cost_per_tco2", 0.0), 2) if summ.get("avg_cost_per_tco2") is not None else "-")

                curve = opt.get("abatement_curve") or []
                if isinstance(curve, list) and curve:
                    dfc = pd.DataFrame(curve)
                    show_cols = [c for c in ["title", "reduction_tco2", "cost_per_tco2", "cumulative_reduction_tco2"] if c in dfc.columns]
                    st.dataframe(dfc[show_cols], use_container_width=True)

    st.divider()

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
        fac = _facility_name_for_project(sn.project_id)
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
            comp_rows.append({"metrik": label, "A": lk[key], "B": rk[key], "fark (B-A)": rk[key] - lk[key]})
        st.dataframe(pd.DataFrame(comp_rows), use_container_width=True)

        st.divider()
        st.subheader("Raporlar & Evidence")
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
            }
            title = f"Rapor — {title_suffix}"
            uri, sha = build_pdf(sn.id, title, payload)
            from pathlib import Path

            p = Path(str(uri))
            data = p.read_bytes() if p.exists() else b""
            return data, sha

        with rcol1:
            if st.button("A için PDF hazırla", type="primary", key="dl_pdf_a"):
                data, sha = _download_pdf_for_snapshot(left_snap, f"Snapshot {left_snap.id}")
                st.download_button("PDF indir (A)", data=data, file_name=f"snapshot_{left_snap.id}.pdf", mime="application/pdf", use_container_width=True)

        with rcol2:
            if st.button("B için PDF hazırla", type="primary", key="dl_pdf_b"):
                data, sha = _download_pdf_for_snapshot(right_snap, f"Snapshot {right_snap.id}")
                st.download_button("PDF indir (B)", data=data, file_name=f"snapshot_{right_snap.id}.pdf", mime="application/pdf", use_container_width=True)

        with rcol3:
            st.caption("Evidence Pack yalnızca kilitli (🔒) snapshot’lar için önerilir.")
            target = left_snap if getattr(left_snap, "locked", False) else right_snap
            if st.button(f"Evidence Pack hazırla (ID:{target.id})", type="primary" if getattr(target, "locked", False) else "secondary", key="dl_ep"):
                ep = build_evidence_pack(target.id)
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
            st.download_button(
                "A Excel indir",
                data=xlsx_a,
                file_name=f"snapshot_{left_snap.id}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        with x2:
            xlsx_b = build_xlsx_from_results(right_snap.results_json or "{}")
            st.download_button(
                "B Excel indir",
                data=xlsx_b,
                file_name=f"snapshot_{right_snap.id}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
