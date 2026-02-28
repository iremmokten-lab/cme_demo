<Bu dosya zaten PARÇA 1’de belirtilmişti; burada TAMAMINI VERİYORUM.>

from __future__ import annotations

import json

import pandas as pd
import streamlit as st

from src.db.models import CalculationSnapshot, EvidenceDocument, Project
from src.db.session import db
from src.mrv.audit import append_audit
from src.services import projects as prj
from src.services.workflow import run_full


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


def _list_projects_for_company(company_id: int):
    with db() as s:
        return s.query(Project).filter(Project.company_id == int(company_id)).order_by(Project.id.desc()).limit(500).all()


def ai_optimization_page(user) -> None:
    st.title("🤖 AI & Optimization (Faz 3)")
    st.caption(
        "Benchmark + hotspot + reduction advisor + abatement cost curve. "
        "Bu sayfa deterministik heuristic kullanır (demo)."
    )

    if not prj.is_consultant(user):
        st.error("Bu sayfa sadece danışman rolü içindir.")
        return

    company_id = prj.require_company_id(user)

    st.subheader("Proje seç")
    projects = _list_projects_for_company(company_id)
    if not projects:
        st.info("Önce bir proje oluşturun.")
        return

    labels = []
    pmap = {}
    for p in projects:
        fname = getattr(getattr(p, "facility", None), "name", "") or "-"
        labels.append(f"#{p.id} • {p.name} • {fname}")
        pmap[labels[-1]] = int(p.id)

    sel = st.selectbox("Proje", options=labels, index=0)
    project_id = pmap[sel]

    snaps = prj.list_snapshots_for_project(user, project_id, include_unshared=True, limit=200)
    if not snaps:
        st.info("Bu proje için snapshot yok. Önce veri yükleyip snapshot üretin.")
        return

    st.divider()

    # Snapshot selection
    snap_labels = []
    sids = []
    for sn in snaps:
        lock_tag = "🔒" if getattr(sn, "locked", False) else ""
        share_tag = "👁️" if getattr(sn, "shared_with_client", False) else ""
        scen_name = ""
        try:
            r = _read_results(sn)
            scen = (r.get("scenario") or {}) if isinstance(r, dict) else {}
            scen_name = str(scen.get("name") or "")
        except Exception:
            scen_name = ""
        snap_labels.append(f"{lock_tag}{share_tag} ID:{sn.id} • {sn.created_at} {('• ' + scen_name) if scen_name else ''}")
        sids.append(int(sn.id))

    snap_sel = st.selectbox("Snapshot", options=snap_labels, index=0)
    snapshot_id = sids[snap_labels.index(snap_sel)]

    with db() as s:
        snap = s.get(CalculationSnapshot, int(snapshot_id))

    if not snap:
        st.error("Snapshot bulunamadı.")
        return

    results = _read_results(snap)
    ai = (results.get("ai") or {}) if isinstance(results, dict) else {}

    st.subheader("AI çıktıları")

    # Controls
    with st.expander("Optimizer kısıtları (senaryo)", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            target_pct = st.number_input("Hedef azaltım (%)", min_value=0.0, max_value=80.0, value=15.0, step=1.0)
        with c2:
            max_capex = st.number_input("Maks CAPEX (€)", min_value=0.0, value=250000.0, step=10000.0)
        with c3:
            disc = st.number_input("İskonto oranı", min_value=0.0, max_value=0.30, value=0.08, step=0.01, format="%.2f")

        if st.button("AI çıktısını güncelle (yeniden snapshot)", type="primary"):
            # Deterministik: mevcut config'i al, optimizer_constraints ekle, yeni snapshot üret.
            try:
                cfg = json.loads(snap.config_json or "{}")
            except Exception:
                cfg = {}

            cfg = dict(cfg or {})
            cfg.setdefault("ai", {})
            if not isinstance(cfg["ai"], dict):
                cfg["ai"] = {}
            cfg["ai"]["optimizer_constraints"] = {
                "target_reduction_pct": float(target_pct),
                "max_capex_eur": float(max_capex),
                "discount_rate": float(disc),
            }

            new_snap = run_full(
                project_id=int(project_id),
                config=cfg,
                scenario=(results.get("scenario") or {}),
                methodology_id=getattr(snap, "methodology_id", None),
                created_by_user_id=getattr(user, "id", None),
            )
            st.success(f"Yeni snapshot üretildi: {new_snap.id}")
            st.rerun()

    if not ai:
        st.info("Bu snapshot'ta AI çıktısı yok. Yeni snapshot üreterek AI modülünü çalıştırın.")
        return

    # Benchmark
    bench = (ai.get("benchmark") or {}) if isinstance(ai, dict) else {}
    if bench:
        with st.expander("📊 Benchmark & Outlier", expanded=True):
            fac = bench.get("facility") or {}
            st.write(
                f"**Tesis yoğunluğu:** {(_fmt_tr(fac.get('intensity_tco2_per_ton', 0.0), 3) if fac.get('intensity_tco2_per_ton') is not None else '-') } tCO2/ton"
            )
            st.write(
                f"**Benchmark:** {_fmt_tr(fac.get('benchmark_tco2_per_ton', 0.0), 3)} tCO2/ton | "
                f"**Oran:** {(_fmt_tr(fac.get('ratio_to_benchmark', 0.0), 2) if fac.get('ratio_to_benchmark') is not None else '-') }"
            )

            out = bench.get("outliers") or []
            if out:
                st.warning(f"Outlier/Anomali: {len(out)}")
                st.dataframe(pd.DataFrame(out), use_container_width=True)
            else:
                st.success("Outlier bulunamadı ✅")

            prod = bench.get("products") or []
            if prod:
                st.dataframe(pd.DataFrame(prod), use_container_width=True)

    # Advisor
    adv = (ai.get("advisor") or {}) if isinstance(ai, dict) else {}
    if adv:
        with st.expander("🔥 Hotspot & Reduction Advisor", expanded=True):
            hs = adv.get("hotspots") or {}
            c1, c2, c3 = st.columns(3)
            c1.metric("Direct (tCO2)", _fmt_tr(hs.get("direct_total_tco2", 0.0), 3))
            c2.metric("Indirect (tCO2)", _fmt_tr(hs.get("indirect_total_tco2", 0.0), 3))
            c3.metric("Toplam (tCO2)", _fmt_tr(hs.get("total_tco2", 0.0), 3))

            fuels = hs.get("by_fuel_tco2") or []
            if fuels:
                st.write("**Yakıt bazlı (tCO2)**")
                st.dataframe(pd.DataFrame(fuels), use_container_width=True)

            measures = adv.get("measures") or []
            if measures:
                st.write("**Öneriler**")
                st.dataframe(pd.DataFrame(measures), use_container_width=True)

            miss = adv.get("evidence_missing_categories") or []
            if miss:
                st.warning("Eksik evidence kategorileri: " + ", ".join([str(x) for x in miss]))

    # Optimizer
    opt = (ai.get("optimizer") or {}) if isinstance(ai, dict) else {}
    if opt:
        with st.expander("📈 Abatement Cost Curve & Portfolio", expanded=True):
            curve = opt.get("abatement_curve") or []
            if curve:
                dfc = pd.DataFrame(curve)
                st.dataframe(dfc, use_container_width=True)
                try:
                    chart_df = dfc[["cumulative_reduction_tco2", "cost_per_tco2"]].copy()
                    chart_df = chart_df.set_index("cumulative_reduction_tco2")
                    st.line_chart(chart_df)
                except Exception:
                    pass

            port = opt.get("portfolio") or {}
            summ = (port.get("summary") or {}) if isinstance(port, dict) else {}
            sc1, sc2, sc3, sc4 = st.columns(4)
            sc1.metric("Seçili önlem", str(summ.get("selected_count", 0)))
            sc2.metric("CAPEX (€)", _fmt_tr(summ.get("capex_eur", 0.0), 0))
            sc3.metric("Azaltım (tCO2)", _fmt_tr(summ.get("reduction_tco2", 0.0), 2))
            sc4.metric("Ort. €/tCO2", _fmt_tr(summ.get("avg_cost_per_tco2", 0.0), 2) if summ.get("avg_cost_per_tco2") is not None else "-")

            sel = port.get("selected") or []
            if sel:
                st.write("**Seçilen portföy**")
                st.dataframe(pd.DataFrame(sel), use_container_width=True)

    st.divider()

    # Evidence completeness quick look
    with db() as s:
        ev_docs = s.query(EvidenceDocument).filter(EvidenceDocument.project_id == int(project_id)).order_by(EvidenceDocument.uploaded_at.desc()).all()

    cats = {}
    for d in ev_docs:
        c = str(getattr(d, "category", "documents") or "documents")
        cats[c] = cats.get(c, 0) + 1

    with st.expander("📎 Evidence özeti", expanded=False):
        if cats:
            st.dataframe(pd.DataFrame([{"kategori": k, "adet": v} for k, v in sorted(cats.items())]), use_container_width=True)
        else:
            st.info("Bu proje için evidence dokümanı yok.")

    append_audit(
        "ai_optimization_viewed",
        {"snapshot_id": int(snapshot_id), "project_id": int(project_id)},
        user_id=getattr(user, "id", None),
        company_id=company_id,
        entity_type="ai",
        entity_id=int(snapshot_id),
    )
