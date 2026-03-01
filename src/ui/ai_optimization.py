from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.models import CalculationSnapshot, Project, Report
from src.db.session import db
from src.mrv.audit import append_audit, infer_company_id_for_user
from src.services.authz import require_role


def _safe_json_loads(s: str, default):
    try:
        return json.loads(s or "")
    except Exception:
        return default


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _compute_ai_payload(project_id: int, results: dict, cfg: dict) -> dict:
    """UI için deterministik Faz 4 AI payload üretimi (DB'ye yazmaz)."""
    try:
        from src.engine.advisor import build_reduction_advice
        from src.engine.benchmark import build_benchmark_report
        from src.engine.optimizer import build_optimizer_payload
        from src.engine.scenario import simulate_cost_scenario
    except Exception:
        return {}

    input_bundle = (results or {}).get("input_bundle") or {}
    facility = (input_bundle.get("facility") or {}) if isinstance(input_bundle, dict) else {}
    kpis = (results or {}).get("kpis") or {}

    breakdown = (results or {}).get("breakdown") or {}
    energy_breakdown = (breakdown.get("energy") or {}) if isinstance(breakdown, dict) else {}

    cbam = (results or {}).get("cbam") or {}
    cbam_table = (results or {}).get("cbam_table")

    categories = []
    try:
        from src.db.models import EvidenceDocument

        with db() as s:
            cats = (
                s.execute(
                    select(EvidenceDocument.category)
                    .where(EvidenceDocument.project_id == int(project_id))
                    .distinct()
                )
                .scalars()
                .all()
            )
        categories = [str(c or "").strip() for c in cats if c]
    except Exception:
        categories = []

    bench = build_benchmark_report(facility=facility, kpis=kpis, cbam=cbam, cbam_table=cbam_table)
    advice = build_reduction_advice(
        kpis=kpis,
        energy_breakdown=energy_breakdown,
        cbam=cbam,
        evidence_categories_present=categories,
    )

    constraints = (((cfg or {}).get("ai") or {}).get("optimizer_constraints") or {}) if isinstance(cfg, dict) else {}
    if not isinstance(constraints, dict):
        constraints = {}

    total_tco2 = _f((kpis or {}).get("total_tco2"), 0.0)
    opt = build_optimizer_payload(total_tco2=total_tco2, measures=(advice.get("measures") or []), constraints=constraints)

    scenario = {}
    try:
        selected = (((opt or {}).get("portfolio") or {}).get("selected") or [])
        scenario = simulate_cost_scenario(results=(results or {}), config=(cfg or {}), portfolio_selected=selected)
    except Exception:
        scenario = {}

    return {
        "benchmark": bench,
        "advisor": advice,
        "optimizer": opt,
        "scenario": scenario,
        "meta": {"phase": "faz4", "optimizer_constraints": constraints, "evidence_categories_present": categories},
    }


def ai_optimization_page(user):
    require_role(user, allowed={"consultant", "consultant_admin", "client", "verifier", "verifier_admin"})

    st.title("🤖 Faz 4 — AI Optimizasyon & Senaryo Motoru")
    st.caption(
        "Bu ekran, snapshot sonuçları üzerinden deterministik şekilde öneri üretir, portföy seçer ve maliyet senaryosu simüle eder."
    )

    append_audit(
        "page_viewed",
        {"page": "ai_optimization"},
        user_id=getattr(user, "id", None),
        company_id=infer_company_id_for_user(user),
        entity_type="page",
        entity_id=None,
    )

    with db() as s:
        projects = (
            s.execute(select(Project).where(Project.company_id == infer_company_id_for_user(user)).order_by(Project.created_at.desc()))
            .scalars()
            .all()
        )

    if not projects:
        st.info("Henüz proje yok. Önce Consultant Panel'den bir proje oluşturun.")
        return

    p_map = {f"{p.id} — {p.name}": p.id for p in projects}
    p_key = st.selectbox("Proje", list(p_map.keys()))
    project_id = int(p_map[p_key])

    with db() as s:
        snaps = (
            s.execute(select(CalculationSnapshot).where(CalculationSnapshot.project_id == project_id).order_by(CalculationSnapshot.created_at.desc()))
            .scalars()
            .all()
        )

    if not snaps:
        st.warning("Bu proje için henüz snapshot yok. Önce hesaplama çalıştırın.")
        return

    snap_map = {f"{sn.id} — {sn.created_at.strftime('%Y-%m-%d %H:%M')} — {'LOCK' if sn.locked else 'draft'}": sn.id for sn in snaps}
    snap_key = st.selectbox("Snapshot", list(snap_map.keys()))
    snapshot_id = int(snap_map[snap_key])

    with db() as s:
        snap = s.get(CalculationSnapshot, snapshot_id)

    results = _safe_json_loads(snap.results_json, {})
    cfg = _safe_json_loads(snap.config_json, {})

    # AI payload: snapshot içine yazılmışsa onu kullan; yoksa UI'da hesapla
    ai_payload = {}
    try:
        ai_payload = (results or {}).get("ai") or {}
        if not isinstance(ai_payload, dict) or ai_payload.get("meta", {}).get("phase") != "faz4":
            ai_payload = _compute_ai_payload(project_id, results, cfg)
    except Exception:
        ai_payload = _compute_ai_payload(project_id, results, cfg)

    tabs = st.tabs(["🔥 Hotspots & Öneriler", "📈 Portföy & Senaryo", "📦 Rapor & İndirme"])

    with tabs[0]:
        st.subheader("🔥 Hotspots")
        bench = (ai_payload.get("benchmark") or {}) if isinstance(ai_payload, dict) else {}
        hotspots = (bench.get("hotspots") or []) if isinstance(bench, dict) else []
        if hotspots:
            st.dataframe(pd.DataFrame(hotspots), use_container_width=True)
        else:
            st.info("Hotspot bulunamadı (veri yetersiz olabilir).")

        st.subheader("🛠️ Önerilen Aksiyonlar")
        adv = (ai_payload.get("advisor") or {}) if isinstance(ai_payload, dict) else {}
        measures = (adv.get("measures") or []) if isinstance(adv, dict) else []
        if not measures:
            st.info("Öneri üretilemedi (veri yetersiz olabilir).")
        else:
            dfm = pd.DataFrame(measures)
            # çok uzun olmasın
            st.dataframe(dfm.head(30), use_container_width=True)

    with tabs[1]:
        st.subheader("📈 Abatement Cost Curve (MACC)")
        opt = (ai_payload.get("optimizer") or {}) if isinstance(ai_payload, dict) else {}
        curve = (opt.get("abatement_curve") or []) if isinstance(opt, dict) else []
        if curve:
            st.dataframe(pd.DataFrame(curve).head(50), use_container_width=True)
        else:
            st.info("MACC üretilemedi.")

        st.subheader("✅ Seçilen Portföy (Optimizer)")
        port = (opt.get("portfolio") or {}) if isinstance(opt, dict) else {}
        selected = (port.get("selected") or []) if isinstance(port, dict) else []
        summary = (port.get("summary") or {}) if isinstance(port, dict) else {}
        if summary:
            cols = st.columns(4)
            cols[0].metric("Aksiyon", str(summary.get("selected_count")))
            cols[1].metric("Azaltım (tCO₂/yıl)", f"{_f(summary.get('reduction_tco2')):,.3f}")
            cols[2].metric("CAPEX (EUR)", f"{_f(summary.get('capex_eur')):,.2f}")
            cols[3].metric("Yıllık Maliyet (EUR)", f"{_f(summary.get('annualized_cost_eur')):,.2f}")
        if selected:
            st.dataframe(pd.DataFrame(selected), use_container_width=True)
        else:
            st.info("Portföy seçilemedi (kısıtlar çok sıkı olabilir).")

        st.subheader("💶 Maliyet Senaryosu (Tahmini)")
        sc = (ai_payload.get("scenario") or {}) if isinstance(ai_payload, dict) else {}
        if sc:
            base = sc.get("baseline") or {}
            scen = sc.get("scenario") or {}
            delta = sc.get("delta") or {}
            cols = st.columns(3)
            cols[0].metric("Baz Emisyon (tCO₂)", f"{_f(base.get('total_emissions_tco2')):,.3f}")
            cols[1].metric("Senaryo Emisyon (tCO₂)", f"{_f(scen.get('total_emissions_tco2')):,.3f}")
            cols[2].metric("ETS Δ (EUR)", f"{_f(delta.get('ets_cost_eur')):,.2f}")

            st.caption("Not: Bu bir 'tahmini senaryo' çıktısıdır. Resmî beyan değildir.")
        else:
            st.info("Senaryo simülasyonu üretilemedi.")

    with tabs[2]:
        st.subheader("📦 AI Raporu (JSON + PDF)")
        st.write("Bu rapor, evidence pack içine dahil edilebilecek şekilde dosya üretir ve Report tablosuna kaydeder.")

        if st.button("AI Raporunu Üret & Kaydet", type="primary"):
            try:
                from src.services.ai_reports import persist_ai_reports_as_db_reports

                persist_ai_reports_as_db_reports(project_id=project_id, snapshot_id=snapshot_id, created_by_user_id=getattr(user, "id", None))
                st.success("AI raporu üretildi ve kaydedildi ✅")
            except Exception as e:
                st.error(str(e))

        with db() as s:
            reps = (
                s.execute(
                    select(Report)
                    .where(Report.project_id == int(project_id), Report.snapshot_id == int(snapshot_id), Report.report_type.in_(["ai_optimization_json", "ai_optimization_pdf"]))
                    .order_by(Report.created_at.desc())
                )
                .scalars()
                .all()
            )

        if not reps:
            st.info("Henüz kayıtlı AI raporu yok.")
            st.stop()

        for r in reps:
            p = Path(str(r.file_path or ""))
            if not p.exists():
                continue
            if r.report_type.endswith("_json"):
                st.download_button("AI JSON indir", data=p.read_bytes(), file_name="ai_optimization.json", mime="application/json")
            if r.report_type.endswith("_pdf"):
                st.download_button("AI PDF indir", data=p.read_bytes(), file_name="ai_optimization.pdf", mime="application/pdf")
