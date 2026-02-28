from __future__ import annotations

import json

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.models import CalculationSnapshot, EvidenceDocument
from src.db.session import db
from src.engine.optimizer import build_optimizer_payload
from src.services.projects import require_company_id


def _read_results(snapshot: CalculationSnapshot) -> dict:
    try:
        return json.loads(snapshot.results_json or "{}")
    except Exception:
        return {}


def _fmt_tr(x, digits=2) -> str:
    try:
        s = f"{float(x):,.{digits}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "0"


def _evidence_categories_for_project(project_id: int) -> list[str]:
    with db() as s:
        docs = s.execute(select(EvidenceDocument.category).where(EvidenceDocument.project_id == int(project_id))).scalars().all()
    cats = sorted({str(d or "").strip().lower() for d in docs if d})
    return cats


def ai_optimization_page(user) -> None:
    st.title("🤖 AI & Optimizasyon (Faz 3)")
    st.caption("Benchmark • Outlier • Reduction Advisor • Abatement Cost Curve • Portfolio")

    role = str(getattr(user, "role", "") or "").lower()
    if not role.startswith("consult"):
        st.error("Bu sayfa sadece danışman rolü içindir.")
        return

    require_company_id(user)

    with db() as s:
        snaps = s.execute(select(CalculationSnapshot).order_by(CalculationSnapshot.created_at.desc()).limit(250)).scalars().all()

    if not snaps:
        st.info("Henüz snapshot yok.")
        return

    labels = [f"#{sn.id} • Proje:{sn.project_id} • {str(sn.created_at)[:19]}" for sn in snaps]
    sel = st.selectbox("Snapshot seç", labels, index=0)
    sid = int(sel.split("•")[0].replace("#", "").strip())

    with db() as s:
        sn = s.get(CalculationSnapshot, sid)

    if not sn:
        st.error("Snapshot bulunamadı.")
        return

    res = _read_results(sn)
    ai = (res.get("ai") or {}) if isinstance(res, dict) else {}

    kpis = (res.get("kpis") or {}) if isinstance(res, dict) else {}
    total_tco2 = float(kpis.get("total_tco2", 0.0) or 0.0)

    c1, c2, c3 = st.columns(3)
    c1.metric("Toplam Emisyon (tCO2)", _fmt_tr(total_tco2, 3))
    c2.metric("CBAM Exposure (€)", _fmt_tr(float(kpis.get("cbam_cost_eur", 0.0) or 0.0), 2))
    c3.metric("ETS Exposure (TL)", _fmt_tr(float(kpis.get("ets_cost_tl", 0.0) or 0.0), 2))

    st.divider()

    tab1, tab2, tab3 = st.tabs(["Benchmark & Outlier", "Reduction Advisor", "Optimizer"])

    with tab1:
        st.subheader("Benchmark")
        bench = ai.get("benchmark") if isinstance(ai, dict) else None
        if not bench:
            st.info("Bu snapshot için benchmark üretilmemiş. Yeni snapshot üretirken Faz 3 engine çalışır.")
        else:
            fac = (bench.get("facility") or {}) if isinstance(bench, dict) else {}
            st.write("**Tesis Yoğunluğu (tCO2/ton)**")
            st.write(
                {
                    "intensity": fac.get("intensity_tco2_per_ton"),
                    "benchmark": fac.get("benchmark_tco2_per_ton"),
                    "ratio": fac.get("ratio_to_benchmark"),
                    "sector": fac.get("sector"),
                }
            )

            st.write("**Ürün Bazlı Yoğunluklar**")
            prows = bench.get("products") or []
            if isinstance(prows, list) and prows:
                df = pd.DataFrame(prows)
                cols = [c for c in ["sku", "cbam_good_key", "quantity_ton", "embedded_tco2", "intensity_tco2_per_ton", "benchmark_tco2_per_ton", "ratio_to_benchmark"] if c in df.columns]
                st.dataframe(df[cols], use_container_width=True)
            else:
                st.caption("Ürün satırı yok.")

            st.write("**Outlier Flags**")
            outs = bench.get("outliers") or []
            if isinstance(outs, list) and outs:
                st.dataframe(pd.DataFrame(outs), use_container_width=True)
            else:
                st.caption("Outlier yok.")

    with tab2:
        st.subheader("Reduction Advisor")
        adv = ai.get("advisor") if isinstance(ai, dict) else None
        if not adv:
            st.info("Bu snapshot için advisor üretilmemiş.")
        else:
            hs = adv.get("hotspots") or {}
            st.write("**Hotspots**")
            st.write(
                {
                    "direct_tco2": hs.get("direct_total_tco2"),
                    "indirect_tco2": hs.get("indirect_total_tco2"),
                    "top_fuels": (hs.get("by_fuel_tco2") or [])[:5],
                }
            )

            missing = adv.get("evidence_missing_categories") or []
            if missing:
                st.warning(f"Eksik evidence kategorileri: {', '.join([str(x) for x in missing])}")

            measures = adv.get("measures") or []
            if isinstance(measures, list) and measures:
                df = pd.DataFrame(measures)
                cols = [c for c in ["id", "title", "category", "expected_reduction_pct_of_total", "capex_eur", "opex_delta_eur_per_year"] if c in df.columns]
                st.dataframe(df[cols], use_container_width=True)

                with st.expander("Öneri detayları", expanded=False):
                    st.json(measures)
            else:
                st.caption("Öneri yok.")

    with tab3:
        st.subheader("Optimizer")

        c1, c2, c3 = st.columns(3)
        with c1:
            target_pct = st.number_input("Hedef azaltım (%)", min_value=0.0, max_value=80.0, value=15.0, step=1.0)
        with c2:
            max_capex = st.number_input("Max CAPEX (€)", min_value=0.0, value=200000.0, step=10000.0)
        with c3:
            discount = st.number_input("İskonto oranı", min_value=0.0, max_value=0.30, value=0.08, step=0.01)

        adv = ai.get("advisor") if isinstance(ai, dict) else None
        measures = (adv.get("measures") if isinstance(adv, dict) else None) or []

        if not measures:
            st.info("Önce advisor çıktısı gerekir.")
        else:
            payload = build_optimizer_payload(
                total_tco2=float(total_tco2),
                measures=list(measures),
                constraints={
                    "target_reduction_pct": float(target_pct),
                    "max_capex_eur": float(max_capex),
                    "discount_rate": float(discount),
                },
            )

            curve = payload.get("abatement_curve") or []
            if curve:
                dfc = pd.DataFrame(curve)
                st.write("**Abatement Cost Curve (MACC)**")
                show_cols = [c for c in ["id", "title", "reduction_tco2", "capex_eur", "annualized_cost_eur", "cost_per_tco2", "cumulative_reduction_tco2"] if c in dfc.columns]
                st.dataframe(dfc[show_cols], use_container_width=True)

            port = payload.get("portfolio") or {}
            st.write("**Önerilen Portfolio**")
            st.write(port.get("summary") or {})
            sel = port.get("selected") or []
            if sel:
                st.dataframe(pd.DataFrame(sel), use_container_width=True)
