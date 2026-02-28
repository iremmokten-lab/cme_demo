from __future__ import annotations

import json
from typing import Any, Dict, List

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.models import CalculationSnapshot, Project
from src.db.session import db
from src.engine.advisor import generate_reduction_recommendations
from src.mrv.audit import append_audit, infer_company_id_for_user
from src.services.authz import require_role


def _safe_json_loads(s: str, default):
    try:
        return json.loads(s or "")
    except Exception:
        return default


def ai_optimization_page(user):
    require_role(user, allowed={"consultant", "consultant_admin", "client", "verifier", "verifier_admin"})

    st.title("🤖 AI Carbon Reduction Engine")
    st.caption(
        "Bu ekran, en yüksek emisyon ve maliyet sürücülerini tespit eder ve regülasyon-grade kanıt gereksinimleri "
        "ile birlikte azaltım aksiyon önerileri üretir."
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

    recs = generate_reduction_recommendations(results=results, config=cfg)

    st.subheader("🔥 Hotspots")
    hs = recs.get("hotspots") or []
    if not hs:
        st.info("Hotspot bulunamadı (veri yetersiz olabilir).")
    else:
        st.dataframe(pd.DataFrame(hs), use_container_width=True)

    st.subheader("🛠️ Önerilen Aksiyonlar")
    acts = recs.get("actions") or []
    if not acts:
        st.info("Öneri üretilemedi (veri yetersiz olabilir).")
    else:
        for i, a in enumerate(acts, 1):
            with st.expander(f"{i}. {a.get('title','Aksiyon')} — Beklenen Azaltım: {a.get('expected_emission_reduction_tco2',0):.2f} tCO₂"):
                st.write(a.get("description", ""))
                cols = st.columns(3)
                cols[0].metric("Beklenen Azaltım (tCO₂)", f"{float(a.get('expected_emission_reduction_tco2',0.0)):.2f}")
                cols[1].metric("Beklenen Maliyet Değişimi (EUR)", f"{float(a.get('expected_cost_change_eur',0.0)):.2f}")
                cols[2].metric("CBAM/ETS Etki (EUR)", f"{float(a.get('expected_exposure_change_eur',0.0)):.2f}")

                st.markdown("**Kanıt Gereksinimleri (Evidence Requirements)**")
                st.write(a.get("evidence_requirements") or [])

                st.markdown("**Hesaplama Referansı (Calculation Reference)**")
                st.code(json.dumps(a.get("calculation_reference") or {}, ensure_ascii=False, indent=2), language="json")

    st.divider()
    st.subheader("📦 Evidence Pack'e ekleme notu")
    st.write(
        "Bu öneriler, snapshot sonucu üzerinde deterministik olarak üretilir ve evidence pack manifest içine eklenebilir. "
        "Uygulama, rapor üretimi sırasında öneri özetini Compliance Report içine dahil eder."
    )
