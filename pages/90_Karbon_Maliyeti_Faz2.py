from __future__ import annotations

import json
from typing import Any, Dict

import pandas as pd
import streamlit as st
from sqlalchemy import select

from src.db.models import CalculationSnapshot, Project, Report
from src.db.session import init_db, db
from src.services.authz import get_or_create_demo_user
from src.services.carbon_cost_engine import compare_carbon_cost
from src.services.carbon_cost_reports import save_carbon_cost_reports


st.set_page_config(page_title="Karbon Maliyeti (Faz 2)", layout="wide")

init_db()
user = get_or_create_demo_user()

st.title("Karbon Maliyeti Motoru (Faz 2)")
st.caption("ETS + CBAM maliyetlerini snapshot sonuçlarından üretir. Güncelleme yapmaz; rapor üretir.")

with st.sidebar:
    st.write(f"👤 {user.email}")
    st.caption(f"Rol: {user.role}")
    st.divider()
    st.caption("Bu sayfa: 1) Snapshot seç 2) Karbon maliyeti raporunu üret/görüntüle 3) Senaryo karşılaştır.")


def _read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _fmt_tr(x: Any, digits: int = 2) -> str:
    try:
        s = f"{float(x):,.{digits}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(x)


with db() as s:
    projects = s.execute(select(Project).order_by(Project.created_at.desc())).scalars().all()

if not projects:
    st.info("Henüz proje yok.")
    st.stop()

proj_map = {p.id: p for p in projects}
proj_id = st.selectbox("Proje", options=list(proj_map.keys()), format_func=lambda x: f"{proj_map[x].name} (id={x})")

with db() as s:
    snaps = (
        s.execute(select(CalculationSnapshot).where(CalculationSnapshot.project_id == int(proj_id)).order_by(CalculationSnapshot.created_at.desc()))
        .scalars()
        .all()
    )

if not snaps:
    st.warning("Bu projede snapshot yok. Önce Consultant Panel'den snapshot üretin.")
    st.stop()

snap_map = {sn.id: sn for sn in snaps}
snap_id = st.selectbox("Snapshot", options=list(snap_map.keys()), format_func=lambda x: f"Snapshot {x} | {snap_map[x].created_at}")
sn = snap_map[int(snap_id)]

st.divider()
st.subheader("1) Seçili snapshot için karbon maliyeti raporu")

col1, col2 = st.columns([1, 1])
with col1:
    if st.button("Karbon maliyeti raporunu üret / güncelle", type="primary"):
        try:
            save_carbon_cost_reports(project_id=int(proj_id), snapshot_id=int(snap_id), created_by_user_id=getattr(user, "id", None))
            st.success("Rapor üretildi ✅")
            st.rerun()
        except Exception as e:
            st.error(str(e))

with col2:
    st.caption("Not: Raporlar storage/reports/<snapshot_id>/ altında tutulur ve evidence pack'e eklenebilir.")

# mevcut raporu bul
with db() as s:
    rep_json = (
        s.execute(select(Report).where(Report.snapshot_id == int(snap_id)).where(Report.report_type == "carbon_cost").order_by(Report.created_at.desc()))
        .scalars()
        .first()
    )
    rep_pdf = (
        s.execute(select(Report).where(Report.snapshot_id == int(snap_id)).where(Report.report_type == "carbon_cost_pdf").order_by(Report.created_at.desc()))
        .scalars()
        .first()
    )

payload = {}
if rep_json and rep_json.file_path:
    payload = _read_json(rep_json.file_path)

if payload:
    ets = payload.get("ets") or {}
    cbam = payload.get("cbam") or {}
    totals = payload.get("totals") or {}

    st.markdown("### Özet")
    k1, k2, k3 = st.columns(3)
    k1.metric("Toplam (€)", _fmt_tr((totals or {}).get("total_cost_eur", 0.0)))
    k2.metric("Toplam (TL)", _fmt_tr((totals or {}).get("total_cost_tl", 0.0)))
    k3.metric("CBAM Sertifika", _fmt_tr(((cbam or {}).get("liability") or {}).get("certificates_required", (cbam or {}).get("certificates_required", 0.0)), 2))

    st.markdown("### Detay (Tablo)")
    rows = [
        {"Kalem": "ETS maliyeti (€)", "Değer": (ets or {}).get("cost_eur", 0.0)},
        {"Kalem": "CBAM maliyeti (€)", "Değer": (cbam or {}).get("estimated_payable_amount_eur", 0.0)},
        {"Kalem": "Toplam (€)", "Değer": (totals or {}).get("total_cost_eur", 0.0)},
        {"Kalem": "ETS maliyeti (TL)", "Değer": (ets or {}).get("cost_tl", 0.0)},
        {"Kalem": "CBAM maliyeti (TL)", "Değer": (cbam or {}).get("estimated_payable_amount_tl", 0.0)},
        {"Kalem": "Toplam (TL)", "Değer": (totals or {}).get("total_cost_tl", 0.0)},
    ]
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True)

    if rep_pdf and rep_pdf.file_path:
        try:
            with open(rep_pdf.file_path, "rb") as f:
                st.download_button("PDF indir", data=f.read(), file_name=f"carbon_cost_snapshot_{snap_id}.pdf", mime="application/pdf")
        except Exception:
            pass
    if rep_json and rep_json.file_path:
        try:
            with open(rep_json.file_path, "rb") as f:
                st.download_button("JSON indir", data=f.read(), file_name=f"carbon_cost_snapshot_{snap_id}.json", mime="application/json")
        except Exception:
            pass
else:
    st.info("Henüz bu snapshot için karbon maliyeti raporu üretilmemiş.")

st.divider()
st.subheader("2) Senaryo karşılaştır (Snapshot B - Snapshot A)")

snap_ids = list(snap_map.keys())
if len(snap_ids) < 2:
    st.info("Karşılaştırma için en az 2 snapshot gerekir.")
else:
    a_id = st.selectbox("Snapshot A", options=snap_ids, index=min(1, len(snap_ids)-1), key="a")
    b_id = st.selectbox("Snapshot B", options=snap_ids, index=0, key="b")

    def load_cost(snapshot_id: int) -> Dict[str, Any]:
        with db() as s:
            r = (
                s.execute(select(Report).where(Report.snapshot_id == int(snapshot_id)).where(Report.report_type == "carbon_cost").order_by(Report.created_at.desc()))
                .scalars()
                .first()
            )
        if r and r.file_path:
            return _read_json(r.file_path)
        return {}

    if st.button("Karşılaştır", type="secondary"):
        A = load_cost(int(a_id))
        B = load_cost(int(b_id))
        if not A or not B:
            st.warning("İki snapshot için de carbon_cost raporu olmalı. Önce raporları üretin.")
        else:
            diff = compare_carbon_cost(A, B)
            st.json(diff)
