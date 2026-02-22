import json
from datetime import datetime, timezone
from io import StringIO

import numpy as np
import pandas as pd
import streamlit as st
from sqlalchemy import inspect

from src.db.session import engine, SessionLocal
from src.db.models import Base, DatasetUpload, CalculationSnapshot
from src.services.projects import (
    list_companies,
    get_or_create_company,
    list_projects,
    create_project,
)
from src.services.persistence import save_upload, save_snapshot

st.set_page_config(page_title="CME Platform MVP — Sprint 1", layout="wide")

APP_VERSION = "sprint1-step1"

DISCLAIMER_TEXT = (
    "Önemli Not: Bu rapor yönetim amaçlı tahmini bir allocation/hesaplama çıktısıdır.\n"
    "Resmî beyan değildir."
)


def init_db():
    # DB tablolarını oluştur
    _ = inspect(engine)  # engine hazır mı diye
    Base.metadata.create_all(bind=engine)


def safe_float(x):
    try:
        if pd.isna(x):
            return np.nan
        return float(x)
    except Exception:
        return np.nan


def kg_to_t(x):
    return float(x) / 1000.0


def require_cols(df, cols, name):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name} eksik kolonlar: {missing}")


def compute_energy(energy_df: pd.DataFrame):
    require_cols(
        energy_df,
        ["energy_carrier", "scope", "activity_amount", "emission_factor_kgco2_per_unit"],
        "energy.csv",
    )
    df = energy_df.copy()
    df["scope"] = df["scope"].apply(safe_float)
    df["activity_amount"] = df["activity_amount"].apply(safe_float)
    df["emission_factor_kgco2_per_unit"] = df["emission_factor_kgco2_per_unit"].apply(safe_float)

    df["emissions_kgco2"] = df["activity_amount"] * df["emission_factor_kgco2_per_unit"]

    total = float(df["emissions_kgco2"].sum()) if len(df) else 0.0
    s1 = float(df.loc[df["scope"] == 1, "emissions_kgco2"].sum()) if len(df) else 0.0
    s2 = float(df.loc[df["scope"] == 2, "emissions_kgco2"].sum()) if len(df) else 0.0

    return df, {
        "total_tco2": kg_to_t(total),
        "scope1_tco2": kg_to_t(s1),
        "scope2_tco2": kg_to_t(s2),
        "total_kgco2": total,
    }


def allocate_energy(prod_df: pd.DataFrame, total_energy_kgco2: float):
    require_cols(prod_df, ["sku", "quantity"], "production.csv")
    df = prod_df.copy()
    df["quantity"] = df["quantity"].apply(safe_float)
    total_qty = float(df["quantity"].sum()) if len(df) else 0.0

    if total_qty <= 0:
        df["alloc_energy_kgco2"] = 0.0
        df["alloc_energy_kgco2_per_unit"] = 0.0
        return df

    df["alloc_energy_kgco2"] = (df["quantity"] / total_qty) * float(total_energy_kgco2)
    df["alloc_energy_kgco2_per_unit"] = np.where(df["quantity"] > 0, df["alloc_energy_kgco2"] / df["quantity"], 0.0)
    return df


def compute_cbam(prod_df: pd.DataFrame, eua_price: float, total_energy_kgco2: float):
    require_cols(
        prod_df,
        ["sku", "quantity", "export_to_eu_quantity", "input_emission_factor_kg_per_unit"],
        "production.csv",
    )
    df = prod_df.copy()
    if "cbam_covered" not in df.columns:
        df["cbam_covered"] = 1

    df["quantity"] = df["quantity"].apply(safe_float)
    df["export_to_eu_quantity"] = df["export_to_eu_quantity"].apply(safe_float)
    df["input_emission_factor_kg_per_unit"] = df["input_emission_factor_kg_per_unit"].apply(safe_float)
    df["cbam_covered"] = df["cbam_covered"].fillna(1).astype(int)

    df = allocate_energy(df, float(total_energy_kgco2))

    df["export_for_cbam"] = np.where(df["cbam_covered"] == 1, df["export_to_eu_quantity"], 0.0)
    df["total_factor"] = df["alloc_energy_kgco2_per_unit"] + df["input_emission_factor_kg_per_unit"]
    df["embedded_kg"] = df["export_for_cbam"] * df["total_factor"]
    df["embedded_t"] = df["embedded_kg"].apply(kg_to_t)
    df["cbam_cost_eur"] = df["embedded_t"] * float(eua_price)

    totals = {
        "embedded_tco2": float(df["embedded_t"].sum()) if len(df) else 0.0,
        "cbam_cost_eur": float(df["cbam_cost_eur"].sum()) if len(df) else 0.0,
    }
    return df, totals


def compute_ets(scope1_tco2: float, free_alloc: float, banked: float, price: float, fx: float):
    net = max(0.0, float(scope1_tco2) - float(free_alloc) - float(banked))
    cost = net * float(price) * float(fx)
    return {"net": net, "cost": cost}


# ---------- APP START ----------
init_db()

st.title("CME Platform MVP")
st.caption(DISCLAIMER_TEXT)

with st.sidebar:
    db = SessionLocal()

    companies = list_companies(db)
    company_names = ["Yeni company"] + [c.name for c in companies]
    company_choice = st.selectbox("Company", company_names)

    if company_choice == "Yeni company":
        name = st.text_input("Company adı")
        if st.button("Create"):
            get_or_create_company(db, name)
            st.rerun()
        st.stop()

    company = [c for c in companies if c.name == company_choice][0]

    projects = list_projects(db, company.id)
    project_names = ["Yeni project"] + [p.name for p in projects]
    project_choice = st.selectbox("Project", project_names)

    if project_choice == "Yeni project":
        name = st.text_input("Project adı")
        if st.button("Create project"):
            create_project(db, company.id, name)
            st.rerun()
        st.stop()

    project = [p for p in projects if p.name == project_choice][0]

    st.divider()
    eua_price = st.slider("EUA (€/t)", 0.0, 200.0, 80.0)
    fx = st.number_input("FX (TL/EUR)", value=35.0)
    free_alloc = st.number_input("Free allocation (tCO2)", value=0.0)
    banked = st.number_input("Banked allowances (tCO2)", value=0.0)

    st.divider()
    energy_file = st.file_uploader("energy.csv", type=["csv"])
    prod_file = st.file_uploader("production.csv", type=["csv"])

tabs = st.tabs(["Run", "History"])

with tabs[0]:
    if energy_file is None:
        st.info("energy.csv yükleyin")
        st.stop()

    energy_bytes = energy_file.getvalue()
    energy_df = pd.read_csv(StringIO(energy_bytes.decode("utf-8")))

    prod_df = None
    prod_bytes = None
    if prod_file is not None:
        prod_bytes = prod_file.getvalue()
        prod_df = pd.read_csv(StringIO(prod_bytes.decode("utf-8")))

    energy_calc, energy_summary = compute_energy(energy_df)
    ets = compute_ets(
        energy_summary["scope1_tco2"],
        free_alloc,
        banked,
        eua_price,
        fx,
    )

    cbam_df = None
    cbam_totals = None
    if prod_df is not None:
        cbam_df, cbam_totals = compute_cbam(prod_df, eua_price, energy_summary["total_kgco2"])

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Energy (tCO2)", f'{energy_summary["total_tco2"]:.3f}')
    col2.metric("Scope1 (tCO2)", f'{energy_summary["scope1_tco2"]:.3f}')
    col3.metric("ETS net (tCO2)", f'{ets["net"]:.3f}')
    col4.metric("ETS cost (TL)", f'{ets["cost"]:.2f}')

    if cbam_df is not None:
        st.markdown("### CBAM SKU table")
        st.dataframe(cbam_df, use_container_width=True)
        st.markdown("**CBAM totals**")
        st.json(cbam_totals)

    if st.button("Save snapshot", type="primary"):
        db = SessionLocal()
        u_energy = save_upload(db, project.id, "energy", energy_file.name, energy_bytes)

        input_hashes = {"energy": u_energy.sha256}
        if prod_bytes is not None:
            u_prod = save_upload(db, project.id, "production", prod_file.name, prod_bytes)
            input_hashes["production"] = u_prod.sha256

        results = {
            "energy": energy_summary,
            "ets": ets,
            "cbam": cbam_totals,
        }
        snap = save_snapshot(
            db,
            project.id,
            APP_VERSION,
            {"eua_price": eua_price, "fx": fx, "free_alloc": free_alloc, "banked": banked},
            input_hashes,
            results,
        )
        st.success(f"Saved snapshot {snap.id}")

with tabs[1]:
    db = SessionLocal()

    uploads = db.query(DatasetUpload).filter(DatasetUpload.project_id == project.id).all()
    if uploads:
        dfu = pd.DataFrame([{
            "id": u.id,
            "type": u.dataset_type,
            "time": u.uploaded_at,
            "file": u.original_filename,
            "sha256": u.sha256,
        } for u in uploads])
        st.markdown("### Uploads")
        st.dataframe(dfu, use_container_width=True)

    snaps = db.query(CalculationSnapshot).filter(CalculationSnapshot.project_id == project.id).all()
    if snaps:
        dfs = pd.DataFrame([{
            "id": s.id,
            "time": s.created_at,
            "engine": s.engine_version,
            "hash": s.result_hash,
        } for s in snaps])
        st.markdown("### Snapshots")
        st.dataframe(dfs, use_container_width=True)
