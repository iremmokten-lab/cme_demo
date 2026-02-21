import streamlit as st
import pandas as pd
import numpy as np
import json
from datetime import datetime, timezone
from io import StringIO

from sqlalchemy import inspect
from src.db.session import engine, SessionLocal
from src.db.models import Base, Company, Project, DatasetUpload, CalculationSnapshot
from src.services.projects import list_companies, get_or_create_company, list_projects, create_project
from src.services.persistence import save_upload, save_snapshot


st.set_page_config(page_title="CME Platform MVP — Sprint 1", layout="wide")

APP_VERSION = "sprint1-step1"

DISCLAIMER_TEXT = (
    "Önemli Not: Bu rapor yönetim amaçlı tahmini bir allocation/hesaplama çıktısıdır. "
    "Resmî beyan değildir."
)


def init_db():
    inspector = inspect(engine)
    Base.metadata.create_all(bind=engine)


init_db()


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def safe_float(x):
    try:
        if pd.isna(x):
            return np.nan
        return float(x)
    except:
        return np.nan


def kg_to_t(x):
    return float(x) / 1000.0


def require_cols(df, cols, name):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name} eksik kolonlar: {missing}")


def compute_energy(energy_df):

    require_cols(
        energy_df,
        ["energy_carrier", "scope", "activity_amount", "emission_factor_kgco2_per_unit"],
        "energy.csv",
    )

    df = energy_df.copy()

    df["scope"] = df["scope"].apply(safe_float)
    df["activity_amount"] = df["activity_amount"].apply(safe_float)
    df["emission_factor_kgco2_per_unit"] = df[
        "emission_factor_kgco2_per_unit"
    ].apply(safe_float)

    df["emissions_kgco2"] = (
        df["activity_amount"] * df["emission_factor_kgco2_per_unit"]
    )

    total = df["emissions_kgco2"].sum()
    s1 = df[df["scope"] == 1]["emissions_kgco2"].sum()
    s2 = df[df["scope"] == 2]["emissions_kgco2"].sum()

    return df, {
        "total_tco2": kg_to_t(total),
        "scope1_tco2": kg_to_t(s1),
        "scope2_tco2": kg_to_t(s2),
        "total_kgco2": total,
    }


def allocate_energy(prod_df, total_energy):

    require_cols(prod_df, ["sku", "quantity"], "production.csv")

    df = prod_df.copy()

    df["quantity"] = df["quantity"].apply(safe_float)

    total_qty = df["quantity"].sum()

    if total_qty <= 0:
        df["alloc_energy_kgco2_per_unit"] = 0
        return df

    df["alloc_energy_kgco2"] = (df["quantity"] / total_qty) * total_energy

    df["alloc_energy_kgco2_per_unit"] = df["alloc_energy_kgco2"] / df["quantity"]

    return df


def compute_cbam(prod_df, eua_price, total_energy):

    require_cols(
        prod_df,
        [
            "sku",
            "quantity",
            "export_to_eu_quantity",
            "input_emission_factor_kg_per_unit",
        ],
        "production.csv",
    )

    df = prod_df.copy()

    if "cbam_covered" not in df.columns:
        df["cbam_covered"] = 1

    df["quantity"] = df["quantity"].apply(safe_float)
    df["export_to_eu_quantity"] = df["export_to_eu_quantity"].apply(safe_float)
    df["input_emission_factor_kg_per_unit"] = df[
        "input_emission_factor_kg_per_unit"
    ].apply(safe_float)

    df = allocate_energy(df, total_energy)

    df["export_for_cbam"] = np.where(
        df["cbam_covered"] == 1,
        df["export_to_eu_quantity"],
        0,
    )

    df["total_factor"] = (
        df["alloc_energy_kgco2_per_unit"]
        + df["input_emission_factor_kg_per_unit"]
    )

    df["embedded_kg"] = df["export_for_cbam"] * df["total_factor"]

    df["embedded_t"] = df["embedded_kg"].apply(kg_to_t)

    df["cbam_cost_eur"] = df["embedded_t"] * eua_price

    totals = {
        "embedded_tco2": df["embedded_t"].sum(),
        "cbam_cost_eur": df["cbam_cost_eur"].sum(),
    }

    return df, totals


def compute_ets(scope1, free_alloc, banked, price, fx):

    net = max(0, scope1 - free_alloc - banked)

    cost = net * price * fx

    return {"net": net, "cost": cost}


st.title("CME Platform MVP")


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

    eua_price = st.slider("EUA", 0.0, 200.0, 80.0)

    fx = st.number_input("FX", value=35.0)

    free_alloc = st.number_input("Free allocation", value=0.0)

    banked = st.number_input("Banked allowances", value=0.0)

    st.divider()

    energy_file = st.file_uploader("energy.csv")

    prod_file = st.file_uploader("production.csv")


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

        cbam_df, cbam_totals = compute_cbam(
            prod_df,
            eua_price,
            energy_summary["total_kgco2"],
        )

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Energy", energy_summary["total_tco2"])
    col2.metric("Scope1", energy_summary["scope1_tco2"])
    col3.metric("ETS net", ets["net"])
    col4.metric("ETS cost", ets["cost"])

    if cbam_df is not None:

        st.dataframe(cbam_df)

    if st.button("Save snapshot"):

        db = SessionLocal()

        u_energy = save_upload(
            db,
            project.id,
            "energy",
            energy_file.name,
            energy_bytes,
        )

        input_hashes = {"energy": u_energy.sha256}

        if prod_bytes:

            u_prod = save_upload(
                db,
                project.id,
                "production",
                prod_file.name,
                prod_bytes,
            )

            input_hashes["prod"] = u_prod.sha256

        results = {
            "energy": energy_summary,
            "ets": ets,
            "cbam": cbam_totals,
        }

        snap = save_snapshot(
            db,
            project.id,
            APP_VERSION,
            {"price": eua_price},
            input_hashes,
            results,
        )

        st.success(f"Saved snapshot {snap.id}")


with tabs[1]:

    db = SessionLocal()

    uploads = (
        db.query(DatasetUpload)
        .filter(DatasetUpload.project_id == project.id)
        .all()
    )

    if uploads:

        df = pd.DataFrame(
            [
                {
                    "id": u.id,
                    "type": u.dataset_type,
                    "time": u.uploaded_at,
                    "file": u.original_filename,
                }
                for u in uploads
            ]
        )

        st.dataframe(df)

    snaps = (
        db.query(CalculationSnapshot)
        .filter(CalculationSnapshot.project_id == project.id)
        .all()
    )

    if snaps:

        df = pd.DataFrame(
            [
                {
                    "id": s.id,
                    "time": s.created_at,
                    "hash": s.result_hash,
                }
                for s in snaps
            ]
        )

        st.dataframe(df)
