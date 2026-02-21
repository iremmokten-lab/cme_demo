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

# -----------------------------
# App Config
# -----------------------------
st.set_page_config(page_title="CME Platform MVP — Sprint 1", layout="wide")
APP_VERSION = "sprint1-step1"
DISCLAIMER_TEXT = (
    "Önemli Not: Bu rapor yönetim amaçlı tahmini bir allocation/hesaplama çıktısıdır. "
    "Resmî beyan/uyum dokümanı değildir."
)

# -----------------------------
# DB init (auto-create)
# -----------------------------
def init_db():
    inspector = inspect(engine)
    # create all if not exists
    Base.metadata.create_all(bind=engine)

init_db()

def utcnow():
    return datetime.now(timezone.utc).isoformat()

# -----------------------------
# Minimal calc engine (same logic)
# -----------------------------
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
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise ValueError(f"{name} eksik kolon(lar): " + ", ".join(miss))

def compute_energy(energy_df: pd.DataFrame):
    require_cols(energy_df, ["energy_carrier","scope","activity_amount","emission_factor_kgco2_per_unit"], "energy.csv")
    df = energy_df.copy()
    df["scope"] = df["scope"].apply(safe_float)
    df["activity_amount"] = df["activity_amount"].apply(safe_float)
    df["emission_factor_kgco2_per_unit"] = df["emission_factor_kgco2_per_unit"].apply(safe_float)

    bad_scope = ~df["scope"].isin([1,2])
    if bad_scope.any():
        i = int(np.where(bad_scope.to_numpy())[0][0])
        st_row = i + 2
        raise ValueError(f"energy.csv: scope satır {st_row} 1 veya 2 olmalı.")

    if (df["activity_amount"].isna() | (df["activity_amount"] < 0)).any():
        i = int(np.where((df["activity_amount"].isna() | (df["activity_amount"] < 0)).to_numpy())[0][0])
        raise ValueError(f"energy.csv: activity_amount satır {i+2} boş/negatif.")
    if (df["emission_factor_kgco2_per_unit"].isna() | (df["emission_factor_kgco2_per_unit"] < 0)).any():
        i = int(np.where((df["emission_factor_kgco2_per_unit"].isna() | (df["emission_factor_kgco2_per_unit"] < 0)).to_numpy())[0][0])
        raise ValueError(f"energy.csv: emission_factor satır {i+2} boş/negatif.")

    df["scope"] = df["scope"].astype(int)
    df["emissions_kgco2"] = df["activity_amount"] * df["emission_factor_kgco2_per_unit"]

    total_kg = float(df["emissions_kgco2"].sum())
    s1_kg = float(df.loc[df["scope"]==1,"emissions_kgco2"].sum())
    s2_kg = float(df.loc[df["scope"]==2,"emissions_kgco2"].sum())

    return df, {
        "total_tco2": kg_to_t(total_kg),
        "scope1_tco2": kg_to_t(s1_kg),
        "scope2_tco2": kg_to_t(s2_kg),
        "total_kgco2": total_kg,
    }

def allocate_energy(prod_df: pd.DataFrame, total_energy_kgco2: float):
    require_cols(prod_df, ["sku","quantity"], "production.csv")
    df = prod_df.copy()
    df["quantity"] = df["quantity"].apply(safe_float)
    if (df["quantity"].isna() | (df["quantity"] < 0)).any():
        i = int(np.where((df["quantity"].isna() | (df["quantity"] < 0)).to_numpy())[0][0])
        raise ValueError(f"production.csv: quantity satır {i+2} boş/negatif.")
    total_qty = float(df["quantity"].sum())
    if total_qty <= 0:
        df["alloc_energy_kgco2_per_unit"] = 0.0
        return df
    df["alloc_energy_kgco2"] = (df["quantity"] / total_qty) * float(total_energy_kgco2)
    df["alloc_energy_kgco2_per_unit"] = np.where(df["quantity"]>0, df["alloc_energy_kgco2"]/df["quantity"], 0.0)
    return df

def compute_cbam(prod_df: pd.DataFrame, eua_price: float, total_energy_kgco2: float):
    require_cols(prod_df, ["sku","quantity","export_to_eu_quantity","input_emission_factor_kg_per_unit"], "production.csv")
    df = prod_df.copy()
    if "cbam_covered" not in df.columns:
        df["cbam_covered"] = 1
    df["quantity"] = df["quantity"].apply(safe_float)
    df["export_to_eu_quantity"] = df["export_to_eu_quantity"].apply(safe_float)
    df["input_emission_factor_kg_per_unit"] = df["input_emission_factor_kg_per_unit"].apply(safe_float)
    df["cbam_covered"] = df["cbam_covered"].apply(safe_float).fillna(1)

    bad_cov = ~df["cbam_covered"].isin([0,1])
    if bad_cov.any():
        i = int(np.where(bad_cov.to_numpy())[0][0])
        raise ValueError(f"production.csv: cbam_covered satır {i+2} 0/1 olmalı.")
    df["cbam_covered"] = df["cbam_covered"].astype(int)

    # validate nonnegative
    for c in ["export_to_eu_quantity","input_emission_factor_kg_per_unit","quantity"]:
        if (df[c].isna() | (df[c] < 0)).any():
            i = int(np.where((df[c].isna() | (df[c] < 0)).to_numpy())[0][0])
            raise ValueError(f"production.csv: {c} satır {i+2} boş/negatif.")

    df = allocate_energy(df, total_energy_kgco2)
    df["export_for_cbam"] = np.where(df["cbam_covered"]==1, df["export_to_eu_quantity"], 0.0)

    warn = None
    if ((df["cbam_covered"]==1) & (df["export_to_eu_quantity"] > df["quantity"]) & (df["quantity"]>0)).any():
        warn = "Uyarı: export_to_eu_quantity > quantity olan satır var."

    df["total_factor_kg_per_unit"] = df["alloc_energy_kgco2_per_unit"] + df["input_emission_factor_kg_per_unit"]
    df["embedded_kg"] = df["export_for_cbam"] * df["total_factor_kg_per_unit"]
    df["embedded_t"] = df["embedded_kg"].apply(kg_to_t)
    df["cbam_cost_eur"] = df["embedded_t"] * float(eua_price)

    totals = {
        "embedded_tco2": float(df["embedded_t"].sum()),
        "cbam_cost_eur": float(df["cbam_cost_eur"].sum()),
        "covered_sku_count": int((df["cbam_covered"]==1).sum()),
        "not_covered_sku_count": int((df["cbam_covered"]==0).sum()),
    }
    return df, totals, warn

def compute_ets(scope1_tco2, free_alloc, banked, eua_price, fx):
    net = max(0.0, float(scope1_tco2) - float(free_alloc) - float(banked))
    cost = net * float(eua_price) * float(fx)
    return {"scope1_tco2": float(scope1_tco2), "net_eua_tco2": net, "ets_cost_tl": cost}

# -----------------------------
# UI
# -----------------------------
st.title("CME Platform MVP — Sprint 1 (DB + Project + History)")

with st.sidebar:
    st.subheader("Company / Project")
    db = SessionLocal()

    companies = list_companies(db)
    company_names = ["(Yeni Company oluştur)"] + [c.name for c in companies]
    company_choice = st.selectbox("Company seç", company_names)

    if company_choice == "(Yeni Company oluştur)":
        new_company_name = st.text_input("Yeni company adı")
        if st.button("Company oluştur"):
            c = get_or_create_company(db, new_company_name)
            st.success(f"Company oluşturuldu: {c.name}")
            st.rerun()
        st.stop()

    company = next(c for c in companies if c.name == company_choice)

    projects = list_projects(db, company.id)
    project_names = ["(Yeni Project oluştur)"] + [p.name for p in projects]
    project_choice = st.selectbox("Project seç", project_names)

    if project_choice == "(Yeni Project oluştur)":
        new_project_name = st.text_input("Yeni project adı")
        if st.button("Project oluştur"):
            p = create_project(db, company.id, new_project_name)
            st.success(f"Project oluşturuldu: {p.name}")
            st.rerun()
        st.stop()

    project = next(p for p in projects if p.name == project_choice)

    st.divider()
    st.subheader("Run Parametreleri")
    eua_price = st.slider("EUA price (€/tCO2)", 0.0, 200.0, 80.0, 1.0)
    fx = st.number_input("FX (TL/€)", min_value=0.0, value=35.0, step=0.5)
    free_alloc = st.number_input("Free allocation (tCO2)", min_value=0.0, value=0.0, step=100.0)
    banked = st.number_input("Banked allowances (tCO2)", min_value=0.0, value=0.0, step=100.0)

    st.divider()
    st.subheader("Dosyalar")
    energy_file = st.file_uploader("energy.csv yükle", type=["csv"])
    prod_file = st.file_uploader("production.csv yükle (opsiyonel)", type=["csv"])

tabs = st.tabs(["Run", "History"])

# -----------------------------
# RUN TAB
# -----------------------------
with tabs[0]:
    st.subheader("Run")

    if energy_file is None:
        st.info("energy.csv yükleyin.")
        st.stop()

    try:
        energy_bytes = energy_file.getvalue()
        energy_df = pd.read_csv(StringIO(energy_bytes.decode("utf-8")))
    except Exception:
        # fallback
        energy_df = pd.read_csv(energy_file)

    prod_df = None
    prod_bytes = None
    if prod_file is not None:
        try:
            prod_bytes = prod_file.getvalue()
            prod_df = pd.read_csv(StringIO(prod_bytes.decode("utf-8")))
        except Exception:
            prod_df = pd.read_csv(prod_file)

    try:
        energy_calc_df, energy_summary = compute_energy(energy_df)
        ets = compute_ets(energy_summary["scope1_tco2"], free_alloc, banked, eua_price, fx)

        cbam_df = None
        cbam_totals = None
        cbam_warn = None
        if prod_df is not None:
            cbam_df, cbam_totals, cbam_warn = compute_cbam(prod_df, eua_price, energy_summary["total_kgco2"])
            if cbam_warn:
                st.warning(cbam_warn)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Energy total (tCO2)", f"{energy_summary['total_tco2']:.4f}")
        c2.metric("Scope 1 (tCO2)", f"{energy_summary['scope1_tco2']:.4f}")
        c3.metric("ETS net (tCO2)", f"{ets['net_eua_tco2']:.4f}")
        c4.metric("ETS cost (TL)", f"{ets['ets_cost_tl']:,.2f}")

        if cbam_totals:
            st.divider()
            st.caption(f"CBAM covered: {cbam_totals['covered_sku_count']} | CBAM dışı: {cbam_totals['not_covered_sku_count']}")
            st.metric("CBAM €", f"{cbam_totals['cbam_cost_eur']:.2f}")
            st.dataframe(cbam_df.sort_values("cbam_cost_eur", ascending=False), use_container_width=True, height=360)

        st.divider()
        st.caption(DISCLAIMER_TEXT)

        if st.button("Save to DB (Upload + Snapshot)"):
            db = SessionLocal()

            # Save uploads
            u_energy = save_upload(
                db=db,
                project_id=project.id,
                dataset_type="energy",
                original_filename=energy_file.name,
                content_bytes=energy_bytes,
                schema_version="v1",
            )
            input_hashes = {"energy_sha256": u_energy.sha256}

            u_prod = None
            if prod_df is not None and prod_bytes is not None:
                u_prod = save_upload(
                    db=db,
                    project_id=project.id,
                    dataset_type="production",
                    original_filename=prod_file.name,
                    content_bytes=prod_bytes,
                    schema_version="v1",
                )
                input_hashes["production_sha256"] = u_prod.sha256

            config = {
                "eua_price": float(eua_price),
                "fx": float(fx),
                "free_allocation": float(free_alloc),
                "banked_allowances": float(banked),
                "ts_utc": utcnow(),
                "company": company.name,
                "project": project.name,
            }
            results = {
                "energy_summary": energy_summary,
                "ets_summary": ets,
                "cbam_totals": cbam_totals,
            }
            snap = save_snapshot(
                db=db,
                project_id=project.id,
                engine_version=APP_VERSION,
                config=config,
                input_hashes=input_hashes,
                results=results,
            )
            st.success(f"Kaydedildi. Snapshot ID: {snap.id}")
            st.rerun()

    except Exception as e:
        st.error(str(e))

# -----------------------------
# HISTORY TAB
# -----------------------------
with tabs[1]:
    st.subheader("History")

    db = SessionLocal()

    st.markdown("### Upload history (son 20)")
    uploads = (
        db.query(DatasetUpload)
        .filter(DatasetUpload.project_id == project.id)
        .order_by(DatasetUpload.uploaded_at.desc())
        .limit(20)
        .all()
    )
    if not uploads:
        st.info("Henüz upload yok.")
    else:
        up_df = pd.DataFrame([{
            "id": u.id,
            "type": u.dataset_type,
            "uploaded_at": u.uploaded_at,
            "filename": u.original_filename,
            "sha256": u.sha256,
            "schema": u.schema_version,
            "size_bytes": len(u.content_bytes) if u.content_bytes else 0,
        } for u in uploads])
        st.dataframe(up_df, use_container_width=True, height=280)

    st.markdown("### Snapshots (son 20)")
    snaps = (
        db.query(CalculationSnapshot)
        .filter(CalculationSnapshot.project_id == project.id)
        .order_by(CalculationSnapshot.created_at.desc())
        .limit(20)
        .all()
    )
    if not snaps:
        st.info("Henüz snapshot yok.")
    else:
        snap_df = pd.DataFrame([{
            "id": s.id,
            "created_at": s.created_at,
            "engine_version": s.engine_version,
            "result_hash": s.result_hash,
        } for s in snaps])
        st.dataframe(snap_df, use_container_width=True, height=240)

        chosen = st.selectbox("Detay görmek için snapshot seç", [s.id for s in snaps])
        snap = next(s for s in snaps if s.id == chosen)
        st.code(snap.config_json, language="json")
        st.code(snap.input_hashes_json, language="json")
        st.code(snap.results_json, language="json")
