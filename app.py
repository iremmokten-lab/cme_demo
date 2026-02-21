import streamlit as st
import pandas as pd
from io import StringIO

from src.services.ingestion import read_csv_uploaded
from src.services.workflow import run_calculation
from src.data.templates import (
    ENERGY_TEMPLATE_CSV, PRODUCTION_TEMPLATE_CSV,
    ENERGY_DEMO_CSV, PRODUCTION_DEMO_CSV,
)
from src.mrv.audit import append_jsonl, now_iso

st.set_page_config(page_title="CME Demo — CBAM + ETS", layout="wide")

APP_VERSION = "refactor-v1"
AUDIT_LOG_PATH = "runs.jsonl"
DISCLAIMER_TEXT = (
    "Önemli Not: Bu rapor yönetim amaçlı tahmini bir allocation/hesaplama çıktısıdır. "
    "Resmî beyan/uyum dokümanı değildir."
)

def parse_csv_string(s: str) -> pd.DataFrame:
    return pd.read_csv(StringIO(s))

st.title("CME Demo — CBAM (Ürün) + ETS (Tesis)")

# session defaults
if "demo_mode" not in st.session_state:
    st.session_state["demo_mode"] = False
if "free_allocation" not in st.session_state:
    st.session_state["free_allocation"] = 0.0
if "banked_allowances" not in st.session_state:
    st.session_state["banked_allowances"] = 0.0

with st.sidebar:
    st.subheader("Genel Parametreler")
    eua_price = st.slider("EUA price (€/tCO2)", 0.0, 200.0, 80.0, 1.0)
    fx = st.number_input("FX (TL/€)", min_value=0.0, value=35.0, step=0.5)

    st.divider()
    st.subheader("Tek tık demo")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Demo aç", type="primary"):
            st.session_state["demo_mode"] = True
    with c2:
        if st.button("Demo kapat"):
            st.session_state["demo_mode"] = False

    st.divider()
    st.subheader("CSV indir (Template/Demo)")
    st.download_button("energy template", ENERGY_TEMPLATE_CSV.encode("utf-8"), "energy_template.csv", "text/csv")
    st.download_button("production template", PRODUCTION_TEMPLATE_CSV.encode("utf-8"), "production_template.csv", "text/csv")
    st.download_button("energy demo", ENERGY_DEMO_CSV.encode("utf-8"), "energy_demo.csv", "text/csv")
    st.download_button("production demo", PRODUCTION_DEMO_CSV.encode("utf-8"), "production_demo.csv", "text/csv")

    st.divider()
    st.caption("Dosyalar")
    energy_file = st.file_uploader("energy.csv", type=["csv"])
    prod_file = st.file_uploader("production.csv (CBAM)", type=["csv"])

tabs = st.tabs(["Dashboard", "CBAM", "ETS"])

# Load data
energy_df = None
prod_df = None
err = None

try:
    if st.session_state["demo_mode"]:
        energy_df = parse_csv_string(ENERGY_DEMO_CSV)
        prod_df = parse_csv_string(PRODUCTION_DEMO_CSV)
    else:
        if energy_file is not None:
            energy_df = read_csv_uploaded(energy_file)
        if prod_file is not None:
            prod_df = read_csv_uploaded(prod_file)
except Exception as e:
    err = str(e)

if energy_df is None and not st.session_state["demo_mode"]:
    st.info("Başlamak için energy.csv yükleyin ya da Demo açın.")
    st.stop()

if err:
    st.error(err)
    st.stop()

# ETS inputs
with tabs[2]:
    st.subheader("ETS — Tesis Bazlı")
    st.session_state["free_allocation"] = st.number_input("Free allocation (tCO2)", min_value=0.0, value=float(st.session_state["free_allocation"]), step=100.0)
    st.session_state["banked_allowances"] = st.number_input("Banked allowances (tCO2)", min_value=0.0, value=float(st.session_state["banked_allowances"]), step=100.0)

# Run workflow
try:
    result = run_calculation(
        energy_df=energy_df,
        production_df=prod_df,
        eua_price=float(eua_price),
        fx=float(fx),
        free_allocation=float(st.session_state["free_allocation"]),
        banked_allowances=float(st.session_state["banked_allowances"]),
    )
except Exception as e:
    st.error(str(e))
    st.stop()

energy_summary = result["energy_summary"]
ets_summary = result["ets_summary"]
cbam_df = result["cbam_df"]
cbam_totals = result["cbam_totals"]
cbam_warning = result["cbam_warning"]

# Dashboard
with tabs[0]:
    st.subheader("Dashboard")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Energy total (tCO2)", f"{energy_summary['total_tco2']:.4f}")
    c2.metric("Scope 1 (tCO2)", f"{energy_summary['scope1_tco2']:.4f}")
    if cbam_totals:
        c3.metric("CBAM embedded (tCO2)", f"{cbam_totals['total_cbam_embedded_tco2']:.4f}")
        c4.metric("CBAM €", f"{cbam_totals['total_cbam_cost_eur']:.2f}")
    else:
        c3.metric("CBAM embedded (tCO2)", "-")
        c4.metric("CBAM €", "-")

    st.divider()
    st.metric("ETS cost (TL)", f"{ets_summary['ets_cost_tl']:,.2f}")

    st.caption(DISCLAIMER_TEXT)

    if st.button("Audit log yaz (runs.jsonl)"):
        append_jsonl(AUDIT_LOG_PATH, {
            "ts_utc": now_iso(),
            "app_version": APP_VERSION,
            "inputs": {
                "eua_price": float(eua_price),
                "fx": float(fx),
                "free_allocation": float(st.session_state["free_allocation"]),
                "banked_allowances": float(st.session_state["banked_allowances"]),
                "demo_mode": bool(st.session_state["demo_mode"]),
            },
            "energy_summary": energy_summary,
            "ets_summary": ets_summary,
            "cbam_totals": cbam_totals,
        })
        st.success("Audit log yazıldı.")

# CBAM
with tabs[1]:
    st.subheader("CBAM — Ürün Bazlı")
    if cbam_warning:
        st.warning(cbam_warning)
    if cbam_df is None:
        st.info("CBAM için production.csv yükleyin (veya Demo açın).")
    else:
        st.caption(f"CBAM covered: {cbam_totals['covered_sku_count']} | CBAM dışı: {cbam_totals['not_covered_sku_count']}")
        st.dataframe(cbam_df.sort_values("risk_score_0_100", ascending=False), use_container_width=True)

# ETS output
with tabs[2]:
    st.subheader("ETS çıktıları")
    st.metric("Scope 1 total (tCO2)", f"{ets_summary['scope1_tco2']:.4f}")
    st.metric("Net EUA requirement (tCO2)", f"{ets_summary['net_eua_requirement_tco2']:.4f}")
    st.metric("ETS cost (TL)", f"{ets_summary['ets_cost_tl']:,.2f}")

st.divider()
st.caption("Demo notu: Çıktılar yönetim amaçlı tahminidir; resmî beyan değildir.")
