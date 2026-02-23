import streamlit as st
import pandas as pd
from io import StringIO

st.title("CSV Templates ve Demo Data")

st.write(
"""
Bu sayfadan sistemde kullanılan tüm veri setlerinin
**şablonlarını** veya **demo verilerini** indirebilirsiniz.

İndirilen dosyaları doğrudan **Veri Yükleme** sayfasına yükleyebilirsiniz.
"""
)

# ---------------------------------------------------
# ENERGY
# ---------------------------------------------------

def energy_demo():
    df = pd.DataFrame(
        [
            ["natural_gas", 120000, "m3", 0.038, 56.1, 0.99, "facility_meter"],
            ["diesel", 8000, "litre", 0.036, 74.1, 0.99, "invoice"],
            ["electricity", 450000, "kwh", None, None, None, "grid"],
        ],
        columns=[
            "fuel_type",
            "quantity",
            "unit",
            "ncv",
            "emission_factor",
            "oxidation_factor",
            "source",
        ],
    )
    return df


def energy_template():
    df = pd.DataFrame(
        columns=[
            "fuel_type",
            "quantity",
            "unit",
            "ncv",
            "emission_factor",
            "oxidation_factor",
            "source",
        ]
    )
    return df


# ---------------------------------------------------
# PRODUCTION
# ---------------------------------------------------

def production_demo():
    df = pd.DataFrame(
        [
            ["SKU-001", "7208", 1000, 400],
            ["SKU-002", "7601", 800, 250],
        ],
        columns=[
            "sku",
            "cn_code",
            "quantity",
            "export_to_eu_quantity",
        ],
    )
    return df


def production_template():
    df = pd.DataFrame(
        columns=[
            "sku",
            "cn_code",
            "quantity",
            "export_to_eu_quantity",
        ]
    )
    return df


# ---------------------------------------------------
# MATERIALS
# ---------------------------------------------------

def materials_demo():
    df = pd.DataFrame(
        [
            ["SKU-001", "Steel Slab", 1200, "kg", 1.9, "kgCO2/kg", "Demo Supplier"],
            ["SKU-002", "Aluminium Billet", 800, "kg", 8.5, "kgCO2/kg", "Demo Supplier"],
        ],
        columns=[
            "sku",
            "material_name",
            "material_quantity",
            "material_unit",
            "emission_factor",
            "emission_factor_unit",
            "supplier",
        ],
    )
    return df


def materials_template():
    df = pd.DataFrame(
        columns=[
            "sku",
            "material_name",
            "material_quantity",
            "material_unit",
            "emission_factor",
            "emission_factor_unit",
            "supplier",
        ]
    )
    return df


# ---------------------------------------------------
# MONITORING PLAN
# ---------------------------------------------------

def monitoring_demo():
    df = pd.DataFrame(
        [
            ["Main Plant", "Tier 2", "Standard Method", "Utility Bills", "Annual QA Check", "Energy Manager"],
        ],
        columns=[
            "facility_name",
            "tier_level",
            "method",
            "data_source",
            "qa_procedure",
            "responsible_person",
        ],
    )
    return df


def monitoring_template():
    df = pd.DataFrame(
        columns=[
            "facility_name",
            "tier_level",
            "method",
            "data_source",
            "qa_procedure",
            "responsible_person",
        ]
    )
    return df


# ---------------------------------------------------
# DOWNLOAD HELPER
# ---------------------------------------------------

def download_button(df, filename):
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=f"{filename} indir",
        data=csv,
        file_name=filename,
        mime="text/csv",
    )


# ---------------------------------------------------
# UI
# ---------------------------------------------------

st.header("Energy")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Template")
    download_button(energy_template(), "energy_template.csv")

with col2:
    st.subheader("Demo Data")
    download_button(energy_demo(), "energy_demo.csv")


st.divider()

st.header("Production")

col1, col2 = st.columns(2)

with col1:
    download_button(production_template(), "production_template.csv")

with col2:
    download_button(production_demo(), "production_demo.csv")


st.divider()

st.header("Materials (CBAM Precursor)")

col1, col2 = st.columns(2)

with col1:
    download_button(materials_template(), "materials_template.csv")

with col2:
    download_button(materials_demo(), "materials_demo.csv")


st.divider()

st.header("Monitoring Plan (ETS)")

col1, col2 = st.columns(2)

with col1:
    download_button(monitoring_template(), "monitoring_template.csv")

with col2:
    download_button(monitoring_demo(), "monitoring_demo.csv")


st.divider()

st.success(
"""
Bu dosyaları indirip doğrudan **Veri Yükleme** sayfasına yükleyebilirsiniz.
"""
)
