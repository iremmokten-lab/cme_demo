import streamlit as st
import pandas as pd
from io import BytesIO
import zipfile

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
    return pd.DataFrame(
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


def energy_template():
    return pd.DataFrame(
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


# ---------------------------------------------------
# PRODUCTION
# ---------------------------------------------------

def production_demo():
    return pd.DataFrame(
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


def production_template():
    return pd.DataFrame(
        columns=[
            "sku",
            "cn_code",
            "quantity",
            "export_to_eu_quantity",
        ]
    )


# ---------------------------------------------------
# MATERIALS
# ---------------------------------------------------

def materials_demo():
    return pd.DataFrame(
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


def materials_template():
    return pd.DataFrame(
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


# ---------------------------------------------------
# MONITORING PLAN
# ---------------------------------------------------

def monitoring_demo():
    return pd.DataFrame(
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


def monitoring_template():
    return pd.DataFrame(
        columns=[
            "facility_name",
            "tier_level",
            "method",
            "data_source",
            "qa_procedure",
            "responsible_person",
        ]
    )


# ---------------------------------------------------
# CSV DOWNLOAD
# ---------------------------------------------------

def csv_bytes(df):
    return df.to_csv(index=False).encode("utf-8")


def download_button(df, filename):
    st.download_button(
        label=f"{filename} indir",
        data=csv_bytes(df),
        file_name=filename,
        mime="text/csv",
    )


# ---------------------------------------------------
# ZIP DOWNLOAD
# ---------------------------------------------------

def build_demo_zip():

    files = {
        "energy_demo.csv": energy_demo(),
        "production_demo.csv": production_demo(),
        "materials_demo.csv": materials_demo(),
        "monitoring_demo.csv": monitoring_demo(),
    }

    buffer = BytesIO()

    with zipfile.ZipFile(buffer, "w") as z:

        for name, df in files.items():
            z.writestr(name, df.to_csv(index=False))

    buffer.seek(0)

    return buffer


st.header("🚀 Tüm Demo Datasetleri")

zip_file = build_demo_zip()

st.download_button(
    "Tüm Demo Datasetleri indir (ZIP)",
    data=zip_file,
    file_name="carbon_demo_dataset.zip",
    mime="application/zip",
)

st.divider()

# ---------------------------------------------------
# ENERGY
# ---------------------------------------------------

st.header("Energy")

c1, c2 = st.columns(2)

with c1:
    st.subheader("Template")
    download_button(energy_template(), "energy_template.csv")

with c2:
    st.subheader("Demo Data")
    download_button(energy_demo(), "energy_demo.csv")

st.divider()

# ---------------------------------------------------
# PRODUCTION
# ---------------------------------------------------

st.header("Production")

c1, c2 = st.columns(2)

with c1:
    download_button(production_template(), "production_template.csv")

with c2:
    download_button(production_demo(), "production_demo.csv")

st.divider()

# ---------------------------------------------------
# MATERIALS
# ---------------------------------------------------

st.header("Materials (CBAM Precursor)")

c1, c2 = st.columns(2)

with c1:
    download_button(materials_template(), "materials_template.csv")

with c2:
    download_button(materials_demo(), "materials_demo.csv")

st.divider()

# ---------------------------------------------------
# MONITORING PLAN
# ---------------------------------------------------

st.header("Monitoring Plan (ETS)")

c1, c2 = st.columns(2)

with c1:
    download_button(monitoring_template(), "monitoring_template.csv")

with c2:
    download_button(monitoring_demo(), "monitoring_demo.csv")

st.divider()

st.success(
"""
İndirilen dosyaları **Veri Yükleme** sayfasına yükleyerek sistemi hemen test edebilirsiniz.
"""
)
