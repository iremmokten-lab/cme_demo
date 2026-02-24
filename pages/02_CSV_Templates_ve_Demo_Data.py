import streamlit as st
import pandas as pd
from io import BytesIO
import zipfile

st.title("CSV Templates ve Demo Data")

st.write(
    """
Bu sayfadan sistemde kullanılan tüm veri setlerinin **şablonlarını** veya **demo verilerini**
indirip doğrudan **Veri Yükleme** sayfasına yükleyebilirsiniz.

✅ Bu dosyalar, uygulamanın şu an beklediği şemayla uyumludur.
"""
)

# energy.csv (yeni şema minimum):
#   month, facility_id, fuel_type, fuel_quantity, fuel_unit
#
# production.csv minimum:
#   month, facility_id, sku, cn_code, quantity, unit
# export_to_eu_quantity opsiyonel ama CBAM için faydalı
#
# materials.csv: precursor için (sku bazlı)
DEFAULT_FACILITY_ID = 1


def energy_template():
    return pd.DataFrame(
        columns=[
            "month",
            "facility_id",
            "fuel_type",
            "fuel_quantity",
            "fuel_unit",
        ]
    )


def energy_demo():
    return pd.DataFrame(
        [
            ["2025-01", DEFAULT_FACILITY_ID, "natural_gas", 120000, "m3"],
            ["2025-01", DEFAULT_FACILITY_ID, "diesel", 8000, "litre"],
            ["2025-01", DEFAULT_FACILITY_ID, "electricity", 450000, "kwh"],
            ["2025-02", DEFAULT_FACILITY_ID, "natural_gas", 110000, "m3"],
            ["2025-02", DEFAULT_FACILITY_ID, "electricity", 420000, "kwh"],
        ],
        columns=[
            "month",
            "facility_id",
            "fuel_type",
            "fuel_quantity",
            "fuel_unit",
        ],
    )


def production_template():
    return pd.DataFrame(
        columns=[
            "month",
            "facility_id",
            "sku",
            "cn_code",
            "quantity",
            "unit",
            "export_to_eu_quantity",
        ]
    )


def production_demo():
    return pd.DataFrame(
        [
            ["2025-01", DEFAULT_FACILITY_ID, "SKU-001", "7208", 1000, "ton", 400],
            ["2025-01", DEFAULT_FACILITY_ID, "SKU-002", "7601", 800, "ton", 250],
            ["2025-02", DEFAULT_FACILITY_ID, "SKU-001", "7208", 950, "ton", 360],
            ["2025-02", DEFAULT_FACILITY_ID, "SKU-002", "7601", 780, "ton", 240],
        ],
        columns=[
            "month",
            "facility_id",
            "sku",
            "cn_code",
            "quantity",
            "unit",
            "export_to_eu_quantity",
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


def materials_demo():
    return pd.DataFrame(
        [
            ["SKU-001", "Steel Slab", 1200, "kg", 1.9, "kgCO2e/kg", "Demo Supplier"],
            ["SKU-001", "Electrode", 25, "kg", 3.2, "kgCO2e/kg", "Demo Supplier"],
            ["SKU-002", "Aluminium Billet", 800, "kg", 8.5, "kgCO2e/kg", "Demo Supplier"],
            ["SKU-002", "Cardboard", 60, "kg", 0.7, "kgCO2e/kg", "Demo Supplier"],
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


def csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def download_button(df: pd.DataFrame, filename: str):
    st.download_button(
        label=f"{filename} indir",
        data=csv_bytes(df),
        file_name=filename,
        mime="text/csv",
    )


def build_demo_zip() -> BytesIO:
    files = {
        "energy_demo.csv": energy_demo(),
        "production_demo.csv": production_demo(),
        "materials_demo.csv": materials_demo(),
    }

    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for name, df in files.items():
            z.writestr(name, df.to_csv(index=False))
    buffer.seek(0)
    return buffer


st.header("🚀 Tek Tık Demo Dataset Paketi")
st.caption("Bu ZIP içindeki dosyalar doğrudan Veri Yükleme ekranına uygundur.")

st.download_button(
    "Tüm Demo Datasetleri indir (ZIP)",
    data=build_demo_zip(),
    file_name="carbon_demo_dataset.zip",
    mime="application/zip",
)

st.divider()

st.header("Energy (energy.csv)")
c1, c2 = st.columns(2)
with c1:
    st.subheader("Template")
    download_button(energy_template(), "energy_template.csv")
with c2:
    st.subheader("Demo Data")
    download_button(energy_demo(), "energy_demo.csv")

st.divider()

st.header("Production (production.csv)")
c1, c2 = st.columns(2)
with c1:
    st.subheader("Template")
    download_button(production_template(), "production_template.csv")
with c2:
    st.subheader("Demo Data")
    download_button(production_demo(), "production_demo.csv")

st.divider()

st.header("Materials (materials.csv) — CBAM Precursor")
c1, c2 = st.columns(2)
with c1:
    st.subheader("Template")
    download_button(materials_template(), "materials_template.csv")
with c2:
    st.subheader("Demo Data")
    download_button(materials_demo(), "materials_demo.csv")

st.divider()

st.success(
    """
✅ Artık ZIP’ten çıkan demo dosyalarını yükleyince validator hatası almazsınız.
Eğer facility_id farklıysa, bu dosyada DEFAULT_FACILITY_ID değerini kendi tesis ID’nize göre güncelleyin.
"""
)
