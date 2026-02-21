import streamlit as st

st.title("CSV Templates ve Demo Data")

st.write(
"""
Buradan uygulama için gerekli CSV dosyalarını indirebilirsin.

Template = boş şablon  
Demo = örnek veri
"""
)

# -------------------
# ENERGY TEMPLATE
# -------------------

energy_template = """energy_carrier,scope,activity_amount,emission_factor_kgco2_per_unit
electricity,2,,
natural_gas,1,,
diesel,1,,
"""

# -------------------
# PRODUCTION TEMPLATE
# -------------------

production_template = """sku,quantity,export_to_eu_quantity,input_emission_factor_kg_per_unit
SKU-1,,,
SKU-2,,,
SKU-3,,,
"""

# -------------------
# ENERGY DEMO
# -------------------

energy_demo = """energy_carrier,scope,activity_amount,emission_factor_kgco2_per_unit
natural_gas,1,1000,2.00
diesel,1,200,2.68
electricity,2,5000,0.40
electricity,2,2000,0.35
"""

# -------------------
# PRODUCTION DEMO
# -------------------

production_demo = """sku,quantity,export_to_eu_quantity,input_emission_factor_kg_per_unit
SKU-A,1000,200,1.20
SKU-B,500,50,0.80
SKU-C,200,0,2.00
SKU-D,300,100,1.50
"""

st.divider()

st.subheader("Template Dosyaları")

st.download_button(
    "energy_template.csv indir",
    energy_template,
    file_name="energy_template.csv",
    mime="text/csv"
)

st.download_button(
    "production_template.csv indir",
    production_template,
    file_name="production_template.csv",
    mime="text/csv"
)

st.divider()

st.subheader("Demo Veri")

st.download_button(
    "energy_demo.csv indir",
    energy_demo,
    file_name="energy_demo.csv",
    mime="text/csv"
)

st.download_button(
    "production_demo.csv indir",
    production_demo,
    file_name="production_demo.csv",
    mime="text/csv"
)

st.divider()

st.info("İndir → Ana sayfaya dön → Upload et")
