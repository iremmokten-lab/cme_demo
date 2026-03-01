import streamlit as st
from src.connectors.excel_connector import load_excel


def render_excel_import():

    st.title("Excel Veri Yükleme Merkezi")

    dataset_type = st.selectbox(
        "Dataset türü",
        ["facility", "energy", "production"]
    )

    uploaded = st.file_uploader("Excel dosyası yükle")

    if uploaded:

        try:
            result = load_excel(uploaded, dataset_type)

            st.success("Dosya doğrulandı")

            st.write("Dataset hash:")
            st.code(result["hash"])

            st.dataframe(result["dataframe"])

        except Exception as e:
            st.error(str(e))
