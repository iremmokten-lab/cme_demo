import streamlit as st
from src.ui.consultant import consultant_app

st.set_page_config(page_title="Consultant Panel", layout="wide")

st.title("Consultant Panel")

consultant_app()
