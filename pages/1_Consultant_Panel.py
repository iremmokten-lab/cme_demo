import streamlit as st
from src.ui.consultant import consultant_app

st.set_page_config(page_title="Consultant Panel", layout="wide")

st.title("Consultant Panel")

# Geçici kullanıcı objesi
user = {
    "name": "Consultant",
    "role": "consultant",
    "email": "consultant@demo.com"
}

consultant_app(user)
