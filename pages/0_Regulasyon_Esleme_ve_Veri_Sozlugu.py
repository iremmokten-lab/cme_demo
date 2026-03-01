
# -*- coding: utf-8 -*-
import streamlit as st

from src.db.session import init_db
from src.services.authz import get_or_create_demo_user
from src.ui.excel_import_center import render_excel_import_center

st.set_page_config(page_title="Excel Veri Yükleme Merkezi", layout="wide")

init_db()
user = get_or_create_demo_user()

render_excel_import_center(user)
