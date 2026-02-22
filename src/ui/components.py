import streamlit as st

def h2(title: str, caption: str | None = None):
    st.subheader(title)
    if caption:
        st.caption(caption)
