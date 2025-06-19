import streamlit as st


def set_layout():
    st.set_page_config(layout="wide", page_title="Runtime Analytics", page_icon="📊")
    st.markdown("<h1 style='color:#4A90E2;'>Runtime Analytics Dashboard</h1>", unsafe_allow_html=True)
