import streamlit as st
import time


def render_sidebar():
    mode = st.sidebar.radio("Data Source", ["Parse from logs", "Live from DB"], index=1)
    auto_refresh = st.sidebar.checkbox("Auto-refresh every 30s", value=False)

    if auto_refresh and mode == "Live from DB":
        time.sleep(30)
        st.rerun()

    return mode, auto_refresh
