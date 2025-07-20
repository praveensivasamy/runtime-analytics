from __future__ import annotations

import logging

import streamlit as st

logger = logging.getLogger(__name__)


def set_layout():
    logger.debug("Setting up Streamlit layout and header...")
    st.set_page_config(layout="wide", page_title="Runtime Analytics", page_icon="📊")
    st.markdown("<h1 style='color:#4A90E2;'>Runtime Analytics Dashboard</h1>", unsafe_allow_html=True)