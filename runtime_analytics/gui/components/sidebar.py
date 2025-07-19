from __future__ import annotations

import logging
import time

import streamlit as st

logger = logging.getLogger(__name__)


def render_sidebar():
    logger.info("Rendering sidebar with data source options...")
    mode = st.sidebar.radio("Data Source", ["Parse from logs", "Live from DB"], index=1)
    auto_refresh = st.sidebar.checkbox("Auto-refresh every 30s", value=False)

    if auto_refresh and mode == "Live from DB":
        logger.info("Auto-refresh enabled for Live from DB mode")
        time.sleep(30)
        st.rerun()

    return mode, auto_refresh
