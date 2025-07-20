from __future__ import annotations

import io
import logging
import pandas as pd
import streamlit as st

from runtime_analytics.prompts import PREDEFINED_PROMPTS
from runtime_analytics.utils.display import highlight_rank

logger = logging.getLogger(__name__)


def render(tab) -> None:
    with tab:
        st.subheader("📊 Predefined Prompt Explorer")
        st.markdown(
            "Use predefined prompts to explore and analyze your data. "
            "Select a prompt from the dropdown below, adjust parameters if needed, "
            "and run the analysis."
        )

        # Prompt selection with default option
        prompt_options = ["-- Select a prompt --"] + list(PREDEFINED_PROMPTS.keys())
        selected_prompt_name = st.selectbox("Choose a predefined prompt", prompt_options)

        if selected_prompt_name == "-- Select a prompt --":
            st.info("Please select a prompt to continue.")
            return

        # Extract prompt config
        prompt_info = PREDEFINED_PROMPTS[selected_prompt_name]
        func = prompt_info["function"]
        params = prompt_info.get("params", {}).copy()
        data_source = prompt_info.get("data_source")

        # Handle dynamic GUI inputs
        if "n" in params:
            params["n"] = st.slider(
                "Select number of results to show (n)",
                min_value=1,
                max_value=50,
                value=params["n"],
            )

        if params.get("date_filter") == "custom_range":
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date")
            with col2:
                end_date = st.date_input("End Date")
            params["start_date"] = str(start_date)
            params["end_date"] = str(end_date)

        try:
            with st.spinner("Running prompt and loading data..."):
                if not callable(data_source):
                    st.error("Prompt is missing a valid data source function.")
                    return

                df_prompt = data_source(params)

                if df_prompt.empty:
                    st.warning("No logs found for the selected filters or date range.")
                    return

                result: pd.DataFrame = func(df_prompt, params).reset_index(drop=True)

                if result.empty:
                    st.warning("The function returned no results.")
                    return

                # Timestamp caption
                if "timestamp" in result.columns:
                    result["timestamp"] = pd.to_datetime(result["timestamp"], errors="coerce")
                    min_ts = result["timestamp"].min()
                    max_ts = result["timestamp"].max()
                    st.caption(
                        f"Results from **{min_ts.strftime('%d-%m-%Y %H:%M')}** "
                        f"to **{max_ts.strftime('%d-%m-%Y %H:%M')}** "
                        f"with **{len(result)}** records."
                    )

                # Conditional styling
                styled_result = result
                if "duration" in result.columns:
                    styled_result = styled_result.style.background_gradient(subset=["duration"], cmap="OrRd")
                if "rank" in result.columns:
                    styled_result = styled_result.map(highlight_rank, subset=["rank"])

                # Display results
                st.dataframe(styled_result, use_container_width=True)

                # CSV download
                csv_buffer = io.StringIO()
                result.to_csv(csv_buffer, index=False)
                st.download_button(
                    "📥 Download CSV",
                    data=csv_buffer.getvalue(),
                    file_name=f"{selected_prompt_name.replace(' ', '_')}.csv",
                    mime="text/csv",
                )

        except Exception as e:
            logger.exception("Predefined prompt execution failed")
            st.error(f"Error running prompt: {e}")