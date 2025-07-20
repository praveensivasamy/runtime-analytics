from __future__ import annotations

from datetime import date, timedelta
from io import StringIO
import pandas as pd
import streamlit as st

from runtime_analytics.repositories.job_log_repository import select_logs_from_db_with_filters
from runtime_analytics.utils.data_comparison_plotter_utils import (
    create_duration_comparison_chart,
    create_drilldown_chart,
)

def render(tab) -> None:
    with tab:
        st.subheader("\U0001F4C8 Duration by Job Type Comparison")

        with st.expander("Step 1: Choose Dates for Comparison", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                selected_date: date = st.date_input("Select a riskdate", value=None)
            with col2:
                use_custom_compare = st.checkbox("Compare with another date?")
                if use_custom_compare:
                    reference_date = st.date_input("Select comparison date")
                else:
                    reference_date = selected_date - timedelta(days=7) if selected_date else None

        if not selected_date:
            st.info("Please select a date to load data.")
            return

        current_df = select_logs_from_db_with_filters(filters={"run_date": selected_date.strftime("%Y-%m-%d")})
        if current_df.empty:
            st.warning("No logs found for selected date.")
            return

        reference_df = select_logs_from_db_with_filters(filters={"run_date": reference_date.strftime("%Y-%m-%d")})
        ref_agg_df = (
            reference_df.groupby("type", as_index=False)["duration"].sum()
            if not reference_df.empty
            else pd.DataFrame(columns=["type", "duration"])
        )

        agg_df = current_df.groupby("type", as_index=False)["duration"].sum()
        job_counts = current_df.groupby("type")["id"].count().reset_index(name="job_count")

        merged_df = agg_df.merge(ref_agg_df, on="type", how="left", suffixes=("_current", "_ref"))
        merged_df = merged_df.merge(job_counts, on="type", how="left")

        merged_df["duration_current_min"] = merged_df["duration_current"] / 60
        merged_df["duration_ref_min"] = merged_df["duration_ref"] / 60
        merged_df["delta"] = merged_df["duration_current_min"] - merged_df["duration_ref_min"]

        st.markdown("### Total Duration by Job Type (in minutes)")
        st.caption(
            f"Selected date: **{selected_date.strftime('%d-%b-%Y')}**, "
            f"compared with: **{reference_date.strftime('%d-%b-%Y')}**"
        )

        fig = create_duration_comparison_chart(merged_df, selected_date, reference_date)
        st.plotly_chart(fig, use_container_width=True)

        # Export table with arrows
        def format_delta_arrow(delta: float) -> str:
            symbol = "🔺" if delta > 0 else "🔻"
            color = "red" if delta > 0 else "green"
            return f'<span style="color:{color}">{symbol} {delta:.1f}</span>'

        export_df = merged_df[["type", "duration_current_min", "duration_ref_min", "delta"]].copy()
        export_df["delta"] = merged_df["delta"].apply(format_delta_arrow)

        styled_table = export_df.rename(
            columns={
                "type": "Job Type",
                "duration_current_min": f"Duration ({selected_date.strftime('%d-%b')})",
                "duration_ref_min": f"Duration ({reference_date.strftime('%d-%b')})",
                "delta": "Delta (mins)",
            }
        )
        st.markdown("### Export Table with Trend Arrows")
        st.write(styled_table.to_html(escape=False, index=False), unsafe_allow_html=True)

        raw_export_df = merged_df[["type", "duration_current_min", "duration_ref_min", "delta"]].rename(
            columns={
                "type": "Job Type",
                "duration_current_min": f"Duration ({selected_date.strftime('%d-%b')})",
                "duration_ref_min": f"Duration ({reference_date.strftime('%d-%b')})",
                "delta": "Delta (mins)",
            }
        )
        buffer = StringIO()
        raw_export_df.to_csv(buffer, index=False)
        st.download_button("Download CSV", buffer.getvalue(), file_name="duration_by_type_comparison.csv", mime="text/csv")

        # Drilldown
        st.markdown("---")
        st.subheader("Drilldown by ID for Selected Job Type")

        job_types = merged_df["type"].dropna().unique().tolist()
        selected_job_type = st.selectbox("Select a Job Type for Drilldown", job_types)
        if not selected_job_type:
            return

        combined_df = pd.concat([current_df, reference_df])
        combined_df = combined_df[combined_df["type"] == selected_job_type].copy()
        combined_df["run_date"] = pd.to_datetime(combined_df["run_date"]).dt.date
        combined_df["duration_minutes"] = combined_df["duration"] / 60

        drill_df = combined_df[combined_df["run_date"].isin([selected_date, reference_date])].copy()
        drill_df["timestamp"] = pd.to_datetime(drill_df["timestamp"], errors="coerce")
        drill_df = drill_df.sort_values("timestamp", ascending=False)
        drill_df = drill_df.drop_duplicates(subset="id", keep="first")

        pivot_df = (
            drill_df.pivot(index="id", columns="run_date", values="duration_minutes")
            .fillna(0)
            .reset_index()
        )

        config_map = drill_df.set_index("id")["config_count"]
        pivot_df["config_count"] = pivot_df["id"].map(config_map).fillna(0).astype(int)

        dur_current = pivot_df.get(selected_date, pd.Series(0))
        dur_ref = pivot_df.get(reference_date, pd.Series(0))
        pivot_df["delta"] = dur_current - dur_ref

        fig_bar = create_drilldown_chart(pivot_df, selected_date, reference_date, selected_job_type)
        st.plotly_chart(fig_bar, use_container_width=True)

        st.markdown("### Export Drilldown Data")
        drill_export_df = drill_df[["run_date", "id", "config_count", "duration"]].copy()
        drill_export_df["duration_minutes"] = drill_export_df["duration"] / 60
        drill_buffer = StringIO()
        drill_export_df.to_csv(drill_buffer, index=False)
        st.download_button("Download Drilldown CSV", drill_buffer.getvalue(), file_name="drilldown_by_id.csv", mime="text/csv")