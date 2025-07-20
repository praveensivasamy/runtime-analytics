import logging
import sqlite3

import pandas as pd
import streamlit as st
from sklearn.metrics import mean_squared_error

from runtime_analytics.app_config.config import settings

logger = logging.getLogger(__name__)


def render(tab):
    with tab:
        logger.debug("Rendering Sequence & Drift Analysis tab...")
        st.subheader("Sequence & Drift Analysis")

        # Check if the trained model and prediction table exist
        db_path = settings.log_db_path
        model_path = settings.base_dir / "ml" / "pipeline" / "trained" / "duration_prediction_model.pkl"

        if not model_path.exists():
            st.warning("Prediction model not found. Please train the model first using the Admin tab.")
            return

        try:
            with sqlite3.connect(db_path) as conn:
                table_names = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
                if "job_logs_with_predictions" not in table_names["name"].values:
                    st.warning("Predicted results not found. Please run predictions from the Admin tab after training the model.")
                    return

                df_pred = pd.read_sql("SELECT * FROM job_logs_with_predictions", conn)

            df_pred["timestamp"] = pd.to_datetime(df_pred["timestamp"], format="%Y-%m-%d %H:%M:%S,%f", errors="coerce")

        except Exception as e:
            st.error(f"Could not load predictions: {e}")
            return

        st.markdown("#### Filter by Time Range")
        days = st.slider("Select number of days to include", min_value=1, max_value=90, value=30)
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
        df_filtered = df_pred[df_pred["timestamp"] >= cutoff]

        st.markdown("#### Sequence Anomalies")
        anomalies = df_filtered[df_filtered["sequence_diff"] != 1]
        if anomalies.empty:
            st.info("No skipped job sequences detected.")
        else:
            st.warning("Detected sequence skips:")
            st.dataframe(anomalies[["run_date", "job_order", "job_sequence", "sequence_diff"]])
            st.download_button("Download Anomalies CSV", data=anomalies.to_csv(index=False), file_name="sequence_anomalies.csv")

        st.markdown("#### Prediction Drift Over Time")
        st.line_chart(df_filtered.set_index("timestamp")[["duration", "predicted_duration"]].sort_index())

        st.markdown("#### Sequence Delta Trends (Last 50 Rows)")
        st.dataframe(
            df_filtered[
                [
                    "timestamp",
                    "job_order",
                    "job_sequence",
                    "duration",
                    "predicted_duration",
                    "duration_seq_delta",
                    "predicted_seq_delta",
                ]
            ].tail(50)
        )

        st.markdown("#### RMSE by Job Order")
        # Step 1: Group by 'job_order' and calculate RMSE for each group
        rmse_values = df_filtered.groupby("job_order").apply(
            lambda g: mean_squared_error(g["duration"], g["predicted_duration"])
        )  # Calculate RMSE

        # Step 2: Reset the index to convert the result into a DataFrame
        rmse_by_job = rmse_values.reset_index()

        # Step 3: Rename columns to make the result more readable
        rmse_by_job.columns = ["job_order", "rmse"]

        # Step 4: Sort the results by RMSE in descending order
        rmse_by_job_sorted = rmse_by_job.sort_values("rmse", ascending=False)

        # Now, `rmse_by_job_sorted` contains the job order and corresponding RMSE, sorted in descending order.

        st.bar_chart(rmse_by_job.set_index("job_order")["rmse"])