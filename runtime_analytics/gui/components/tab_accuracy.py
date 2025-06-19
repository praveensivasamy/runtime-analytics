import sqlite3
import streamlit as st
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt

from runtime_analytics.app_config.config import settings


def render(tab):
    with tab:
        st.subheader("Prediction Accuracy")

        db_path = settings.log_db_path
        model_path = settings.base_dir / "ml" / "pipeline" / "trained" / "duration_prediction_model.pkl"

        if not model_path.exists():
            st.warning("Prediction model not found. Please train the model using the Admin tab.")
            return

        try:
            with sqlite3.connect(db_path) as conn:
                table_names = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
                if "job_logs_with_predictions" not in table_names["name"].values:
                    st.warning("Predictions table not found. Please run predictions from the Admin tab.")
                    return

                df = pd.read_sql("SELECT * FROM job_logs_with_predictions", conn)

            df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S,%f", errors="coerce")

        except Exception as e:
            st.error(f"Error loading prediction data: {e}")
            return

        st.markdown("### Evaluation Metrics")

        y_true = df["duration"]
        y_pred = df["predicted_duration"]

        rmse = mean_squared_error(y_true, y_pred, squared=False)
        mae = mean_absolute_error(y_true, y_pred)
        r2 = r2_score(y_true, y_pred)

        st.metric("RMSE", f"{rmse:.2f} seconds")
        st.metric("MAE", f"{mae:.2f} seconds")
        st.metric("R² Score", f"{r2:.4f}")

        st.markdown("### Actual vs Predicted Duration Scatter Plot")

        fig1, ax1 = plt.subplots()
        ax1.scatter(y_true, y_pred, alpha=0.4)
        ax1.plot([y_true.min(), y_true.max()], [y_true.min(), y_true.max()], color="red", linestyle="--")
        ax1.set_xlabel("Actual Duration")
        ax1.set_ylabel("Predicted Duration")
        ax1.set_title("Actual vs Predicted")
        st.pyplot(fig1)

        st.markdown("### Residual Plot")

        residuals = y_true - y_pred
        fig2, ax2 = plt.subplots()
        ax2.hist(residuals, bins=30, edgecolor="black")
        ax2.set_title("Distribution of Residuals")
        ax2.set_xlabel("Residual (Actual - Predicted)")
        st.pyplot(fig2)
