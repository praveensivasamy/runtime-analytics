from __future__ import annotations

import time

import streamlit as st

from runtime_analytics.app_config.config import settings
from runtime_analytics.ml.pipeline.predict_duration import predict_response_times
from runtime_analytics.ml.pipeline.train_duration_prediction import train_pipeline_model


def render(tab):
    with tab:
        st.subheader("Admin Tools")
        st.markdown(
            "Use these controls to manage training, prediction, and prompt data.")
        if st.button("Regenerate Training Prompts CSV"):
            with st.spinner("Regenerating training prompts..."):
                from runtime_analytics.scripts.generate_training_csv import generate_training_prompts_csv

                output_path = settings.resource_dir / "training_prompts.csv"
                generate_training_prompts_csv(output_path)
                st.success("`training_prompts.csv` has been regenerated.")

        if st.button("Train Prompt Model"):
            with st.spinner("Training prompt model..."):
                from runtime_analytics.scripts.train_prompt_model_cli import main as train_prompt_model

                train_prompt_model()
                st.success("✅ Prompt model training complete.")

        if st.button("Train Duration Model"):
            start = time.time()
            with st.spinner("Training duration prediction model..."):
                train_pipeline_model()
            elapsed = time.time() - start
            st.success(f"✅ Duration model retrained in {elapsed:.2f} seconds.")

        if st.button("Run Predictions"):
            start = time.time()
            with st.spinner("Generating predictions from latest data..."):
                predict_response_times()
            elapsed = time.time() - start
            st.success(
                f"✅ Predictions updated in 'job_logs_with_predictions' in {elapsed:.2f} seconds.")
