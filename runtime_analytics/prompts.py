from __future__ import annotations

from runtime_analytics.repositories import job_log_repository as repo
from runtime_analytics.repositories.job_log_repository import fetch_data_for_prompt
from runtime_analytics.services.analytics import (
    aggregate_by_field,
    filter_jobs,
    job_count_by_type,
    prediction_accuracy_per_job_type,
    select_jobs_by_metric_rank,
    top_anomaly_scores,
    unique_jobs_per_day,
)

# Function registry (optional use)
FUNCTION_MAP = {
    "select_jobs_by_metric_rank": select_jobs_by_metric_rank,
    "job_count_by_type": job_count_by_type,
    "unique_jobs_per_day": unique_jobs_per_day,
    "aggregate_by_field": aggregate_by_field,
    "filter_jobs": filter_jobs,
    "prediction_accuracy_per_job_type": prediction_accuracy_per_job_type,
    "top_anomaly_scores": top_anomaly_scores,
}

# Predefined Prompts
PREDEFINED_PROMPTS = {
    "Top slow jobs (yesterday)": {
        "data_source": lambda params: repo.get_logs_by_period("yesterday"),
        "function": select_jobs_by_metric_rank,
        "params": {"n": 10, "metric": "duration", "ascending": False},
    },
    "Top Slow Jobs This Week": {
        "data_source": lambda params: repo.get_logs_by_period("week"),
        "function": select_jobs_by_metric_rank,
        "params": {"n": 10, "metric": "duration", "ascending": False},
    },
    "Top Slow Jobs This Month": {
        "data_source": lambda params: repo.get_logs_by_period("month"),
        "function": select_jobs_by_metric_rank,
        "params": {"n": 10, "metric": "duration", "ascending": False},
    },
    "Top Slow Jobs This Year": {
        "data_source": lambda params: repo.get_logs_by_period("year"),
        "function": select_jobs_by_metric_rank,
        "params": {"n": 10, "metric": "duration", "ascending": False},
    },
    "Top Slow Jobs for Date Range": {
        "data_source": fetch_data_for_prompt,
        "function": select_jobs_by_metric_rank,
        "params": {
            "n": 10,
            "metric": "duration",
            "ascending": False,
            "date_filter": "custom_range",
        },
    },
    "Job Count by Type": {
        "data_source": lambda params: repo.get_all_logs(),
        "function": job_count_by_type,
        "params": {},
    },
    "Unique Jobs Per Day": {
        "data_source": lambda params: repo.get_all_logs(),
        "function": unique_jobs_per_day,
        "params": {},
    },
    "Average Duration by Type": {
        "data_source": lambda params: repo.get_all_logs(),
        "function": aggregate_by_field,
        "params": {
            "group_by": "type",
            "agg_field": "duration_sec",
            "operations": ["mean"],
        },
    },
    "Prediction Accuracy per Job Type": {
        "data_source": lambda params: repo.get_all_logs(),
        "function": prediction_accuracy_per_job_type,
        "params": {},
    },
    "Top Anomaly Scores": {
        "data_source": lambda params: repo.get_all_logs(),
        "function": top_anomaly_scores,
        "params": {"n": 10},
    },
}