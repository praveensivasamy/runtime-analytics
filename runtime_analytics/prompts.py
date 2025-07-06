from runtime_analytics.services.analytics import (
    get_top_slow_jobs,
    aggregate_by_field,
    filter_jobs,
    job_count_by_type,
    prediction_accuracy_per_job_type,
    top_anomaly_scores,
    top_slow_jobs_grouped,
    unique_jobs_per_day,
)

from runtime_analytics.repositories import job_log_repository as repo


# Maps string function names to actual implementations for use in GUI, CLI, etc.
FUNCTION_MAP = {
    "get_top_slow_jobs": get_top_slow_jobs,
    "top_slow_jobs_grouped": top_slow_jobs_grouped,
    "job_count_by_type": job_count_by_type,
    "unique_jobs_per_day": unique_jobs_per_day,
    "aggregate_by_field": aggregate_by_field,
    "filter_jobs": filter_jobs,
    "prediction_accuracy_per_job_type": prediction_accuracy_per_job_type,
    "top_anomaly_scores": top_anomaly_scores,
}


# Predefined prompt entries
PREDEFINED_PROMPTS = {
    "Top slow jobs (yesterday)": {
        "data_source": repo.get_logs_for_yesterday,
        "function": get_top_slow_jobs,
        "params": {"days_offset": 1},
    },
    "Top Slow Jobs This Week": {
        "data_source": lambda: repo.get_logs_by_period("week"),
        "function": top_slow_jobs_grouped,
        "params": {"n": 10, "date_filter": "week"},
    },
    "Top Slow Jobs This Month": {
        "data_source": lambda: repo.get_logs_by_period("month"),
        "function": top_slow_jobs_grouped,
        "params": {"n": 10, "date_filter": "month"},
    },
    "Top Slow Jobs This Year": {
        "data_source": lambda: repo.get_logs_by_period("year"),
        "function": top_slow_jobs_grouped,
        "params": {"n": 10, "date_filter": "year"},
    },
    "Job Count by Type": {
        "data_source": repo.get_all_logs,
        "function": job_count_by_type,
        "params": {},
    },
    "Unique Jobs Per Day": {
        "data_source": repo.get_all_logs,
        "function": unique_jobs_per_day,
        "params": {},
    },
    "Average Duration by Type": {
        "data_source": repo.get_all_logs,
        "function": aggregate_by_field,
        "params": {"group_by": "type", "agg_field": "duration_sec", "operations": ["mean"]},
    },
    "Prediction Accuracy per Job Type": {
        "data_source": repo.get_all_logs,
        "function": prediction_accuracy_per_job_type,
        "params": {},
    },
    "Top Anomaly Scores": {
        "data_source": repo.get_all_logs,
        "function": top_anomaly_scores,
        "params": {"n": 10},
    },
}
