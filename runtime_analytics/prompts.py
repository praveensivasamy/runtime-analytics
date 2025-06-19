from runtime_analytics.analyzer import (
    aggregate_by_field,
    filter_jobs,
    job_count_by_type,
    prediction_accuracy_per_job_type,
    top_anomaly_scores,
    top_slow_jobs_grouped,
    unique_jobs_per_day,
)


# Maps string function names to actual implementations for use in GUI, CLI, etc.
FUNCTION_MAP = {
    "top_slow_jobs_grouped": top_slow_jobs_grouped,
    "job_count_by_type": job_count_by_type,
    "unique_jobs_per_day": unique_jobs_per_day,
    "aggregate_by_field": aggregate_by_field,
    "filter_jobs": filter_jobs,
    "prediction_accuracy_per_job_type": prediction_accuracy_per_job_type,
    "top_anomaly_scores": top_anomaly_scores,
}

# Optionally, you can define some static, human-friendly prompts for the sidebar menu
PREDEFINED_PROMPTS = {
    "Top Slow Jobs This Week": {
        "function": top_slow_jobs_grouped,
        "params": {"n": 10, "date_filter": "week"},
    },
    "Top Slow Jobs This Month": {
        "function": top_slow_jobs_grouped,
        "params": {"n": 10, "date_filter": "month"},
    },
    "Top Slow Jobs This Year": {
        "function": top_slow_jobs_grouped,
        "params": {"n": 10, "date_filter": "year"},
    },
    "Job Count by Type": {"function": job_count_by_type, "params": {}},
    "Unique Jobs Per Day": {"function": unique_jobs_per_day, "params": {}},
    "Average Duration by Type": {
        "function": aggregate_by_field,
        "params": {"group_by": "type", "agg_field": "duration_sec", "operations": ["mean"]},
    },
    "Prediction Accuracy per Job Type": {
        "function": prediction_accuracy_per_job_type,
        "params": {},
    },
    "Top Anomaly Scores": {
        "function": top_anomaly_scores,
        "params": {
            "n": 10,
        },
    },
}
