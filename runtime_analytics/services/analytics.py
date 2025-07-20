from typing import Any

import pandas as pd

from runtime_analytics.repositories.job_log_repository import get_all_logs, get_logs_by_period, get_logs_for_time_range
from runtime_analytics.utils.filters import apply_filters


def aggregate_by_field(df: pd.DataFrame, params: dict[str, Any] | None = None) -> pd.DataFrame:
    if params is None:
        params = {}
    group_by = params.get("group_by", "type")
    agg_field = params.get("agg_field", "duration")
    operations = params.get("operations", ["mean"])

    agg_funcs = {f"{agg_field}_{op}": (agg_field, op) for op in operations}
    return df.groupby(group_by).agg(**agg_funcs).reset_index()


def job_count_by_type(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby("type").agg(run_count=("id", "count"), total_config_count=("config_count", "sum")).reset_index()


def filter_jobs(df: pd.DataFrame, params: dict[str, Any] | None = None) -> pd.DataFrame:
    if params is None or not params.get("filters"):
        return df
    filters = params["filters"]
    query_parts = [f"{k} == {v!r}" for k, v in filters.items()]
    return df.query(" & ".join(query_parts))


def select_jobs_by_metric_rank(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    Rank jobs by duration with a domain-aware tiebreaker:
    - Ascending (fast jobs): higher config_count is better
    - Descending (slow jobs): lower config_count is worse
    """
    n = params.get("n", 10)
    metric = params.get("metric", "duration")
    ascending = params.get("ascending", True)

    if metric not in df.columns or "job_id" not in df.columns or "config_count" not in df.columns:
        raise ValueError("Missing required column(s): job_id, metric, or config_count")

    df_unique = df.drop_duplicates(subset="job_id").copy()

    # Determine tie-breaker sort direction based on ascending
    # Fast jobs: tie-break with config_count DESC (higher is better)
    # Slow jobs: tie-break with config_count ASC (lower is worse)
    tiebreaker_order = False if ascending else True

    df_sorted = df_unique.sort_values(by=[metric, "config_count"], ascending=[ascending, tiebreaker_order]).reset_index(drop=True)

    df_sorted.insert(0, "rank", range(1, len(df_sorted) + 1))
    return df_sorted.head(n)


def unique_jobs_per_day(
    df: pd.DataFrame,
) -> pd.DataFrame:
    return df.groupby(["run_date", "id"]).size().reset_index(name="count")


def prediction_accuracy_per_job_type(
    df: pd.DataFrame,
) -> pd.DataFrame:
    if "predicted_duration" not in df.columns or "duration" not in df.columns:
        return pd.DataFrame(columns=["type", "total_runs", "avg_absolute_error", "avg_relative_error"])

    df = df.dropna(subset=["predicted_duration", "duration"])
    df["abs_error"] = (df["predicted_duration"] - df["duration"]).abs()
    df["rel_error"] = df["abs_error"] / (df["duration"] + 1e-5)

    return (
        df.groupby("type")
        .agg(
            total_runs=("id", "count"),
            avg_absolute_error=("abs_error", "mean"),
            avg_relative_error=("rel_error", "mean"),
        )
        .reset_index()
    )


def top_anomaly_scores(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    if "anomaly_score" not in df.columns:
        return pd.DataFrame(
            columns=[
                "riskdate",
                "id",
                "type",
                "anomaly_score",
                "run_timestamp",
                "config_count",
                "run_id",
            ]
        )

    df["run_group"] = df["riskdate"].astype(str) + "_" + df["id"].astype(str) + "_" + df["type"].astype(str)
    df["run_number"] = df.groupby("run_group")["run_timestamp"].rank("first").astype(int)
    df["total_runs"] = df.groupby("run_group")["run_timestamp"].transform("count")

    top_groups = df.groupby("run_group")["anomaly_score"].max().sort_values(ascending=False).head(n).index
    df_top = df[df["run_group"].isin(top_groups)].copy()
    df_top["run_id"] = df_top["run_number"].astype(str) + " of " + df_top["total_runs"].astype(str)

    return df_top[
        [
            "riskdate",
            "id",
            "type",
            "anomaly_score",
            "run_timestamp",
            "config_count",
            "run_id",
        ]
    ].sort_values(by="anomaly_score", ascending=False)