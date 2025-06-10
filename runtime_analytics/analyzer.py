from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

def aggregate_by_field(
    df: pd.DataFrame,
    group_by: str,
    agg_field: str,
    operations: List[str]
) -> pd.DataFrame:
    agg_funcs = {f"{agg_field}_{op}": (agg_field, op) for op in operations}
    result = df.groupby(group_by).agg(**agg_funcs).reset_index()
    return result


def job_count_by_type(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby("type").agg(
        run_count=("id", "count"),
        total_config_count=("config_count", "sum")
    ).reset_index()


def filter_jobs(
    df: pd.DataFrame,
    filters: Optional[Dict[str, any]] = None
) -> pd.DataFrame:
    if not filters:
        return df
    query_parts = [f"{k} == {repr(v)}" for k, v in filters.items()]
    query_str = " & ".join(query_parts)
    return df.query(query_str)




def top_slow_jobs_grouped(
    df: pd.DataFrame,
    n: int = 10,
    date_filter: str = None,
    start_date: str = None,
    end_date: str = None
) -> pd.DataFrame:
    df = df.copy()

    # Convert timestamp and riskdate
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["run_timestamp"] = df["timestamp"]
    df["riskdate"] = pd.to_datetime(df["riskdate"])

    # Filter by timestamp (actual run time)
    now = datetime.now()
    if date_filter == "week":
        start_date = now - timedelta(days=now.weekday())
    elif date_filter == "month":
        start_date = now.replace(day=1)
    elif date_filter == "year":
        start_date = now.replace(month=1, day=1)

    if start_date:
        df = df[df["run_timestamp"] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df["run_timestamp"] <= pd.to_datetime(end_date)]

    # Define job group by job keys
    df["run_group"] = (
        df["riskdate"].astype(str).str.strip() + "_" +
        df["id"].astype(str).str.strip() + "_" +
        df["type"].astype(str).str.strip()
    )

    # Compute max duration per group and assign rank
    group_durations = df.groupby("run_group")["duration_sec"].max()
    top_groups = group_durations.sort_values(ascending=False).head(n).reset_index()
    top_groups["rank"] = top_groups.index + 1

    # Filter top jobs
    df_top = df[df["run_group"].isin(top_groups["run_group"])].copy()

    # Add run count and run_id (e.g., 2 of 5)
    df_top["run_number"] = df_top.groupby("run_group")["run_timestamp"].rank(method="first").astype(int)
    df_top["total_runs"] = df_top.groupby("run_group")["run_group"].transform("count")
    df_top["run_id"] = df_top["run_number"].astype(str) + " of " + df_top["total_runs"].astype(str)

    # Merge back the rank
    df_top = df_top.merge(top_groups[["run_group", "rank"]], on="run_group", how="left")

    # Optional: predicted_duration may not exist
    if "predicted_duration" not in df_top.columns:
        df_top["predicted_duration"] = None

    # Final result
    return df_top[
        ["rank", "riskdate", "id", "type", "predicted_duration",
         "duration_sec", "run_timestamp", "config_count", "run_id"]
    ].sort_values(by=["rank", "run_timestamp"])



def unique_jobs_per_day(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(["run_date", "id"]).size().reset_index(name="count")

def prediction_accuracy_per_job_type(df: pd.DataFrame) -> pd.DataFrame:
    if "predicted_duration" not in df.columns or "duration_sec" not in df.columns:
        return pd.DataFrame(columns=["type", "total_runs", "avg_absolute_error", "avg_relative_error"])

    df = df.dropna(subset=["predicted_duration", "duration_sec"])

    return df.groupby("type").agg(
        total_runs=("id", "count"),
        avg_absolute_error=("predicted_duration", lambda x: (x - df.loc[x.index, "duration_sec"]).abs().mean()),
        avg_relative_error=("predicted_duration", lambda x: ((x - df.loc[x.index, "duration_sec"]).abs() / (df.loc[x.index, "duration_sec"] + 1e-5)).mean())
    ).reset_index()

def top_anomaly_scores(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    if "anomaly_score" not in df.columns:
        return pd.DataFrame(columns=["riskdate", "id", "type", "anomaly_score", "run_timestamp", "config_count", "run_id"])

    # Create run groups
    df["run_group"] = df["riskdate"].astype(str) + "_" + df["id"].astype(str) + "_" + df["type"].astype(str)

    # Add run number and total
    df["run_number"] = df.groupby("run_group")["run_timestamp"].rank("first").astype(int)
    df["total_runs"] = df.groupby("run_group")["run_timestamp"].transform("count")

    # Pick top jobs by highest anomaly score (group-level)
    top_groups = (
        df.groupby("run_group")["anomaly_score"]
        .max()
        .sort_values(ascending=False)
        .head(n)
        .index
    )

    df_top = df[df["run_group"].isin(top_groups)].copy()
    df_top["run_id"] = df_top["run_number"].astype(str) + " of " + df_top["total_runs"].astype(str)

    return df_top[
        ["riskdate", "id", "type", "anomaly_score", "run_timestamp", "config_count", "run_id"]
    ].sort_values(by=["anomaly_score"], ascending=False)
