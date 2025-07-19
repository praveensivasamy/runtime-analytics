import pandas as pd


def aggregate_by_field(df: pd.DataFrame, group_by: str, agg_field: str, operations: list[str], **kwargs) -> pd.DataFrame:
    agg_funcs = {f"{agg_field}_{op}": (agg_field, op) for op in operations}
    return df.groupby(group_by).agg(**agg_funcs).reset_index()


def job_count_by_type(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return df.groupby("type").agg(run_count=("id", "count"), total_config_count=("config_count", "sum")).reset_index()


def filter_jobs(df: pd.DataFrame, filters: dict[str, object] | None = None, **kwargs) -> pd.DataFrame:
    if not filters:
        return df
    query_parts = [f"{k} == {v!r}" for k, v in filters.items()]
    return df.query(" & ".join(query_parts))


def get_top_slow_jobs(df: pd.DataFrame, top_n: int = 10, **kwargs) -> pd.DataFrame:
    return df.sort_values(by="duration", ascending=False).head(top_n)


def top_slow_jobs_grouped(df: pd.DataFrame, n: int = 10, **kwargs) -> pd.DataFrame:
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    top_jobs = (
        df.groupby(["job_id", "timestamp"], as_index=False)["duration"].max().sort_values(by="duration", ascending=False).head(n)
    )
    top_jobs["rank"] = range(1, len(top_jobs) + 1)

    df_top = df.merge(
        top_jobs[["job_id", "timestamp", "rank"]],
        on=["job_id", "timestamp"],
        how="inner",
    )

    df_top["month"] = df_top["timestamp"].dt.strftime("%B")
    df_top["week"] = df_top["timestamp"].dt.strftime("%U_%Y")
    df_top["week_number"] = df_top["timestamp"].dt.isocalendar().week
    df_top["year"] = df_top["timestamp"].dt.year
    df_top["day"] = df_top["timestamp"].dt.strftime("%a")

    if "predicted_duration" not in df_top.columns:
        df_top["predicted_duration"] = None

    return df_top[
        [
            "rank",
            "timestamp",
            "riskdate",
            "id",
            "type",
            "predicted_duration",
            "duration",
            "config_count",
            "job_order",
            "job_run_count",
            "job_sequence",
            "job_count",
            "day",
            "month",
            "week",
            "year",
            "week_number",
        ]
    ].sort_values(by=["rank", "timestamp"])


def unique_jobs_per_day(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return df.groupby(["run_date", "id"]).size().reset_index(name="count")


def prediction_accuracy_per_job_type(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
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


def top_anomaly_scores(df: pd.DataFrame, n: int = 10, **kwargs) -> pd.DataFrame:
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
