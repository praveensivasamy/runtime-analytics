import pandas as pd

def sns_duration_by_riskdate(df: pd.DataFrame) -> pd.DataFrame:
    sns_df = df[df["type"] == "SNSI"]
    result = sns_df.groupby("riskdate").agg(
        run_count=("id", "count"),
        total_duration_sec=("duration_sec", "sum"),
        avg_duration_sec=("duration_sec", "mean"),
        total_config_count=("config_count", "sum")
    ).reset_index()
    return result

def job_count_by_type(df: pd.DataFrame) -> pd.DataFrame:
    run_counts = df.groupby("type").size().reset_index(name="run_count")
    config_sums = df.groupby("type")["config_count"].sum().reset_index(name="total_config_count")
    return pd.merge(run_counts, config_sums, on="type")

def stdv_long_jobs(df: pd.DataFrame) -> pd.DataFrame:
    return df[(df["type"] == "STDV") & (df["duration_sec"] > 300)]
