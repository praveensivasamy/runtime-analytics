import sqlite3
from pathlib import Path
import pandas as pd
from runtime_analytics.log_parser import parse_log_line
from runtime_analytics.app_config.config import settings

def extract_features(df: pd.DataFrame) -> pd.DataFrame:
    df["run_timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["run_date"] = df["run_timestamp"].dt.date
    df["exec_day_of_week"] = df["run_timestamp"].dt.dayofweek
    df["log_hour"] = df["run_timestamp"].dt.hour
    df["weekend"] = df["exec_day_of_week"].isin([5, 6]).astype(int)
    df["month_end"] = df["run_timestamp"].dt.is_month_end.astype(int)
    df["quarter_end"] = df["run_timestamp"].dt.is_quarter_end.astype(int)
    df["year_end"] = df["run_timestamp"].dt.is_year_end.astype(int)
    df["job_key"] = df["riskdate"].astype(str) + "_" + df["id"].astype(str) + "_" + df["type"].astype(str)
    run_count_df = df.groupby("run_date")["job_key"].nunique().rename("run_count")
    df = df.merge(run_count_df, on="run_date", how="left")

    re_run_df = df.groupby(["run_date", "job_key"]).size().rename("re_run_count").reset_index()
    df = df.merge(re_run_df, on=["run_date", "job_key"], how="left")

    return df

def load_logs(file_path: str, save_to_db=True, db_path=settings.log_db_path) -> pd.DataFrame:
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    parsed = [parse_log_line(line) for line in lines if line.strip()]
    parsed = [p for p in parsed if p]

    df = pd.DataFrame(parsed)
    if df.empty:
        return df

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["riskdate"] = pd.to_datetime(df["riskdate"]).dt.date
    df["run_date"] = pd.to_datetime(df["run_date"]).dt.date
    df["config_count"] = pd.to_numeric(df["config_count"], errors="coerce").astype("Int64")
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["duration_sec"] = pd.to_numeric(df["duration_sec"], errors="coerce").astype("Int64")

    df = extract_features(df)

    if save_to_db:
        with sqlite3.connect(db_path) as conn:
            df.to_sql("job_logs", conn, if_exists="append", index=False)

    return df

def load_logs_from_folder(folder_path: str, save_to_db=True, db_path=settings.log_db_path) -> pd.DataFrame:
    log_dir = Path(folder_path)
    all_lines = []

    for file in log_dir.glob("*.txt"):
        with file.open("r", encoding="utf-8") as f:
            lines = f.readlines()
        parsed = [parse_log_line(line) for line in lines if line.strip()]
        all_lines.extend(filter(None, parsed))

    if not all_lines:
        return pd.DataFrame()

    df = pd.DataFrame(all_lines)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["riskdate"] = pd.to_datetime(df["riskdate"]).dt.date
    df["run_date"] = pd.to_datetime(df["run_date"]).dt.date
    df["config_count"] = pd.to_numeric(df["config_count"], errors="coerce").astype("Int64")
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["duration_sec"] = pd.to_numeric(df["duration_sec"], errors="coerce").astype("Int64")

    df = extract_features(df)

    if save_to_db:
        with sqlite3.connect(db_path) as conn:
            df.to_sql("job_logs", conn, if_exists="append", index=False)

    return df
