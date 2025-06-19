import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

from runtime_analytics.app_config.config import settings
from runtime_analytics.log_parser import parse_log_line
from runtime_analytics.app_db.db_operations import save_df_to_db


def extract_features(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure columns are properly converted
    df["riskdate"] = pd.to_datetime(df["riskdate"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["run_date"] = df["timestamp"].dt.strftime("%Y-%m-%d")

    df["day"] = df["timestamp"].dt.strftime("%a")
    df["month"] = df["timestamp"].dt.strftime("%B")
    df["year"] = df["timestamp"].dt.year
    df["week"] = df["timestamp"].dt.strftime("%U_%Y")
    df["log_hour"] = df["timestamp"].dt.hour
    df["month_end"] = df["timestamp"].dt.is_month_end.astype(int)
    df["quarter_end"] = df["timestamp"].dt.is_quarter_end.astype(int)
    df["year_end"] = df["timestamp"].dt.is_year_end.astype(int)

    df["job_id"] = df["riskdate"] + "_" + df["id"].astype(str) + "_" + df["type"].astype(str)
    df["job_count"] = df.groupby("run_date")["job_id"].transform("count")
    df["job_sequence"] = df.groupby(["run_date", "job_id"])["timestamp"].rank(method="first").astype(int)
    df["job_run_count"] = df.groupby(["run_date", "job_id"]).cumcount() + 1
    df["job_order"] = df["job_sequence"].astype(str) + " of " + df["job_count"].astype(str)

    return df


def load_logs_from_folder(folder_path: str, save_to_db: bool = True) -> pd.DataFrame:
    log_dir = Path(folder_path)
    all_lines: list[dict] = []

    for file in log_dir.glob("*.txt"):
        with file.open("r", encoding="utf-8") as f:
            lines = f.readlines()
        parsed = [parse_log_line(line) for line in lines if line.strip()]
        all_lines.extend(filter(None, parsed))

    if not all_lines:
        return pd.DataFrame()

    df = pd.DataFrame(all_lines)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S,%f", errors="coerce")
    df["riskdate"] = pd.to_datetime(df["riskdate"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["run_date"] = pd.to_datetime(df["run_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["config_count"] = pd.to_numeric(df["config_count"], errors="coerce").astype("Int64")
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["duration"] = pd.to_numeric(df["duration"], errors="coerce").astype("Int64")

    df = extract_features(df)

    if save_to_db:
        save_df_to_db(df, if_exists="append")

    return df
