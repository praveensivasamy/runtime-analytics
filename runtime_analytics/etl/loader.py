from __future__ import annotations

import logging
import shutil
from pathlib import Path

import pandas as pd

from runtime_analytics.app_db.db_operations import save_df_to_db
from runtime_analytics.etl.log_parser import parse_log_line

logger = logging.getLogger(__name__)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S,%f", errors="coerce")
    df["riskdate"] = pd.to_datetime(df["riskdate"], errors="coerce")
    df["run_date"] = pd.to_datetime(df["run_date"], errors="coerce")

    for col in ["config_count", "id", "duration"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df


def extract_features(df: pd.DataFrame) -> pd.DataFrame:
    df["day"] = df["timestamp"].dt.strftime("%a")
    df["month"] = df["timestamp"].dt.strftime("%B")
    df["year"] = df["timestamp"].dt.year
    df["week"] = df["timestamp"].dt.strftime("%U_%Y")
    df["log_hour"] = df["timestamp"].dt.hour
    df["month_end"] = df["timestamp"].dt.is_month_end.astype(int)
    df["quarter_end"] = df["timestamp"].dt.is_quarter_end.astype(int)
    df["year_end"] = df["timestamp"].dt.is_year_end.astype(int)

    df["job_id"] = df["riskdate"].dt.strftime("%Y-%m-%d") + "_" + df["id"].astype(str) + "_" + df["type"].astype(str)
    df["job_count"] = df.groupby("run_date")["job_id"].transform("count")
    df["job_sequence"] = df.groupby(["run_date", "job_id"])["timestamp"].rank(method="first").astype(int)
    df["job_run_count"] = df.groupby(["run_date", "job_id"]).cumcount() + 1
    df["job_order"] = df["job_sequence"].astype(str) + " of " + df["job_count"].astype(str)

    return df


def load_logs_from_folder(folder_path: str, save_to_db: bool = True) -> pd.DataFrame:
    log_dir = Path(folder_path)
    loaded_dir = log_dir / "loaded"
    loaded_dir.mkdir(exist_ok=True)

    all_lines: list[dict] = []
    files_to_move: list[Path] = []

    for file in log_dir.glob("*.txt"):
        try:
            with file.open("r", encoding="utf-8") as f:
                lines = f.readlines()
            parsed = [parse_log_line(line) for line in lines if line.strip()]
            parsed = list(filter(None, parsed))

            if parsed:
                all_lines.extend(parsed)
                # only move successfully parsed files
                files_to_move.append(file)
            else:
                logger.warning(f"No valid lines found in {file.name}, skipping move.")
        except Exception as e:
            logger.warning(f"Failed to parse {file.name}: {e}")

    if not all_lines:
        logger.warning("No valid log lines found in folder.")
        return pd.DataFrame()

    df = pd.DataFrame(all_lines)
    df = normalize_columns(df)
    df = extract_features(df)

    if save_to_db:
        save_df_to_db(df, if_exists="append")

    # Move successfully processed files
    for file in files_to_move:
        try:
            shutil.move(str(file), loaded_dir / file.name)
            logger.info(f"Moved {file.name} to {loaded_dir}")
        except Exception as e:
            logger.error(f"Failed to move {file.name}: {e}")

    return df
