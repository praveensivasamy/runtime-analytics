from __future__ import annotations

import logging
import sqlite3

import pandas as pd

from runtime_analytics.app_config.config import settings

logger = logging.getLogger(__name__)

# All expected columns in the DB
EXPECTED_COLUMNS = [
    "riskdate",
    "id",
    "type",
    "timestamp",
    "run_date",
    "duration",
    "config_count",
    "job_id",
    "day",
    "month",
    "year",
    "week",
    "log_hour",
    "month_end",
    "quarter_end",
    "year_end",
    "job_count",
    "job_sequence",
    "job_run_count",
    "job_order",
]


def ensure_db_initialized(table_name: str = "job_logs"):
    """Ensure the table exists with correct schema."""
    with sqlite3.connect(settings.log_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                riskdate TEXT,
                id INTEGER,
                type TEXT,
                timestamp TEXT,
                run_date TEXT,
                duration REAL,
                config_count INTEGER,
                job_id TEXT,
                day TEXT,
                month TEXT,
                year INTEGER,
                week TEXT,
                log_hour INTEGER,
                month_end INTEGER,
                quarter_end INTEGER,
                year_end INTEGER,
                job_count INTEGER,
                job_sequence INTEGER,
                job_run_count INTEGER,
                job_order TEXT,
                PRIMARY KEY (riskdate, id, type, timestamp)
            )
            """
        )
        conn.commit()
    logger.info(f"Table '{table_name}' is initialized.")


def log_sql_queries(query: str, values: list):
    logger.info(f"Executing SQL: {query} | Values: {values[:5]}{'...' if len(values) > 5 else ''}")


def _to_python_type(val):
    if pd.isna(val):
        return None
    if isinstance(val, (pd.Timestamp, pd.Timedelta)):
        return str(val)
    if hasattr(val, "item"):
        return val.item()
    return val


def save_df_to_db(df: pd.DataFrame, table_name: str = "job_logs"):
    required_columns = {"riskdate", "id", "type", "timestamp"}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"Missing required columns: {missing}")

    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[EXPECTED_COLUMNS]

    df["row_key"] = (
        df["riskdate"].astype(str)
        + "::"
        + df["id"].astype(str)
        + "::"
        + df["type"].astype(str)
        + "::"
        + df["timestamp"].astype(str)
    )
    df = df.drop_duplicates(subset="row_key")

    columns = [col for col in df.columns if col != "row_key"]
    values = [tuple(_to_python_type(v) for v in row) for row in df[columns].itertuples(index=False)]

    logger.info(f"Inserting {len(values)} records into '{table_name}'")
    placeholders = ",".join("?" for _ in columns)
    insert_sql = f"INSERT OR IGNORE INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"

    with sqlite3.connect(settings.log_db_path) as conn:
        cursor = conn.cursor()
        for i in range(0, len(values), 5000):
            cursor.executemany(insert_sql, values[i : i + 5000])
        conn.commit()

    logger.info(f"{len(values)} records processed and saved to '{table_name}'")
