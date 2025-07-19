from __future__ import annotations

import sqlite3

import pandas as pd
from loguru import logger

from runtime_analytics.app_config.config import settings


def ensure_db_initialized(table_name: str = "job_logs"):
    """Ensure the table exists with correct schema."""
    with sqlite3.connect(settings.log_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                riskdate TEXT,
                id TEXT,
                type TEXT,
                timestamp TEXT,
                duration REAL,
                config_count INTEGER,
                job_id TEXT,
                PRIMARY KEY (riskdate, id, type, timestamp)
            )
        """
        )
        conn.commit()
    logger.info(f"Table '{table_name}' is initialized.")


def log_sql_queries(query: str, values: list):
    logger.debug(f"Executing SQL: {query} | Values: {values[:5]}{'...' if len(values) > 5 else ''}")


def save_df_to_db(df: pd.DataFrame, table_name: str = "job_logs", if_exists: str = "append"):
    required_columns = {"riskdate", "id", "type", "timestamp"}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"Missing required columns: {missing}")

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
    records = df[columns].to_records(index=False)
    values = list(records)

    logger.info(f"Inserting {len(values)} records into '{table_name}'")

    placeholders = ",".join("?" for _ in columns)
    insert_sql = f"INSERT OR IGNORE INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"

    log_sql_queries(insert_sql, values)

    with sqlite3.connect(settings.log_db_path) as conn:
        cursor = conn.cursor()
        for i in range(0, len(values), 5000):
            batch = values[i : i + 5000]
            cursor.executemany(insert_sql, batch)
        conn.commit()

    logger.success(f"{len(values)} records processed and saved to '{table_name}'")
