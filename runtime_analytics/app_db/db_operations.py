# File: runtime_analytics/app_db/db_operations.py

import sqlite3
import pandas as pd
from runtime_analytics.app_config.config import settings


def ensure_db_initialized(table_name: str = "job_logs") -> None:
    with sqlite3.connect(settings.log_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                riskdate TEXT,
                id INTEGER,
                type TEXT,
                config_count INTEGER,
                run_date TEXT,
                duration INTEGER,
                timestamp TEXT,
                day TEXT,
                month TEXT,
                year INTEGER,
                week TEXT,
                log_hour INTEGER,
                month_end INTEGER,
                quarter_end INTEGER,
                year_end INTEGER,
                job_id TEXT,
                job_count INTEGER,
                job_sequence INTEGER,
                job_run_count INTEGER,
                job_order TEXT,
                PRIMARY KEY (riskdate, id, type, timestamp)  -- Unique Key Combination
            )
            """
        )
        conn.commit()


def save_df_to_db(df: pd.DataFrame, table_name: str = "job_logs", if_exists: str = "append", batch_size: int = 5000) -> None:
    ensure_db_initialized(table_name)

    required_cols = {
        "riskdate",
        "id",
        "type",
        "config_count",
        "run_date",
        "duration",
        "timestamp",
        "day",
        "month",
        "year",
        "week",
        "log_hour",
        "month_end",
        "quarter_end",
        "year_end",
        "job_id",
        "job_count",
        "job_sequence",
        "job_run_count",
        "job_order",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    with sqlite3.connect(settings.log_db_path) as conn:
        log_sql_queries(conn)

        # Step 1: Check if logs already exist in the database (using unique keys: riskdate, id, type, timestamp)
        if if_exists == "append":
            existing = pd.read_sql(f"SELECT riskdate, id, type, timestamp FROM {table_name}", conn)

            if not existing.empty:
                # Ensure both 'timestamp' columns are in the same format
                existing["timestamp"] = pd.to_datetime(existing["timestamp"], format="%Y-%m-%d %H:%M:%S,%f", errors="coerce")
                df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S,%f", errors="coerce")

                # Cast 'riskdate' to string for consistency
                existing["riskdate"] = existing["riskdate"].astype(str)
                df["riskdate"] = df["riskdate"].astype(str)

                # Merge new data with existing data on unique keys (riskdate, id, type, timestamp)
                df = df.merge(existing, on=["riskdate", "id", "type", "timestamp"], how="left", indicator=True)

                # Keep only the new rows (those that are not already in the database)
                df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])

                # If no new rows to insert, print and exit
                if df.empty:
                    print("No new logs to insert.")
                    return

        # Step 2: Insert new logs in batches
        if not df.empty:
            for start in range(0, len(df), batch_size):
                batch = df.iloc[start : start + batch_size]
                try:
                    # Insert the batch into the database
                    batch.to_sql(table_name, conn, if_exists=if_exists, index=False)
                    print(f"Inserted batch {start // batch_size + 1} of {len(df) // batch_size + 1} batches.")
                except sqlite3.IntegrityError as e:
                    print(f"Error inserting batch {start // batch_size + 1}: {e}")
                    continue  # Skip this batch and continue with the next one


def log_sql_queries(conn):
    # Enable SQL query logging in SQLite (prints all SQL queries being executed)
    conn.set_trace_callback(print)  # This will print every SQL query executed
    return conn
