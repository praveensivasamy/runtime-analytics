import sqlite3
import pandas as pd
from typing import Optional

from runtime_analytics.app_config.config import settings
from runtime_analytics.loader import load_logs_from_folder
from runtime_analytics.app_db.db_operations import save_df_to_db, ensure_db_initialized


def create_indexes(table_name: str = "job_logs") -> None:
    with sqlite3.connect(settings.log_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_type ON {table_name} (type);")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_riskdate ON {table_name} (riskdate);")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_id_type_riskdate ON {table_name} (id, type, riskdate);")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_job_id ON {table_name} (job_id);")
        conn.commit()


def load_df_from_db(table_name: str = "job_logs") -> pd.DataFrame:
    ensure_db_initialized(table_name)
    with sqlite3.connect(settings.log_db_path) as conn:
        return pd.read_sql(f"SELECT * FROM {table_name}", conn)


def init_or_update_db(force_refresh: bool = False) -> None:
    table_name = "logs"
    ensure_db_initialized(table_name)

    latest_ts: Optional[pd.Timestamp] = None
    if not force_refresh:
        with sqlite3.connect(settings.log_db_path) as conn:
            result = conn.execute(f"SELECT MAX(timestamp) FROM {table_name}").fetchone()
            if result and result[0]:
                latest_ts = pd.to_datetime(result[0])

    # Load logs from the folder
    df = load_logs_from_folder(settings.log_dir, save_to_db=False)

    if df.empty:
        print("No logs found.")
        return

    # Coerce errors while converting to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    if not force_refresh and latest_ts is not None:
        # Filter out logs already present in the database (based on timestamp)
        df = df[df["timestamp"] > latest_ts]

    if df.empty:
        print("No new log entries to insert.")
        return

    # Save the new logs to the database (with duplicates removed)
    save_df_to_db(df, if_exists="append")
    create_indexes()
    print(f"{len(df)} new rows inserted {'(full refresh)' if force_refresh else '(filtered by timestamp)'}.")
