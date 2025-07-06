import sqlite3
import pandas as pd
from typing import Optional, Dict, Any

from loguru import logger
from runtime_analytics.app_config.config import settings
from runtime_analytics.etl.loader import load_logs_from_folder
from runtime_analytics.app_db.db_operations import save_df_to_db, ensure_db_initialized


def create_indexes(table_name: str = "job_logs") -> None:
    logger.info(f"Creating indexes on table: {table_name}")
    with sqlite3.connect(settings.log_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_type ON {table_name} (type);")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_riskdate ON {table_name} (riskdate);")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_id_type_riskdate ON {table_name} (id, type, riskdate);")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_job_id ON {table_name} (job_id);")
        conn.commit()
    logger.info("Index creation complete.")


def load_df_from_db(table_name: str = "job_logs", filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Load data from the database table with optional SQL-level filters.

    Supported filter formats:
    - {"type": "DAILY"}
    - {"run_date": ["2024-07-01", "2024-07-02"]}
    """
    ensure_db_initialized(table_name)
    logger.info(f"Loading data from table: {table_name} with filters: {filters}")

    where_clauses = []
    values = []

    if filters:
        for key, val in filters.items():
            if isinstance(val, list):
                placeholders = ",".join(["?"] * len(val))
                where_clauses.append(f"{key} IN ({placeholders})")
                values.extend(val)
            else:
                where_clauses.append(f"{key} = ?")
                values.append(val)

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    query = f"SELECT * FROM {table_name} {where_sql}"

    with sqlite3.connect(settings.log_db_path) as conn:
        df = pd.read_sql(query, conn, params=values)

    logger.info(f"Loaded {len(df)} rows from table: {table_name}")
    return df


def init_or_update_db(force_refresh: bool = False) -> None:
    table_name = "logs"
    ensure_db_initialized(table_name)

    latest_ts: Optional[pd.Timestamp] = None
    if not force_refresh:
        with sqlite3.connect(settings.log_db_path) as conn:
            result = conn.execute(f"SELECT MAX(timestamp) FROM {table_name}").fetchone()
            if result and result[0]:
                latest_ts = pd.to_datetime(result[0])

    df = load_logs_from_folder(settings.bootstrap_dir, save_to_db=False)

    if df.empty:
        logger.warning("No logs found.")
        return

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    if not force_refresh and latest_ts is not None:
        df = df[df["timestamp"] > latest_ts]

    if df.empty:
        logger.info("No new log entries to insert.")
        return

    save_df_to_db(df, if_exists="append")
    create_indexes()
    logger.success(f"{len(df)} new rows inserted {'(full refresh)' if force_refresh else '(filtered by timestamp)'}")
