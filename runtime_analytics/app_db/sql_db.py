import sqlite3
import pandas as pd
from runtime_analytics.app_config.config import settings
from runtime_analytics.loader import load_logs_from_folder


def save_df_to_db(df: pd.DataFrame, table_name: str = "logs", if_exists: str = "replace"):
    with sqlite3.connect(settings.log_db_path) as conn:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)

def load_df_from_db(table_name: str = "logs") -> pd.DataFrame:
    with sqlite3.connect(settings.log_db_path) as conn:
        return pd.read_sql(f"SELECT * FROM {table_name}", conn)

def create_indexes(table_name: str = "logs"):
    with sqlite3.connect(settings.log_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_type ON {table_name} (type);")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_riskdate ON {table_name} (riskdate);")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_id_type_riskdate ON {table_name} (id, type, riskdate);")
        conn.commit()



def init_or_update_db(table_name: str = "logs") -> None:
    ensure_db_initialized(table_name)
    df = load_logs_from_folder(settings.log_dir)
    if not df.empty:
        save_df_to_db(df, table_name=table_name)

import sqlite3
import pandas as pd
from pathlib import Path
from runtime_analytics.app_config.config import settings


def ensure_db_initialized(table_name: str = "logs") -> None:
    """Creates the table if it doesn't exist."""
    conn = sqlite3.connect(settings.log_db_path)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TEXT,
            config_count INTEGER,
            riskdate TEXT,
            id INTEGER,
            type TEXT,
            run_date TEXT,
            duration_str TEXT,
            duration_sec INTEGER
        )
    """)
    conn.commit()
    conn.close()


def load_df_from_db(table_name: str = "logs") -> pd.DataFrame:
    ensure_db_initialized(table_name)
    conn = sqlite3.connect(settings.log_db_path)
    return pd.read_sql(f"SELECT * FROM {table_name}", conn)


def save_df_to_db(df: pd.DataFrame, table_name: str = "logs", if_exists: str = "append") -> None:
    ensure_db_initialized(table_name)
    conn = sqlite3.connect(settings.log_db_path)

    # Filter out duplicates based on primary keys: riskdate, id, type
    existing = pd.read_sql(f"SELECT riskdate, id, type FROM {table_name}", conn)
    if not existing.empty:
        df = df.merge(existing, on=["riskdate", "id", "type"], how="left", indicator=True)
        df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])

    if not df.empty:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)

    conn.close()
