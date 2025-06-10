# Add 'job_key' as a derived field in the SQL DB handling
import sqlite3
import pandas as pd

from runtime_analytics.app_config.config import settings
from runtime_analytics.loader import load_logs_from_folder, extract_features

def ensure_db_initialized(table_name: str = "logs") -> None:
    with sqlite3.connect(settings.log_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                riskdate TEXT,
                id INTEGER,
                type TEXT,
                config_count INTEGER,
                run_date TEXT,
                duration_sec INTEGER,
                run_timestamp TEXT,
                run_count INTEGER,
                re_run_count INTEGER,
                exec_day_of_week INTEGER,
                log_hour INTEGER,
                weekend INTEGER,
                month_end INTEGER,
                quarter_end INTEGER,
                year_end INTEGER,
                job_key TEXT
            )
        """)
        conn.commit()

def create_indexes(table_name: str = "logs") -> None:
    with sqlite3.connect(settings.log_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_type ON {table_name} (type);")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_riskdate ON {table_name} (riskdate);")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_id_type_riskdate ON {table_name} (id, type, riskdate);")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_job_key ON {table_name} (job_key);")
        conn.commit()

def load_df_from_db(table_name: str = "logs") -> pd.DataFrame:
    ensure_db_initialized(table_name)
    with sqlite3.connect(settings.log_db_path) as conn:
        return pd.read_sql(f"SELECT * FROM {table_name}", conn)

def save_df_to_db(df: pd.DataFrame, table_name: str = "logs", if_exists: str = "append") -> None:
    ensure_db_initialized(table_name)

    if "timestamp" in df.columns:
        df["run_timestamp"] = pd.to_datetime(df["timestamp"]).dt.floor("s")
        df.drop(columns=["timestamp"], inplace=True)

    df = extract_features(df)  # Enrich with engineered fields and job_key

    required_cols = {
        "riskdate", "id", "type", "config_count", "run_date", "duration_sec",
        "run_timestamp", "run_count", "re_run_count", "exec_day_of_week",
        "log_hour", "weekend", "month_end", "quarter_end", "year_end", "job_key"
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    with sqlite3.connect(settings.log_db_path) as conn:
        if if_exists == "append":
            existing = pd.read_sql(f"SELECT riskdate, id, type FROM {table_name}", conn)
            if not existing.empty:
                existing["riskdate"] = existing["riskdate"].astype(str)
                df["riskdate"] = df["riskdate"].astype(str)
                df = df.merge(existing, on=["riskdate", "id", "type"], how="left", indicator=True)
                df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])

        if not df.empty:
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)

def init_or_update_db(force_refresh: bool = False):
    table_name = "logs"
    ensure_db_initialized(table_name)

    latest_ts = None
    if not force_refresh:
        with sqlite3.connect(settings.log_db_path) as conn:
            result = conn.execute(f"SELECT MAX(run_timestamp) FROM {table_name}").fetchone()
            if result and result[0]:
                latest_ts = pd.to_datetime(result[0])

    df = load_logs_from_folder(settings.log_dir, save_to_db=False)

    if df.empty:
        print("No logs found.")
        return

    if "timestamp" in df.columns:
        df["run_timestamp"] = pd.to_datetime(df["timestamp"]).dt.floor("s")
        df.drop(columns=["timestamp"], inplace=True)

    if not force_refresh and latest_ts is not None:
        df = df[df["run_timestamp"] > latest_ts]

    if df.empty:
        print("No new log entries to insert.")
        return

    save_df_to_db(df, if_exists="append")
    create_indexes()
    print(f"{len(df)} new rows inserted {'(full refresh)' if force_refresh else '(filtered by timestamp)'}.")
