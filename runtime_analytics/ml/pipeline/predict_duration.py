import sqlite3
import pandas as pd
import joblib
import logging
from runtime_analytics.app_config.config import settings
from sklearn.pipeline import Pipeline
from runtime_analytics.etl.loader import extract_features
from runtime_analytics.app_db.db_operations import save_df_to_db

# Set up logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(), logging.FileHandler("predict_duration_model.log")],
)
logger = logging.getLogger(__name__)

NUMERICAL_FEATURES = ["config_count", "log_hour", "month_end", "quarter_end", "year_end", "job_count", "job_sequence", "job_run_count"]
CATEGORICAL_FEATURES = ["day", "month", "week", "job_order", "type"]
FEATURE_COLUMNS = NUMERICAL_FEATURES + CATEGORICAL_FEATURES
MODEL_PATH = settings.base_dir / "ml" / "pipeline" / "trained" / "duration_prediction_model.pkl"


def create_table_if_not_exists(df: pd.DataFrame, table_name: str, conn: sqlite3.Connection):
    """Create the table dynamically based on DataFrame columns."""
    column_defs = ", ".join([f"{col} {get_sqlite_type(df[col])}" for col in df.columns])
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_defs}
        );
    """
    conn.execute(create_table_query)
    conn.commit()


def get_sqlite_type(series: pd.Series) -> str:
    """Return the appropriate SQLite type for a DataFrame column."""
    dtype = series.dtype
    if dtype == "int64":
        return "INTEGER"
    elif dtype == "float64":
        return "REAL"
    elif dtype == "datetime64[ns]":
        return "TEXT"  # Store datetime as TEXT in SQLite
    else:
        return "TEXT"  # Default to TEXT for other types


def predict_response_times(db_path=None, model_path=None, top_n=10, save_to_db=True):
    db_path = db_path or settings.log_db_path
    model_path = model_path or MODEL_PATH
    logger.info(f"Loading model from {model_path}...")
    pipeline: Pipeline = joblib.load(model_path)

    try:
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql("SELECT * FROM job_logs", conn)

        logger.info(f"Loaded {len(df)} records from the database.")

        df = df.dropna(subset=FEATURE_COLUMNS)
        df = extract_features(df)

        logger.info("Making predictions...")
        df["predicted_duration"] = pipeline.predict(df[FEATURE_COLUMNS])
        df["rank"] = df["predicted_duration"].rank(method="min", ascending=False).astype(int)

        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S,%f", errors="coerce")
        df = df.sort_values(["run_date", "job_sequence", "timestamp"])

        df["duration_delta"] = df["duration"].diff()
        df["predicted_delta"] = df["predicted_duration"].diff()
        df["timestamp_gap"] = df["timestamp"].diff().dt.total_seconds()

        # Job-based deltas
        df["duration_per_job"] = df.groupby("job_count")["duration"].transform("mean")
        df["predicted_per_job"] = df.groupby("job_count")["predicted_duration"].transform("mean")
        df["sequence_diff"] = df.groupby("job_count")["job_sequence"].diff()
        df["duration_seq_delta"] = df.groupby("job_count")["duration"].diff()
        df["predicted_seq_delta"] = df.groupby("job_count")["predicted_duration"].diff()

        if save_to_db:
            # Ensure the table is created if not exists
            table_name = "job_logs_with_predictions"
            create_table_if_not_exists(df, table_name, conn)
            save_df_to_db(df, "job_logs_with_predictions", if_exists="append")

        logger.info(f"Predictions for {len(df)} records saved to the database.")
        return df.sort_values("predicted_duration", ascending=False).head(top_n)

    except Exception as e:
        logger.error(f"Error during prediction: {e}")
        raise


if __name__ == "__main__":
    predict_response_times()
