import logging
import sqlite3
import time
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from runtime_analytics.app_config.config import settings

# Set up logging to console only
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Feature definitions
NUMERICAL_FEATURES = [
    "config_count",
    "log_hour",
    "month_end",
    "quarter_end",
    "year_end",
    "job_count",
    "job_sequence",
    "job_run_count",
]
CATEGORICAL_FEATURES = ["day", "month", "week", "job_order", "type"]
FEATURE_COLUMNS = NUMERICAL_FEATURES + CATEGORICAL_FEATURES
TARGET = "duration"
MODEL_PATH = settings.base_dir / "ml" / "pipeline" / "trained" / "duration_prediction_model.pkl"


def train_pipeline_model(
    db_path=None,
    model_path=None,
    test_size=0.2,
    random_state=42,
    verbose=True,
):
    db_path = db_path or settings.log_db_path
    model_path = model_path or MODEL_PATH

    start_time = time.time()
    logger.info("Starting duration model training...")

    try:
        # Create the 'model_metrics' table if it doesn't exist
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS model_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    rmse REAL,
                    sample_count INTEGER,
                    train_time_sec REAL,
                    training_status TEXT
                )
            """
            )
            conn.commit()

        logger.info("Database table 'model_metrics' is ready.")

        # Load data from the database
        logger.info("Loading data from the database...")
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql("SELECT * FROM job_logs", conn)

        logger.info(f"Data loaded with {len(df)} rows.")
        df = df.dropna(subset=FEATURE_COLUMNS + [TARGET])

        X = df[FEATURE_COLUMNS]
        y = df[TARGET]

        logger.info(f"Training set contains {len(df)} samples")

        # Create a pipeline with preprocessor and regressor
        preprocessor = ColumnTransformer(
            [
                ("num", StandardScaler(), NUMERICAL_FEATURES),
                ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
            ]
        )

        pipeline = Pipeline(
            [
                ("preprocessor", preprocessor),
                (
                    "regressor",
                    RandomForestRegressor(n_estimators=100, random_state=random_state, n_jobs=-1),
                ),
            ]
        )

        logger.info("Splitting data...")
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)

        logger.info("Fitting pipeline to training data...")
        pipeline.fit(X_train, y_train)

        # Make predictions and calculate RMSE
        y_pred = pipeline.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)
        logger.info(f"Model trained with RMSE: {rmse:.2f}")

        # Save the model
        joblib.dump(pipeline, model_path)

        elapsed = time.time() - start_time
        logger.info(f"Training complete. Model saved to {model_path}")
        logger.info(f"Total training time: {elapsed:.2f} seconds")

        # Log training metrics
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO model_metrics (timestamp, rmse, sample_count, train_time_sec, training_status)
                VALUES (?, ?, ?, ?, ?)
            """,
                (datetime.now().isoformat(), rmse, len(df), elapsed, "Completed"),
            )
            conn.commit()

        if verbose:
            print(f"Model trained in {elapsed:.2f} seconds with RMSE: {rmse:.2f} using {len(df)} samples.")

    except Exception as e:
        logger.error(f"An error occurred during training: {e}")
        raise


if __name__ == "__main__":
    train_pipeline_model()
