import os
import sqlite3

import joblib
import pandas as pd

from runtime_analytics.app_config.config import settings

def predict_response_times(
    db_path=None,
    model_path=None,
    top_n=10,
    save_to_db=True,
):
    db_path = db_path or settings.log_db_path
    model_path = model_path or (settings.base_dir / "ml" / "duration_model.pkl")

    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model not found at: {model_path}")

    model = joblib.load(model_path)

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql("SELECT * FROM job_logs", conn)

    # Ensure timestamp is in datetime format
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Feature Engineering
    df["log_hour"] = df["timestamp"].dt.hour
    df["log_dayofweek"] = df["timestamp"].dt.dayofweek
    df["log_is_weekend"] = df["log_dayofweek"].isin([5, 6]).astype(int)

    df["is_month_end"] = df["timestamp"].dt.is_month_end.astype(int)
    df["is_quarter_end"] = df["timestamp"].dt.is_quarter_end.astype(int)
    df["is_year_end"] = (
        (df["timestamp"].dt.month == 12) & (df["timestamp"].dt.day == 31)
    ).astype(int)

    # Calculate run_count and re_run_count
    daily_job_keys = df.groupby("run_date")[["riskdate", "id", "type"]].agg(lambda x: list(zip(x["riskdate"], x["id"], x["type"])))
    daily_job_keys = daily_job_keys["riskdate"].apply(lambda x: pd.Series(x)).stack().reset_index()
    daily_job_keys.columns = ["run_date", "_", "job_key"]

    run_counts = (
        daily_job_keys.groupby("run_date")["job_key"].nunique().rename("run_count").reset_index()
    )
    rerun_counts = (
        daily_job_keys.groupby(["run_date", "job_key"]).size().reset_index(name="re_run_count")
    )
    df["job_key"] = list(zip(df["riskdate"], df["id"], df["type"]))

    df = df.merge(run_counts, on="run_date", how="left")
    df = df.merge(rerun_counts, on=["run_date", "job_key"], how="left")

    # Fill missing re_run_count with 1 (first run)
    df["re_run_count"] = df["re_run_count"].fillna(1)

    # Final feature set for prediction
    features = [
        "config_count", "run_count", "re_run_count",
        "log_hour", "log_dayofweek", "log_is_weekend",
        "is_month_end", "is_quarter_end", "is_year_end"
    ]

    df = df.dropna(subset=features)
    df["predicted_duration"] = model.predict(df[features])
    df["rank"] = df["predicted_duration"].rank(method="min", ascending=False).astype(int)

    if save_to_db:
        with sqlite3.connect(db_path) as conn:
            df.to_sql("job_logs_with_predictions", conn, if_exists="replace", index=False)

    return df.sort_values("predicted_duration", ascending=False).head(top_n)


if __name__ == "__main__":
    results = predict_response_times()
    print(results[["riskdate", "id", "type", "predicted_duration", "rank"]])
