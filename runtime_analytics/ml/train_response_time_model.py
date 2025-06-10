import sqlite3

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

from runtime_analytics.app_config.config import settings


def train_model(
    db_path=None,
    model_path=None,
    features=None,
    test_size=0.2,
    random_state=42,
    verbose=True,
):
    db_path = db_path or settings.log_db_path
    model_path = model_path or (settings.base_dir  / "ml" / "duration_model.pkl")

    if features is None:
        features = ["config_count", "exec_day_of_week", "days_since_risk", "log_hour"]

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql("SELECT * FROM job_logs", conn)

    df = df.dropna(subset=features + ["duration_sec"])
    X = df[features]
    y = df["duration_sec"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )

    model = RandomForestRegressor(n_estimators=100, random_state=random_state)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    rmse = mean_squared_error(y_test, y_pred)

    joblib.dump(model, model_path)

    if verbose:
        print(f"Model trained and saved to: {model_path}")
        print(f"Validation RMSE: {rmse:.2f} seconds")

    return {"rmse": rmse, "model_path": model_path, "features_used": features, "samples": len(df)}


if __name__ == "__main__":
    train_model()
