from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from loguru import logger

from runtime_analytics.app_db.db_loader import load_df_from_db
from runtime_analytics.utils.filters import apply_filters


def get_logs_for_yesterday() -> pd.DataFrame:
    """Fetch logs where run_date is yesterday."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"Fetching logs for yesterday: {yesterday}")
    df = load_df_from_db()
    return df[df["run_date"] == yesterday]


def get_logs_for_time_range(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch logs within a time range."""
    logger.info(f"Fetching logs from {start_date} to {end_date}")
    df = load_df_from_db()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    mask = (df["timestamp"] >= pd.to_datetime(start_date)) & (df["timestamp"] <= pd.to_datetime(end_date))
    return df[mask]


def get_logs_by_period(period: str) -> pd.DataFrame:
    """Fetch logs for a given period like 'week', 'month', or 'year'."""
    now = datetime.now()
    if period == "week":
        start = now - timedelta(days=now.weekday())
    elif period == "month":
        start = now.replace(day=1)
    elif period == "year":
        start = now.replace(month=1, day=1)
    else:
        logger.warning(f"Unknown period requested: {period}")
        return pd.DataFrame()

    logger.info(f"Fetching logs since {start.strftime('%Y-%m-%d')} for period: {period}")
    df = load_df_from_db()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df[df["timestamp"] >= start]


def get_all_logs() -> pd.DataFrame:
    """Return all logs from DB."""
    logger.info("Fetching all logs from DB")
    return load_df_from_db()


def fetch_logs_for_prompt(params: dict) -> pd.DataFrame:
    """
    Central function to fetch logs based on user-interpreted prompt params.
    Supports:
    - date_filter: week, month, year
    - start_date, end_date
    - filters: dict of exact or conditional filters

    TODO: Push filters to SQL in load_df_from_db() when DB size grows
    """
    date_filter = params.get("date_filter")
    start_date = params.get("start_date")
    end_date = params.get("end_date")
    filters = params.get("filters", {})

    if start_date and end_date:
        df = get_logs_for_time_range(start_date, end_date)
    elif date_filter:
        df = get_logs_by_period(date_filter)
    else:
        df = get_all_logs()

    df = apply_filters(df, filters)
    logger.info(f"Applied filters. Final record count: {len(df)}")
    return df
