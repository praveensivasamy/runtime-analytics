from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta

import pandas as pd

from runtime_analytics.app_config.config import settings

logger = logging.getLogger(__name__)


def select_logs_from_db_with_filters(filters: dict[str, str] = None) -> pd.DataFrame:
    query = "SELECT * FROM job_logs"
    conditions = []

    if filters:
        for key, value in filters.items():
            if ">=" in key or "<=" in key or "!=" in key or "=" in key:
                field, op = key.split(" ", 1)
                conditions.append(f"{field} {op} '{value}'")
            else:
                conditions.append(f"{key} = '{value}'")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    else:
        raise ValueError("No filters provided for the database query.")


    logger.info(f"Executing SQL: {query}")

    with sqlite3.connect(settings.log_db_path) as conn:
        df = pd.read_sql_query(query, conn)

    logger.info(f"Retrieved {len(df)} rows from job_logs")
    return df


def get_logs_for_time_range(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch logs within a time range."""
    logger.info(f"Fetching logs from {start_date} to {end_date}")
    filters = {
        "timestamp >=": start_date,
        "timestamp <=": end_date,
    }
    return select_logs_from_db_with_filters(filters=filters).reset_index(drop=True)


def get_logs_by_period(period: str) -> pd.DataFrame:
    """Fetch logs for a given period like 'week', 'month', or 'year'."""
    now = datetime.now()
    if period == "week":
        start = now - timedelta(days=now.weekday())
    elif period == "month":
        start = now.replace(day=1)
    elif period == "year":
        start = now.replace(month=1, day=1)
    elif period == "yesterday":
        start = now - timedelta(days=1)
        logger.info(f"Fetching logs for yesterday: {start.strftime('%Y-%m-%d')}")
    else:
        logger.warning(f"Unknown period requested: {period}")
        return pd.DataFrame()

    logger.info(f"Fetching logs since {start.strftime('%Y-%m-%d')} for period: {period}")
    return select_logs_from_db_with_filters(filters={"timestamp >=": start.strftime("%Y-%m-%d")}).reset_index(drop=True)


def get_all_logs() -> pd.DataFrame:
    """Return all logs from DB."""
    logger.info("Fetching all logs from DB")
    return select_logs_from_db_with_filters().reset_index(drop=True)


def fetch_data_for_prompt(params: dict) -> pd.DataFrame:
    """
    Central function to fetch logs based on user-interpreted prompt params.
    Supports:
    - date_filter: week, month, year
    - start_date, end_date
    - filters: dict of exact or conditional filters
    """
    date_filter = params.get("date_filter")
    start_date = params.get("start_date")
    end_date = params.get("end_date")
    filters = params.get("filters", {})

    base_filters = {}
    if start_date and end_date:
        base_filters["timestamp >="] = start_date
        base_filters["timestamp <="] = end_date
    elif date_filter:
        now = datetime.now()
        if date_filter == "week":
            base_filters["timestamp >="] = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")
        elif date_filter == "month":
            base_filters["timestamp >="] = now.replace(day=1).strftime("%Y-%m-%d")
        elif date_filter == "year":
            base_filters["timestamp >="] = now.replace(month=1, day=1).strftime("%Y-%m-%d")

    full_filters = {**base_filters, **filters}
    df = select_logs_from_db_with_filters(filters=full_filters)
    logger.info(f"Applied SQL-level filters. Final record count: {len(df)}")
    return df.reset_index(drop=True)