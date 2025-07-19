import pandas as pd
from loguru import logger


def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    if not filters:
        return df

    for key, value in filters.items():
        original_count = len(df)
        try:
            if isinstance(value, list):
                if key not in df.columns:
                    logger.warning(
                        f"Skipping filter: {key} not in DataFrame columns.")
                    continue
                df = df[df[key].isin(value)]
                logger.info(
                    f"Applied filter: {key} in {value} -> {len(df)} rows (from {original_count})")

            elif key.endswith(" >= "):
                field = key.replace(" >= ", "")
                if field not in df.columns:
                    logger.warning(
                        f"Skipping filter: {field} not in DataFrame.")
                    continue
                df = df[df[field] >= value]
                logger.info(
                    f"Applied filter: {field} >= {value} -> {len(df)} rows (from {original_count})")

            elif key.endswith(" <= "):
                field = key.replace(" <= ", "")
                if field not in df.columns:
                    logger.warning(
                        f"Skipping filter: {field} not in DataFrame.")
                    continue
                df = df[df[field] <= value]
                logger.info(
                    f"Applied filter: {field} <= {value} -> {len(df)} rows (from {original_count})")

            elif key.endswith(" > "):
                field = key.replace(" > ", "")
                if field not in df.columns:
                    logger.warning(
                        f"Skipping filter: {field} not in DataFrame.")
                    continue
                df = df[df[field] > value]
                logger.info(
                    f"Applied filter: {field} > {value} -> {len(df)} rows (from {original_count})")

            elif key.endswith(" < "):
                field = key.replace(" < ", "")
                if field not in df.columns:
                    logger.warning(
                        f"Skipping filter: {field} not in DataFrame.")
                    continue
                df = df[df[field] < value]
                logger.info(
                    f"Applied filter: {field} < {value} -> {len(df)} rows (from {original_count})")

            else:
                if key not in df.columns:
                    logger.warning(f"Skipping filter: {key} not in DataFrame.")
                    continue
                df = df[df[key] == value]
                logger.info(
                    f"Applied filter: {key} == {value} -> {len(df)} rows (from {original_count})")

        except Exception as e:
            logger.error(f"Error applying filter {key}: {e}")

    return df
