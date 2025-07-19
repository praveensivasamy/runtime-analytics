from __future__ import annotations

import logging
import shutil
from pathlib import Path

import pandas as pd

from runtime_analytics.app_db.db_operations import save_df_to_db
from runtime_analytics.etl.log_parser import parse_log_line

logger = logging.getLogger(__name__)


class ETLLogLoader:
    def __init__(self, folder_path: str, save_to_db: bool = True, batch_size: int = 1000):
        """
        Initializes the ETL loader for log files.
        """
        self.folder_path = Path(folder_path)
        self.save_to_db = save_to_db
        self.batch_size = batch_size
        self.loaded_dir = self.folder_path / "loaded"
        self.loaded_dir.mkdir(exist_ok=True)
        self.milestones = [0.25, 0.50, 0.75, 1.0]

    def load(self) -> pd.DataFrame:
        """
        Main method to load logs, parse, transform, and optionally save them.
        """
        total_lines = self._count_total_lines()
        if total_lines == 0:
            logger.warning("No valid log lines found in folder.")
            return pd.DataFrame()

        processed_lines = 0
        next_milestone_idx = 0
        all_records: list[dict] = []

        for file in self.folder_path.glob("*.txt"):
            try:
                with file.open("r", encoding="utf-8") as f:
                    lines = f.readlines()

                batch: list[dict] = []
                for line in lines:
                    if not line.strip():
                        continue

                    parsed = parse_log_line(line)
                    processed_lines += 1

                    if parsed:
                        batch.append(parsed)
                        if len(batch) >= self.batch_size:
                            df_batch = self._process_and_save_batch(batch)
                            all_records.extend(df_batch.to_dict(orient="records"))
                            batch.clear()

                    # Progress tracking
                    progress = processed_lines / total_lines
                    if next_milestone_idx < len(self.milestones) and progress >= self.milestones[next_milestone_idx]:
                        logger.info(f"Processed {int(progress * 100)}% ({processed_lines}/{total_lines} lines)")
                        next_milestone_idx += 1

                if batch:
                    df_batch = self._process_and_save_batch(batch)
                    all_records.extend(df_batch.to_dict(orient="records"))

                shutil.move(str(file), self.loaded_dir / file.name)
                logger.info(f"Moved {file.name} to {self.loaded_dir}")

            except Exception as e:
                logger.warning(f"Failed to parse {file.name}: {e}")

        return pd.DataFrame(all_records)

    def _count_total_lines(self) -> int:
        total = 0
        for file in self.folder_path.glob("*.txt"):
            try:
                with file.open("r", encoding="utf-8") as f:
                    total += sum(1 for _ in f)
            except Exception as e:
                logger.warning(f"Failed to count lines in {file.name}: {e}")
        return total

    def _process_and_save_batch(self, batch: list[dict]) -> pd.DataFrame:
        if not batch:
            return pd.DataFrame()

        df = pd.DataFrame(batch)
        df = self._normalize_columns(df)
        df = self._extract_features(df)
        df = self._format_datetime_strings(df)

        if self.save_to_db:
            save_df_to_db(df, if_exists="append")

        return df

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S,%f", errors="coerce")
        df["riskdate"] = pd.to_datetime(df["riskdate"], errors="coerce")
        df["run_date"] = pd.to_datetime(df["run_date"], errors="coerce")
        for col in ["config_count", "id", "duration"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        return df

    def _extract_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df["day"] = df["timestamp"].dt.strftime("%a")
        df["month"] = df["timestamp"].dt.strftime("%B")
        df["year"] = df["timestamp"].dt.year
        df["week"] = df["timestamp"].dt.strftime("%U_%Y")
        df["log_hour"] = df["timestamp"].dt.hour
        df["month_end"] = df["timestamp"].dt.is_month_end.astype(int)
        df["quarter_end"] = df["timestamp"].dt.is_quarter_end.astype(int)
        df["year_end"] = df["timestamp"].dt.is_year_end.astype(int)

        df["job_id"] = df["riskdate"].dt.strftime("%Y-%m-%d") + "_" + df["id"].astype(str) + "_" + df["type"].astype(str)
        df["job_count"] = df.groupby("run_date")["job_id"].transform("count")
        df["job_sequence"] = df.groupby(["run_date", "job_id"])["timestamp"].rank(method="first").astype(int)
        df["job_run_count"] = df.groupby(["run_date", "job_id"]).cumcount() + 1
        df["job_order"] = df["job_sequence"].astype(str) + " of " + df["job_count"].astype(str)

        return df

    def _format_datetime_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        df["riskdate"] = pd.to_datetime(df["riskdate"], errors="coerce").dt.strftime("%Y-%m-%d")
        df["run_date"] = pd.to_datetime(df["run_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        return df
