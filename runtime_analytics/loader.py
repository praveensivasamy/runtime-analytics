import pandas as pd
from pathlib import Path
from runtime_analytics.log_parser import parse_log_line


def load_logs(file_path: str) -> pd.DataFrame:
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    parsed = [parse_log_line(line) for line in lines]
    parsed = [p for p in parsed if p]

    df = pd.DataFrame(parsed)
    print(df.head(5))
    if df.empty:
        return df

    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S,%f", errors="coerce")
    df["riskdate"] = pd.to_datetime(df["riskdate"]).dt.date
    df["run_date"] = pd.to_datetime(df["run_date"]).dt.date
    df["config_count"] = df["config_count"].astype(int)
    df["id"] = df["id"].astype(int)
    df["duration_sec"] = df["duration_sec"].astype(int)
    return df


def load_logs_from_folder(folder_path: str) -> pd.DataFrame:
    """
    Loads and parses all `.txt` log files in the specified folder.
    """
    log_dir = Path(folder_path)
    all_lines = []

    for file in log_dir.glob("*.txt"):
        with file.open("r", encoding="utf-8") as f:
            lines = f.readlines()
        parsed = [parse_log_line(line) for line in lines]
        all_lines.extend(filter(None, parsed))

    if not all_lines:
        return pd.DataFrame()

    df = pd.DataFrame(all_lines)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S,%f", errors="coerce")
    df["riskdate"] = pd.to_datetime(df["riskdate"]).dt.date
    df["run_date"] = pd.to_datetime(df["run_date"]).dt.date
    df["config_count"] = df["config_count"].astype(int)
    df["id"] = df["id"].astype(int)
    df["duration_sec"] = df["duration_sec"].astype(int)
    return df
