# File: runtime_analytics/app_config/logger.py

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


def setup_logging(log_level: str = "INFO") -> None:
    logs_path = Path("logs")
    logs_path.mkdir(exist_ok=True)

    # Use built-in default level INFO if none provided or invalid
    level = getattr(logging, log_level.upper(), logging.INFO)

    formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)s | %(name)s | %(funcName)s:%(lineno)d | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    file_handler = TimedRotatingFileHandler(logs_path / "runtime_analytics.log", when="midnight", interval=1, backupCount=7, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
