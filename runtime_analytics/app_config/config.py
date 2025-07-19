from __future__ import annotations

import logging
import logging.config
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class AppConfig(BaseSettings):
    """
    Application settings, loaded from environment variables and defaults.

    Automatically initializes logging using a logging.config file.
    """

    base_dir: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    bootstrap_dir: Path = Field(default_factory=lambda: AppConfig._resolve_path("bootstrap"))
    resource_dir: Path = Field(default_factory=lambda: AppConfig._resolve_path("resources"))
    log_db_path: Path = Field(default_factory=lambda: AppConfig._resolve_path("analytics_meta.db"))
    log_config_path: Path = Field(default_factory=lambda: AppConfig._resolve_path("resources/logging.conf"))

    class Config:
        env_prefix = "APP_"
        case_sensitive = False

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._configure_logging()

    def _configure_logging(self) -> None:
        if self.log_config_path.exists():
            logging.config.fileConfig(self.log_config_path, disable_existing_loggers=False)
            logging.getLogger(__name__).debug(f"Logging initialized from {self.log_config_path}")
        else:
            logging.basicConfig(level=logging.INFO)
            logging.warning(f"{self.log_config_path} not found. Using basic logging config.")

    @staticmethod
    def _resolve_path(relative_path: str) -> Path:
        return Path(__file__).resolve().parent.parent / relative_path


# Singleton-style global settings object
settings = AppConfig()
