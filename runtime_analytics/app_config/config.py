from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class AppConfig(BaseSettings):
    base_dir: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    bootstrap_dir: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent / "bootstrap")
    resource_dir: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent / "resources")
    log_db_path: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent / "logs.db")

    class Config:
        env_prefix = "APP_"
        case_sensitive = False


settings = AppConfig()
