from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field

class AppConfig(BaseSettings):
    base_dir: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    log_dir: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent / "logs")
    resource_dir: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent / "resources")
    prompt_config_file: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent / "resources" / "prompt_config.yaml")
    log_db_path: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent / "logs.db")

    class Config:
        env_prefix = "APP_"
        case_sensitive = False

settings = AppConfig()
