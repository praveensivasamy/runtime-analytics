import yaml
import pandas as pd
from typing import Callable, Any
from runtime_analytics.analyzer import (
    sns_duration_by_riskdate,
    job_count_by_type,
    stdv_long_jobs,
)

# Registry of available analysis functions
AVAILABLE_FUNCTIONS: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
    "sns_duration_by_riskdate": sns_duration_by_riskdate,
    "job_count_by_type": job_count_by_type,
    "stdv_long_jobs": stdv_long_jobs,
}


def load_prompt_map_from_yaml(
    yaml_path: str,
) -> tuple[dict[str, Callable[[pd.DataFrame], pd.DataFrame]], dict[str, dict[str, Any]]]:
    """
    Load prompt definitions from YAML and map to callable functions and metadata.
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    prompt_map: dict[str, Callable] = {}
    metadata: dict[str, dict[str, Any]] = {}

    for prompt, entry in config.get("prompts", {}).items():
        if isinstance(entry, dict):
            function_name = entry.get("function")
            description = entry.get("description", "")
            tags = entry.get("tags", [])
        else:
            function_name = entry
            description = ""
            tags = []

        func = AVAILABLE_FUNCTIONS.get(function_name)
        if func:
            prompt_map[prompt.lower()] = func
            metadata[prompt.lower()] = {"description": description, "tags": tags}

    return prompt_map, metadata
