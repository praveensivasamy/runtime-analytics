import re
from datetime import datetime, timedelta

import yaml
from loguru import logger
from sentence_transformers import SentenceTransformer, util

from runtime_analytics.app_config.config import settings


def load_prompt_catalog(yaml_path=None):
    if yaml_path is None:
        yaml_path = settings.resource_dir / "prompt_catalog.yaml"
    with open(yaml_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def extract_params_from_prompt(prompt: str) -> dict:
    prompt = prompt.lower()
    params = {}

    # Extract top N
    match = re.search(r"top (\d+)", prompt)
    if match:
        params["n"] = int(match.group(1))

    # Fast or slow
    if "fastest" in prompt:
        params["ascending"] = True
    if "slow" in prompt or "slowest" in prompt:
        params["ascending"] = False

    # Fixed filters
    today = datetime.today()
    if "this week" in prompt:
        start = today - timedelta(days=today.weekday())
        params.setdefault("filters", {})["run_date >= "] = start.strftime("%Y-%m-%d")
    elif "this month" in prompt:
        start = today.replace(day=1)
        params.setdefault("filters", {})["run_date >= "] = start.strftime("%Y-%m-%d")
    elif "this year" in prompt:
        start = today.replace(month=1, day=1)
        params.setdefault("filters", {})["run_date >= "] = start.strftime("%Y-%m-%d")
    elif "yesterday" in prompt:
        start = today - timedelta(days=1)
        params.setdefault("filters", {})["run_date = "] = start.strftime("%Y-%m-%d")
    elif "last 7 days" in prompt or "past 7 days" in prompt:
        start = today - timedelta(days=7)
        params.setdefault("filters", {})["run_date >= "] = start.strftime("%Y-%m-%d")

    return params


class PromptInterpreter:
    def __init__(self, model_name="all-MiniLM-L6-v2", catalog_path=None):
        self.model = SentenceTransformer(model_name)
        self.catalog_path = catalog_path or (settings.resource_dir / "prompt_catalog.yaml")
        self.catalog = load_prompt_catalog(self.catalog_path)

    def interpret(self, prompt: str) -> dict:
        logger.info(f"Interpreting prompt: {prompt}")
        user_embedding = self.model.encode(prompt, convert_to_tensor=True)

        best_intent = None
        best_score = -1

        for entry in self.catalog:
            for example in entry.get("examples", []):
                example_embedding = self.model.encode(example, convert_to_tensor=True)
                score = float(util.cos_sim(user_embedding, example_embedding))
                if score > best_score:
                    best_score = score
                    best_intent = entry

        if not best_intent:
            logger.warning("No match found for prompt.")
            return {"function": None, "params": {}}

        params = best_intent.get("default_params", {}).copy()
        extracted = extract_params_from_prompt(prompt)
        params.update(extracted)

        logger.info(f"Matched function: {best_intent['function']} with params: {params}")
        return {"function": best_intent["function"], "params": params}


# Allow top-level import
_interpreter = PromptInterpreter()


def interpret_prompt(prompt: str) -> dict:
    return _interpreter.interpret(prompt)
