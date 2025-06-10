import re
from datetime import datetime, timedelta

import dateutil.parser
import yaml
from sentence_transformers import SentenceTransformer, util

from runtime_analytics.app_config.config import settings


def load_prompt_catalog(yaml_path=None):
    if yaml_path is None:
        yaml_path = settings.resource_dir / "prompt_catalog.yaml"
    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def extract_params_from_prompt(prompt: str) -> dict:
    prompt = prompt.lower()
    params = {}

    # Extract top N jobs
    match = re.search(r"top (\d+)", prompt)
    if match:
        params["n"] = int(match.group(1))
    # Fastest jobs
    if "fastest" in prompt:
        params["ascending"] = True
    if "slow" in prompt or "slowest" in prompt:
        params["ascending"] = False

    # Date filters: this week
    if "this week" in prompt:
        today = datetime.today()
        start_of_week = today - timedelta(days=today.weekday())
        params.setdefault("filters", {})
        params["filters"]["run_date >= "] = start_of_week.strftime("%Y-%m-%d")

    return params

class PromptInterpreter:
    def __init__(self, model_name="all-MiniLM-L6-v2", catalog_path=None):
        self.model = SentenceTransformer(model_name)
        self.catalog_path = catalog_path or (settings.resource_dir / "prompt_catalog.yaml")
        self.catalog = load_prompt_catalog(self.catalog_path)

    def interpret(self, prompt: str) -> dict:
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
            return {"function": None, "params": {}}

        params = best_intent.get("default_params", {}).copy()
        extracted = extract_params_from_prompt(prompt)
        params.update(extracted)

        return {"function": best_intent["function"], "params": params}


def extract_params_from_prompt(prompt: str) -> dict:
    prompt = prompt.lower()
    params = {}

    #Extract top N
    match = re.search(r"top (\d+)", prompt)
    if match:
        params["n"] = int(match.group(1))

    #Fast or slow
    if "fastest" in prompt:
        params["ascending"] = True
    if "slow" in prompt or "slowest" in prompt:
        params["ascending"] = False

    # Fixed filters
    if "this week" in prompt:
        params["date_filter"] = "week"
    elif "this month" in prompt:
        params["date_filter"] = "month"
    elif "this year" in prompt:
        params["date_filter"] = "year"

    # Relative filters
    if "last 7 days" in prompt or "past 7 days" in prompt:
        params["start_date"] = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    elif "last 15 days" in prompt:
        params["start_date"] = (datetime.today() - timedelta(days=15)).strftime("%Y-%m-%d")
    elif "past 2 weeks" in prompt:
        params["start_date"] = (datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d")
    elif "last month" in prompt:
        today = datetime.today()
        first_this_month = today.replace(day=1)
        last_month_end = first_this_month - timedelta(days=1)
        start_last_month = last_month_end.replace(day=1)
        params["start_date"] = start_last_month.strftime("%Y-%m-%d")
        params["end_date"] = last_month_end.strftime("%Y-%m-%d")

    #Absolute ranges: from X to Y
    match = re.search(r"from ([a-zA-Z0-9 ,]+?) to ([a-zA-Z0-9 ,]+)", prompt)
    if match:
        try:
            start = dateutil.parser.parse(match.group(1)).date()
            end = dateutil.parser.parse(match.group(2)).date()
            params["start_date"] = start.strftime("%Y-%m-%d")
            params["end_date"] = end.strftime("%Y-%m-%d")
        except Exception:
            pass  # ignore invalid dates

    return params


# Simple interface for outside modules
_interpreter_instance = None
def interpret_prompt(prompt: str) -> dict:
    global _interpreter_instance
    if _interpreter_instance is None:
        _interpreter_instance = PromptInterpreter()
    return _interpreter_instance.interpret(prompt)
