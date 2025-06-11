import csv
from pathlib import Path

import yaml

from runtime_analytics.app_config.config import settings


def generate_training_prompts_csv(output_file: Path):
    yaml_path = settings.prompt_config_file
    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    prompts = config.get("prompts", {})
    examples = []

    for prompt_text, meta in prompts.items():
        description = meta.get("description", "")
        if description:
            # Simple heuristic: use description as seed query
            query = description.replace("Show", "Display").replace("Compute", "Find").strip().capitalize()
            examples.append((query, prompt_text))
        else:
            examples.append((f"Run {prompt_text}", prompt_text))

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["query", "prompt"])
        writer.writerows(examples)

    print(f"Generated: {output_file}")

if __name__ == "__main__":
    csv_path = settings.resource_dir / "training_prompts.csv"
    generate_training_prompts_csv(csv_path)
