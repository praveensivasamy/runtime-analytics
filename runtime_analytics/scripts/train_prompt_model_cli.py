
import pandas as pd

from runtime_analytics.app_config.config import settings
from runtime_analytics.ml.train_prompt_model import train_and_save_prompt_model


def main():
    csv_path = settings.resource_dir / "training_prompts.csv"
    if not csv_path.exists():
        print("No training_prompts.csv found. Please generate it first.")
        return

    df = pd.read_csv(csv_path)
    train_and_save_prompt_model(df)

if __name__ == "__main__":
    main()
