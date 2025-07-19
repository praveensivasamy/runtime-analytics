from __future__ import annotations

import argparse
import logging
import subprocess
import sys

import pandas as pd

from runtime_analytics.app_config.config import settings
from runtime_analytics.app_config.logger import setup_logging
from runtime_analytics.app_db.db_loader import create_indexes, init_or_update_db, load_df_from_db
from runtime_analytics.app_db.db_operations import save_df_to_db
from runtime_analytics.etl.loader import load_logs_from_folder
from runtime_analytics.ml.pipeline.predict_duration import predict_response_times
from runtime_analytics.prompt_interpreter import interpret_prompt
from runtime_analytics.prompts import FUNCTION_MAP, PREDEFINED_PROMPTS

# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)


def run_cli(list_prompts=False, prompt_query=None, export_format="csv", from_logs=False):
    # Load data (from logs or DB)
    if from_logs:
        df = load_logs_from_folder(settings.bootstrap_dir)
        if df.empty:
            logger.warning("No valid logs found in the folder.")
            sys.exit(1)

        save_df_to_db(df, if_exists="append")
        create_indexes()
    else:
        df = load_df_from_db()
        if df.empty:
            logger.warning("No data found in the database.")
            sys.exit(1)

    # Predict duration
    df = predict_response_times(save_to_db=False)

    # List prompts
    if list_prompts:
        print("=== Available Predefined Prompts ===")
        for prompt in PREDEFINED_PROMPTS:
            print(f"- {prompt}")
        print("\nYou may also use free-form English queries!")
        return

    if not prompt_query:
        logger.error("Please provide a prompt with --prompt or use --list-prompts.")
        sys.exit(1)

    # Match predefined prompt or interpret free-text prompt
    prompt_key = prompt_query.strip().lower()
    result = None

    if prompt_key in (k.lower() for k in PREDEFINED_PROMPTS):
        for k, v in PREDEFINED_PROMPTS.items():
            if k.lower() == prompt_key:
                result = v["function"](df, **v["params"])
                break
    else:
        query = interpret_prompt(prompt_query)
        func_name = query.get("function")
        params = query.get("params", {})
        func = FUNCTION_MAP.get(func_name)

        if not func:
            logger.error(f"Could not interpret prompt: {prompt_query}")
            sys.exit(1)

        result = func(df, **params)

    # Handle and export result
    if isinstance(result, pd.DataFrame):
        print(result)

        if export_format:
            filename = f"output.{export_format}"
            try:
                if export_format == "csv":
                    result.to_csv(filename, index=False)
                elif export_format == "json":
                    result.to_json(filename, orient="records", indent=2)
                logger.info(f"Exported result to {filename}")
            except Exception as e:
                logger.error(f"Failed to export result: {e}")
    else:
        print(result)


def run_gui():
    subprocess.run([sys.executable, "-m", "streamlit", "run", "prompt_gui.py"], check=False)


def main():
    # Ensure DB schema and indexes
    init_or_update_db(force_refresh=True)

    parser = argparse.ArgumentParser(description="Runtime Analytics Launcher")
    parser.add_argument("--mode", choices=["cli", "gui"], default="cli", help="Run in 'cli' (default) or 'gui' mode")
    parser.add_argument("--list-prompts", action="store_true", help="List all available predefined prompts")
    parser.add_argument("--prompt", type=str, help="Free-form query or predefined prompt name")
    parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Export format for results")
    parser.add_argument("--from-logs", action="store_true", help="Parse from logs instead of DB")
    parser.add_argument("--train-prompts", action="store_true", help="Train the prompt matching model")

    args = parser.parse_args()

    if args.mode == "cli":
        run_cli(
            list_prompts=args.list_prompts,
            prompt_query=args.prompt,
            export_format=args.format,
            from_logs=args.from_logs,
        )
    elif args.mode == "gui":
        run_gui()

    if args.train_prompts:
        from runtime_analytics.scripts.train_prompt_model_cli import main as train_model_main

        train_model_main()


if __name__ == "__main__":
    main()
