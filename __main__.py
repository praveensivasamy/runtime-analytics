import argparse
import subprocess
import sys

import pandas as pd

from runtime_analytics.app_config.config import settings
from runtime_analytics.app_db.sql_db import init_or_update_db
from runtime_analytics.loader import load_logs_from_folder
from runtime_analytics.ml.predict_response_time import predict_response_times
from runtime_analytics.prompt_interpreter import interpret_prompt
from runtime_analytics.prompts import FUNCTION_MAP, PREDEFINED_PROMPTS


def run_cli(list_prompts=False, prompt_query=None, export_format="csv", from_logs=False):
    # Load and predict data
    if from_logs:
        df = load_logs_from_folder(settings.log_dir)
        if df.empty:
            print("No valid logs found.")
            return
        from runtime_analytics.app_db.sql_db import create_indexes, save_df_to_db
        save_df_to_db(df, if_exists="append")
        create_indexes()
        df = predict_response_times(save_to_db=False)
    else:
        from runtime_analytics.app_db.sql_db import load_df_from_db
        df = load_df_from_db()
        if df.empty:
            print("No data found in DB.")
            return
        df = predict_response_times(save_to_db=False)

    if list_prompts:
        print("=== Available Predefined Prompts ===")
        for prompt in PREDEFINED_PROMPTS:
            print(f"- {prompt}")
        print("\nYou may also use free-form English queries!")
        return

    if not prompt_query:
        print("Please provide a prompt with --prompt. Use --list-prompts to view options.")
        return

    # First try to match a predefined prompt
    prompt_key = prompt_query.strip().lower()
    if prompt_key in (k.lower() for k in PREDEFINED_PROMPTS):
        for k, v in PREDEFINED_PROMPTS.items():
            if k.lower() == prompt_key:
                result = v["function"](df, **v["params"])
                break
    else:
        # NLP interpreter
        query = interpret_prompt(prompt_query)
        func_name = query.get("function")
        params = query.get("params", {})
        func = FUNCTION_MAP.get(func_name)
        if not func:
            print(f"Could not interpret prompt: {prompt_query}")
            return
        result = func(df, **params)

    # Show and/or export result
    if isinstance(result, pd.DataFrame):
        print(result)
        if export_format:
            filename = f"output.{export_format}"
            if export_format == "csv":
                result.to_csv(filename, index=False)
            elif export_format == "json":
                result.to_json(filename, orient="records", indent=2)
            print(f"Exported result to {filename}")
    else:
        print(result)

def run_gui():
    subprocess.run([sys.executable, "-m", "streamlit", "run", "prompt_gui.py"])

def main():
    init_or_update_db(force_refresh=True)
    parser = argparse.ArgumentParser(description="Runtime Analytics Launcher")
    parser.add_argument("--mode", choices=["cli", "gui"], default="cli", help="Run in 'cli' (default) or 'gui' mode")
    parser.add_argument("--list-prompts", action="store_true", help="List all available predefined prompts")
    parser.add_argument("--prompt", type=str, help="Natural language query or predefined prompt name")
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
        from runtime_analytics.scripts.train_prompt_model_cli import (
            main as train_model_main,
        )
        train_model_main()

if __name__ == "__main__":
    main()
