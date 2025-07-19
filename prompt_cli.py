from __future__ import annotations

import argparse

import pandas as pd

from runtime_analytics.app_config.config import settings
from runtime_analytics.app_db.db_loader import create_indexes, load_df_from_db
from runtime_analytics.app_db.db_operations import save_df_to_db
from runtime_analytics.etl.loader import load_logs_from_folder
from runtime_analytics.ml.pipeline.predict_duration import predict_response_times
from runtime_analytics.prompt_interpreter import interpret_prompt
from runtime_analytics.prompts import FUNCTION_MAP, PREDEFINED_PROMPTS


def main():
    parser = argparse.ArgumentParser(description="Runtime Analytics CLI")
    parser.add_argument("--prompt", type=str, help="Natural language query or predefined prompt name")
    parser.add_argument("--from-logs", action="store_true", help="Parse from logs instead of DB")
    parser.add_argument("--output-csv", type=str, help="Output results to CSV file")
    args = parser.parse_args()

    # Load and predict data
    if args.from_logs:
        df = load_logs_from_folder(settings.bootstrap_dir)
        if df.empty:
            print("No valid logs found.")
            return
        save_df_to_db(df, if_exists="append")
        create_indexes()
        df = predict_response_times(save_to_db=False)
    else:
        df = load_df_from_db()
        if df.empty:
            print("No data found in DB.")
            return
        df = predict_response_times(save_to_db=False)

    if not args.prompt:
        print("Please provide a prompt via --prompt argument.")
        print("Available predefined prompts:")
        for p in PREDEFINED_PROMPTS:
            print(f" - {p}")
        return

    # Try predefined prompts first
    prompt_key = args.prompt.strip().lower()
    if prompt_key in (k.lower() for k in PREDEFINED_PROMPTS):
        # Match by name, case-insensitive
        for k, v in PREDEFINED_PROMPTS.items():
            if k.lower() == prompt_key:
                result = v["function"](df, **v["params"])
                break
    else:
        # Use NLP interpreter for free-form queries
        query = interpret_prompt(args.prompt)
        func_name = query.get("function")
        params = query.get("params", {})
        func = FUNCTION_MAP.get(func_name)
        if not func:
            print(f"Could not interpret prompt: {args.prompt}")
            return
        result = func(df, **params)

    # Show and/or export result
    if isinstance(result, pd.DataFrame):
        print(result)
        if args.output_csv:
            result.to_csv(args.output_csv, index=False)
            print(f"Results saved to {args.output_csv}")
    else:
        print(result)


if __name__ == "__main__":
    main()
