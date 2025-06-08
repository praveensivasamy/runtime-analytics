import argparse
import subprocess
import sys
import pandas as pd

from runtime_analytics.app_db.sql_db import init_or_update_db

# Ensure package is importable even when running from root
sys.path.append(".")

from runtime_analytics.app_config.config import settings
from runtime_analytics.loader import load_logs_from_folder
from runtime_analytics.prompts import load_prompt_map_from_yaml


def run_cli(list_prompts: bool = False, export_prompt: str = None, export_format: str = "csv"):
    prompt_map, metadata = load_prompt_map_from_yaml(str(settings.prompt_config_file))

    if list_prompts:
        print("=== Available Prompts ===")
        for prompt, meta in metadata.items():
            print(f"- {prompt}: {meta['description']}")
        return

    df = load_logs_from_folder(str(settings.log_dir))
    if df.empty:
        print("No valid log data found.")
        return

    if export_prompt:
        func = prompt_map.get(export_prompt.lower())
        if func:
            result = func(df)
            filename = f"output.{export_format}"
            if export_format == "csv":
                result.to_csv(filename, index=False)
            elif export_format == "json":
                result.to_json(filename, orient="records", indent=2)
            print(f"Exported result to {filename}")
        else:
            print("Invalid prompt. Use --list-prompts to view options.")
        return

    print("=== Loaded Log Summary ===")
    print(f"Total entries: {len(df)}")
    print(f"Types found: {df['type'].unique().tolist()}")
    print("\nSNSI Duration by Risk Date:")
    print(prompt_map["sns duration by riskdate"](df).to_string(index=False))


def run_gui():
    subprocess.run([sys.executable, "-m", "streamlit", "run", "prompt_gui.py"])


def main():
    init_or_update_db()
    parser = argparse.ArgumentParser(description="Runtime Analytics Launcher")
    parser.add_argument("--mode", choices=["cli", "gui"], default="cli", help="Run in 'cli' (default) or 'gui' mode")
    parser.add_argument("--list-prompts", action="store_true", help="List all available prompt templates")
    parser.add_argument("--export", type=str, help="Prompt to export result for")
    parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Export format for --export prompt")

    args = parser.parse_args()

    if args.mode == "cli":
        run_cli(list_prompts=args.list_prompts, export_prompt=args.export, export_format=args.format)
    elif args.mode == "gui":
        run_gui()


if __name__ == "__main__":
    main()
