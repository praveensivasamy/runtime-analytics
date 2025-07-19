from __future__ import annotations

import subprocess
import sys
from shlex import join
from typing import Iterable, Sequence

# Directories targeted by linting, formatting, typing, etc.
_TARGET_DIRECTORIES: tuple[str, ...] = ("runtime_analytics", "tests")


def run_command(
    *args: str,
    check_args: Iterable[str] = (),
    fix_args: Iterable[str] = (),
) -> None:
    """
    Run a shell command with optional `--fix` or `--check` mode.

    Args:
        *args: Base command arguments (e.g., "ruff", "check").
        check_args: Arguments to use when in check-only mode (default).
        fix_args: Arguments to use when '--fix' is passed via CLI.

    Behavior:
        - Adds either fix_args or check_args based on presence of '--fix' in sys.argv.
        - Displays the command being run.
        - Captures and prints stdout/stderr on failure.
        - Exits the script with the subprocess return code if the command fails.
    """
    command: list[str] = [*args, *(fix_args if "--fix" in sys.argv else check_args)]
    # Inject verbosity if applicable
    tool = command[0]
    if tool == "ruff":
        command.append("--verbose")
    elif tool == "isort":
        command.append("-v")
    elif tool == "mypy":
        command.append("-v")
    print(f"$ {join(command)}")
    result = subprocess.run(command, check=False, text=True, capture_output=True)

    if result.returncode != 0:
        print(f"\n Failed: (exit code: {result.returncode})")
        print(result.stdout)
        print(result.stderr)
        sys.exit(result.returncode)


def format() -> None:
    """
    Format Python files using Ruff's formatter.

    Targets:
        - runtime_analytics/
        - tests/
    """
    run_command("ruff", "format", *_TARGET_DIRECTORIES)


def lint() -> None:
    """
    Run Ruff to check code style and lint errors.

    Pass-through:
        - Accepts additional CLI arguments from sys.argv[1:].
    """
    run_command("ruff", "check", *_TARGET_DIRECTORIES, *sys.argv[1:])


def sort_imports() -> None:
    """
    Sort imports using isort.

    Behavior:
        - If '--fix' is passed, it will apply sorting.
        - Otherwise, it checks and reports unsorted imports.
    """
    run_command("isort", "--gitignore", ".", check_args=["--check"])


def type_check() -> None:
    """
    Run type checking using MyPy with error codes.

    Targets:
        - runtime_analytics/
        - tests/
    """
    args: list[str] = []
    for package in _TARGET_DIRECTORIES:
        args.extend(["--package", package])
    run_command("mypy", "--show-error-codes", *args)
