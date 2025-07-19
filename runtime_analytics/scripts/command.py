from __future__ import annotations

import subprocess
import sys

# Directories to target
TARGET_DIRECTORIES = ["runtime_analytics", "tests"]


def run_command(cmd, description):
    print(f"\nRunning: {description} -> {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed: {description} (exit code: {e.returncode})")
        sys.exit(e.returncode)


def format():
    """Run Black for code formatting."""
    run_command(["black"] + TARGET_DIRECTORIES, "Black Formatting")


def lint(fix=False):
    """Run linting with pylint and fix issues with autopep8."""
    if fix:
        # Run autopep8 to fix code style issues
        run_command(["autopep8", "--in-place", "--recursive"] +
                    TARGET_DIRECTORIES, "Fixing code with autopep8")

    # Run pylint to check code quality
    cmd = ["pylint"] + TARGET_DIRECTORIES
    run_command(cmd, "Linting with pylint")


def check():
    """Run checks without any fixes using pylint."""
    run_command(["pylint", "--disable=all", "--enable=E,W"] +
                TARGET_DIRECTORIES, "Lint Check (no fix)")


def type_check():
    """Run type checking with mypy."""
    run_command(["mypy"] + TARGET_DIRECTORIES, "Type Checking with mypy")


def sort_imports():
    """Sort imports using isort."""
    run_command(["isort"] + TARGET_DIRECTORIES, "Sort Imports with isort")
