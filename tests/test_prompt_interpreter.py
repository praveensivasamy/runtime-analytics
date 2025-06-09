import pandas as pd
import sqlite3
from runtime_analytics.prompt_interpreter import interpret_prompt
from runtime_analytics.analyzer import top_n_jobs


def test_top_5_slow_jobs_this_week():
    # Step 1: Load sample data from DB
    conn = sqlite3.connect("runtime_analytics/logs.db")
    df = pd.read_sql("SELECT * FROM job_logs_with_predictions", conn)

    # Step 2: Simulate user prompt
    prompt = "Top 5 slow jobs this week"
    result = interpret_prompt(prompt)

    # Step 3: Run the matched function with parameters
    assert result["function"] == "top_n_jobs"
    output = top_n_jobs(df, **result["params"])

    # Step 4: Validate output
    assert not output.empty, "Expected non-empty output"
    assert len(output) <= 5, "Expected max 5 results"
    for col in ["riskdate", "id", "type"]:
        assert col in output.columns, f"Missing key column: {col}"

    print("✅ test_top_5_slow_jobs_this_week passed.")
