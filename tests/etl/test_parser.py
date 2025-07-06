# File: tests/etl/test_parser.py

import pytest
from runtime_analytics.etl.log_parser import parse_log_line

def test_valid_log_line():
    line = (
        "2025-07-06 14:22:31,123 INFO Export completed "
        "config_count:15 riskdate:2025-07-05 id:123 type:batch "
        "on 2025-07-06 2h:30m in duration:42 seconds."
    )

    parsed = parse_log_line(line)
    assert parsed is not None
    assert parsed["timestamp"] == "2025-07-06 14:22:31,123"
    assert parsed["config_count"] == "15"
    assert parsed["riskdate"] == "2025-07-05"
    assert parsed["id"] == "123"
    assert parsed["type"] == "batch"
    assert parsed["run_date"] == "2025-07-06"
    assert parsed["duration"] == "42"


def test_invalid_log_line():
    line = "This is a malformed log entry with missing fields."
    assert parse_log_line(line) is None


def test_partial_log_line():
    line = (
        "2025-07-06 14:22:31,123 INFO Export completed "
        "config_count:15 riskdate:2025-07-05 id:123 type:batch "
        "on 2025-07-06 duration:42 seconds."  # missing hour/minute
    )
    assert parse_log_line(line) is None
