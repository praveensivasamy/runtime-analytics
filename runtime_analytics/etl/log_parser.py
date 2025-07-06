# File: runtime_analytics/etl/parser.py

import re
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)

# Compile once for efficiency
LOG_PATTERN = re.compile(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) INFO Export completed "
    r"config_count:(?P<config_count>\d+) "
    r"riskdate:(?P<riskdate>\d{4}-\d{2}-\d{2}) "
    r"id:(?P<id>\d+) "
    r"type:(?P<type>\w+) "
    r"on (?P<run_date>\d{4}-\d{2}-\d{2}) \d+h:\d+m in duration:(?P<duration>\d+) seconds\."
)


def parse_log_line(line: str) -> Optional[Dict[str, str]]:
    """
    Parses a single log line using a fixed format.
    Returns a dictionary of fields if matched, or None if not matched.
    """
    match = LOG_PATTERN.match(line.strip())
    if match:
        return match.groupdict()
    return None  # or optionally: logger.debug(f"Unmatched log line: {line.strip()}")
