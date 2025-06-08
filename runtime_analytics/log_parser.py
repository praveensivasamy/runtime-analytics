import re
from typing import Optional, Dict

def parse_log_line(line: str) -> Optional[Dict]:
    pattern = (
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) INFO Export completed "
        r"config_count:(?P<config_count>\d+) "
        r"riskdate:(?P<riskdate>\d{4}-\d{2}-\d{2}) "
        r"id:(?P<id>\d+) "
        r"type:(?P<type>\w+) "
        r"on (?P<run_date>\d{4}-\d{2}-\d{2}) (?P<duration_str>\d+h:\d+m) in duration:(?P<duration_sec>\d+) seconds\."
    )
    match = re.match(pattern, line.strip())
    return match.groupdict() if match else None
