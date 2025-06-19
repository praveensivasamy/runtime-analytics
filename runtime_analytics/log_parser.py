import re


def parse_log_line(line: str) -> dict | None:
    pattern = (
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) INFO Export completed "
        r"config_count:(?P<config_count>\d+) "
        r"riskdate:(?P<riskdate>\d{4}-\d{2}-\d{2}) "
        r"id:(?P<id>\d+) "
        r"type:(?P<type>\w+) "
        r"on (?P<run_date>\d{4}-\d{2}-\d{2}) \d+h:\d+m in duration:(?P<duration>\d+) seconds\."
    )
    match = re.match(pattern, line.strip())
    return match.groupdict() if match else None
