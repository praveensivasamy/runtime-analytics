import random
from datetime import datetime, timedelta, time
from pathlib import Path

# Config
types = ["SNSI", "STDV", "STRV", "PSTR", "FSTR"]
ids = list(range(1, 21))
start_date = datetime(2025, 3, 1).date()
end_date = datetime.now().date()
log_lines = []

# Create timestamp on given date
def random_timestamp_on(date_obj):
    hour = random.randint(6, 22)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    microsecond = random.randint(0, 999999)
    dt = datetime.combine(date_obj, time(hour, minute, second, microsecond))
    return dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

# Return previous valid weekday for riskdate
def previous_weekday(date_obj):
    while date_obj.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        date_obj -= timedelta(days=1)
    return date_obj

# Generate logs
current_date = start_date
while current_date <= end_date:
    run_date = current_date
    risk_date = previous_weekday(run_date - timedelta(days=1))

    logs_today = random.randint(80, 120)
    for _ in range(logs_today):
        config_count = random.randint(50, 150)
        run_id = random.choice(ids)
        log_type = random.choice(types)
        duration_sec = random.randint(60, 600)
        hours = duration_sec // 3600
        minutes = (duration_sec % 3600) // 60
        duration_str = f"{hours}h:{minutes}m"
        timestamp = random_timestamp_on(run_date)

        line = (
            f"{timestamp} INFO Export completed config_count:{config_count} "
            f"riskdate:{risk_date} id:{run_id} type:{log_type} "
            f"on {run_date} {duration_str} in duration:{duration_sec} seconds."
        )
        log_lines.append(line)

    current_date += timedelta(days=1)

# Save log file
log_file_path = Path("../runtime_analytics/logs/dummy_runtime_logs.txt")
log_file_path.parent.mkdir(parents=True, exist_ok=True)
with open(log_file_path, "w") as f:
    f.write("\n".join(log_lines))

print(f"{len(log_lines)} logs written across {start_date} to {end_date} → {log_file_path.resolve()}")
