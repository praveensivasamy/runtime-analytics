import random
from datetime import datetime, timedelta

# Configuration
types = ["SNSI", "STDV", "STRV", "PSTR", "FSTR"]
ids = list(range(1, 68))  # IDs from 1 to 67
base_risk_date = datetime(2025, 4, 1)
base_run_date = datetime(2025, 6, 1)
log_lines = []

# Generate 100 dummy log lines
for _ in range(100):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    config_count = random.randint(50, 150)
    risk_date = (base_risk_date + timedelta(days=random.randint(0, 10))).date()
    run_id = random.choice(ids)
    log_type = random.choice(types)
    run_date = (base_run_date + timedelta(days=random.randint(0, 5))).date()
    duration_sec = random.randint(60, 600)
    hours = duration_sec // 3600
    minutes = (duration_sec % 3600) // 60
    duration_str = f"{hours}h:{minutes}m"

    line = (
        f"{timestamp} INFO Export completed config_count:{config_count} "
        f"riskdate:{risk_date} id:{run_id} type:{log_type} "
        f"on {run_date} {duration_str} in duration:{duration_sec} seconds."
    )

    log_lines.append(line)

# Save to a log file
log_file_path = "/mnt/data/dummy_runtime_logs.txt"
with open(log_file_path, "w") as f:
    for line in log_lines:
        f.write(line + "\n")

log_file_path
