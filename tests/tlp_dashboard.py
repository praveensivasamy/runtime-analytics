import streamlit as st
import pandas as pd
import os
import re

st.set_page_config(page_title="TLP Health Dashboard", layout="wide")

log_path = os.path.expanduser("~/.tlp-health.log")
csv_path = os.path.expanduser("~/.tlp-threshold-history.csv")
state_path = os.path.expanduser("~/.tlp-threshold-state")

st.title("🔋 TLP Health Dashboard")

# Thresholds
st.subheader("⚙️ Charge Thresholds")
if os.path.exists(state_path):
    with open(state_path) as f:
        data = f.read().strip().split()
        st.write(f"Start Charge: `{data[0]}%` | Stop Charge: `{data[1]}%`")
else:
    st.warning("No threshold state found")

# Summary Cards
st.subheader("📊 Latest System Snapshot")
latest_data = {"Charge": None, "Capacity": None, "CPU": None, "GPU": None, "NVMe": None, "Fan": None}
if os.path.exists(log_path):
    with open(log_path) as f:
        for line in reversed(f.readlines()):
            if "Charge" in line and latest_data["Charge"] is None:
                match = re.search(r"Charge\\s+=\\s+([0-9.]+)", line)
                if match: latest_data["Charge"] = float(match.group(1))
            elif "Capacity" in line and latest_data["Capacity"] is None:
                match = re.search(r"Capacity\\s+=\\s+([0-9.]+)", line)
                if match: latest_data["Capacity"] = float(match.group(1))
            elif "CPU:" in line and latest_data["CPU"] is None:
                match = re.search(r"CPU:\\s+\\+([0-9.]+)", line)
                if match: latest_data["CPU"] = float(match.group(1))
            elif "GPU:" in line and latest_data["GPU"] is None:
                match = re.search(r"GPU:\\s+\\+([0-9.]+)", line)
                if match: latest_data["GPU"] = float(match.group(1))
            elif "NVMe:" in line and latest_data["NVMe"] is None:
                match = re.search(r"NVMe:\\s+\\+([0-9.]+)", line)
                if match: latest_data["NVMe"] = float(match.group(1))
            elif "fan1" in line and latest_data["Fan"] is None:
                match = re.search(r"fan1:\\s+([0-9]+)", line)
                if match: latest_data["Fan"] = int(match.group(1))
            if all(v is not None for v in latest_data.values()):
                break

cols = st.columns(3)
cols[0].metric("🔋 Charge", f"{latest_data['Charge']}%", delta=None)
cols[1].metric("💾 Capacity", f"{latest_data['Capacity']}%", delta=None)
cols[2].metric("🌬️ Fan RPM", f"{latest_data['Fan']} RPM" if latest_data['Fan'] else "N/A", delta=None)

cols = st.columns(3)
cols[0].metric("🌡️ CPU Temp", f"{latest_data['CPU']}°C" if latest_data['CPU'] else "N/A")
cols[1].metric("🖥️ GPU Temp", f"{latest_data['GPU']}°C" if latest_data['GPU'] else "N/A")
cols[2].metric("💽 NVMe Temp", f"{latest_data['NVMe']}°C" if latest_data['NVMe'] else "N/A")

# Historical Threshold Change
st.subheader("📈 Threshold Change History")
if os.path.exists(csv_path):
    try:
        df = pd.read_csv(csv_path, names=["timestamp", "start", "stop", "prev_start", "prev_stop"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df_plot = df[["timestamp", "start", "stop"]].set_index("timestamp")
        st.line_chart(df_plot)
    except Exception as e:
        st.error(f"Failed to read threshold history: {e}")
else:
    st.info("No threshold history available.")

# Battery + Fan Health
st.subheader("📜 Raw Health Log (Last 20 lines)")
if os.path.exists(log_path):
    with open(log_path) as f:
        lines = f.readlines()[-20:]
        for line in lines:
            st.text(line.strip())
else:
    st.warning("No health log found")

st.caption("Built with ❤️ using Streamlit")