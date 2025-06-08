import streamlit as st
import pandas as pd
import io
import time

from runtime_analytics.app_config.config import settings
from runtime_analytics.loader import load_logs_from_folder
from runtime_analytics.prompts import load_prompt_map_from_yaml
from runtime_analytics.app_db.sql_db import save_df_to_db, load_df_from_db, create_indexes

st.set_page_config(layout="wide", page_title="Runtime Analytics", page_icon="📊")
st.markdown("<h1 style='color:#4A90E2;'>Runtime Analytics Dashboard</h1>", unsafe_allow_html=True)

# Sidebar controls
mode = st.sidebar.radio("📁 Data Source", ["Parse from logs", "Live from DB"], index=0)
auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh every 30s", value=False)

if auto_refresh and mode == "Live from DB":
    time.sleep(30)
    st.experimental_rerun()

if mode == "Parse from logs":
    df = load_logs_from_folder(settings.log_dir)
    if df.empty:
        st.warning("No valid logs found.")
        st.stop()
    save_df_to_db(df, if_exists="append")  # incremental insert
    create_indexes()
    st.success("Parsed logs and appended to DB.")
else:
    df = load_df_from_db()
    if df.empty:
        st.warning("No data in the DB.")
        st.stop()

# Load prompts
prompt_map, metadata = load_prompt_map_from_yaml(settings.prompt_config_file)
prompt_names = list(prompt_map.keys())

# SQL templates
sql_templates = {
    "All logs": "SELECT * FROM logs",
    "Last 10 SNSI jobs": "SELECT * FROM logs WHERE type = 'SNSI' ORDER BY timestamp DESC LIMIT 10",
    "Long jobs over 5 min": "SELECT * FROM logs WHERE duration_sec > 300 ORDER BY duration_sec DESC",
}

# SQL query support
if mode == "Live from DB":
    st.sidebar.markdown("### SQL Query Template")
    selected_template = st.sidebar.selectbox("Templates", list(sql_templates.keys()))
    sql_code = st.sidebar.text_area("Or write your own SQL", sql_templates[selected_template], height=120)
    try:
        df = pd.read_sql(sql_code, f"sqlite:///{settings.log_db_path}")
        st.success("SQL query executed.")
    except Exception as e:
        st.error(f"SQL error: {e}")
        st.stop()

# Prompt filters
st.sidebar.markdown("### 🔍 Filter Prompts by Tag")
all_tags = sorted({tag for meta in metadata.values() for tag in meta["tags"]})
selected_tags = st.sidebar.multiselect("Tags", all_tags)

if selected_tags:
    filtered_prompts = [p for p in prompt_names if any(tag in metadata[p]["tags"] for tag in selected_tags)]
else:
    filtered_prompts = prompt_names

st.sidebar.markdown("### 📊 Choose One or More Prompts")
selected_prompts = st.sidebar.multiselect("Available Prompts", filtered_prompts)

for prompt in selected_prompts:
    st.subheader(f"📌 {prompt}")
    st.markdown(f"*{metadata[prompt]['description']}*")
    result = prompt_map[prompt](df)
    st.dataframe(result)

    if "duration" in prompt and "riskdate" in result.columns:
        st.line_chart(result.set_index("riskdate").select_dtypes(include="number"))

    csv_buffer = io.StringIO()
    result.to_csv(csv_buffer, index=False)
    st.download_button("⬇ Download CSV", data=csv_buffer.getvalue(), file_name=f"{prompt.replace(' ', '_')}.csv", mime="text/csv")

    json_buffer = result.to_json(orient="records", indent=2)
    st.download_button("⬇ Download JSON", data=json_buffer, file_name=f"{prompt.replace(' ', '_')}.json", mime="application/json")
