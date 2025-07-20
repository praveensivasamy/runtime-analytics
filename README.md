# 📊 Runtime Analytics Dashboard

A lightweight Streamlit-based analytics tool to visualize and explore batch job logs.  
It enables users to analyze job durations, track anomalies, compare performance across dates, and explore predefined metrics interactively.

---

## 🔥 Key Features

- **Predefined Prompt Explorer**  
  Run curated data prompts like:
  - Top slow jobs (yesterday/week/month/year)
  - Job count by type
  - Unique jobs per day
  - Duration aggregation by type
  - Prediction accuracy
  - Top anomaly scores

- **Custom Date Range Support**  
  Certain prompts allow flexible date filtering with `start_date` and `end_date`.

- **Duration Comparison by Job Type**  
  Visualize side-by-side duration trends for a selected date vs. a reference date (default: 7 days before).  
  Includes:
  - Grouped bar chart with color-coded deltas and trend arrows
  - Tooltip showing job counts per type
  - CSV export for tabular view

- **Drilldown by Job ID**  
  - Select a job type and compare individual job durations across two dates.
  - Hover tooltips show `ID`, `config_count`, and `run_date`.
  - Trend arrows displayed above bars.
  - Auto-selects latest timestamp per `ID`.

- **Export Options**  
  Export results to CSV:
  - Summary comparison
  - Drilldown detail

---

## 🧰 Technology Used

- **Python 3.12**
- **Streamlit** – UI rendering and interactivity
- **Pandas** – Data manipulation and transformation
- **Plotly** – Interactive bar charts and annotations
- **SQLite** – Lightweight data storage backend
- **Shiv** – For production packaging as `.pyz` bundle

---

## 📌 Supported Prompts

These are available under the **Predefined Prompt Explorer** tab:

| Prompt Name                        | Description                              |
|-----------------------------------|------------------------------------------|
| Top slow jobs (yesterday/week/…)  | Ranks slowest jobs by total duration     |
| Job Count by Type                 | Aggregates job volume per type           |
| Unique Jobs Per Day               | Shows job ID uniqueness per run_date     |
| Average Duration by Type          | Mean duration aggregated per job type    |
| Prediction Accuracy per Job Type  | Compares predicted vs. actual duration   |
| Top Anomaly Scores                | Lists outliers with highest anomaly score|
| Top Slow Jobs for Date Range      | Select custom date range for analysis    |

---

## 📁 Project Structure

This app is modularized into:
- `repositories/` – database interaction layer
- `services/` – data analytics & transformations
- `utils/` – charting and helpers
- `gui/components/` – Streamlit tabs and GUI logic

---

## 📦 Deployment

The app is designed for production using [**shiv**](https://github.com/linkedin/shiv) to bundle the entire codebase as a single `.pyz` executable.  
No server is needed — perfect for internal or offline use.

---