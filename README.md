# Runtime Analytics 🧠

A Python project for analyzing structured runtime logs using both CLI and GUI interfaces.

## 🚀 Features

- Prompt-based log analysis
- CLI and Streamlit GUI modes
- Export to CSV/JSON
- Tags and categories for prompt filtering
- Multi-prompt comparison

## 🧩 Usage

### CLI Mode (default)

```bash
python -m runtime_analytics
```

### List Prompts

```bash
python -m runtime_analytics --list-prompts
```

### Export to CSV or JSON

```bash
python -m runtime_analytics --export "job count by type" --format json
```

### GUI Mode

```bash
python -m runtime_analytics --mode gui
```

## 📁 Logs

Place your `.txt` log files inside:

```
runtime_analytics_project/logs/
```

## 📦 Install Requirements

```bash
pip install -r requirements.txt
```

## 🧪 Example Prompts

- `sns duration by riskdate`
- `job count by type`
- `stdv > 300`
