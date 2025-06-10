import sqlite3

from runtime_analytics.app_config.config import settings


def ensure_learning_table_exists():
    conn = sqlite3.connect(settings.log_db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS learned_prompts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            matched_prompt TEXT,
            confidence REAL,
            accepted INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    conn.close()


def log_prompt_learning(query: str, matched_prompt: str, confidence: float, accepted: bool = False):
    ensure_learning_table_exists()
    conn = sqlite3.connect(settings.log_db_path)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO learned_prompts (query, matched_prompt, confidence, accepted)
        VALUES (?, ?, ?, ?);
    """, (query, matched_prompt, confidence, int(accepted)))
    conn.commit()
    conn.close()
