import sqlite3
import os
import sys
# Добавляем путь к папке, где лежит config.py
PROJECT_ROOT = '/mnt/d/final_data_collection/airflow/dags'
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from config import SQLITE_DB_PATH


def init_database():
    """Initialize SQLite database with required tables"""
    os.makedirs(os.path.dirname(SQLITE_DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()
    
    # Create news_events table (cleaned data)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS news_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id TEXT UNIQUE,
        title TEXT NOT NULL,
        description TEXT,
        content TEXT,
        link TEXT,
        source_id TEXT,
        source_name TEXT,
        category TEXT,
        country TEXT,
        language TEXT,
        pubDate TEXT,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        cleaned_at TIMESTAMP
    )
    ''')
    
    # Create daily_summary table (analytics)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_summary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        summary_date DATE NOT NULL,
        total_articles INTEGER,
        unique_sources INTEGER,
        top_category TEXT,
        top_country TEXT,
        avg_title_length REAL,
        articles_by_category TEXT,
        articles_by_source TEXT,
        articles_by_country TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(summary_date)
    )
    ''')
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {SQLITE_DB_PATH}")


def get_connection():
    """Get database connection"""
    return sqlite3.connect(SQLITE_DB_PATH)


if __name__ == "__main__":
    init_database()
