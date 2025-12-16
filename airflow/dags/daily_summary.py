import sqlite3
from tabulate import tabulate  # Для красивой таблички, нужно установить: pip install tabulate
from config import SQLITE_DB_PATH

def show_daily_summary(limit=10):
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute(f'''
        SELECT summary_date, total_articles, unique_sources, top_category, top_country, avg_title_length
        FROM daily_summary
        ORDER BY summary_date DESC
        LIMIT {limit}
    ''')
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        print("Аналитика пока пустая.")
        return
    
    headers = ["Date", "Total articles", "Unique sources", "Top Category", "Top country", "Average title lenghth"]
    print(tabulate(rows, headers=headers, tablefmt="pretty"))

if __name__ == "__main__":
    show_daily_summary()
