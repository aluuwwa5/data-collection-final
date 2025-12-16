import sqlite3
import json
from config import SQLITE_DB_PATH

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()

# Выбираем все строки (или с лимитом)
cursor.execute("SELECT * FROM news_events LIMIT 20")
rows = cursor.fetchall()

# Получаем имена колонок
columns = [description[0] for description in cursor.description]

# Выводим красиво
for row in rows:
    record = dict(zip(columns, row))
    print(json.dumps(record, indent=4, ensure_ascii=False))

conn.close()
