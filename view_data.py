import sqlite3
import json

conn = sqlite3.connect('data/news_app.db')
cursor = conn.cursor()

print("=== NEWS EVENTS ===")
cursor.execute("SELECT COUNT(*) FROM news_events")
print(f"Total articles: {cursor.fetchone()[0]}")

print("\n=== TOP 5 ARTICLES ===")
cursor.execute("SELECT title, source_name, category FROM news_events LIMIT 5")
for row in cursor.fetchall():
    print(f"- {row[0][:60]}... ({row[1]}, {row[2]})")

print("\n=== DAILY SUMMARY ===")
cursor.execute("SELECT * FROM daily_summary ORDER BY summary_date DESC LIMIT 1")
summary = cursor.fetchone()
if summary:
    print(f"Date: {summary[1]}")
    print(f"Total: {summary[2]}")
    print(f"Sources: {summary[3]}")
    print(f"Top Category: {summary[4]}")
    print(f"Top Country: {summary[5]}")

conn.close()
