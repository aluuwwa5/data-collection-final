import json
import sqlite3
from datetime import datetime
from kafka import KafkaConsumer
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW,
    SQLITE_DB_PATH,
    BATCH_SIZE
)


class NewsDataCleaner:
    def __init__(self):
        self.conn = sqlite3.connect(SQLITE_DB_PATH)
        self.cursor = self.conn.cursor()
    
    def clean_article(self, raw_article):
        """Clean and validate article data"""
        data = raw_article.get('data', {})
        
        article_id = data.get('article_id', '')
        title = data.get('title', '').strip()
        description = data.get('description', '').strip() if data.get('description') else None
        content = data.get('content', '').strip() if data.get('content') else None
        link = data.get('link', '').strip()
        
        source_id = data.get('source_id', '')
        source_name = data.get('source_name', 'Unknown')
        
        category = ','.join(data.get('category', [])) if isinstance(data.get('category'), list) else data.get('category', 'uncategorized')
        country = ','.join(data.get('country', [])) if isinstance(data.get('country'), list) else data.get('country', 'unknown')
        language = data.get('language', 'en')
        pub_date = data.get('pubDate', '')
        
        if not article_id or not title:
            return None
        
        if len(title) < 10:
            return None
        
        title = ' '.join(title.split())
        if description:
            description = ' '.join(description.split())
        if content:
            content = ' '.join(content.split())
        
        return {
            'article_id': article_id,
            'title': title,
            'description': description,
            'content': content,
            'link': link,
            'source_id': source_id,
            'source_name': source_name,
            'category': category.lower(),
            'country': country.lower(),
            'language': language.lower(),
            'pubDate': pub_date,
            'cleaned_at': datetime.utcnow().isoformat()
        }
    
    def insert_article(self, article):
        """Insert cleaned article into database"""
        try:
            self.cursor.execute('''
            INSERT OR IGNORE INTO news_events 
            (article_id, title, description, content, link, source_id, source_name, 
             category, country, language, pubDate, cleaned_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                article['article_id'],
                article['title'],
                article['description'],
                article['content'],
                article['link'],
                article['source_id'],
                article['source_name'],
                article['category'],
                article['country'],
                article['language'],
                article['pubDate'],
                article['cleaned_at']
            ))
            return True
        except sqlite3.IntegrityError:
            return False
        except Exception as e:
            print(f"Error inserting article: {e}")
            return False
    
    def process_batch(self):
        """Read from Kafka, clean, and store in SQLite"""
        consumer = KafkaConsumer(
            KAFKA_TOPIC_RAW,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='news_cleaner_group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=30000
        )
        
        processed = 0
        inserted = 0
        skipped = 0
        
        print(f"Starting batch processing from Kafka topic '{KAFKA_TOPIC_RAW}'")
        
        for message in consumer:
            try:
                raw_article = message.value
                cleaned = self.clean_article(raw_article)
                
                if cleaned:
                    if self.insert_article(cleaned):
                        inserted += 1
                    else:
                        skipped += 1
                else:
                    skipped += 1
                
                processed += 1
                
                if processed >= BATCH_SIZE:
                    break
                    
            except Exception as e:
                print(f"Error processing message: {e}")
                skipped += 1
        
        self.conn.commit()
        consumer.close()
        
        print(f"✓ Batch complete: {processed} processed, {inserted} inserted, {skipped} skipped")
        return processed, inserted, skipped
    
    def close(self):
        self.conn.close()


def run_cleaner():
    """Entry point for Airflow"""
    cleaner = NewsDataCleaner()
    try:
        cleaner.process_batch()
    finally:
        cleaner.close()


if __name__ == "__main__":
    run_cleaner()
