import time
import json
import requests
from kafka import KafkaProducer
from datetime import datetime
from config import (
    NEWSDATA_BASE_URL,
    NEWSDATA_PARAMS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW,
    FETCH_INTERVAL_SECONDS,
    MAX_ITERATIONS
)


class NewsProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.session = requests.Session()
    
    def fetch_news(self):
        """Fetch news from NewsData.io API"""
        try:
            response = self.session.get(NEWSDATA_BASE_URL, params=NEWSDATA_PARAMS, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') == 'success':
                return data.get('results', [])
            else:
                print(f"API Error: {data}")
                return []
        except Exception as e:
            print(f"Error fetching news: {e}")
            return []
    
    def send_to_kafka(self, articles):
        """Send articles to Kafka topic"""
        sent_count = 0
        for article in articles:
            try:
                message = {
                    'data': article,
                    'ingested_at': datetime.utcnow().isoformat(),
                    'source': 'newsdata.io'
                }
                self.producer.send(KAFKA_TOPIC_RAW, value=message)
                sent_count += 1
            except Exception as e:
                print(f"Error sending to Kafka: {e}")
        
        self.producer.flush()
        return sent_count
    
    def run_once(self):
        """Run a single fetch and send cycle"""
        print(f"Fetching news at {datetime.utcnow()}")
        articles = self.fetch_news()
        
        if articles:
            sent = self.send_to_kafka(articles)
            print(f"✓ Fetched {len(articles)} articles, sent {sent} to Kafka")
        else:
            print("✗ No articles fetched")
        
        print("\n✓ NewsProducer completed successfully")
        self.producer.close()


def run_producer():
    """Entry point for Airflow"""
    producer = NewsProducer()
    producer.run_once()


if __name__ == "__main__":
    run_producer()
