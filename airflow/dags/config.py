
import os

NEWSDATA_API_KEY = "pub_5efaf91e66f94c27bcbc798111d25bf6" 
NEWSDATA_BASE_URL = "https://newsdata.io/api/1/news"
NEWSDATA_PARAMS = {
    "apikey": NEWSDATA_API_KEY,
    "language": "en",
    "category": "technology,business,science",
}

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC_RAW = 'news_raw'

# SQLite Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_PATH = os.path.join(BASE_DIR, 'data', 'news_app.db')

# Job 1 Configuration
FETCH_INTERVAL_SECONDS = 120

MAX_ITERATIONS = 1
# Job 2 Configuration
BATCH_SIZE = 100

# Job 3 Configuration
ANALYTICS_LOOKBACK_DAYS = 7

