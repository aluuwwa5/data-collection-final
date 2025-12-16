import json
import sqlite3
from datetime import datetime, timedelta
from collections import Counter
from config import SQLITE_DB_PATH, ANALYTICS_LOOKBACK_DAYS


class NewsAnalytics:
    def __init__(self):
        self.conn = sqlite3.connect(SQLITE_DB_PATH)
        self.cursor = self.conn.cursor()
    
    def get_recent_articles(self):
        """Fetch articles from the last 24 hours"""
        yesterday = (datetime.utcnow() - timedelta(days=1)).isoformat()
        
        self.cursor.execute('''
        SELECT article_id, title, description, source_name, category, country, language, pubDate
        FROM news_events
        WHERE cleaned_at >= ?
        ''', (yesterday,))
        
        columns = ['article_id', 'title', 'description', 'source_name', 'category', 'country', 'language', 'pubDate']
        articles = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        
        return articles
    
    def compute_analytics(self, articles):
        """Compute aggregated metrics"""
        if not articles:
            return None
        
        total_articles = len(articles)
        unique_sources = len(set(a['source_name'] for a in articles if a['source_name']))
        
        categories = [a['category'] for a in articles if a['category']]
        category_counts = Counter(categories)
        top_category = category_counts.most_common(1)[0][0] if category_counts else 'unknown'
        
        countries = []
        for a in articles:
            if a['country']:
                countries.extend(a['country'].split(','))
        country_counts = Counter(countries)
        top_country = country_counts.most_common(1)[0][0] if country_counts else 'unknown'
        
        source_counts = Counter(a['source_name'] for a in articles if a['source_name'])
        
        title_lengths = [len(a['title']) for a in articles if a['title']]
        avg_title_length = sum(title_lengths) / len(title_lengths) if title_lengths else 0
        
        analytics = {
            'summary_date': datetime.utcnow().date().isoformat(),
            'total_articles': total_articles,
            'unique_sources': unique_sources,
            'top_category': top_category,
            'top_country': top_country,
            'avg_title_length': round(avg_title_length, 2),
            'articles_by_category': json.dumps(dict(category_counts.most_common(10))),
            'articles_by_source': json.dumps(dict(source_counts.most_common(10))),
            'articles_by_country': json.dumps(dict(country_counts.most_common(10)))
        }
        
        return analytics
    
    def save_summary(self, analytics):
        """Save analytics to daily_summary table"""
        try:
            self.cursor.execute('''
            INSERT OR REPLACE INTO daily_summary 
            (summary_date, total_articles, unique_sources, top_category, top_country,
             avg_title_length, articles_by_category, articles_by_source, articles_by_country)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                analytics['summary_date'],
                analytics['total_articles'],
                analytics['unique_sources'],
                analytics['top_category'],
                analytics['top_country'],
                analytics['avg_title_length'],
                analytics['articles_by_category'],
                analytics['articles_by_source'],
                analytics['articles_by_country']
            ))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error saving summary: {e}")
            return False
    
    def run_analytics(self):
        """Run daily analytics job"""
        print(f"Starting daily analytics for {datetime.utcnow().date()}")
        
        articles = self.get_recent_articles()
        print(f"Found {len(articles)} articles from last 24 hours")
        
        if not articles:
            print("No articles to analyze")
            return
        
        analytics = self.compute_analytics(articles)
        
        if analytics:
            print("\n=== Daily Summary ===")
            print(f"Date: {analytics['summary_date']}")
            print(f"Total Articles: {analytics['total_articles']}")
            print(f"Unique Sources: {analytics['unique_sources']}")
            print(f"Top Category: {analytics['top_category']}")
            print(f"Top Country: {analytics['top_country']}")
            print(f"Avg Title Length: {analytics['avg_title_length']}")
            
            if self.save_summary(analytics):
                print("\n✓ Analytics saved to daily_summary table")
            else:
                print("\n✗ Failed to save analytics")
        
    def close(self):
        self.conn.close()


def run_analytics():
    """Entry point for Airflow"""
    analytics = NewsAnalytics()
    try:
        analytics.run_analytics()
    finally:
        analytics.close()


if __name__ == "__main__":
    run_analytics()
