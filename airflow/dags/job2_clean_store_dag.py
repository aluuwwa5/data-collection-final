from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys
import os

PROJECT_ROOT = '/mnt/d/final_data_collection'
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='job2_news_cleaning',
    default_args=default_args,
    description='Hourly batch: Read Kafka, clean data, store in SQLite',
    schedule='@hourly',
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=['news', 'cleaning', 'sqlite'],
)
def news_cleaning_dag():
    @task(task_id='clean_and_store_news')
    def run_news_cleaner():
        import sys
        project_root = '/mnt/d/final_data_collection'
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        
        from src.job2_cleaner import run_cleaner
        run_cleaner()
        return "News cleaning completed"
    
    run_news_cleaner()

news_cleaning_dag()