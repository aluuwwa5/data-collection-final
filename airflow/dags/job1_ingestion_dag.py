from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys
import os

# Добавляем путь к проекту ГЛОБАЛЬНО
PROJECT_ROOT = '/mnt/d/final_data_collection'
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='job1_news_ingestion',
    default_args=default_args,
    description='Continuous news ingestion from NewsData.io to Kafka',
    schedule='*/2 * * * *',
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=['news', 'ingestion', 'kafka'],
)
def news_ingestion_dag():
    """
    DAG for continuous news ingestion from NewsData.io API to Kafka
    """
    
    @task(task_id='fetch_and_produce_news')
    def run_news_producer():
        import sys
        # Добавляем путь ВНУТРИ task тоже
        project_root = '/mnt/d/final_data_collection'
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        
        from src.job1_producer import run_producer
        run_producer()
        return "News ingestion completed"
    
    # Execute task
    run_news_producer()

# Instantiate the DAG
news_ingestion_dag()