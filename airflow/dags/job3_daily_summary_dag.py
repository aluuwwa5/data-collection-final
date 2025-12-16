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
    'retry_delay': timedelta(minutes=10),
}

@dag(
    dag_id='job3_daily_analytics',
    default_args=default_args,
    description='Daily analytics: Compute summary from SQLite',
    schedule='@daily',
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=['news', 'analytics', 'summary'],
)
def daily_analytics_dag():
    """
    DAG for computing daily analytics and summary statistics
    """
    
    @task(task_id='compute_daily_summary')
    def run_news_analytics():
        import sys
        # Добавляем путь ВНУТРИ task тоже
        project_root = '/mnt/d/final_data_collection'
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        
        from src.job3_analytics import run_analytics
        run_analytics()
        return "Analytics computation completed"
    
    # Execute task
    run_news_analytics()

# Instantiate the DAG
daily_analytics_dag()