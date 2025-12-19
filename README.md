# Streaming and Batch News Data Pipeline

Project Overview

This project implements a complete end-to-end data pipeline for collecting, cleaning, storing, and analyzing frequently updating real-world news data.

The pipeline combines streaming ingestion and batch processing and is orchestrated using Apache Airflow.  
Kafka is used as a message broker for streaming data, and SQLite is used for storage and analytics.

The project was developed as a Final Group Project for the Data Collection & Preparation course.

# Project Goal

The main goal of the project is to demonstrate the ability to:

  - Collect real-world data from an external API
  - Simulate streaming ingestion using Kafka
  - Clean and normalize raw data
  - Store structured data in a database
  - Perform daily analytical aggregation
  - Orchestrate all steps using Airflow DAGs

The pipeline strictly follows the assignment requirements and consists of three Airflow jobs.


# Data Source

API used: NewsData.io

Why this API:
  - Frequently updated new articles every few minutes
  - Structured JSON responses
  - Stable and documented
  - Provides real, meaningful data
  - Allowed category (high-frequency news API)
  - Not used in previous SIS assignments

Each article includes fields such as title, description, content, source, category, country, language, and publication date.



# Architecture Overview

High-level data flow:

  NewsData.io API
  DAG 1: Continuous Ingestion
  Kafka (raw_news topic)
  DAG 2: Hourly Cleaning & Storage
  SQLite (news_events table)
  DAG 3: Daily Analytics

SQLite - daily_summary table
Each stage is implemented as a separate Airflow DAG to ensure modularity and clarity.


# DAGs Description

 DAG 1: Continuous Ingestion API - Kafka

  - DAG name: `job1_news_ingestion`
  - Type: Pseudo-streaming manual / long-running
  - Purpose: Fetch fresh news articles from the API and publish raw JSON messages to Kafka
  - Kafka topic: `raw_news`

No cleaning or validation is performed at this stage.  
The goal is to preserve raw data exactly as received from the API.


 DAG 2: Hourly Cleaning & Storage Kafka  SQLite

  - DAG name: `job2_news_cleaning`
  - Schedule: `@hourly`
  - Purpose: Clean and normalize raw Kafka messages and store them in SQLite
  - Target table: `news_events`

Main cleaning steps:
  - Parse JSON messages
  - Remove invalid or incomplete records
  - Normalize categories and country fields
  - Convert timestamps to a consistent format
  - Add metadata fields `ingested_at`, `cleaned_at`

 DAG 3: Daily Analytics SQLite  SQLite

  - DAG name: `job3_daily_analytics`
  - Schedule: `@daily`
  - Purpose: Compute daily aggregated analytics from cleaned data
  - Target table: `daily_summary`

Computed metrics:
  - Total number of articles per day
  - Number of unique news sources
  - Most frequent category
  - Most frequent country
  - Average title length



# Storage Layer

The project uses SQLite as a lightweight relational database.

 Tables

 `news_events`
Stores cleaned news articles one row per article.

Includes:
  - Article metadata title, source, category, country, language
  - Content fields
  - Publication date
  - Ingestion and cleaning timestamps

 `daily_summary`
Stores aggregated daily analytics one row per day.

Includes:
  - Date
  - Total articles
  - Unique sources
  - Top category
  - Top country
  - Average title length



## Project Structure:
```
airflow/
│
├── dags/
│   ├── config.py
│   ├── job1_ingestion_dag.py
│   ├── job2_clean_store_dag.py
│   ├── job3_daily_summary_dag.py
│   ├── daily_summary.py
│   └── news_events.py
│
├── data/
│   └── news_app.db
│
├── airflow.cfg
├── airflow.db
├── airflow.db-shm
├── airflow.db-wal
├── airflow.db.backup
└── simple_auth_manager_passwords.json
│
report/
└── report(1).pdf
│
src/
│
├── __init__.py
├── db_utils.py
├── job1_producer.py
├── job2_cleaner.py
└── job3_analytics.py
│
data/
└── news_app.db
│
view_data.py
requirements.txt
.gitignore
```

 How to Run the Project


### 1. Create virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install dependencies
```
pip install -r requirements.txt
```



3. Run Kafka in KRaft mode

	Kafka is NOT stored in the repository (Kafka archive is added to .gitignore)
	
	Unpack Kafka
```
tar -xzf kafka_2.13-3.6.0.tgz
cd kafka_2.13-3.6.0
```
Initialize KRaft
```
export KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

bin/kafka-storage.sh format \
  -t $KAFKA_CLUSTER_ID \
  -c config/kraft/server.properties
```
Start Kafka
```
bin/kafka-server-start.sh config/kraft/server.properties
```


Create Kafka topic

In a new terminal:
```
cd kafka_2.13-3.6.0

bin/kafka-topics.sh \
  --create \
  --topic news_events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```
Check topics
```
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```


Run Airflow (standalone)
```
export AIRFLOW_HOME=$(pwd)/airflow
airflow standalone
```
``` After startup:
	•	Web UI: http://localhost:8080
	•	Login and password will be printed in the terminal
```



Airflow DAGs

In the Airflow UI, enable the following DAGs:

    job1_ingestion_dag
	
	job2_clean_store_dag
	
	job3_daily_summary_dag
	


View Data

Using Python scripts
```
python3 view_data.py
python3 daily_summary.py
python3 news_events.py
```


Using SQLite CLI
```
sqlite3 data/news.db

.tables
SELECT * FROM news_events LIMIT 10;
SELECT * FROM daily_summary;
```


 
# Results:
    Kafka is populated with real-time news data
    Cleaned data is stored in SQLite news_events
    Daily analytics are stored in SQLite daily_summary
    All DAGs complete successfully (green status in Airflow)
 
# Limitations & Future Improvements:
    SQLite is suitable for this project but not for large-scale systems
    API rate limits restrict ingestion speed
    More advanced analytics sentiment analysis could be added
    The pipeline can be extended to use PostgreSQL or cloud storage
 
# Authors:
    22B030425	Sagatkyzy Firuza	
    22B030417	Omar Alua	
    23B030349	Yesserkey Dana 
Final Group Project
Data Collection & Preparation

