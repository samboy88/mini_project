from airflow import DAG
from airflow.operators.python import PythonOperator
import time 
from datetime import datetime
from sqlalchemy import create_engine, Table, Column, MetaData, String, DateTime, JSON
from sqlalchemy.dialects.postgresql import insert
import requests


def github_get(url, GITHUB_TOKEN,params=None):
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()

def ingest_raw_core(owner, repo, events_list,DB_URL,GITHUB_TOKEN,pages=10, per_page=100):
    engine = create_engine(DB_URL)
    metadata = MetaData()
    raw_core_events = Table(
        "raw_core_events", metadata,
        Column("ingest_timestamp", DateTime),
        Column("repo", String(255)),
        Column("event_type", String(50)),
        Column("raw_payload", JSON),
        Column("source_url", String(500))
    )
    
    #Create table if it does not exist
    metadata.create_all(engine)
    repo_full = f"{owner}/{repo}"
    ingest_timestamp_now=datetime.now()
    with engine.begin() as conn:
        for endpoint in events_list:
            url = f"https://api.github.com/repos/{owner}/{repo}/{endpoint}"
            for page in range(1, pages + 1):
                if endpoint == "events" and page > 3:
                    break
                params = {"page": page, "per_page": per_page, "state": "all"}
                data = github_get(url, GITHUB_TOKEN,params=params)
                if not data:
                    break  
                for item in data:
                    conn.execute(
                        insert(raw_core_events).values(
                            ingest_timestamp=ingest_timestamp_now,
                            repo=repo_full,
                            event_type=endpoint,
                            raw_payload=item,
                            source_url=url
                        ).prefix_with("IGNORE")
                    )  
                time.sleep(0.5)  # avoid rate limits
                print(f"Retrieved page {page} for {endpoint} ({len(data)} total items)")

def run_ingestion():
    # CONFIGURATION - In real project, these can be automated and parameterized
    REPO_OWNER = "pallets" 
    REPO_NAME = "flask"
    GITHUB_TOKEN = "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" #Added for fetching historical data, but should be rotated/removed for security best practices. Consider using Airflow Variables or Secrets Manager for production use.
    DB_URL = "mysql+mysqlconnector://samboy_88:awesome_person@mysql:3306/github_event_analysis" # update with your actual DB credentials
    EVENTS= ["events","pulls","commits","issues","comments"] # we can add more endpoints here as needed
    ingest_raw_core(REPO_OWNER, REPO_NAME, EVENTS,DB_URL,GITHUB_TOKEN,pages=100)


with DAG(
    dag_id="raw_ingestion_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily", 
    catchup=False,
):
    ingest_raw = PythonOperator( 
        task_id="ingest_raw", 
        python_callable=run_ingestion, 
        )
