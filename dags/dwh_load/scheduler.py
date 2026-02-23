from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator 
from datetime import datetime
import time
from datetime import datetime
from airflow.sensors.external_task import ExternalTaskSensor

import os

DBT_DIR = "/opt/airflow/dbt/github_event_analysis"


with DAG(
    dag_id="dwh_load",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
):
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"cd {DBT_DIR} && dbt run --select silver"
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f"cd {DBT_DIR} && dbt test --select silver"
    )

    dbt_run_dim = BashOperator(
        task_id="dbt_run_dim",
        bash_command=f"cd {DBT_DIR} && dbt run --select dimension"
    )

    dbt_test_dim = BashOperator(
        task_id="dbt_test_dim",
        bash_command=f"cd {DBT_DIR} && dbt test --select dimension"
    )


    dbt_run_fact = BashOperator(
        task_id="dbt_run_fact",
        bash_command=f"cd {DBT_DIR} && dbt run --select fact"
    )

    dbt_test_fact = BashOperator(
        task_id="dbt_test_fact",
        bash_command=f"cd {DBT_DIR} && dbt test --select fact"
    )

    dbt_run_semantic_layer = BashOperator(
        task_id="dbt_run_semantic_layer",
        bash_command=f"cd {DBT_DIR} && dbt run --select semantic_layer"
    )


    start >> dbt_run_silver >> dbt_test_silver
    dbt_test_silver >> dbt_run_dim >> dbt_test_dim
    dbt_test_dim >> dbt_run_fact >> dbt_test_fact >> dbt_run_semantic_layer >>end
