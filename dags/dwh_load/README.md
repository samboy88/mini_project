# dwh_load Scheduler

This folder contains Airflow DAGs and scripts responsible for loading data into the Data Warehouse (DWH).

## Contents
- **scheduler.py**: Main script for scheduling and orchestrating DWH load tasks.

## Purpose
The scripts in this folder automate the process of loading, transforming, and updating data in the data warehouse.

## Usage
1. Ensure Airflow is running (see project-level README for setup).
2. DAGs in this folder will be picked up by Airflow automatically if the folder is included in the Airflow DAGs path.
3. Monitor and manage DAG execution via the Airflow UI.