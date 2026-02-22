# dwh_load Scheduler

This folder contains Airflow DAGs and scripts responsible for loading data into the Data Warehouse (DWH).

## Contents
- **scheduler.py**: Main script for scheduling and orchestrating DWH load tasks.

## Purpose
The scripts in this folder automate the process of loading, transforming, and updating data in the data warehouse. They are typically triggered by Airflow and may include tasks such as:
- Extracting data from source systems
- Transforming data for analytics
- Loading data into DWH tables

## Usage
1. Ensure Airflow is running (see project-level README for setup).
2. DAGs in this folder will be picked up by Airflow automatically if the folder is included in the Airflow DAGs path.
3. Monitor and manage DAG execution via the Airflow UI.

## Customization
- Modify `scheduler.py` to add, update, or remove DWH load tasks as needed.
- Add additional scripts for new data sources or transformations.

## Best Practices
- Keep scripts modular and reusable.
- Log all operations for traceability.
- Test DAGs and scripts before deploying to production.

## Contact
For questions or improvements, please contact the project maintainer or open an issue in the main repository.
