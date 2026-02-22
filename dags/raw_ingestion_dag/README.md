# Raw Ingestion DAG Folder

This folder is part of the GitHub Event Analysis project and contains Airflow DAGs/scripts for ingesting raw GitHub event data into a MySQL database. It is a key component in the project's data pipeline, enabling automated extraction and storage of event data for further transformation and analytics.

## Folder Structure
- **scheduler.py**: Main Airflow DAG script for raw data ingestion.
- **README.md**: Documentation for this folder.

## Usage
1. Place this folder in your Airflow DAGs directory.
2. Ensure Airflow and MySQL are running (see project-level README).
3. The DAG defined in `scheduler.py` will be scheduled to run daily, ingesting new GitHub events.

## Database Model
The ingestion process creates and populates a table named `raw_core_events` in the MySQL database. The schema is:

| Column           | Type         | Description                       |
|------------------|-------------|-----------------------------------|
| ingest_timestamp | DateTime     | Timestamp of ingestion            |
| repo             | String(255)  | GitHub repository name            |
| event_type       | String(50)   | Type of GitHub event              |
| raw_payload      | JSON         | Raw event data from GitHub        |
| source_url       | String(500)  | Source API URL                    |

This table acts as the landing zone for all raw event data, supporting downstream transformations and analytics.

## Dataflow Architecture

```mermaid
graph TD
	A[GitHub API] --> B[Airflow DAG (scheduler.py)]
	B --> C[MySQL: raw_core_events]
	C --> D[dbt Transformations]
	D --> E[Data Warehouse Models]
```

- **A:** Data is fetched from the GitHub API for specified repositories and event types.
- **B:** Airflow DAG orchestrates the extraction and loads data into MySQL.
- **C:** Raw data is stored in the `raw_core_events` table.
- **D:** dbt transforms raw data into structured models (bronze, silver, gold layers).
- **E:** Final models are used for analytics and reporting.

## Best Practices
- Use environment variables or Airflow secrets for sensitive credentials (e.g., GitHub token).
- Monitor DAG execution and logs via Airflow UI.
- Test ingestion and transformations before production deployment.

## Contact
For questions or improvements, contact the project maintainer or open an issue in the main repository.
