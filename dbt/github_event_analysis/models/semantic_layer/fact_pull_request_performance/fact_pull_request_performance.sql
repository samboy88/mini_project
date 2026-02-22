{{ config(materialized='table') }}

WITH 
fact_event AS (
    SELECT 
        event_id,
        event_type_id,
        pull_request_id,
        user_id,
		_ingest_timestamp
    FROM {{ ref('fact_event') }}
    WHERE pull_request_id IS NOT NULL
),
event_type AS (
    SELECT
        event_type_id,
        event_type
    FROM {{ ref('dim_event_type') }}
),
pr AS (
	SELECT *
	FROM {{ ref('dim_pull_request') }}
)
SELECT
	pr.pull_request_id,
	pr.repo_name,
	pr.source_pr_id,
	pr.source_pr_number,
	pr.title,
	pr.current_state,
	pr.is_draft,
	pr.pr_created_date,
	pr.pr_merged_date,
	pr.pr_closed_date,
	pr.pr_updated_date,
	pr.source_author_id,
	pr.source_assignee_id,
	pr.source_closed_by_user_id,
	pr.merge_commit_sha,
	pr._load_date,
	pr._ingest_timestamp,
	COUNT(events.event_id) AS event_count,
	MIN(events._ingest_timestamp) AS first_event_time,
	MAX(events._ingest_timestamp) AS last_event_time,
	DATEDIFF(pr.pr_merged_date, pr.pr_created_date) AS time_to_merge_days,
	DATEDIFF(pr.pr_closed_date, pr.pr_created_date) AS time_to_close_days,
	DATEDIFF(
        COALESCE(pr.pr_closed_date, pr.pr_merged_date, pr.pr_updated_date), 
        pr.pr_created_date) AS pr_lifetime_days
FROM pr
LEFT JOIN fact_event events ON pr.pull_request_id = events.pull_request_id
LEFT JOIN event_type ON events.event_type_id = event_type.event_type_id
GROUP BY
	pr.pull_request_id,
	pr.repo_name,
	pr.source_pr_id,
	pr.source_pr_number,
	pr.title,
	pr.current_state,
	pr.is_draft,
	pr.pr_created_date,
	pr.pr_merged_date,
	pr.pr_closed_date,
	pr.pr_updated_date,
	pr.source_author_id,
	pr.source_assignee_id,
	pr.source_closed_by_user_id,
	pr.merge_commit_sha,
	pr._load_date,
	pr._ingest_timestamp
