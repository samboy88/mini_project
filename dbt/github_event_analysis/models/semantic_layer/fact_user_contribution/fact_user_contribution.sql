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
user AS (
    SELECT
        user_id,
        user_login
    FROM {{ ref('dim_user') }}
)
SELECT
	u.user_id,
	u.user_login,
	COUNT(events.event_id) 					AS count_of_contributions,
	COUNT(DISTINCT events.pull_request_id) 	AS count_of_pull_requests_contributed_to,
    COUNT(DISTINCT events.event_type_id) 	AS unique_event_type_count
FROM user u
LEFT JOIN fact_event events ON u.user_id = events.user_id
LEFT JOIN event_type ON events.event_type_id = event_type.event_type_id
GROUP BY
	u.user_id,
	u.user_login