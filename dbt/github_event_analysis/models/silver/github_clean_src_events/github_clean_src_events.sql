{{ config(materialized='table', unique_key='id') }}

SELECT 
    repo,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.id'))                                                                     as ID,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.type'))                                                                   as type,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.repo.id'))                                                                as repo_id,
    SUBSTRING_INDEX(JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.repo.name')),'/',-1)                                      as repo_name,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.actor.id'))                                                               as user_id,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.actor.login'))                                                            as user_login,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.payload.number'))                                                         as issue_number,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.payload.action'))                                                         as action,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.payload.pull_request.id'))                                                as pr_id,
    SUBSTRING_INDEX(JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.payload.pull_request.url')),'/',-1)                       as pr_number,
    CAST(replace(replace(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.created_at')), 'null'), 'T', ' '), 'Z', '') AS DATETIME)      as created_at,
    CURRENT_TIMESTAMP                                                                                                   AS _load_date,
    ingest_timestamp                                                                                                    AS _ingest_timestamp

FROM {{ source('raw', 'raw_core_events') }}
    WHERE TRUE
        AND event_type = 'events' 
        AND (JSON_EXTRACT(raw_payload, '$.type')) IN ('PullRequestReviewEvent','CreateEvent','PullRequestEvent')

{% if is_incremental() %}
AND ingest_timestamp > (select max(_ingest_timestamp) from {{ this }})
{% endif %}
