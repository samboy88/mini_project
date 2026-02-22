{{ config(materialized='incremental', unique_key='event_id') }}

SELECT 
   MD5(CONCAT(CONVERT(repo , CHAR), '~',
        CONVERT(ID , CHAR)  ) )             AS event_id,
    dim_event_type.event_type_id              AS event_type_id,
    dim_pr.pull_request_id                  AS pull_request_id,
    dim_user.user_id                        AS user_id,
    dim_repo.repo_id                        AS repo_id,
    stg.ID                                  AS source_event_id,
    stg.issue_number                        AS source_issue_number,
    stg.action                              AS action,
    CURRENT_TIMESTAMP                       AS _load_date,
    stg._ingest_timestamp                   AS _ingest_timestamp
FROM {{ ref('github_clean_src_events') }} stg
    INNER JOIN {{ ref('dim_pull_request') }} dim_pr 
        ON stg.pr_id = dim_pr.source_pr_id
    INNER JOIN {{ ref('dim_user') }} dim_user 
        ON stg.user_id = dim_user.source_user_id
    INNER JOIN {{ ref('dim_repo') }} dim_repo 
        ON stg.repo_id = dim_repo.source_repo_id
    INNER JOIN {{ ref('dim_event_type') }} dim_event_type
        ON stg.type = dim_event_type.event_type
    WHERE TRUE
{% if is_incremental() %}
    AND stg._ingest_timestamp > (select max(_ingest_timestamp) from {{ this }})
{% endif %}
