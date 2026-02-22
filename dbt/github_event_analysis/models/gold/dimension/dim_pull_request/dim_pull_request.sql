{{ config(materialized='table', unique_key='pull_request_id') }}

SELECT 
   MD5(CONCAT(CONVERT(repo , CHAR), '~',
        CONVERT(ID , CHAR)  ) )             AS pull_request_id,
    repo                                    AS repo_name,
    ID                                      AS source_pr_id,
    number                                  AS source_pr_number,
    title                                   AS title,
    state                                   AS current_state,
    is_draft                                AS is_draft,
    author_id                               AS source_author_id,
    assignee                                AS source_assignee_id,
    created_at                              AS pr_created_date,
    updated_at                              AS pr_updated_date,
    merged_at                               AS pr_merged_date,
    merge_commit_sha                        AS merge_commit_sha,
    closed_at                               AS pr_closed_date,
    closed_by                               AS source_closed_by_user_id,
    CURRENT_TIMESTAMP                       AS _load_date,
    _ingest_timestamp                       AS _ingest_timestamp

FROM {{ ref('github_clean_src_pull') }}
    WHERE TRUE
{% if is_incremental() %}
    AND _ingest_timestamp > (select max(_ingest_timestamp) from {{ this }})
{% endif %}
