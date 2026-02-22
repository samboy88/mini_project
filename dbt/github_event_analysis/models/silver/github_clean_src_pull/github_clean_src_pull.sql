{{ config(materialized='table', unique_key='id') }}

SELECT 
    repo,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.id'))                 as ID,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.base.repo.id'))       as repo_id,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.base.repo.name'))     as repo_name,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.state'))              as state,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.draft'))              as is_draft,
    CAST(
            replace(replace(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.merged_at')), 'null'), 'T', ' '), 'Z', '')
    AS DATETIME) AS merged_at,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.merge_commit_sha'))   as merge_commit_sha,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.title') )             as title,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.number'))             as number,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.user.id') )           as author_id,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.user.login'))         as author_login,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.assignee'))           as assignee,
    CAST(
            replace(replace(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.closed_at')), 'null'), 'T', ' '), 'Z', '')
        AS DATETIME)      as closed_at,
    JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.closed_by'))          as closed_by,
    CAST(
            replace(replace(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.created_at')), 'null'), 'T', ' '), 'Z', '')
    AS DATETIME)      as created_at,
    CAST(
            replace(replace(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(raw_payload, '$.updated_at')), 'null'), 'T', ' '), 'Z', '')
    AS DATETIME)      as updated_at,
    CURRENT_TIMESTAMP                                               AS _load_date,
    ingest_timestamp                                                AS _ingest_timestamp

FROM {{ source('raw', 'raw_core_events') }}
    WHERE TRUE
    AND event_type = 'pulls'

{% if is_incremental() %}
    AND ingest_timestamp > (select max(_ingest_timestamp) from {{ this }})
{% endif %}

