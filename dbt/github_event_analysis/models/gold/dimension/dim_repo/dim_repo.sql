{{ config(materialized='table') }}

WITH src_raw AS 
(
select 
    repo_id                                         as source_repo_id,
    repo_name                                       as repo_name
from {{ ref('github_clean_src_events') }}
where repo_id is not null

UNION ALL

select 
    repo_id                                         as source_repo_id,
    repo_name                                       as repo_name
from {{ ref('github_clean_src_pull') }}
where repo_id is not null
)
SELECT 
    MD5(CONCAT(CONVERT(source_repo_id , CHAR), '~',
        CONVERT(repo_name , CHAR)  ) )              AS repo_id,
        source_repo_id,
        repo_name
FROM src_raw
GROUP BY 1,2,3

