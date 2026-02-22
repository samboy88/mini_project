{{ config(materialized='table') }}

WITH CTE_RAW AS
(
select distinct
    user_id                                         as source_user_id,
    user_login                                      as user_login
from {{ ref('github_clean_src_events') }}
where user_id is not null

UNION ALL

select distinct
    author_id                                       as source_user_id,
    author_login                                    as user_login
from {{ ref('github_clean_src_pull') }}
where author_id is not null
)
SELECT 
    MD5(CONCAT(CONVERT(source_user_id , CHAR), '~',
        CONVERT(user_login , CHAR)  ) )           AS user_id,
        source_user_id,
        user_login
FROM   CTE_RAW
GROUP BY 1,2,3      



