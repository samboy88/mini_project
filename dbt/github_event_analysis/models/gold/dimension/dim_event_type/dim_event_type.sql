{{ config(materialized='table') }}

select 
    MD5(CONCAT(CONVERT(type , CHAR)  ) )              AS event_type_id,
    type                                       as event_type
from {{ ref('github_clean_src_events') }}
where type is not null
GROUP BY 1,2
