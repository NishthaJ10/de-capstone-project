select
    post_id,
    count(*) as num_comments
from    
    {{ref('stg_posts')}}
group by 
    post_id
-- #new