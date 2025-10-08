select
    distinct user_id
from
    {{ref('stg_posts')}}
group by
    user_id