
  
    

create or replace transient table USER_ACTIVITY_DB.ANALYTICS.fct_posts
    
    
    
    as (select
    p.post_id,
    p.user_id,
    p.title,
    coalesce(c.num_comments, 0) as number_of_comments
from
    USER_ACTIVITY_DB.ANALYTICS.stg_posts p
left join
    USER_ACTIVITY_DB.ANALYTICS.int_comments_per_post c on p.post_id = c.post_id
    )
;


  