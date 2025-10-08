
  
    

create or replace transient table USER_ACTIVITY_DB.ANALYTICS.dim_users
    
    
    
    as (select
    distinct user_id
from
    USER_ACTIVITY_DB.ANALYTICS.stg_posts
group by
    user_id
    )
;


  