
  create or replace   view USER_ACTIVITY_DB.ANALYTICS.int_comments_per_post
  
  
  
  
  as (
    select
    post_id,
    count(*) as num_comments
from    
    USER_ACTIVITY_DB.ANALYTICS.stg_posts
group by 
    post_id
  );

