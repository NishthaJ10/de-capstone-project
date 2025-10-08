
  create or replace   view USER_ACTIVITY_DB.ANALYTICS.stg_comments
  
  
  
  
  as (
    select 
    raw_json:postID::integer as post_id,
    raw_json:id::integer as comment_id,
    raw_json:name::string as name,
    raw_json:email::string as email,
    raw_json:body::string as body
from 
    user_activity_db.raw_data.comments
  );

