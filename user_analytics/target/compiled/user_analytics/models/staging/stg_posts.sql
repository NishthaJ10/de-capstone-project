select 
    raw_json:userId::integer as user_id,
    raw_json:id::integer as post_id,
    raw_json:title::string as title,
    raw_json:body::string as body
from 
    user_activity_db.raw_data.posts
where
    user_id is not null