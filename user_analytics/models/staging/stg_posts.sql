select 
    raw_json:userID::integer as user_id,
    raw_json:id::integer as post_id,
    raw_json:title::string as title,
    raw_json:body::string as body
from 
    {{source('raw_data','posts')}}
where
    user_id is not null
