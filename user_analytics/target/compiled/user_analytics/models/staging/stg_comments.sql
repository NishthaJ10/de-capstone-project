with valid_posts as (
    select post_id from USER_ACTIVITY_DB.ANALYTICS.stg_posts
)

select
    raw_json:postId::integer as post_id,
    raw_json:id::integer as comment_id,
    raw_json:name::string as comment_name,
    raw_json:email::string as email,
    raw_json:body::string as comment_body
from
    user_activity_db.raw_data.comments
where
    post_id in (select post_id from valid_posts) -- Add this filter