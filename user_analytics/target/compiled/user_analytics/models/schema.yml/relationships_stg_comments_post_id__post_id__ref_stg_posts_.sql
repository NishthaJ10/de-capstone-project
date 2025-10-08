
    
    

with child as (
    select post_id as from_field
    from USER_ACTIVITY_DB.ANALYTICS.stg_comments
    where post_id is not null
),

parent as (
    select post_id as to_field
    from USER_ACTIVITY_DB.ANALYTICS.stg_posts
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


