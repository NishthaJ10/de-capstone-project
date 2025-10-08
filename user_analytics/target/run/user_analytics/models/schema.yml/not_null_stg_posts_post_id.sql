
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select post_id
from USER_ACTIVITY_DB.ANALYTICS.stg_posts
where post_id is null



  
  
      
    ) dbt_internal_test