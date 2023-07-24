with user_group_messages as (
    select 
        hk_group_id,
        count(distinct hk_user_id) as cnt_users_in_group_with_messages
    from ST23052702__DWH.l_user_group_activity
    group by hk_group_id
)
select hk_group_id,
       cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages desc
limit 10;