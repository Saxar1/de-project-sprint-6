with user_group_log as (
    select 
        luga.hk_group_id,
        count(distinct luga.hk_user_id) as cnt_added_users,
        hg.registration_dt as reg_dt
    from ST23052702__DWH.s_auth_history sah
    join ST23052702__DWH.l_user_group_activity luga using (hk_l_user_group_activity)
    join ST23052702__DWH.h_groups hg on luga.hk_group_id = hg.hk_group_id
    where event = 'add'
    group by luga.hk_group_id, hg.registration_dt 
    order by hg.registration_dt
    limit 10
),
user_group_messages as (
    select 
        hk_group_id,
        count(distinct hk_user_id) as cnt_users_in_group_with_messages
    from ST23052702__DWH.l_user_group_activity
    group by hk_group_id
)
select ugl.hk_group_id,
       ugl.cnt_added_users,
       ugm.cnt_users_in_group_with_messages,
       ugm.cnt_users_in_group_with_messages::float / ugl.cnt_added_users as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by group_conversion desc;