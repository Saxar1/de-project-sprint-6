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
)
select hk_group_id,
       cnt_added_users
from user_group_log
order by reg_dt  
limit 10;