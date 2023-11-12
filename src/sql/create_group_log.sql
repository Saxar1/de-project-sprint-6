CREATE TABLE ST23052702__STAGING.group_log (
  group_id INT NOT NULL,
  user_id INT,
  user_id_from INT,
  "event" VARCHAR(10),
  group_log_ts TIMESTAMP
) 
ORDER BY group_log_ts 
PARTITION BY group_log_ts::date
GROUP BY calendar_hierarchy_day(group_log_ts::date, 3, 2)