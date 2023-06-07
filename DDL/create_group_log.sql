CREATE TABLE ST23052702__STAGING.group_log (
  group_id INT NOT NULL,
  user_id INT,
  user_id_from INT,
  "event" VARCHAR(10),
  "datetime" TIMESTAMP
) 
ORDER BY "datetime" 
PARTITION BY "datetime"::date
GROUP BY calendar_hierarchy_day("datetime"::date, 3, 2)