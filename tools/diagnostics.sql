-- diagnostics.sql

-- This script collects key system information to help diagnose 
-- and troubleshoot performance issues.

-- This data can help identify system bottlenecks, 
-- debug problems, and guide optimization efforts.

-- example:
-- cat diagnostics.sql | mysql -h127.0.0.1 -P9030 -uroot |gzip > diagnostics.gz

show frontends\G;
show backends\G;
show variables\G;
admin show frontend config\G;
select * from information_schema.loads\G;
select * from information_schema.routine_load_jobs\G;
select * from information_schema.be_metrics\G;
select * from information_schema.be_bvars\G;
select * from information_schema.be_cloud_native_compactions\G;
select BE_ID, NAME, VALUE from information_schema.be_configs\G;
show proc '/compactions'\G;
select BE_ID, LOG from (select BE_ID, TIMESTAMP, LOG from information_schema.be_logs where LEVEL='W' or LEVEL='E' order by TIMESTAMP DESC limit 5000) t order by BE_ID, TIMESTAMP ASC;
