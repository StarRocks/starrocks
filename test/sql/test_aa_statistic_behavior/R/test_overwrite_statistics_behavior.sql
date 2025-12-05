-- name: test_overwrite_statistics_behavior @sequential @slow
drop database if exists test_overwrite_statistics_behavior;
-- result:
-- !result
create database test_overwrite_statistics_behavior;
-- result:
-- !result
use test_overwrite_statistics_behavior;
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "true");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("loads_history_sync_interval_second" = "1000000");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_auto_collect_statistics" = "false");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect" = "false");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "false");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("statistic_collect_interval_sec" = "6");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("tablet_stat_update_interval_second" = "3");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_trigger_analyze_job_immediate" = "false");
-- result:
-- !result
shell: sleep 110
-- result:
0

-- !result
shell: sleep 110
-- result:
0

-- !result
shell: sleep 110
-- result:
0

-- !result
update default_catalog.information_schema.be_configs set `value` = "1" where name= "tablet_stat_cache_update_interval_second";
-- result:
-- !result
drop all analyze job;
-- result:
-- !result
shell: sleep 12
-- result:
0

-- !result
drop table if exists unpartitioned_table;
-- result:
-- !result
create table unpartitioned_table (k1 int) properties("replication_num"="1");
-- result:
-- !result
insert into unpartitioned_table select generate_series from table(generate_series(1, 1000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 1000","ESTIMATE")
-- result:
None
-- !result
insert into unpartitioned_table select generate_series from table(generate_series(1, 2000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 3000","ESTIMATE")
-- result:
None
-- !result
insert into unpartitioned_table select generate_series from table(generate_series(1, 3000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 6000","ESTIMATE")
-- result:
None
-- !result
insert overwrite test_overwrite_statistics_behavior.unpartitioned_table select generate_series from table(generate_series(1, 5000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 5000","ESTIMATE")
-- result:
None
-- !result
insert overwrite test_overwrite_statistics_behavior.unpartitioned_table select generate_series from table(generate_series(1, 7000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 7000","ESTIMATE")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "true");
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
insert into unpartitioned_table select generate_series from table(generate_series(1, 3000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 10000","ESTIMATE")
-- result:
None
-- !result
insert overwrite test_overwrite_statistics_behavior.unpartitioned_table select generate_series from table(generate_series(1, 5000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 5000","ESTIMATE")
-- result:
None
-- !result
shell: sleep 6
-- result:
0

-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "false");
-- result:
-- !result
drop stats unpartitioned_table;
-- result:
-- !result
drop table unpartitioned_table;
-- result:
-- !result
create table unpartitioned_table (k1 int) properties("replication_num"="1");
-- result:
-- !result
insert into unpartitioned_table select generate_series from table(generate_series(1, 1000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 1000","UNKNOWN")
-- result:
None
-- !result
insert into unpartitioned_table select generate_series from table(generate_series(1, 4000));
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 5000","UNKNOWN")
-- result:
None
-- !result
shell: sleep 6
-- result:
0

-- !result
insert overwrite unpartitioned_table select generate_series from table(generate_series(1, 3000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 8000","UNKNOWN")
-- result:
None
-- !result
analyze table unpartitioned_table;
-- result:
test_overwrite_statistics_behavior.unpartitioned_table	analyze	status	OK
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 3000","ESTIMATE")
-- result:
None
-- !result
insert overwrite unpartitioned_table select generate_series from table(generate_series(1, 3000));
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 3000","ESTIMATE")
-- result:
None
-- !result
drop stats test_overwrite_statistics_behavior.unpartitioned_table;
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect" = "true");
-- result:
-- !result
create analyze full  all properties  ("statistic_exclude_pattern"="^(?!.*test_overwrite_statistics_behavior).*$");
-- result:
-- !result
shell: sleep 13
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 3000","ESTIMATE")
-- result:
None
-- !result
drop all analyze job;
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect" = "false");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "true");
-- result:
-- !result
shell: sleep 13
-- result:
0

-- !result
drop stats unpartitioned_table;
-- result:
-- !result
drop table unpartitioned_table;
-- result:
-- !result
create table unpartitioned_table (k1 int) properties("replication_num"="1");
-- result:
-- !result
insert into unpartitioned_table select generate_series from table(generate_series(1, 250000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 250000","ESTIMATE", "250000.0")
-- result:
None
-- !result
insert into unpartitioned_table select generate_series from table(generate_series(250000, 500000));
-- result:
-- !result
shell: sleep 7
-- result:
0

-- !result
insert overwrite unpartitioned_table select generate_series from table(generate_series(500000, 1000000));
-- result:
-- !result
shell: sleep 7
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 500001","ESTIMATE", "250000.0")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "false");
-- result:
-- !result
shell: sleep 7
-- result:
0

-- !result
insert overwrite unpartitioned_table select generate_series from table(generate_series(60000, 70000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 10001","ESTIMATE", "70000.0")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "true");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("loads_history_sync_interval_second" = "1000000");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_auto_collect_statistics" = "false");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect" = "false");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "true");
-- result:
-- !result
sleep 13;
-- result:
E: (1064, "Getting syntax error at line 1, column 0. Detail message: Unexpected input 'sleep', the most similar input is {'SELECT', 'SET', 'HELP', 'STOP', 'ALTER', '(', ';'}.")
-- !result
drop table if exists range_partitioned_table;
-- result:
-- !result
create table range_partitioned_table (
    k1 int,
    k2 int,
    k3 varchar(20)
)
partition by range(k1) (
    partition p1 values [("0"), ("1000")),
    partition p2 values [("1000"), ("2000")),
    partition p3 values [("2000"), ("3000")),
    partition p4 values [("3000"), ("4000"))
)
properties("replication_num"="1");
-- result:
-- !result
insert into range_partitioned_table select generate_series, generate_series, "data1" from table(generate_series(1, 100));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 0 and k1 < 1000;","cardinality: 100","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table;","cardinality: 100","ESTIMATE")
-- result:
None
-- !result
insert into range_partitioned_table select generate_series, generate_series, "data2" from table(generate_series(500, 2500));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 0 and k1 < 1000;","cardinality: 100","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 1000 and k1 < 2000;","cardinality: 1000","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 2000 and k1 < 3000;","cardinality: 501","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table;","cardinality: 1601","ESTIMATE")
-- result:
None
-- !result
insert overwrite range_partitioned_table partition(p2)
select generate_series, generate_series, "overwrite" from table(generate_series(1000, 1499));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 1000 and k1 < 2000;","cardinality: 500","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table;","cardinality: 1101","ESTIMATE")
-- result:
None
-- !result
insert overwrite range_partitioned_table
select generate_series, generate_series, "full_overwrite" from table(generate_series(0, 3999));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 0 and k1 < 1000;","cardinality: 1000","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 1000 and k1 < 2000;","cardinality: 1000","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 2000 and k1 < 3000;","cardinality: 1000","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 3000 and k1 < 4000;","cardinality: 1000","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table;","cardinality: 4000","ESTIMATE")
-- result:
None
-- !result
insert into range_partitioned_table select generate_series, generate_series, "p4_data" from table(generate_series(3000, 3299));
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 3000 and k1 < 4000;","cardinality: 1075","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table;","cardinality: 4075","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 1500 and k1 < 2800;","cardinality: 1300","ESTIMATE")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "false");
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
insert overwrite range_partitioned_table partition(p1) select generate_series, generate_series, "p1_new" from table(generate_series(0, 499));
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 0 and k1 < 1000;","cardinality: 500","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table;","cardinality: 3500","ESTIMATE")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "false");
-- result:
-- !result
drop stats range_partitioned_table;
-- result:
-- !result
insert into range_partitioned_table select generate_series, generate_series, "no_stat" from table(generate_series(500, 799));
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 0 and k1 < 1000;","cardinality: 75","UNKNOWN")
-- result:
None
-- !result
analyze table range_partitioned_table;
-- result:
test_overwrite_statistics_behavior.range_partitioned_table	analyze	status	OK
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table;","cardinality: 4100","ESTIMATE")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "true");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "true");
-- result:
-- !result
shell: sleep 13
-- result:
0

-- !result
drop table if exists list_partitioned_table;
-- result:
-- !result
create table list_partitioned_table (
    region int,
    k1 int,
    k2 varchar(20)
)
partition by list(region) (
    partition p1 values in ("1"),
    partition p2 values in ("2"),
    partition p3 values in ("3"),
    partition p4 values in ("4")
)
properties("replication_num"="1");
-- result:
-- !result
insert into list_partitioned_table select 1, generate_series, "data1" from table(generate_series(1, 100));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 1;","cardinality: 100","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table;","cardinality: 100","ESTIMATE")
-- result:
None
-- !result
insert into list_partitioned_table select 1, generate_series, "data2" from table(generate_series(101, 200));
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
insert into list_partitioned_table select 2, generate_series, "data3" from table(generate_series(1, 500));
-- result:
-- !result
insert into list_partitioned_table select 3, generate_series, "data4" from table(generate_series(1, 300));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 1;","cardinality: 100","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 2;","cardinality: 500","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 3;","cardinality: 300","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table;","cardinality: 900","ESTIMATE")
-- result:
None
-- !result
insert overwrite list_partitioned_table partition(p2) select 2, generate_series, "overwrite" from table(generate_series(1, 600));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 2;","cardinality: 600","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table;","cardinality: 1000","ESTIMATE")
-- result:
None
-- !result
insert overwrite list_partitioned_table
select case when generate_series <= 1000 then 1
            when generate_series <= 2000 then 2
            when generate_series <= 3000 then 3
            else 4 end as region,
       generate_series, "full_overwrite"
from table(generate_series(1, 4000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 1;","cardinality: 1000","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 2;","cardinality: 1000","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 3;","cardinality: 1000","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 4;","cardinality: 1000","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table;","cardinality: 4000","ESTIMATE")
-- result:
None
-- !result
insert into list_partitioned_table select 4, generate_series, "p4_data" from table(generate_series(1, 300));
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 4;","cardinality: 1075","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table;","cardinality: 4075","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region in (2, 3);","cardinality: 2000","ESTIMATE")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "false");
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
insert overwrite list_partitioned_table partition(p1) select 1, generate_series, "p1_new" from table(generate_series(1, 500));
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 1;","cardinality: 500","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table;","cardinality: 3500","ESTIMATE")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "false");
-- result:
-- !result
drop stats list_partitioned_table;
-- result:
-- !result
insert into list_partitioned_table select 1, generate_series, "no_stat" from table(generate_series(1, 300));
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 1;","cardinality: 75","UNKNOWN")
-- result:
None
-- !result
analyze table list_partitioned_table;
-- result:
test_overwrite_statistics_behavior.list_partitioned_table	analyze	status	OK
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table;","cardinality: 4100","ESTIMATE")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "true");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "true");
-- result:
-- !result
shell: sleep 13
-- result:
0

-- !result
drop table if exists expr_range_partitioned_table;
-- result:
-- !result
create table expr_range_partitioned_table (
    dt datetime,
    k1 int,
    k2 varchar(20)
)
partition by date_trunc('day', dt)
properties("replication_num"="1");
-- result:
-- !result
insert into expr_range_partitioned_table values 
    ('2024-01-01 10:00:00', 1, 'data1'),
    ('2024-01-01 11:00:00', 2, 'data2'),
    ('2024-01-01 12:00:00', 3, 'data3');
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-01 00:00:00' and dt < '2024-01-02 00:00:00';","cardinality: 3","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table;","cardinality: 3","ESTIMATE")
-- result:
None
-- !result
insert into expr_range_partitioned_table values 
    ('2024-01-02 10:00:00', 1, 'data5'),
    ('2024-01-02 11:00:00', 2, 'data6'),
    ('2024-01-03 10:00:00', 1, 'data7');
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-01 00:00:00' and dt < '2024-01-02 00:00:00';","cardinality: 3","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-02 00:00:00' and dt < '2024-01-03 00:00:00';","cardinality: 2","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-03 00:00:00' and dt < '2024-01-04 00:00:00';","cardinality: 1","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table;","cardinality: 6","ESTIMATE")
-- result:
None
-- !result
set dynamic_overwrite=true;
-- result:
-- !result
insert overwrite expr_range_partitioned_table values ('2024-01-01 08:00:00', 100, 'overwrite');
-- result:
-- !result
set dynamic_overwrite=false;
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-01 00:00:00' and dt < '2024-01-02 00:00:00';","cardinality: 1","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table;","cardinality: 4","ESTIMATE")
-- result:
None
-- !result
insert into expr_range_partitioned_table
select date_add('2024-01-04 00:00:00', interval generate_series hour) as dt, 
       generate_series, 'day4'
from table(generate_series(0, 11));
-- result:
-- !result
insert into expr_range_partitioned_table
select date_add('2024-01-05 00:00:00', interval generate_series hour) as dt, 
       generate_series, 'day5'
from table(generate_series(0, 5));
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-01 00:00:00' and dt < '2024-01-02 00:00:00';","cardinality: 1","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-02 00:00:00' and dt < '2024-01-03 00:00:00';","cardinality: 2","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-03 00:00:00' and dt < '2024-01-04 00:00:00';","cardinality: 1","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-04 00:00:00' and dt < '2024-01-05 00:00:00';","cardinality: 12","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-05 00:00:00' and dt < '2024-01-06 00:00:00';","cardinality: 6","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table;","cardinality: 22","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-02 00:00:00' and dt < '2024-01-04 00:00:00';","cardinality: 3","ESTIMATE")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "false");
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
set dynamic_overwrite=true;
-- result:
-- !result
insert overwrite expr_range_partitioned_table
select date_add('2024-01-01 00:00:00', interval generate_series hour) as dt, 
       generate_series, 'new_day'
from table(generate_series(0, 9));
-- result:
-- !result
set dynamic_overwrite=false;
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-01 00:00:00' and dt < '2024-01-02 00:00:00';","cardinality: 10","ESTIMATE")
-- result:
None
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table;","cardinality: 31","ESTIMATE")
-- result:
None
-- !result
insert overwrite expr_range_partitioned_table partition(p20240101)
select '2024-01-01 09:00:00',generate_series, 'new_day' from table(generate_series(0, 4));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-01 00:00:00' and dt < '2024-01-02 00:00:00';","cardinality: 5","ESTIMATE")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "false");
-- result:
-- !result
drop stats expr_range_partitioned_table;
-- result:
-- !result
insert into expr_range_partitioned_table select '2024-01-06 10:00:00', generate_series, 'no_stat' from table(generate_series(1, 15));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-06 00:00:00' and dt < '2024-01-07 00:00:00';","cardinality: 2","UNKNOWN")
-- result:
None
-- !result
analyze table expr_range_partitioned_table;
-- result:
test_overwrite_statistics_behavior.expr_range_partitioned_table	analyze	status	OK
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table;","cardinality: 41","ESTIMATE")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "true");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "true");
-- result:
-- !result
shell: sleep 13
-- result:
0

-- !result
drop table if exists expr_range_v2_partitioned_table;
-- result:
-- !result
create table expr_range_v2_partitioned_table (
    dt_str varchar(20),
    k1 int,
    k2 varchar(20)
)
partition by range(str2date(dt_str, '%Y-%m-%d')) (
    partition p1 values [('2024-01-01'), ('2024-01-02')),
    partition p2 values [('2024-01-02'), ('2024-01-03')),
    partition p3 values [('2024-01-03'), ('2024-01-04'))
)
properties("replication_num"="1");
-- result:
-- !result
insert into expr_range_v2_partitioned_table values 
    ('2024-01-01', 1, 'data1'),
    ('2024-01-01', 2, 'data2'),
    ('2024-01-01', 3, 'data3');
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_v2_partitioned_table;","cardinality: 3","ESTIMATE")
-- result:
None
-- !result
insert into expr_range_v2_partitioned_table values 
    ('2024-01-02', 1, 'data5'),
    ('2024-01-02', 2, 'data6'),
    ('2024-01-03', 1, 'data7');
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_v2_partitioned_table;","cardinality: 6","ESTIMATE")
-- result:
None
-- !result
insert overwrite expr_range_v2_partitioned_table partition(p1) values ('2024-01-01', 100, 'overwrite');
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_v2_partitioned_table;","cardinality: 4","ESTIMATE")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "false");
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
insert overwrite expr_range_v2_partitioned_table partition(p1) select '2024-01-01', generate_series, 'p1_new' from table(generate_series(1, 10));
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_v2_partitioned_table;","cardinality: 13","ESTIMATE")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "false");
-- result:
-- !result
drop stats expr_range_v2_partitioned_table;
-- result:
-- !result
alter table expr_range_v2_partitioned_table add partition p4 values [('2024-01-04'), ('2024-01-05'));
-- result:
-- !result
insert into expr_range_v2_partitioned_table values 
    ('2024-01-04', 1, 'no_stat'),
    ('2024-01-04', 2, 'no_stat'),
    ('2024-01-04', 3, 'no_stat');
-- result:
-- !result
shell: sleep 6
-- result:
0

-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_v2_partitioned_table;","cardinality: 4","UNKNOWN")
-- result:
None
-- !result
analyze table expr_range_v2_partitioned_table;
-- result:
test_overwrite_statistics_behavior.expr_range_v2_partitioned_table	analyze	status	OK
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_v2_partitioned_table;","cardinality: 16","ESTIMATE")
-- result:
None
-- !result
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "true");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect" = "true");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_auto_collect_statistics" = "true");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("enable_trigger_analyze_job_immediate" = "true");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("statistic_collect_interval_sec" = "300");
-- result:
-- !result
ADMIN SET FRONTEND CONFIG ("tablet_stat_update_interval_second" = "180");
-- result:
-- !result