-- name: test_overwrite_statistics_behavior @sequential @slow
drop database if exists test_overwrite_statistics_behavior;
create database test_overwrite_statistics_behavior;
use test_overwrite_statistics_behavior;
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "true");
ADMIN SET FRONTEND CONFIG ("loads_history_sync_interval_second" = "1000000");
ADMIN SET FRONTEND CONFIG ("enable_auto_collect_statistics" = "false");
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect" = "false");
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "false");
ADMIN SET FRONTEND CONFIG ("statistic_collect_interval_sec" = "6");
ADMIN SET FRONTEND CONFIG ("tablet_stat_update_interval_second" = "3");
ADMIN SET FRONTEND CONFIG ("enable_trigger_analyze_job_immediate" = "false");
shell: sleep 110
shell: sleep 110
shell: sleep 110

update default_catalog.information_schema.be_configs set `value` = "1" where name= "tablet_stat_cache_update_interval_second";

drop all analyze job;
shell: sleep 12
-- test unpartitioned table
drop table if exists unpartitioned_table;
create table unpartitioned_table (k1 int) properties("replication_num"="1");
insert into unpartitioned_table select generate_series from table(generate_series(1, 1000));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 1000","ESTIMATE")
insert into unpartitioned_table select generate_series from table(generate_series(1, 2000));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 3000","ESTIMATE")
insert into unpartitioned_table select generate_series from table(generate_series(1, 3000));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 6000","ESTIMATE")
insert overwrite test_overwrite_statistics_behavior.unpartitioned_table select generate_series from table(generate_series(1, 5000));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 5000","ESTIMATE")
insert overwrite test_overwrite_statistics_behavior.unpartitioned_table select generate_series from table(generate_series(1, 7000));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 7000","ESTIMATE")

ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "true");
shell: sleep 6
insert into unpartitioned_table select generate_series from table(generate_series(1, 3000));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 10000","ESTIMATE")
insert overwrite test_overwrite_statistics_behavior.unpartitioned_table select generate_series from table(generate_series(1, 5000));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 5000","ESTIMATE")
shell: sleep 6

ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "false");
drop stats unpartitioned_table;
drop table unpartitioned_table;
create table unpartitioned_table (k1 int) properties("replication_num"="1");
insert into unpartitioned_table select generate_series from table(generate_series(1, 1000));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 1000","UNKNOWN")
insert into unpartitioned_table select generate_series from table(generate_series(1, 4000));
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 5000","UNKNOWN")
shell: sleep 6
insert overwrite unpartitioned_table select generate_series from table(generate_series(1, 3000));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 8000","UNKNOWN")

analyze table unpartitioned_table;
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 3000","ESTIMATE")
insert overwrite unpartitioned_table select generate_series from table(generate_series(1, 3000));
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 3000","ESTIMATE")
drop stats test_overwrite_statistics_behavior.unpartitioned_table;
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect" = "true");
create analyze full  all properties  ("statistic_exclude_pattern"="^(?!.*test_overwrite_statistics_behavior).*$");
shell: sleep 13
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 3000","ESTIMATE")
drop all analyze job;
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect" = "false");
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "true");
shell: sleep 13
drop stats unpartitioned_table;
drop table unpartitioned_table;
create table unpartitioned_table (k1 int) properties("replication_num"="1");
insert into unpartitioned_table select generate_series from table(generate_series(1, 250000));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 250000","ESTIMATE", "250000.0")
insert into unpartitioned_table select generate_series from table(generate_series(250000, 500000));
shell: sleep 7
insert overwrite unpartitioned_table select generate_series from table(generate_series(500000, 1000000));
shell: sleep 7
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 500001","ESTIMATE", "250000.0")
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "false");
shell: sleep 7
insert overwrite unpartitioned_table select generate_series from table(generate_series(60000, 70000));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.unpartitioned_table;","cardinality: 10001","ESTIMATE", "70000.0")

-- test range partition table
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "true");
ADMIN SET FRONTEND CONFIG ("loads_history_sync_interval_second" = "1000000");
ADMIN SET FRONTEND CONFIG ("enable_auto_collect_statistics" = "false");
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect" = "false");
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "true");
sleep 13;
drop table if exists range_partitioned_table;
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
insert into range_partitioned_table select generate_series, generate_series, "data1" from table(generate_series(1, 100));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 0 and k1 < 1000;","cardinality: 100","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table;","cardinality: 100","ESTIMATE")

insert into range_partitioned_table select generate_series, generate_series, "data2" from table(generate_series(500, 2500));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 0 and k1 < 1000;","cardinality: 100","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 1000 and k1 < 2000;","cardinality: 1000","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 2000 and k1 < 3000;","cardinality: 501","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table;","cardinality: 1601","ESTIMATE")

insert overwrite range_partitioned_table partition(p2)
select generate_series, generate_series, "overwrite" from table(generate_series(1000, 1499));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 1000 and k1 < 2000;","cardinality: 500","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table;","cardinality: 1101","ESTIMATE")

insert overwrite range_partitioned_table
select generate_series, generate_series, "full_overwrite" from table(generate_series(0, 3999));

function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 0 and k1 < 1000;","cardinality: 1000","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 1000 and k1 < 2000;","cardinality: 1000","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 2000 and k1 < 3000;","cardinality: 1000","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 3000 and k1 < 4000;","cardinality: 1000","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table;","cardinality: 4000","ESTIMATE")

insert into range_partitioned_table select generate_series, generate_series, "p4_data" from table(generate_series(3000, 3299));
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 3000 and k1 < 4000;","cardinality: 1075","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table;","cardinality: 4075","ESTIMATE")

function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 1500 and k1 < 2800;","cardinality: 1300","ESTIMATE")

ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "false");
shell: sleep 6
insert overwrite range_partitioned_table partition(p1) select generate_series, generate_series, "p1_new" from table(generate_series(0, 499));
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 0 and k1 < 1000;","cardinality: 500","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table;","cardinality: 3500","ESTIMATE")

ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "false");
drop stats range_partitioned_table;
insert into range_partitioned_table select generate_series, generate_series, "no_stat" from table(generate_series(500, 799));
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table where k1 >= 0 and k1 < 1000;","cardinality: 75","UNKNOWN")

analyze table range_partitioned_table;
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.range_partitioned_table;","cardinality: 4100","ESTIMATE")

-- test list partition table
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "true");
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "true");
shell: sleep 13
drop table if exists list_partitioned_table;
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
insert into list_partitioned_table select 1, generate_series, "data1" from table(generate_series(1, 100));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 1;","cardinality: 100","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table;","cardinality: 100","ESTIMATE")

insert into list_partitioned_table select 1, generate_series, "data2" from table(generate_series(101, 200));
shell: sleep 6
insert into list_partitioned_table select 2, generate_series, "data3" from table(generate_series(1, 500));
insert into list_partitioned_table select 3, generate_series, "data4" from table(generate_series(1, 300));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 1;","cardinality: 100","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 2;","cardinality: 500","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 3;","cardinality: 300","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table;","cardinality: 900","ESTIMATE")

insert overwrite list_partitioned_table partition(p2) select 2, generate_series, "overwrite" from table(generate_series(1, 600));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 2;","cardinality: 600","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table;","cardinality: 1000","ESTIMATE")

insert overwrite list_partitioned_table
select case when generate_series <= 1000 then 1
            when generate_series <= 2000 then 2
            when generate_series <= 3000 then 3
            else 4 end as region,
       generate_series, "full_overwrite"
from table(generate_series(1, 4000));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 1;","cardinality: 1000","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 2;","cardinality: 1000","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 3;","cardinality: 1000","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 4;","cardinality: 1000","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table;","cardinality: 4000","ESTIMATE")

insert into list_partitioned_table select 4, generate_series, "p4_data" from table(generate_series(1, 300));
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 4;","cardinality: 1075","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table;","cardinality: 4075","ESTIMATE")

function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region in (2, 3);","cardinality: 2000","ESTIMATE")

ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "false");
shell: sleep 6
insert overwrite list_partitioned_table partition(p1) select 1, generate_series, "p1_new" from table(generate_series(1, 500));
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 1;","cardinality: 500","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table;","cardinality: 3500","ESTIMATE")

ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "false");
drop stats list_partitioned_table;
insert into list_partitioned_table select 1, generate_series, "no_stat" from table(generate_series(1, 300));
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table where region = 1;","cardinality: 75","UNKNOWN")

analyze table list_partitioned_table;
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.list_partitioned_table;","cardinality: 4100","ESTIMATE")

-- test expression range partition
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "true");
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "true");
shell: sleep 13
drop table if exists expr_range_partitioned_table;
create table expr_range_partitioned_table (
    dt datetime,
    k1 int,
    k2 varchar(20)
)
partition by date_trunc('day', dt)
properties("replication_num"="1");
insert into expr_range_partitioned_table values 
    ('2024-01-01 10:00:00', 1, 'data1'),
    ('2024-01-01 11:00:00', 2, 'data2'),
    ('2024-01-01 12:00:00', 3, 'data3');
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-01 00:00:00' and dt < '2024-01-02 00:00:00';","cardinality: 3","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table;","cardinality: 3","ESTIMATE")

insert into expr_range_partitioned_table values 
    ('2024-01-02 10:00:00', 1, 'data5'),
    ('2024-01-02 11:00:00', 2, 'data6'),
    ('2024-01-03 10:00:00', 1, 'data7');
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-01 00:00:00' and dt < '2024-01-02 00:00:00';","cardinality: 3","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-02 00:00:00' and dt < '2024-01-03 00:00:00';","cardinality: 2","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-03 00:00:00' and dt < '2024-01-04 00:00:00';","cardinality: 1","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table;","cardinality: 6","ESTIMATE")

set dynamic_overwrite=true;
insert overwrite expr_range_partitioned_table values ('2024-01-01 08:00:00', 100, 'overwrite');
set dynamic_overwrite=false;
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-01 00:00:00' and dt < '2024-01-02 00:00:00';","cardinality: 1","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table;","cardinality: 4","ESTIMATE")

insert into expr_range_partitioned_table
select date_add('2024-01-04 00:00:00', interval generate_series hour) as dt, 
       generate_series, 'day4'
from table(generate_series(0, 11));
insert into expr_range_partitioned_table
select date_add('2024-01-05 00:00:00', interval generate_series hour) as dt, 
       generate_series, 'day5'
from table(generate_series(0, 5));
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-01 00:00:00' and dt < '2024-01-02 00:00:00';","cardinality: 1","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-02 00:00:00' and dt < '2024-01-03 00:00:00';","cardinality: 2","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-03 00:00:00' and dt < '2024-01-04 00:00:00';","cardinality: 1","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-04 00:00:00' and dt < '2024-01-05 00:00:00';","cardinality: 12","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-05 00:00:00' and dt < '2024-01-06 00:00:00';","cardinality: 6","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table;","cardinality: 22","ESTIMATE")

function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-02 00:00:00' and dt < '2024-01-04 00:00:00';","cardinality: 3","ESTIMATE")

ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "false");
shell: sleep 6
set dynamic_overwrite=true;
insert overwrite expr_range_partitioned_table
select date_add('2024-01-01 00:00:00', interval generate_series hour) as dt, 
       generate_series, 'new_day'
from table(generate_series(0, 9));
set dynamic_overwrite=false;
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-01 00:00:00' and dt < '2024-01-02 00:00:00';","cardinality: 10","ESTIMATE")
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table;","cardinality: 31","ESTIMATE")

insert overwrite expr_range_partitioned_table partition(p20240101)
select '2024-01-01 09:00:00',generate_series, 'new_day' from table(generate_series(0, 4));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-01 00:00:00' and dt < '2024-01-02 00:00:00';","cardinality: 5","ESTIMATE")

ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "false");
drop stats expr_range_partitioned_table;
insert into expr_range_partitioned_table select '2024-01-06 10:00:00', generate_series, 'no_stat' from table(generate_series(1, 15));
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table where dt >= '2024-01-06 00:00:00' and dt < '2024-01-07 00:00:00';","cardinality: 2","UNKNOWN")

analyze table expr_range_partitioned_table;
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_partitioned_table;","cardinality: 41","ESTIMATE")

-- test expression range partition v2
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "true");
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "true");
shell: sleep 13
drop table if exists expr_range_v2_partitioned_table;
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
insert into expr_range_v2_partitioned_table values 
    ('2024-01-01', 1, 'data1'),
    ('2024-01-01', 2, 'data2'),
    ('2024-01-01', 3, 'data3');
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_v2_partitioned_table;","cardinality: 3","ESTIMATE")

insert into expr_range_v2_partitioned_table values 
    ('2024-01-02', 1, 'data5'),
    ('2024-01-02', 2, 'data6'),
    ('2024-01-03', 1, 'data7');
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_v2_partitioned_table;","cardinality: 6","ESTIMATE")

insert overwrite expr_range_v2_partitioned_table partition(p1) values ('2024-01-01', 100, 'overwrite');
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_v2_partitioned_table;","cardinality: 4","ESTIMATE")

ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "false");
shell: sleep 6
insert overwrite expr_range_v2_partitioned_table partition(p1) select '2024-01-01', generate_series, 'p1_new' from table(generate_series(1, 10));
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_v2_partitioned_table;","cardinality: 13","ESTIMATE")

ADMIN SET FRONTEND CONFIG ("enable_statistic_collect_on_first_load" = "false");
drop stats expr_range_v2_partitioned_table;
alter table expr_range_v2_partitioned_table add partition p4 values [('2024-01-04'), ('2024-01-05'));
insert into expr_range_v2_partitioned_table values 
    ('2024-01-04', 1, 'no_stat'),
    ('2024-01-04', 2, 'no_stat'),
    ('2024-01-04', 3, 'no_stat');
shell: sleep 6
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_v2_partitioned_table;","cardinality: 4","UNKNOWN")

analyze table expr_range_v2_partitioned_table;
function: assert_explain_costs_contains("select * from test_overwrite_statistics_behavior.expr_range_v2_partitioned_table;","cardinality: 16","ESTIMATE")

-- reset fe config
ADMIN SET FRONTEND CONFIG ("enable_sync_tablet_stats" = "true");
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect" = "true");
ADMIN SET FRONTEND CONFIG ("enable_auto_collect_statistics" = "true");
ADMIN SET FRONTEND CONFIG ("enable_trigger_analyze_job_immediate" = "true");
ADMIN SET FRONTEND CONFIG ("statistic_collect_interval_sec" = "300");
ADMIN SET FRONTEND CONFIG ("tablet_stat_update_interval_second" = "180");



