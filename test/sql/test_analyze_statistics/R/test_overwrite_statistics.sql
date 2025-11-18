-- name: test_overwrite_statistics @sequential
drop database if exists test_overwrite_statistics;
-- result:
-- !result
create database test_overwrite_statistics;
-- result:
-- !result
use test_overwrite_statistics;
-- result:
-- !result
create table test_overwrite_stats_table (k1 int) properties("replication_num"="1");
-- result:
-- !result
delete from _statistics_.column_statistics where table_name='test_overwrite_statistics.test_overwrite_stats_table';
-- result:
-- !result
insert into test_overwrite_stats_table select 123;
-- result:
-- !result
select count(*) from _statistics_.column_statistics where table_name = 'test_overwrite_statistics.test_overwrite_stats_table';
-- result:
1
-- !result
insert overwrite test_overwrite_stats_table select 123;
-- result:
-- !result
select count(*) from _statistics_.column_statistics where table_name = 'test_overwrite_statistics.test_overwrite_stats_table';
-- result:
2
-- !result
CREATE TABLE sales_data (
    id BIGINT,
    sale_date DATE
)
DUPLICATE KEY(id)
PARTITION BY RANGE(sale_date) (
    PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),
    PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01')),
    PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),
    PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01'))
)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
-- result:
-- !result
delete from _statistics_.column_statistics where table_name='test_overwrite_statistics.sales_data';
-- result:
-- !result
INSERT INTO sales_data VALUES
(1, '2024-01-15'),
(2, '2024-01-20'),
(3, '2024-02-10'),
(4, '2024-02-15'),
(5, '2024-03-05'),
(6, '2024-03-12'),
(7, '2024-04-08'),
(8, '2024-04-18');
-- result:
-- !result
select count(*) from _statistics_.column_statistics where table_name = 'test_overwrite_statistics.sales_data';
-- result:
8
-- !result
INSERT OVERWRITE test_overwrite_statistics.sales_data partition("p202401") VALUES (101, '2024-01-10');
-- result:
-- !result
select count(*) from _statistics_.column_statistics where table_name = 'test_overwrite_statistics.sales_data';
-- result:
10
-- !result
select * from information_schema.analyze_status where `Database`='test_overwrite_statistics' and `Table`='sales_data' and Status='FAILED';
-- result:
-- !result
delete from _statistics_.column_statistics where table_name='test_overwrite_statistics.test_overwrite_with_full';
-- result:
-- !result
create table test_overwrite_statistics.test_overwrite_with_full (k1 int) properties("replication_num"="1");
-- result:
-- !result
insert overwrite test_overwrite_statistics.test_overwrite_with_full select generate_series from table(generate_series(1, 5000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics.test_overwrite_with_full;","cardinality: 5000", "ESTIMATE")
-- result:
None
-- !result
insert overwrite test_overwrite_statistics.test_overwrite_with_full select generate_series from table(generate_series(1, 5000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics.test_overwrite_with_full;","cardinality: 5000", "ESTIMATE")
-- result:
None
-- !result
delete from _statistics_.column_statistics where table_name='test_overwrite_statistics.test_overwrite_with_sample';
-- result:
-- !result
create table test_overwrite_statistics.test_overwrite_with_sample (k1 int) properties("replication_num"="1");
-- result:
-- !result
insert overwrite test_overwrite_statistics.test_overwrite_with_sample select generate_series from table(generate_series(1, 300000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics.test_overwrite_with_sample;", "ESTIMATE")
-- result:
None
-- !result
insert overwrite test_overwrite_statistics.test_overwrite_with_sample select generate_series from table(generate_series(1, 300000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics.test_overwrite_with_sample;", "ESTIMATE")
-- result:
None
-- !result
drop table sales_data;
-- result:
-- !result
delete from _statistics_.column_statistics where table_name='test_overwrite_statistics.sales_data';
-- result:
-- !result
CREATE TABLE sales_data (
    id BIGINT,
    sale_date DATE
)
DUPLICATE KEY(id)
PARTITION BY RANGE(sale_date) (
    PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),
    PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01')),
    PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),
    PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01'))
)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
-- result:
-- !result
alter table test_overwrite_statistics.sales_data set("enable_statistic_collect_on_first_load"="false");
-- result:
-- !result
INSERT INTO sales_data VALUES
(1, '2024-01-15'),
(2, '2024-01-20'),
(3, '2024-02-10'),
(4, '2024-02-15'),
(5, '2024-03-05'),
(6, '2024-03-12'),
(7, '2024-04-08'),
(8, '2024-04-18');
-- result:
-- !result
select count(*) from _statistics_.column_statistics where table_name = 'test_overwrite_statistics.sales_data';
-- result:
0
-- !result
INSERT OVERWRITE test_overwrite_statistics.sales_data partition("p202401") VALUES (101, '2024-01-10');
-- result:
-- !result
select count(*) from _statistics_.column_statistics where table_name = 'test_overwrite_statistics.sales_data';
-- result:
0
-- !result
alter table test_overwrite_statistics.sales_data set("enable_statistic_collect_on_first_load"="true");
-- result:
-- !result
INSERT OVERWRITE test_overwrite_statistics.sales_data partition("p202401") VALUES (102, '2024-01-10');
-- result:
-- !result
select count(*) from _statistics_.column_statistics where table_name = 'test_overwrite_statistics.sales_data';
-- result:
2
-- !result
drop stats test_overwrite_statistics.test_overwrite_stats_table;
-- result:
-- !result
drop table test_overwrite_statistics.test_overwrite_stats_table;
-- result:
-- !result
delete from _statistics_.column_statistics where table_name='test_overwrite_statistics.test_overwrite_stats_table';
-- result:
-- !result
create table test_overwrite_stats_table (k1 int) properties("replication_num"="1");
-- result:
-- !result
insert into test_overwrite_stats_table select generate_series from table(generate_series(1, 1000));
-- result:
-- !result
insert overwrite test_overwrite_stats_table select generate_series from table(generate_series(10000, 20000));
-- result:
-- !result
function: assert_explain_costs_contains("select * from test_overwrite_statistics.test_overwrite_stats_table;", "20000.0")
-- result:
None
-- !result