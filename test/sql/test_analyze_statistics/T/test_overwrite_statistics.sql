-- name: test_overwrite_statistics @sequential

drop database if exists test_overwrite_statistics;
create database test_overwrite_statistics;
use test_overwrite_statistics;
create table test_overwrite_stats_table (k1 int) properties("replication_num"="1");

delete from _statistics_.column_statistics where table_name='test_overwrite_statistics.test_overwrite_stats_table';

insert into test_overwrite_stats_table select 123;
select count(*) from _statistics_.column_statistics where table_name = 'test_overwrite_statistics.test_overwrite_stats_table';
insert overwrite test_overwrite_stats_table select 123;
select count(*) from _statistics_.column_statistics where table_name = 'test_overwrite_statistics.test_overwrite_stats_table';

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

delete from _statistics_.column_statistics where table_name='test_overwrite_statistics.sales_data';

INSERT INTO sales_data VALUES
(1, '2024-01-15'),
(2, '2024-01-20'),
(3, '2024-02-10'),
(4, '2024-02-15'),
(5, '2024-03-05'),
(6, '2024-03-12'),
(7, '2024-04-08'),
(8, '2024-04-18');
select count(*) from _statistics_.column_statistics where table_name = 'test_overwrite_statistics.sales_data';

INSERT OVERWRITE test_overwrite_statistics.sales_data partition("p202401") VALUES (101, '2024-01-10');
select count(*) from _statistics_.column_statistics where table_name = 'test_overwrite_statistics.sales_data';

select * from information_schema.analyze_status where `Database`='test_overwrite_statistics' and `Table`='sales_data' and Status='FAILED';

delete from _statistics_.column_statistics where table_name='test_overwrite_statistics.test_overwrite_with_full';
create table test_overwrite_statistics.test_overwrite_with_full (k1 int) properties("replication_num"="1");
insert overwrite test_overwrite_statistics.test_overwrite_with_full select generate_series from table(generate_series(1, 5000));
function: assert_explain_costs_contains("select * from test_overwrite_statistics.test_overwrite_with_full;","cardinality: 5000", "ESTIMATE")
insert overwrite test_overwrite_statistics.test_overwrite_with_full select generate_series from table(generate_series(1, 5000));
function: assert_explain_costs_contains("select * from test_overwrite_statistics.test_overwrite_with_full;","cardinality: 5000", "ESTIMATE")
