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
INSERT OVERWRITE sales_data partition("p202401") VALUES (101, '2024-01-10');
-- result:
-- !result
select count(*) from _statistics_.column_statistics where table_name = 'test_overwrite_statistics.sales_data';
-- result:
10
-- !result