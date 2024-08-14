-- name: test_agg_table_with_sum_state
 CREATE TABLE `t1` ( 
    `k1`  date, 
    `k2`  datetime not null,
    `k3`  char(20), 
    `k4`  varchar(20) not null, 
    `k5`  boolean, 
    `k6`  tinyint not null, 
    `k7`  smallint, 
    `k8`  int not null, 
    `k9`  bigint, 
    `k10` largeint not null, 
    `k11` float, 
    `k12` double not null, 
    `k13` decimal(27,9)
) DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) 
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) 
PROPERTIES (  "replication_num" = "1");
-- result:
-- !result
insert into t1  values('2020-01-01', '2020-01-01 10:10:10', 'aaaa', 'aaaa', 0, 1, 1, 1, 1, 1, 1.11, 11.111, 111.1111),('2020-02-01', '2020-02-01 10:10:10', 'bbbb', 'bbbb', 0, 2, 2, 2, 2, 2, 2.22, 22.222, 222.2222);
-- result:
-- !result
CREATE TABLE test_agg_tbl1(
  k1 VARCHAR(10),
  k2 datetime,
  k6 sum(tinyint),
  k7 sum(smallint),
  k8 sum(int),
  k9 sum(bigint),
  k10 sum(largeint),
  k11 sum(float),
  k12 sum(double),
  k13 sum(decimal(27,9))
)
AGGREGATE KEY(k1, k2)
PARTITION BY (k1) 
DISTRIBUTED BY HASH(k1) BUCKETS 3;
-- result:
-- !result
insert into test_agg_tbl1 select k1, k2, sum_state(k6), sum_state(k7), sum_state(k8), sum_state(k9), sum_state(k10), sum_state(k11), sum_state(k12), sum_state(k13) from t1;
-- result:
-- !result
select sum_merge(k6), sum_merge(k7), sum_merge(k8), sum_merge(k9), sum_merge(k10), sum_merge(k11), sum_merge(k12), sum_merge(k13) from test_agg_tbl1;
-- result:
3	3	3	3	3	3.3300000429153442	33.333	333.333300000
-- !result
select k1, sum_merge(k6), sum_merge(k7), sum_merge(k8), sum_merge(k9), sum_merge(k10), sum_merge(k11), sum_merge(k12), sum_merge(k13) from test_agg_tbl1 group by k1 order by 1 limit 3;
-- result:
2020-01-01	1	1	1	1	1	1.1100000143051147	11.111	111.111100000
2020-02-01	2	2	2	2	2	2.2200000286102295	22.222	222.222200000
-- !result
insert into t1  values('2020-01-02', '2020-01-01 10:10:10', 'aaaa', 'aaaa', 0, 1, 1, 1, 1, 1, 1.11, 11.111, 111.1111),('2020-02-01', '2020-02-01 10:10:10', 'bbbb', 'bbbb', 0, 2, 2, 2, 2, 2, 2.22, 22.222, 222.2222);
-- result:
-- !result
insert into test_agg_tbl1 select k1, k2, sum_state(k6), sum_state(k7), sum_state(k8), sum_state(k9), sum_state(k10), sum_state(k11), sum_state(k12), sum_state(k13) from t1;
-- result:
-- !result
insert into test_agg_tbl1 select k1, k2, sum_state(k6), sum_state(k7), sum_state(k8), sum_state(k9), sum_state(k10), sum_state(k11), sum_state(k12), sum_state(k13) from t1;
-- result:
-- !result
ALTER TABLE test_agg_tbl1 COMPACT;
-- result:
-- !result
select sum_merge(k6), sum_merge(k7), sum_merge(k8), sum_merge(k9), sum_merge(k10), sum_merge(k11), sum_merge(k12), sum_merge(k13) from test_agg_tbl1;
-- result:
15	15	15	15	15	16.65000021457672	166.665	1666.666500000
-- !result
select k1, sum_merge(k6), sum_merge(k7), sum_merge(k8), sum_merge(k9), sum_merge(k10), sum_merge(k11), sum_merge(k12), sum_merge(k13) from test_agg_tbl1 group by k1 order by 1 limit 3;
-- result:
2020-01-01	3	3	3	3	3	3.3300000429153442	33.333	333.333300000
2020-01-02	2	2	2	2	2	2.2200000286102295	22.222	222.222200000
2020-02-01	10	10	10	10	10	11.100000143051147	111.11	1111.111000000
-- !result