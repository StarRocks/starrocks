-- name: test_agg_state_combine_basic
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
CREATE TABLE test_agg_combine_avg(
  k1 VARCHAR(10),
  k6 avg(tinyint),
  k7 avg(smallint),
  k8 avg(int),
  k9 avg(bigint),
  k10 avg(largeint),
  k11 avg(float),
  k12 avg(double),
  k13 avg(decimal(27, 9))
)
AGGREGATE KEY(k1)
PARTITION BY (k1) 
DISTRIBUTED BY HASH(k1) BUCKETS 3;
-- result:
-- !result
CREATE TABLE test_agg_combine_sum(
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
CREATE TABLE test_agg_combine_min(
  k1 VARCHAR(10),
  k6 min(tinyint),
  k7 min(smallint),
  k8 min(int),
  k9 min(bigint),
  k10 min(largeint),
  k11 min(float),
  k12 min(double),
  k13 min(decimal(27, 9))
)
AGGREGATE KEY(k1)
PARTITION BY (k1) 
DISTRIBUTED BY HASH(k1) BUCKETS 3;
-- result:
-- !result
CREATE TABLE test_agg_combine_min_by(
  k1 VARCHAR(10),
  k6 min_by(tinyint, datetime),
  k7 min_by(smallint, datetime),
  k8 min_by(int, datetime),
  k9 min_by(bigint, datetime),
  k10 min_by(largeint, datetime),
  k11 min_by(float, datetime),
  k12 min_by(double, datetime),
  k13 min_by(decimal(27, 9), datetime)
)
AGGREGATE KEY(k1)
PARTITION BY (k1) 
DISTRIBUTED BY HASH(k1) BUCKETS 3;
-- result:
-- !result
insert into test_agg_combine_avg select k1, avg_combine(k6), avg_combine(k7), avg_combine(k8), avg_combine(k9), avg_combine(k10), avg_combine(k11), avg_combine(k12), avg_combine(k13) from t1 group by k1;
-- result:
-- !result
insert into test_agg_combine_sum select k1, k2, sum_combine(k6), sum_combine(k7), sum_combine(k8), sum_combine(k9), sum_combine(k10), sum_combine(k11), sum_combine(k12), sum_combine(k13) from t1 group by k1, k2;
-- result:
-- !result
insert into test_agg_combine_min select k1, min_combine(k6), min_combine(k7), min_combine(k8), min_combine(k9), min_combine(k10), min_combine(k11), min_combine(k12), min_combine(k13) from t1 group by k1;
-- result:
-- !result
insert into test_agg_combine_min_by select k1, min_by_combine(k6, k2), min_by_combine(k7, k2), min_by_combine(k8, k2), min_by_combine(k9, k2), min_by_combine(k10, k2), min_by_combine(k11, k2), min_by_combine(k12, k2), min_by_combine(k13, k2) from t1 group by k1;
-- result:
-- !result
select k1, avg_state_merge(k6), avg_state_merge(k7), avg_state_merge(k8), avg_state_merge(k9), avg_state_merge(k10), avg_state_merge(k11), avg_state_merge(k12), avg_state_merge(k13) from test_agg_combine_avg order by 1 limit 3;
-- result:
2020-01-01	1.0	1.0	1.0	1.0	1.0	1.1100000143051147	11.111	111.111100000000
2020-02-01	2.0	2.0	2.0	2.0	2.0	2.2200000286102295	22.222	222.222200000000
-- !result
select sum_state_merge(k6), sum_state_merge(k7), sum_state_merge(k8), sum_state_merge(k9), sum_state_merge(k10), sum_state_merge(k11), sum_state_merge(k12), sum_state_merge(k13) from test_agg_combine_sum;
-- result:
1	1	1	1	1	1.1100000143051147	11.111	111.111100000
2	2	2	2	2	2.2200000286102295	22.222	222.222200000
-- !result
select k1, sum_state_merge(k6), sum_state_merge(k7), sum_state_merge(k8), sum_state_merge(k9), sum_state_merge(k10), sum_state_merge(k11), sum_state_merge(k12), sum_state_merge(k13) from test_agg_combine_sum order by 1 limit 3;
-- result:
2020-01-01	1	1	1	1	1	1.1100000143051147	11.111	111.111100000
2020-02-01	2	2	2	2	2	2.2200000286102295	22.222	222.222200000
-- !result
select min_state_merge(k6), min_state_merge(k7), min_state_merge(k8), min_state_merge(k9), min_state_merge(k10), min_state_merge(k11), min_state_merge(k12), min_state_merge(k13) from test_agg_combine_min;
-- result:
1	1	1	1	1	1.11	11.111	111.111100000
2	2	2	2	2	2.22	22.222	222.222200000
-- !result
select k1, min_state_merge(k6), min_state_merge(k7), min_state_merge(k8), min_state_merge(k9), min_state_merge(k10), min_state_merge(k11), min_state_merge(k12), min_state_merge(k13) from test_agg_combine_min order by 1 limit 3;
-- result:
2020-01-01	1	1	1	1	1	1.11	11.111	111.111100000
2020-02-01	2	2	2	2	2	2.22	22.222	222.222200000
-- !result
select min_by_state_merge(k6), min_by_state_merge(k7), min_by_state_merge(k8), min_by_state_merge(k9), min_by_state_merge(k10), min_by_state_merge(k11), min_by_state_merge(k12), min_by_state_merge(k13) from test_agg_combine_min_by order by 1 limit 3;
-- result:
1	1	1	1	1	1.11	11.111	111.111100000
2	2	2	2	2	2.22	22.222	222.222200000
-- !result
select k1, min_by_state_merge(k6), min_by_state_merge(k7), min_by_state_merge(k8), min_by_state_merge(k9), min_by_state_merge(k10), min_by_state_merge(k11), min_by_state_merge(k12), min_by_state_merge(k13) from test_agg_combine_min_by order by 1 limit 3;
-- result:
2020-01-01	1	1	1	1	1	1.11	11.111	111.111100000
2020-02-01	2	2	2	2	2	2.22	22.222	222.222200000
-- !result
insert into t1  values('2020-01-02', '2020-01-01 10:10:10', 'aaaa', 'aaaa', 0, 1, 1, 1, 1, 1, 1.11, 11.111, 111.1111),('2020-02-01', '2020-02-01 10:10:10', 'bbbb', 'bbbb', 0, 2, 2, 2, 2, 2, 2.22, 22.222, 222.2222);
-- result:
-- !result
insert into test_agg_combine_avg select k1, avg_state_union(k6, k6), avg_state_union(k7, k7), avg_state_union(k8, k8), avg_state_union(k9, k9), avg_state_union(k10, k10), avg_state_union(k11, k11), avg_state_union(k12, k12), avg_state_union(k13, k13) from test_agg_combine_avg;
-- result:
-- !result
insert into test_agg_combine_sum select k1, k2, sum_state_union(k6, k6), sum_state_union(k7, k7), sum_state_union(k8, k8), sum_state_union(k9, k9), sum_state_union(k10, k10), sum_state_union(k11, k11), sum_state_union(k12, k12), sum_state_union(k13, k13) from test_agg_combine_sum;
-- result:
-- !result
insert into test_agg_combine_min select k1, min_state_union(k6, k6), min_state_union(k7, k7), min_state_union(k8, k8), min_state_union(k9, k9), min_state_union(k10, k10), min_state_union(k11, k11), min_state_union(k12, k12), min_state_union(k13, k13) from test_agg_combine_min;
-- result:
-- !result
insert into test_agg_combine_min_by select k1, min_by_state_union(k6, k6), min_by_state_union(k7, k7), min_by_state_union(k8, k8), min_by_state_union(k9, k9), min_by_state_union(k10, k10), min_by_state_union(k11, k11), min_by_state_union(k12, k12), min_by_state_union(k13, k13) from test_agg_combine_min_by;
-- result:
-- !result
insert into test_agg_combine_avg select k1, avg_combine(k6), avg_combine(k7), avg_combine(k8), avg_combine(k9), avg_combine(k10), avg_combine(k11), avg_combine(k12), avg_combine(k13) from t1 group by k1;
-- result:
-- !result
insert into test_agg_combine_sum select k1, k2, sum_combine(k6), sum_combine(k7), sum_combine(k8), sum_combine(k9), sum_combine(k10), sum_combine(k11), sum_combine(k12), sum_combine(k13) from t1 group by k1, k2;
-- result:
-- !result
insert into test_agg_combine_min select k1, min_combine(k6), min_combine(k7), min_combine(k8), min_combine(k9), min_combine(k10), min_combine(k11), min_combine(k12), min_combine(k13) from t1 group by k1;
-- result:
-- !result
insert into test_agg_combine_min_by select k1, min_by_combine(k6, k2), min_by_combine(k7, k2), min_by_combine(k8, k2), min_by_combine(k9, k2), min_by_combine(k10, k2), min_by_combine(k11, k2), min_by_combine(k12, k2), min_by_combine(k13, k2) from t1 group by k1;
-- result:
-- !result
ALTER TABLE test_agg_combine_sum COMPACT;
-- result:
-- !result
ALTER TABLE test_agg_combine_avg COMPACT;
-- result:
-- !result
ALTER TABLE test_agg_combine_min COMPACT;
-- result:
-- !result
ALTER TABLE test_agg_combine_min_by COMPACT;
-- result:
-- !result
select avg_state_merge(k6), avg_state_merge(k7), avg_state_merge(k8), avg_state_merge(k9), avg_state_merge(k10), avg_state_merge(k11), avg_state_merge(k12), avg_state_merge(k13) from test_agg_combine_avg order by 1 limit 3;
-- result:
1.0	1.0	1.0	1.0	1.0	1.1100000143051147	11.111	111.111100000000
1.0	1.0	1.0	1.0	1.0	1.1100000143051147	11.111	111.111100000000
2.0	2.0	2.0	2.0	2.0	2.2200000286102295	22.222	222.222200000000
-- !result
select sum_state_merge(k6), sum_state_merge(k7), sum_state_merge(k8), sum_state_merge(k9), sum_state_merge(k10), sum_state_merge(k11), sum_state_merge(k12), sum_state_merge(k13) from test_agg_combine_sum order by 1 limit 3;
-- result:
1	1	1	1	1	1.1100000143051147	11.111	111.111100000
4	4	4	4	4	4.440000057220459	44.444	444.444400000
10	10	10	10	10	11.100000143051147	111.11	1111.111000000
-- !result
select k1, sum_state_merge(k6), sum_state_merge(k7), sum_state_merge(k8), sum_state_merge(k9), sum_state_merge(k10), sum_state_merge(k11), sum_state_merge(k12), sum_state_merge(k13) from test_agg_combine_sum order by 1 limit 3;
-- result:
2020-01-01	4	4	4	4	4	4.440000057220459	44.444	444.444400000
2020-01-02	1	1	1	1	1	1.1100000143051147	11.111	111.111100000
2020-02-01	10	10	10	10	10	11.100000143051147	111.11	1111.111000000
-- !result
select min_state_merge(k6), min_state_merge(k7), min_state_merge(k8), min_state_merge(k9), min_state_merge(k10), min_state_merge(k11), min_state_merge(k12), min_state_merge(k13) from test_agg_combine_min order by 1 limit 3;
-- result:
1	1	1	1	1	1.11	11.111	111.111100000
1	1	1	1	1	1.11	11.111	111.111100000
2	2	2	2	2	2.22	22.222	222.222200000
-- !result
select min_by_state_merge(k6), min_by_state_merge(k7), min_by_state_merge(k8), min_by_state_merge(k9), min_by_state_merge(k10), min_by_state_merge(k11), min_by_state_merge(k12), min_by_state_merge(k13) from test_agg_combine_min_by order by 1 limit 3;
-- result:
1	1	1	1	1	1.11	11.111	111.111100000
1	1	1	1	1	1.11	11.111	111.111100000
2	2	2	2	2	2.22	22.222	222.222200000
-- !result
select k1, min_by_state_merge(k6), min_by_state_merge(k7), min_by_state_merge(k8), min_by_state_merge(k9), min_by_state_merge(k10), min_by_state_merge(k11), min_by_state_merge(k12), min_by_state_merge(k13) from test_agg_combine_min_by order by 1 limit 3;
-- result:
2020-01-01	1	1	1	1	1	1.11	11.111	111.111100000
2020-01-02	1	1	1	1	1	1.11	11.111	111.111100000
2020-02-01	2	2	2	2	2	2.22	22.222	222.222200000
-- !result