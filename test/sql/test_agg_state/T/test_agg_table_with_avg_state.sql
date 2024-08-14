-- name: test_agg_table_with_avg_state
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
-- PARTITION BY date_trunc('day', %s) 
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) 
PROPERTIES (  "replication_num" = "1");
insert into t1  values('2020-01-01', '2020-01-01 10:10:10', 'aaaa', 'aaaa', 0, 1, 1, 1, 1, 1, 1.11, 11.111, 111.1111),('2020-02-01', '2020-02-01 10:10:10', 'bbbb', 'bbbb', 0, 2, 2, 2, 2, 2, 2.22, 22.222, 222.2222);

CREATE TABLE test_agg_tbl1(
  k1 VARCHAR(10),
  k6 avg(tinyint),
  k7 avg(smallint),
  k8 avg(int),
  k9 avg(bigint),
  k10 avg(largeint),
  k11 avg(float),
  k12 avg(double),
  k13 avg(decimal(28, 10))
)
AGGREGATE KEY(k1)
PARTITION BY (k1) 
DISTRIBUTED BY HASH(k1) BUCKETS 3;

-- first insert & test result
insert into test_agg_tbl1 select k1, avg_state(k6), avg_state(k7), avg_state(k8), avg_state(k9), avg_state(k10), avg_state(k11), avg_state(k12), avg_state(k13) from t1;

-- query    
select avg_merge(k6), avg_merge(k7), avg_merge(k8), avg_merge(k9), avg_merge(k10), avg_merge(k11), avg_merge(k12), avg_merge(k13) from test_agg_tbl1;
select k1,  avg_merge(k6), avg_merge(k7), avg_merge(k8), avg_merge(k9), avg_merge(k10), avg_merge(k11), avg_merge(k12), avg_merge(k13) from test_agg_tbl1 group by 1 order by 1 limit 3;

-- second insert & test result
insert into t1  values('2020-01-02', '2020-01-01 10:10:10', 'aaaa', 'aaaa', 0, 1, 1, 1, 1, 1, 1.11, 11.111, 111.1111),('2020-02-01', '2020-02-01 10:10:10', 'bbbb', 'bbbb', 0, 2, 2, 2, 2, 2, 2.22, 22.222, 222.2222);
insert into test_agg_tbl1 select k1, avg_state(k6), avg_state(k7), avg_state(k8), avg_state(k9), avg_state(k10), avg_state(k11), avg_state(k12), avg_state(k13) from t1;
insert into test_agg_tbl1 select k1, avg_state(k6), avg_state(k7), avg_state(k8), avg_state(k9), avg_state(k10), avg_state(k11), avg_state(k12), avg_state(k13) from t1;
ALTER TABLE test_agg_tbl1 COMPACT;
select avg_merge(k6), avg_merge(k7), avg_merge(k8), avg_merge(k9), avg_merge(k10), avg_merge(k11), avg_merge(k12), avg_merge(k13) from test_agg_tbl1;
select k1, avg_merge(k6), avg_merge(k7), avg_merge(k8), avg_merge(k9), avg_merge(k10), avg_merge(k11), avg_merge(k12), avg_merge(k13) from test_agg_tbl1 group by 1 order by 1 limit 3;