-- name: test_join_reorder_times

drop database if exists test_join_reorder_times;
create database test_join_reorder_times;
use test_join_reorder_times;
create table if not exists jrdp_t0 (k int, v int)
engine=olap
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");

create table if not exists jrdp_t1 (k int, v int)
engine=olap
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");

create table if not exists jrdp_t2 (k int, v int)
engine=olap
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");

create table if not exists jrdp_t3 (k int, v int)
engine=olap
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");

create table if not exists jrdp_t4 (k int, v int)
engine=olap
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");

insert into jrdp_t0 values (1,10),(2,20),(3,30);
insert into jrdp_t1 values (1,10),(2,20),(3,30);
insert into jrdp_t2 values (1,10),(2,20),(3,30);
insert into jrdp_t3 values (1,10),(2,20),(3,30);
insert into jrdp_t4 values (1,10),(2,20),(3,30);
analyze table jrdp_t0;
analyze table jrdp_t1;
analyze table jrdp_t2;
analyze table jrdp_t3;
analyze table jrdp_t4;

set cbo_max_reorder_node_use_exhaustive=3;
-- No aggregation: JoinReorderDP should be executed only once.
function: assert_query_contains_times("trace times optimizer select * from jrdp_t0 join jrdp_t1 on jrdp_t0.k=jrdp_t1.k join jrdp_t2 on jrdp_t1.k=jrdp_t2.k join jrdp_t3 on jrdp_t2.k=jrdp_t3.k join jrdp_t4 on jrdp_t3.k=jrdp_t4.k","JoinReorderDP", 1)

-- With aggregation: pushDownAggregation may reorder once, and memo phase may reorder again -> expect 2.
function: assert_query_contains_times("trace times optimizer select jrdp_t0.k, sum(jrdp_t4.v) from jrdp_t0 join jrdp_t1 on jrdp_t0.k=jrdp_t1.k join jrdp_t2 on jrdp_t1.k=jrdp_t2.k join jrdp_t3 on jrdp_t2.k=jrdp_t3.k join jrdp_t4 on jrdp_t3.k=jrdp_t4.k group by jrdp_t0.k","JoinReorderDP", 2)
set cbo_max_reorder_node_use_exhaustive=4;

