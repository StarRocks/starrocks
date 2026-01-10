-- name: test_join_reorder_times
drop database if exists test_join_reorder_times;
-- result:
-- !result
create database test_join_reorder_times;
-- result:
-- !result
use test_join_reorder_times;
-- result:
-- !result
create table if not exists jrdp_t0 (k int, v int)
engine=olap
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");
-- result:
-- !result
create table if not exists jrdp_t1 (k int, v int)
engine=olap
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");
-- result:
-- !result
create table if not exists jrdp_t2 (k int, v int)
engine=olap
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");
-- result:
-- !result
create table if not exists jrdp_t3 (k int, v int)
engine=olap
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");
-- result:
-- !result
create table if not exists jrdp_t4 (k int, v int)
engine=olap
duplicate key(k)
distributed by hash(k) buckets 1
properties("replication_num"="1");
-- result:
-- !result
insert into jrdp_t0 values (1,10),(2,20),(3,30);
-- result:
-- !result
insert into jrdp_t1 values (1,10),(2,20),(3,30);
-- result:
-- !result
insert into jrdp_t2 values (1,10),(2,20),(3,30);
-- result:
-- !result
insert into jrdp_t3 values (1,10),(2,20),(3,30);
-- result:
-- !result
insert into jrdp_t4 values (1,10),(2,20),(3,30);
-- result:
-- !result
analyze table jrdp_t0;
-- result:
test_join_reorder_times.jrdp_t0	analyze	status	OK
-- !result
[UC] analyze table jrdp_t1;
-- result:
test_join_reorder_times.jrdp_t1	analyze	status	OK
-- !result
[UC] analyze table jrdp_t2;
-- result:
test_join_reorder_times.jrdp_t2	analyze	status	OK
-- !result
[UC] analyze table jrdp_t3;
-- result:
test_join_reorder_times.jrdp_t3	analyze	status	OK
-- !result
[UC] analyze table jrdp_t4;
-- result:
test_join_reorder_times.jrdp_t4	analyze	status	OK
-- !result
set cbo_max_reorder_node_use_exhaustive=3;
-- result:
-- !result
function: assert_query_contains_times("trace times optimizer select * from jrdp_t0 join jrdp_t1 on jrdp_t0.k=jrdp_t1.k join jrdp_t2 on jrdp_t1.k=jrdp_t2.k join jrdp_t3 on jrdp_t2.k=jrdp_t3.k join jrdp_t4 on jrdp_t3.k=jrdp_t4.k","JoinReorderDP", 1)
-- result:
None
-- !result
function: assert_query_contains_times("trace times optimizer select jrdp_t0.k, sum(jrdp_t4.v) from jrdp_t0 join jrdp_t1 on jrdp_t0.k=jrdp_t1.k join jrdp_t2 on jrdp_t1.k=jrdp_t2.k join jrdp_t3 on jrdp_t2.k=jrdp_t3.k join jrdp_t4 on jrdp_t3.k=jrdp_t4.k group by jrdp_t0.k","JoinReorderDP", 2)
-- result:
None
-- !result
set cbo_max_reorder_node_use_exhaustive=4;
-- result:
-- !result