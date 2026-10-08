-- name: test_reuse_plan_group_key_filter
-- Test Point:
--   1. Common plan extraction fuses pieces that differ only by conjuncts shared by all of them or by
--      conjuncts on group-by keys, keeping plain aggregates and applying the key conjuncts on the consumer.
--   2. The fused result equals the unfused one, including NULL group keys, conjuncts evaluating to NULL,
--      groups without any qualifying row, and pieces that mix key-only and row conjuncts.
-- Method: run every query with cbo_extract_common_plan on and off; both runs must return the same rows.
--         EXPLAIN checks that the key-only shapes are fused without a row_hit gate.
-- Scope: ReuseFusionPlanRule
create database db_${uuid0};
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
create table t0 (k1 int, k2 int, v int) duplicate key(k1) distributed by hash(k1) buckets 3 properties ("replication_num" = "1");
-- result:
-- !result
create table t1 (k int, w int) duplicate key(k) distributed by hash(k) buckets 3 properties ("replication_num" = "1");
-- result:
-- !result
insert into t0 values (1, 1, 5), (1, 1, 20), (1, 2, null), (2, 1, 30), (2, 2, 1), (3, 1, 12), (3, null, 40), (4, 2, 2), (4, 2, 3), (null, 1, 7), (null, 1, 15), (null, null, null), (5, null, 11), (6, 1, null);
-- result:
-- !result
insert into t1 values (1, 100), (2, 200), (null, 300);
-- result:
-- !result
set cbo_extract_common_plan = true;
-- result:
-- !result
-- key-only conjunct on one piece
select * from (select 'a' p, k1, sum(v) s from t0 group by k1 union all select 'b', k1, sum(v) from t0 where k1 > 2 group by k1) t order by 1, 2, 3;
-- result:
a	None	22
a	1	25
a	2	31
a	3	52
a	4	5
a	5	11
a	6	None
b	3	52
b	4	5
b	5	11
b	6	None
-- !result
function: assert_explain_contains('select * from (select \'a\' p, k1, sum(v) s from t0 group by k1 union all select \'b\', k1, sum(v) from t0 where k1 > 2 group by k1) t order by 1, 2, 3', 'MultiCastDataSinks')
-- result:
None
-- !result
function: assert_explain_not_contains('select * from (select \'a\' p, k1, sum(v) s from t0 group by k1 union all select \'b\', k1, sum(v) from t0 where k1 > 2 group by k1) t order by 1, 2, 3', 'row_hit', '_if(')
-- result:
None
-- !result
-- IS NULL on a key keeps exactly the NULL group
select * from (select 'a' p, k1, count(*) c from t0 group by k1 union all select 'b', k1, count(*) from t0 where k1 is null group by k1) t order by 1, 2, 3;
-- result:
a	None	3
a	1	3
a	2	2
a	3	2
a	4	2
a	5	1
a	6	1
b	None	3
-- !result
function: assert_explain_contains('select * from (select \'a\' p, k1, count(*) c from t0 group by k1 union all select \'b\', k1, count(*) from t0 where k1 is null group by k1) t order by 1, 2, 3', 'MultiCastDataSinks')
-- result:
None
-- !result
function: assert_explain_not_contains('select * from (select \'a\' p, k1, count(*) c from t0 group by k1 union all select \'b\', k1, count(*) from t0 where k1 is null group by k1) t order by 1, 2, 3', 'row_hit', '_if(')
-- result:
None
-- !result
-- a key expression that can be UNKNOWN, ORed with IS NULL
select * from (select 'a' p, k1, sum(v) s from t0 group by k1 union all select 'b', k1, sum(v) from t0 where k1 + 1 > 3 or k1 is null group by k1) t order by 1, 2, 3;
-- result:
a	None	22
a	1	25
a	2	31
a	3	52
a	4	5
a	5	11
a	6	None
b	None	22
b	3	52
b	4	5
b	5	11
b	6	None
-- !result
function: assert_explain_contains('select * from (select \'a\' p, k1, sum(v) s from t0 group by k1 union all select \'b\', k1, sum(v) from t0 where k1 + 1 > 3 or k1 is null group by k1) t order by 1, 2, 3', 'MultiCastDataSinks')
-- result:
None
-- !result
function: assert_explain_not_contains('select * from (select \'a\' p, k1, sum(v) s from t0 group by k1 union all select \'b\', k1, sum(v) from t0 where k1 + 1 > 3 or k1 is null group by k1) t order by 1, 2, 3', 'row_hit', '_if(')
-- result:
None
-- !result
-- key-only and row conjuncts on one piece; some groups have no qualifying row
select * from (select 'a' p, k1, sum(v) s, count(*) c from t0 group by k1 union all select 'b', k1, sum(v), count(*) from t0 where k1 > 1 and v > 10 group by k1) t order by 1, 2, 3;
-- result:
a	None	22	3
a	1	25	3
a	2	31	2
a	3	52	2
a	4	5	2
a	5	11	1
a	6	None	1
b	2	30	1
b	3	52	2
b	5	11	1
-- !result
function: assert_explain_contains('select * from (select \'a\' p, k1, sum(v) s, count(*) c from t0 group by k1 union all select \'b\', k1, sum(v), count(*) from t0 where k1 > 1 and v > 10 group by k1) t order by 1, 2, 3', 'MultiCastDataSinks')
-- result:
None
-- !result
-- three pieces over a shared conjunct: none / key-only / row residual
select * from (select 'a' p, k1, sum(v) s from t0 where k2 = 1 group by k1 union all select 'b', k1, sum(v) from t0 where k2 = 1 and k1 >= 2 group by k1 union all select 'c', k1, sum(v) from t0 where k2 = 1 and v > 10 group by k1) t order by 1, 2, 3;
-- result:
a	None	22
a	1	25
a	2	30
a	3	12
a	6	None
b	2	30
b	3	12
b	6	None
c	None	15
c	1	20
c	2	30
c	3	12
-- !result
function: assert_explain_contains('select * from (select \'a\' p, k1, sum(v) s from t0 where k2 = 1 group by k1 union all select \'b\', k1, sum(v) from t0 where k2 = 1 and k1 >= 2 group by k1 union all select \'c\', k1, sum(v) from t0 where k2 = 1 and v > 10 group by k1) t order by 1, 2, 3', 'MultiCastDataSinks')
-- result:
None
-- !result
-- two group-by keys, the key-only conjunct is on the second key and the key order differs
select * from (select 'a' p, k1, k2, sum(v) s from t0 group by k1, k2 union all select 'b', k1, k2, sum(v) from t0 where k2 is not null group by k2, k1) t order by 1, 2, 3, 4;
-- result:
a	None	None	None
a	None	1	22
a	1	1	25
a	1	2	None
a	2	1	30
a	2	2	1
a	3	None	40
a	3	1	12
a	4	2	5
a	5	None	11
a	6	1	None
b	None	1	22
b	1	1	25
b	1	2	None
b	2	1	30
b	2	2	1
b	3	1	12
b	4	2	5
b	6	1	None
-- !result
function: assert_explain_contains('select * from (select \'a\' p, k1, k2, sum(v) s from t0 group by k1, k2 union all select \'b\', k1, k2, sum(v) from t0 where k2 is not null group by k2, k1) t order by 1, 2, 3, 4', 'MultiCastDataSinks')
-- result:
None
-- !result
function: assert_explain_not_contains('select * from (select \'a\' p, k1, k2, sum(v) s from t0 group by k1, k2 union all select \'b\', k1, k2, sum(v) from t0 where k2 is not null group by k2, k1) t order by 1, 2, 3, 4', 'row_hit', '_if(')
-- result:
None
-- !result
-- the consumer does not select the key its gate uses
select * from (select 'a' p, s from (select k1, sum(v) s from t0 group by k1) x union all select 'b', s from (select k1, sum(v) s from t0 where k1 > 2 group by k1) y) t order by 1, 2;
-- result:
a	None
a	5
a	11
a	22
a	25
a	31
a	52
b	None
b	5
b	11
b	52
-- !result
function: assert_explain_contains('select * from (select \'a\' p, s from (select k1, sum(v) s from t0 group by k1) x union all select \'b\', s from (select k1, sum(v) s from t0 where k1 > 2 group by k1) y) t order by 1, 2', 'MultiCastDataSinks')
-- result:
None
-- !result
function: assert_explain_not_contains('select * from (select \'a\' p, s from (select k1, sum(v) s from t0 group by k1) x union all select \'b\', s from (select k1, sum(v) s from t0 where k1 > 2 group by k1) y) t order by 1, 2', 'row_hit', '_if(')
-- result:
None
-- !result
-- TPC-DS Q65 shape: the same subquery twice, one side joined on a nullable key
select a.k1, a.k2, a.s from (select k1, k2, sum(v) s from t0 group by k1, k2) a, (select k1, max(s) mx from (select k1, k2, sum(v) s from t0 group by k1, k2) x group by k1) b, t1 where a.k1 = b.k1 and a.k2 = t1.k and a.s * 2 > b.mx order by 1, 2, 3;
-- result:
1	1	25
2	1	30
4	2	5
-- !result
function: assert_explain_contains('select a.k1, a.k2, a.s from (select k1, k2, sum(v) s from t0 group by k1, k2) a, (select k1, max(s) mx from (select k1, k2, sum(v) s from t0 group by k1, k2) x group by k1) b, t1 where a.k1 = b.k1 and a.k2 = t1.k and a.s * 2 > b.mx order by 1, 2, 3', 'MultiCastDataSinks')
-- result:
None
-- !result
set cbo_extract_common_plan = false;
-- result:
-- !result
-- key-only conjunct on one piece
select * from (select 'a' p, k1, sum(v) s from t0 group by k1 union all select 'b', k1, sum(v) from t0 where k1 > 2 group by k1) t order by 1, 2, 3;
-- result:
a	None	22
a	1	25
a	2	31
a	3	52
a	4	5
a	5	11
a	6	None
b	3	52
b	4	5
b	5	11
b	6	None
-- !result
-- IS NULL on a key keeps exactly the NULL group
select * from (select 'a' p, k1, count(*) c from t0 group by k1 union all select 'b', k1, count(*) from t0 where k1 is null group by k1) t order by 1, 2, 3;
-- result:
a	None	3
a	1	3
a	2	2
a	3	2
a	4	2
a	5	1
a	6	1
b	None	3
-- !result
-- a key expression that can be UNKNOWN, ORed with IS NULL
select * from (select 'a' p, k1, sum(v) s from t0 group by k1 union all select 'b', k1, sum(v) from t0 where k1 + 1 > 3 or k1 is null group by k1) t order by 1, 2, 3;
-- result:
a	None	22
a	1	25
a	2	31
a	3	52
a	4	5
a	5	11
a	6	None
b	None	22
b	3	52
b	4	5
b	5	11
b	6	None
-- !result
-- key-only and row conjuncts on one piece; some groups have no qualifying row
select * from (select 'a' p, k1, sum(v) s, count(*) c from t0 group by k1 union all select 'b', k1, sum(v), count(*) from t0 where k1 > 1 and v > 10 group by k1) t order by 1, 2, 3;
-- result:
a	None	22	3
a	1	25	3
a	2	31	2
a	3	52	2
a	4	5	2
a	5	11	1
a	6	None	1
b	2	30	1
b	3	52	2
b	5	11	1
-- !result
-- three pieces over a shared conjunct: none / key-only / row residual
select * from (select 'a' p, k1, sum(v) s from t0 where k2 = 1 group by k1 union all select 'b', k1, sum(v) from t0 where k2 = 1 and k1 >= 2 group by k1 union all select 'c', k1, sum(v) from t0 where k2 = 1 and v > 10 group by k1) t order by 1, 2, 3;
-- result:
a	None	22
a	1	25
a	2	30
a	3	12
a	6	None
b	2	30
b	3	12
b	6	None
c	None	15
c	1	20
c	2	30
c	3	12
-- !result
-- two group-by keys, the key-only conjunct is on the second key and the key order differs
select * from (select 'a' p, k1, k2, sum(v) s from t0 group by k1, k2 union all select 'b', k1, k2, sum(v) from t0 where k2 is not null group by k2, k1) t order by 1, 2, 3, 4;
-- result:
a	None	None	None
a	None	1	22
a	1	1	25
a	1	2	None
a	2	1	30
a	2	2	1
a	3	None	40
a	3	1	12
a	4	2	5
a	5	None	11
a	6	1	None
b	None	1	22
b	1	1	25
b	1	2	None
b	2	1	30
b	2	2	1
b	3	1	12
b	4	2	5
b	6	1	None
-- !result
-- the consumer does not select the key its gate uses
select * from (select 'a' p, s from (select k1, sum(v) s from t0 group by k1) x union all select 'b', s from (select k1, sum(v) s from t0 where k1 > 2 group by k1) y) t order by 1, 2;
-- result:
a	None
a	5
a	11
a	22
a	25
a	31
a	52
b	None
b	5
b	11
b	52
-- !result
-- TPC-DS Q65 shape: the same subquery twice, one side joined on a nullable key
select a.k1, a.k2, a.s from (select k1, k2, sum(v) s from t0 group by k1, k2) a, (select k1, max(s) mx from (select k1, k2, sum(v) s from t0 group by k1, k2) x group by k1) b, t1 where a.k1 = b.k1 and a.k2 = t1.k and a.s * 2 > b.mx order by 1, 2, 3;
-- result:
1	1	25
2	1	30
4	2	5
-- !result
drop database db_${uuid0} force;
-- result:
-- !result
